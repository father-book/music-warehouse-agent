"""vault（AC-21 密钥集中管理）自测。

这里绝大多数用例是**反向的**：证明"密钥不会出现在某处"，而不是"能取到密钥"。
正向功能只有一行 reveal()，反向防线才是一堆容易退化的地方——
半年后有人重构了异常消息、加了个 print(cfg)，正向用例照样全绿。

所以每一条断言都盯着同一个问题：**真值有没有在某个不该出现的地方露头**。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json
import os
import subprocess
import sys
from pathlib import Path

import vault

PASS = FAIL = 0
FAILS: list[str] = []


def ck(label: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓ %s" % label)
    else:
        FAIL += 1
        FAILS.append(label)
        print("  ✗ %s%s" % (label, ("  → " + str(detail)) if detail else ""))


def ck_no_leak(label: str, text: str, secrets_: list[str]) -> None:
    """一条断言查所有真值：任何一个出现都算泄露。"""
    hit = [s for s in secrets_ if s and s in text]
    ck(label, not hit, "泄露了 %d 个真值；文本=%r" % (len(hit), text[:120]))


from _bootstrap import HERE  # noqa: E402

# 假密钥。绝不碰真实 .env —— 自测的输出会被贴进文档，真值不能出现在里面。
FAKE_KEY = "sk-fake-0123456789abcdefghijklmnABCD"
FAKE_PWD = "doris-fake-password-9999"
FAKE_REDIS = "redis-fake-7777"
SHORT = "ab12"                                # 4 位：掩码与 scrub 的短值分支
ALL_FAKE = [FAKE_KEY, FAKE_PWD, FAKE_REDIS, SHORT]


def _cfg() -> dict:
    return {
        "generator": {"api_key_env": "LLM_API_KEY"},
        "execution": {"password_env": "DORIS_RO_PASSWORD"},
        "cache": {"redis": {"password_env": "REDIS_PASSWORD"}},
    }


def _set_env() -> dict:
    saved = {k: os.environ.get(k) for k in
             ("LLM_API_KEY", "DORIS_RO_PASSWORD", "REDIS_PASSWORD")}
    os.environ["LLM_API_KEY"] = FAKE_KEY
    os.environ["DORIS_RO_PASSWORD"] = FAKE_PWD
    os.environ["REDIS_PASSWORD"] = FAKE_REDIS
    vault.reset_cache()
    # 必须顺便登记：scrub 只能擦"registry 里已有的值"。只设环境不 get，
    # 测试会以为 scrub 坏了，其实是压根没有值可擦。
    vault.warmup(_cfg())
    return saved


def _restore(saved: dict) -> None:
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    vault.reset_cache()


def main() -> int:
    saved = _set_env()
    try:
        _t_repr()
        _t_mask()
        _t_scrub()
        _t_missing()
        _t_validate()
        _t_audit()
        _t_static()
        _t_subprocess()
    finally:
        _restore(saved)

    print("\n%d/%d 通过" % (PASS, PASS + FAIL))
    if FAILS:
        print("失败：")
        for f in FAILS:
            print("  -", f)
    # FAIL 是失败计数：0 个失败 = 全过 = 退出 0。
    # 之前写反了（全过反而退出 1），靠人眼看输出才没炸，现在纠正。
    return 1 if FAIL else 0


# ---------------------------------------------------------------- [1] 打印即掩码
def _t_repr() -> None:
    print("\n[1] 任何'顺手打印'的路径都必须是掩码")
    s = vault.get("LLM_API_KEY")
    ck("能取到值", bool(s) and s.reveal() == FAKE_KEY)

    ck_no_leak("repr() 不含真值", repr(s), ALL_FAKE)
    ck_no_leak("str() 不含真值", str(s), ALL_FAKE)
    ck_no_leak("f-string 不含真值", f"{s}", ALL_FAKE)
    ck_no_leak("'{}'.format 不含真值", "{}".format(s), ALL_FAKE)

    # 这一条最容易退化：有人给 Secret 加个 to_dict 或者 __json__ 就漏了。
    # 现在 json.dumps 应该直接 TypeError —— 强制调用方显式 reveal()。
    try:
        json.dumps({"key": s})
        ck("json.dumps(Secret) 应报错而不是序列化真值", False,
           "居然序列化成功了，说明有人给它加了序列化钩子")
    except TypeError:
        ck("json.dumps(Secret) 应报错而不是序列化真值", True)
    except Exception as e:
        ck("json.dumps(Secret) 应报错而不是序列化真值", False, type(e).__name__)

    # print 整个配置字典——真实世界里最常见的泄露方式
    cfg = _cfg()
    cfg["generator"]["key"] = s
    ck_no_leak("print 整个 config（含 Secret 对象）不含真值",
               repr(cfg) + str(cfg), ALL_FAKE)

    ck("bool(Secret) 表示是否已配置", bool(s) is True and bool(vault.get("NOPE_X")) is False)


# ---------------------------------------------------------------- [2] 掩码分级
def _t_mask() -> None:
    print("\n[2] 掩码分级：短密钥连一个字符都不该露")
    ck("4 位 → 全星", "ab12" not in vault._mask(SHORT) and "*" in vault._mask(SHORT),
       vault._mask(SHORT))
    m15 = vault._mask("abcdefghij12345")
    ck("15 位 → 不露任何原文", "abc" not in m15 and "345" not in m15, m15)
    m35 = vault._mask(FAKE_KEY)
    ck("30+ 位 → 前 3 后 4（可辨识不可用）",
       m35.startswith("sk-") and "ABCD" in m35 and FAKE_KEY not in m35, m35)
    ck("空值 → (空)", vault._mask("") == "(空)", vault._mask(""))


# ---------------------------------------------------------------- [3] scrub
def _t_scrub() -> None:
    print("\n[3] scrub：已登记真值出现在任意文本里都要被擦掉")
    vault.get("LLM_API_KEY"); vault.get("DORIS_RO_PASSWORD"); vault.get("REDIS_PASSWORD")

    leak = "Authorization: Bearer %s" % FAKE_KEY
    ck_no_leak("请求头泄露 → 擦掉", vault.scrub(leak), ALL_FAKE)

    leak2 = "connect failed: password=%s host=192.168.128.163" % FAKE_PWD
    ck_no_leak("连接串泄露 → 擦掉", vault.scrub(leak2), ALL_FAKE)
    ck("擦除后仍保留可诊断信息", "192.168.128.163" in vault.scrub(leak2),
       vault.scrub(leak2))

    # 长值优先：A 是 B 的前缀时，先把长的换掉，短的才不会把长的切坏
    os.environ["PREFIX_LONG"] = "sk-abcdefghijklmnop"
    vault.reset_cache()
    vault.get("PREFIX_LONG")
    out = vault.scrub("x sk-abcdefghijklmnop y")
    ck_no_leak("长值被完整擦除（不被短前缀切坏）", out, ["sk-abcdefghijklmnop"])
    os.environ.pop("PREFIX_LONG", None)
    vault.reset_cache()
    _set_env()

    # 短值不参与 scrub：`"1"` 这种会把正常文本打成马赛克
    os.environ["TINY"] = "8000"
    vault.reset_cache()
    vault.get("TINY")
    ck("短值(4 位)不参与全局替换，避免误伤正常文本",
       vault.scrub("端口 8000 正常") == "端口 8000 正常", vault.scrub("端口 8000 正常"))
    os.environ.pop("TINY", None)
    vault.reset_cache()
    _set_env()

    # 递归对象
    obj = {"a": [{"b": leak}], "n": 1}
    flat = json.dumps(vault.scrub_obj(obj), ensure_ascii=False)
    ck_no_leak("scrub_obj 递归 dict/list 也不漏", flat, ALL_FAKE)


# ---------------------------------------------------------------- [4] 缺失
def _t_missing() -> None:
    print("\n[4] 缺失密钥：异常消息本身也不能泄露别的密钥")
    del os.environ["LLM_API_KEY"]
    vault.reset_cache()
    vault.get("DORIS_RO_PASSWORD")           # 已登记的别的键仍是真值

    try:
        vault.require("LLM_API_KEY")
        ck("require 缺失应抛 SecretMissing", False, "没抛")
    except vault.SecretMissing as e:
        ck("require 缺失应抛 SecretMissing", True)
        ck("异常消息点名缺哪个键", "LLM_API_KEY" in str(e), str(e)[:80])
        ck_no_leak("异常消息不含其他已配置密钥", str(e), [FAKE_PWD])
        ck("异常消息给了补法", ".env" in str(e) or "环境变量" in str(e), str(e)[:80])

    ck("get 缺失返回空值而不是抛错", not vault.get("LLM_API_KEY"))
    ck("require 缺失后 reveal 不会被误调用", vault.get("LLM_API_KEY").reveal() == "")

    _set_env()                                # 恢复


# ---------------------------------------------------------------- [5] 启动校验
def _t_validate() -> None:
    print("\n[5] 启动校验：一次报全，不逐个炸")
    ok, missing = vault.validate(_cfg())
    ck("三键齐全时 ok", ok and not missing, (ok, missing))

    os.environ.pop("LLM_API_KEY", None)
    os.environ.pop("REDIS_PASSWORD", None)
    vault.reset_cache()
    ok, missing = vault.validate(_cfg())
    ck("缺两个键时 ok=False", not ok)
    ck("缺失清单一次报全（2 个）", len(missing) == 2, missing)
    ck("清单含两个键名",
       set(missing) == {"LLM_API_KEY", "REDIS_PASSWORD"}, missing)

    try:
        vault.assert_ready(_cfg())
        ck("assert_ready 缺键应拒绝启动", False, "没抛")
    except vault.SecretMissing as e:
        ck("assert_ready 缺键应拒绝启动", True)
        ck("拒绝启动时消息里有'启动中止'", "启动中止" in str(e), str(e)[:60])
        ck_no_leak("拒绝启动消息不含已配置的 DORIS 密码", str(e), [FAKE_PWD])

    _set_env()
    vault.assert_ready(_cfg())
    ck("补齐后 assert_ready 静默通过", True)

    # 配置里没写的键不该被当成必需项
    ck("配置未引用 GOV_PG_DSN → 不算必需",
       "GOV_PG_DSN" not in vault.required_names(_cfg()))

    # 多执行器写法也要能推导出来
    cfg2 = {"execution": {"executors": [{"password_env": "EX_A"}, {"password_env": "EX_B"}]}}
    ck("executors 列表写法也能推导",
       set(vault.required_names(cfg2)) == {"EX_A", "EX_B"}, vault.required_names(cfg2))


# ---------------------------------------------------------------- [6] 审计视图
def _t_audit() -> None:
    print("\n[6] 审计视图：不能因为'没被问过'而显示成已配置")
    vault.reset_cache()
    os.environ.pop("REDIS_PASSWORD", None)
    # 关键：不 warmup 时，缺失的键压根不在登记表里 → audit 看起来全绿（假绿）
    vault.get("LLM_API_KEY")
    names_before = {it["name"] for it in vault.audit()}
    ck("未 warmup 时缺失键不会出现在审计表（这就是要防的假绿）",
       "REDIS_PASSWORD" not in names_before, names_before)

    vault.reset_cache()
    vault.warmup(_cfg())
    items = {it["name"]: it for it in vault.audit()}
    ck("warmup 后缺失键出现在审计表", "REDIS_PASSWORD" in items, list(items))
    ck("缺失项 configured=False", items["REDIS_PASSWORD"]["configured"] is False)
    ck("缺失项 source=missing", items["REDIS_PASSWORD"]["source"] == "missing")

    vault.get("LLM_API_KEY").reveal()     # reset_cache 会清 reveal 记录，先真取一次
    flat = json.dumps(vault.audit(), ensure_ascii=False)
    ck_no_leak("审计视图 JSON 里没有真值", flat, [FAKE_KEY, FAKE_PWD])
    ck("审计视图有 revealed_by 字段", "revealed_by" in flat)
    ck("取过真值的键会记下调用模块",
       any(it["revealed_by"] for it in vault.audit() if it["name"] == "LLM_API_KEY"),
       [it for it in vault.audit() if it["name"] == "LLM_API_KEY"])

    _set_env()
    vault.warmup(_cfg())
    for w in vault.weak():
        ck("弱密钥清单里不该有 30+ 位的 key", w["name"] != "LLM_API_KEY", w)

    # summary_line 是启动横幅，同样不许漏
    ck_no_leak("summary_line 不含真值", vault.summary_line(), ALL_FAKE)


# ---------------------------------------------------------------- [7] 静态检查
def _t_static() -> None:
    print("\n[7] 静态检查：不许再有绕过 vault 的直读")
    import re
    targets = ["LLM_API_KEY", "DORIS_RO_PASSWORD", "REDIS_PASSWORD"]
    hits: list[str] = []
    for f in HERE.glob("*.py"):
        if f.name.startswith("selftest_") or f.name == "vault.py":
            continue
        txt = f.read_text(encoding="utf-8", errors="ignore")
        for name in targets:
            # os.getenv("KEY") / os.environ["KEY"] / os.environ.get("KEY")
            for m in re.finditer(
                    r'os\.(?:getenv|environ(?:\.get)?)\s*[\(\[]\s*[\'"]%s[\'"]' % name, txt):
                line = txt[:m.start()].count("\n") + 1
                hits.append("%s:%d" % (f.name, line))
    ck("代码里没有 os.getenv/os.environ 直读密钥", not hits, hits)

    # require_env 只能作为兼容层存在，不该再被业务代码调用
    callers = []
    for f in HERE.glob("*.py"):
        if f.name.startswith("selftest_") or f.name in ("env.py", "vault.py"):
            continue
        txt = f.read_text(encoding="utf-8", errors="ignore")
        if "require_env(" in txt:
            callers.append(f.name)
    ck("业务代码不再调 env.require_env（已改走 vault）", not callers, callers)

    ck("vault 文件名没撞标准库",
       vault.__file__.endswith("vault.py"), vault.__file__)


# ---------------------------------------------------------------- [8] 子进程
def _t_subprocess() -> None:
    print("\n[8] 独立进程验证：真实 import 链里标准库没被劫持")
    code = (
        "import _bootstrap  # 项目依赖在 _vendor 下，不先挂路径会 import 到残缺的 site-packages 版\n"
        "import secrets as std, json\n"
        "import httpx, uvicorn\n"          # 这两个内部都会用到随机数/密钥
        "import vault\n"
        "print('STD=' + std.__file__)\n"
        "print('VAULT=' + vault.__file__)\n"
        "s = vault.get('LLM_API_KEY')\n"
        "print('REPR=' + repr(s))\n"
        "print('LEAK=' + ('YES' if " + repr(FAKE_KEY) + " in repr(s) else 'NO'))\n"
    )
    env = dict(os.environ)
    env.update({"LLM_API_KEY": FAKE_KEY, "PYTHONIOENCODING": "utf-8"})
    try:
        r = subprocess.run([sys.executable, "-c", code], cwd=str(HERE),
                           capture_output=True, text=True, timeout=90, env=env)
        out = (r.stdout or "") + (r.stderr or "")
        ck("子进程正常退出", r.returncode == 0, out[-300:])

        std_line = next((ln for ln in out.splitlines() if ln.startswith("STD=")), "")
        ck("标准库 secrets 未被本目录顶替",
           std_line.endswith("secrets.py") and "agent" not in std_line,
           std_line)
        ck("httpx / uvicorn 能正常 import（说明没被劫持）",
           "Traceback" not in out, out[-300:])
        ck("REPR 行不含真值", "LEAK=NO" in out, out[-200:])
        ck_no_leak("子进程全部输出不含真值", out, [FAKE_KEY])
    except subprocess.TimeoutExpired:
        ck("子进程正常退出", False, "超时")


if __name__ == "__main__":
    raise SystemExit(main())
