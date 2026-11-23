"""密钥集中管理（AC-21）。

**要解决的问题不是"密钥存在哪"，而是"密钥会不会在没人注意的地方被打印出来"。**

> 文件为什么不叫 `secrets.py`：`secrets` 是**标准库模块名**，而本目录在
> `sys.path` 最前面。叫这个名字会劫持 httpx / urllib 内部的 `import secrets`，
> 让整个 HTTP 栈在别处莫名其妙地崩。命名撞标准库是"隔半个月才炸"的 bug，
> 所以宁可叫 `vault`。

之前的状态：`env.require_env()` 只是"读一下 os.environ"，返回裸 str。
只要有一个 print / 日志 / 异常消息 / trace JSON 顺手带上配置，整条 key 就进了
日志文件、审计表、或者前端页面——而且没人会立刻发现。

所以这里的做法是让泄露变成"难以发生"，而不是"禁止发生"：

1. **值被包进 `Secret`**。`repr()` / `str()` / `f"{s}"` 全部只返回掩码，
   想拿真值必须写 `.reveal()`。这样 `print(cfg)`、`json.dumps(cfg)`、
   logger 的 `%s` 都不需要人记得脱敏——默认就是安全的。
2. **`scrub()` 兜底**。已经登记过的真值，在任意文本里出现都会被替换掉。
   挂在异常构造处：DB 连接串、HTTP 请求头、模型返回的报错，都过一遍。
3. **`validate()` 在启动时一次性报全**。缺 key 不该等到第一次问数才炸，
   更不该"缺三个只报第一个"——运维一次就能补齐。
4. **`audit()` 供 /api 展示**：哪个键、从哪来（env / .env / 缺失）、多长、
   被哪些模块取过。**永远不含真值**。

边界说明：这是防"手滑"，不是防"恶意"。能改代码的人总能 `.reveal()`。
真实企业里这一层之后还有 KMS/Vault + 短时凭证 + 出网审计，本项目只做到
"同一台机器上不因疏忽而泄露"这一档，文档里也这么写，不夸大。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import os
import threading
from pathlib import Path

import env as _env

AGENT_DIR = Path(__file__).resolve().parent

# 值太短的键不做全局 scrub：`"1"` 这种替换会把正常文本打成马赛克。
_SCRUB_MIN_LEN = 8

_lock = threading.Lock()
_registry: dict[str, "Secret"] = {}
# 记录"谁取过真值"，供 audit() 展示。只记模块名，不记次数细节。
_revealers: dict[str, set[str]] = {}


class Secret:
    """密钥的包装。默认不可见，取真值必须显式 reveal()。"""

    __slots__ = ("name", "_value", "source")

    def __init__(self, name: str, value: str, source: str = "env") -> None:
        self.name = name
        self._value = value or ""
        self.source = source          # env | dotenv | missing

    # ---- 真值出口：唯一的一个 ----
    def reveal(self) -> str:
        if not self._value:
            return ""
        _note_reveal(self.name)
        return self._value

    # 名字起得刺眼，是为了让 code review 时一眼看见"这里在取真值"。
    value = reveal

    # ---- 一切"顺手打印"的路径都返回掩码 ----
    @property
    def masked(self) -> str:
        return _mask(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)

    def __len__(self) -> int:
        return len(self._value)

    def __repr__(self) -> str:
        return "Secret(%s=%s)" % (self.name, self.masked)

    __str__ = __repr__

    def __format__(self, spec: str) -> str:
        # f"{s}" / "{}".format(s) 也走掩码。刻意**不**支持 spec：
        # 需要格式化就先 reveal()，这一步会留在 review diff 里。
        return self.masked

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Secret):
            return self._value == other._value
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.name, self._value))


def _mask(v: str) -> str:
    """掩码策略按长度分级。

    早期版本一律"前 3 后 4"，对 35 位的 API key 没问题（那是标准做法，
    能认出是哪把、又不能直接用），但对 15 位的密码等于泄露了 7 个字符——
    短密钥的每一位都更值钱。所以 20 位以下一律不露任何字符。
    """
    if not v:
        return "(空)"
    if len(v) < 8:
        return "*" * len(v) + " (%d 字符)" % len(v)
    if len(v) < 20:
        return "*" * 6 + " (%d 字符，过短不显字符)" % len(v)
    return "%s…%s (%d 字符)" % (v[:3], v[-4:], len(v))


def _note_reveal(name: str) -> None:
    import sys
    mod = "?"
    try:
        f = sys._getframe(2)
        mod = (f.f_globals.get("__name__") if f else "") or "?"
    except ValueError:
        pass
    with _lock:
        _revealers.setdefault(name, set()).add(mod)


# ---------------------------------------------------------------- 取用
def _source_of(name: str) -> str:
    """.env 里有的算 dotenv，环境变量里算 env。两者都有时以 env 为准（env.py 用
    setdefault，已存在的不会被覆盖，所以这里"存在即 env"是准的）。"""
    p = AGENT_DIR / ".env"
    if p.exists():
        try:
            for raw in p.read_text(encoding="utf-8").splitlines():
                k, sep, _ = raw.strip().partition("=")
                if sep and k.strip() == name:
                    return "dotenv"
        except OSError:
            pass
    return "env"


def get(name: str, default: str = "") -> Secret:
    with _lock:
        s = _registry.get(name)
        if s is not None:
            return s
    raw = os.getenv(name)
    if raw:
        s = Secret(name, raw, _source_of(name))
    else:
        s = Secret(name, default, "missing")
    with _lock:
        _registry[name] = s
    return s


def require(name: str) -> Secret:
    s = get(name)
    if not s:
        raise SecretMissing(
            "必需密钥 %s 未配置。写入 code/agent/.env（已 gitignore）或注入环境变量。"
            % name)
    return s


def reveal(name: str) -> str:
    return require(name).reveal()


class SecretMissing(RuntimeError):
    """缺失密钥。刻意**不**继承 ValueError——它要能被上层当成"配置问题"识别。"""


# ---------------------------------------------------------------- 校验
def required_names(cfg: dict | None = None) -> list[str]:
    """从配置里推导必需项，而不是在代码里硬编码一份名单。

    配置改了（换库、换模型、加 Redis 密码），必需项自动跟着变——
    硬编码名单的结局一定是"配置加了、名单忘了"。
    """
    names: list[str] = []
    c = cfg or {}
    g = (c.get("generator") or {}).get("api_key_env")
    if g:
        names.append(g)

    # 执行面：单连接写 execution，多执行器写 execution.executors。
    # 两种都收，别让"配置写法变了、密钥检查悄悄不查了"这种事发生。
    ex = c.get("execution") or c.get("doris") or {}
    if ex.get("password_env"):
        names.append(ex["password_env"])
    for one in ex.get("executors") or []:
        if one.get("password_env"):
            names.append(one["password_env"])

    r = (c.get("cache") or {}).get("redis") or {}
    if r.get("password_env"):
        names.append(r["password_env"])
    # GOV_PG_DSN 不算"必需"：本地 trust 认证时它本就不该有值。
    return list(dict.fromkeys(names))


def warmup(cfg: dict | None = None) -> None:
    """把必需项全部过一遍 get()。

    不预热的话，**缺失的键根本不会进登记表**，audit() 就会显示"全部已配置"——
    这是最糟的假绿：审计页看起来干净，因为缺的那个压根没被问过。
    """
    for n in required_names(cfg):
        get(n)


def validate(cfg: dict | None = None, extra: list[str] | None = None) -> tuple[bool, list[str]]:
    """启动时校验。返回 (是否齐全, 缺失清单)——缺失项一次报全，不逐个炸。"""
    names = required_names(cfg) + list(extra or [])
    missing = [n for n in dict.fromkeys(names) if not get(n)]
    return (not missing), missing


def assert_ready(cfg: dict | None = None, extra: list[str] | None = None) -> None:
    """启动流程调用：缺失即拒绝启动，并说清怎么补。"""
    ok, missing = validate(cfg, extra)
    if ok:
        return
    raise SecretMissing(
        "启动中止：%d 个必需密钥缺失 —— %s\n"
        "  补法：写入 code/agent/.env（已 gitignore），或注入同名环境变量。\n"
        "  当前已配置：%s" % (
            len(missing), ", ".join(missing),
            ", ".join(sorted(n for n in _registry if _registry[n])) or "（无）"))


def weak(min_len: int = 12) -> list[dict]:
    """长度不足的密钥。**不阻断启动**——本地 Redis 绑 127.0.0.1 时 6 位密码
    在风险上可接受，但"可接受"必须是个写下来的判断，而不是没人注意。

    所以这里只报出来，让启动横幅和 /api/secrets 显式显示"这个短"。
    """
    with _lock:
        items = list(_registry.values())
    return [{"name": s.name, "length": len(s)}
            for s in items if s and len(s) < min_len]


# ---------------------------------------------------------------- 脱敏
def scrub(text: str) -> str:
    """把任意文本里出现的**已知密钥真值**替换成 `<secret:NAME>`。

    挂在异常消息构造处。长值优先替换，避免 `sk-ab` 先把 `sk-abcdef...` 切坏。
    """
    if not text:
        return text
    out = str(text)
    with _lock:
        pairs = [(s._value, s.name) for s in _registry.values()
                 if s._value and len(s._value) >= _SCRUB_MIN_LEN]
    for val, name in sorted(pairs, key=lambda p: -len(p[0])):
        if val in out:
            out = out.replace(val, "<secret:%s>" % name)
    return out


def scrub_obj(obj):
    """递归脱敏 dict/list/str。给 trace、审计、API 返回用。"""
    if isinstance(obj, str):
        return scrub(obj)
    if isinstance(obj, Secret):
        return obj.masked
    if isinstance(obj, dict):
        return {k: scrub_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [scrub_obj(v) for v in obj]
    return obj


# ---------------------------------------------------------------- 审计
def audit() -> list[dict]:
    """供 /api/secrets 展示。**不含真值**，连掩码都只给长度与前缀类型。"""
    with _lock:
        items = list(_registry.values())
        rev = {k: sorted(v) for k, v in _revealers.items()}
    out = []
    for s in sorted(items, key=lambda x: x.name):
        out.append({
            "name": s.name,
            "configured": bool(s),
            "source": s.source,
            "length": len(s),
            "masked": s.masked,
            "revealed_by": rev.get(s.name, []),
        })
    return out


def summary_line() -> str:
    """启动横幅用：一行说清"几个键、齐不齐、缺哪个"。"""
    with _lock:
        items = list(_registry.values())
    ok = [s for s in items if s]
    miss = [s.name for s in items if not s]
    line = "密钥 %d/%d 已配置" % (len(ok), len(items))
    if miss:
        line += "，缺失：" + ", ".join(miss)
    return line


def reset_cache() -> None:
    """自测用：清掉登记表，让下次 get() 重新读环境。"""
    with _lock:
        _registry.clear()
        _revealers.clear()


if __name__ == "__main__":
    from conf import load_config
    cfg = load_config()
    warmup(cfg)                       # 关键：让"缺失的键"也出现在审计表里
    ok, missing = validate(cfg)
    for it in audit():
        print("%-22s %-8s %-8s %s" % (it["name"], it["source"],
                                      "已配置" if it["configured"] else "缺失",
                                      it["masked"]))
    print()
    print("齐全" if ok else "缺失：" + ", ".join(missing))
    for w in weak():
        print("弱密钥：%s 只有 %d 位（本地服务可接受，对外服务不可）"
              % (w["name"], w["length"]))
    s = get("LLM_API_KEY")
    print("repr 泄露检查 →", repr(s))
    # 这里刻意不打印任何真值——连演示也要走 scrub。
    print("scrub 检查     →", scrub("请求头 Authorization: Bearer " + s.reveal()))
