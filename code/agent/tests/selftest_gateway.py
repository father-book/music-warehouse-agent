"""模型网关（AC-18）与出网闸（AC-22）的自测。

为什么要专门写这一套：网关的错误都是**静默**的。
  · 少记一条台账 → 成本看板低一点，没人看得出来；
  · 台账写失败被吞 → 页面一切正常，账上少一行；
  · 出网闸装了个寂寞 → 打印"已启用"，实际一次都没拦过。
这三种都不会报错，只能靠构造场景去抓。所以这里做的事是：

  1. 真起一个假模型服务端（本地 HTTP），走真 httpx、真 socket 钩子；
  2. 用一个**独立的测试库** `agent_gov_selftest`（每次重建），
     不往正式台账里塞垃圾数据——台账有不可变触发器，塞进去就删不掉；
  3. 代理场景必须单独测：本机跑在 HTTP_PROXY 后面时，
     socket 层看到的全是 127.0.0.1，第一版闸就是这么被骗过去的。

    python code/agent/tests/selftest_gateway.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from _bootstrap import HERE  # noqa: E402
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import httpx  # noqa: E402
import psycopg  # noqa: E402

import egress  # noqa: E402
import gateway  # noqa: E402
import llm  # noqa: E402
from governance import store  # noqa: E402
from governance.ratelimit import limiter  # noqa: E402

OK = FAILED = 0
PG_ADMIN = "postgresql://postgres@127.0.0.1:55432/postgres"
TEST_DB = "agent_gov_selftest"
TEST_DSN = "postgresql://postgres@127.0.0.1:55432/" + TEST_DB

# 假服务端的状态放模块级：handler 每个请求都会新建实例，挂实例上会丢。
SRV = {"hits": 0, "mode": "ok", "last_body": None}


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAILED
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAILED += 1
        print("  ✗ %s %s" % (name, extra))


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        SRV["hits"] += 1
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            SRV["last_body"] = json.loads(raw.decode("utf-8"))
        except ValueError:
            SRV["last_body"] = None

        mode = SRV["mode"]
        if mode == "fail400":
            body = b"bad request"
            self.send_response(400)
        elif mode == "fail500":
            body = b"boom"
            self.send_response(500)
        else:
            payload = {
                "model": "fake-model",
                "choices": [{"message": {"content": '{"sql": "SELECT 1"}'}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 7,
                          "total_tokens": 19},
            }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):                      # 代理形态会用 GET 探活
        self.do_POST()

    def log_message(self, *a):             # 别把 127.0.0.1 的访问日志喷一屏
        pass


def _serve() -> tuple[ThreadingHTTPServer, str]:
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d" % srv.server_address[1]


def _cfg(base_url: str, **gw) -> dict:
    return {
        "generator": {
            "model": "fake-model", "base_url": base_url,
            "api_key_env": "LLM_API_KEY", "temperature": 0,
            "max_tokens": 200, "timeout_sec": 20, "total_budget_sec": 20,
        },
        "cost": {"price_in_per_mtok": 2.0, "price_out_per_mtok": 8.0},
        "gateway": {"max_calls_per_min": 0, "daily_cost_cny": 0, **gw},
        "governance": {"dsn": TEST_DSN},
    }


def _fresh_db() -> tuple[bool, str]:
    """重建测试库：台账只追加，跑第二遍时不清库，断言会全部串味。"""
    try:
        admin = psycopg.connect(PG_ADMIN, autocommit=True, connect_timeout=5)
        with admin.cursor() as cur:
            cur.execute("DROP DATABASE IF EXISTS %s WITH (FORCE)" % TEST_DB)
            cur.execute("CREATE DATABASE %s" % TEST_DB)
        admin.close()
        ok, err = store.init_schema(_cfg("http://127.0.0.1"))
        return ok, err
    except Exception as e:
        return False, "%s: %s" % (type(e).__name__, str(e)[:200])


def _rows(trace_id: str) -> list[dict]:
    c = psycopg.connect(TEST_DSN, connect_timeout=5)
    try:
        with c.cursor() as cur:
            cur.execute("SELECT * FROM cost_ledger WHERE trace_id = %s ORDER BY id",
                        (trace_id,))
            names = [d.name for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]
    finally:
        c.close()


def main() -> int:
    os.environ.setdefault("LLM_API_KEY", "sk-selftest")
    os.environ.pop("GOV_PG_DSN", None)      # 别让外部 DSN 把测试引到正式库

    # 本机若配了 HTTP_PROXY（这台机器就有，127.0.0.1:50401），httpx 会把
    # 连本地假服务端的请求也发给代理——第一次跑就卡在这上面。
    # 真实部署里 NO_PROXY 一定会带 localhost，这里照做；[10] 段再显式去掉。
    saved_noproxy = {k: os.environ.get(k) for k in ("NO_PROXY", "no_proxy")}
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = "127.0.0.1,localhost"

    print("[0] 准备：独立测试库 + 假模型服务端")
    ok, err = _fresh_db()
    ck("测试库可用（agent_gov_selftest）", ok, err)
    if not ok:
        print("\n无法创建测试库，台账相关用例无法可信执行，中止。")
        return 1
    srv, base = _serve()
    ck("假模型服务端已起", bool(base), base)
    cfg = _cfg(base)

    # ---------------------------------------------------------------- 1
    print("\n[1] 成功调用：一条台账，字段与 usage 对齐")
    SRV.update(mode="ok", hits=0)
    out = gateway.chat([{"role": "user", "content": "昨天的 DAU 是多少"}], cfg,
                       caller="generate", trace_id="t-ok", ctx={"role": "data_analyst"})
    ck("返回内容正常", "SELECT 1" in (out.get("content") or ""))
    rows = _rows("t-ok")
    ck("落了一条台账", len(rows) == 1, "实际 %d 条" % len(rows))
    if rows:
        r = rows[0]
        ck("token 与响应 usage 一致（12/7）", (r["tokens_in"], r["tokens_out"]) == (12, 7),
           str((r["tokens_in"], r["tokens_out"])))
        ck("成本按单价算对（¥0.00008）", abs(float(r["cost_cny"]) - 0.00008) < 1e-9,
           str(r["cost_cny"]))
        ck("caller / role 记下来了", (r["caller"], r["role"]) == ("generate", "data_analyst"),
           "%s / %s" % (r["caller"], r["role"]))
        ck("prompt 指纹非空且长度 32", len(r["prompt_hash"]) == 32, r["prompt_hash"])
        ck("prompt 首段记得住「问了什么」", "DAU" in (r["prompt_head"] or ""), r["prompt_head"])
        ck("答案首段可回看", "SELECT 1" in (r["answer_head"] or ""), r["answer_head"])
        ck("ledger_ok 回传给调用方", out.get("ledger_ok") is True)
    ck("只发了 1 次请求（没多余重试）", SRV["hits"] == 1, "hits=%d" % SRV["hits"])

    # ---------------------------------------------------------------- 2
    print("\n[2] 失败调用：也要记账（失败才是成本黑洞）")
    SRV.update(mode="fail400", hits=0)
    caught = None
    try:
        gateway.chat([{"role": "user", "content": "x"}], cfg,
                     caller="repair", trace_id="t-fail")
    except llm.LLMError as e:
        caught = e
    ck("失败照常抛 LLMError", caught is not None)
    rows = _rows("t-fail")
    ck("失败也落一条台账", len(rows) == 1, "实际 %d 条" % len(rows))
    if rows:
        ck("ok=false 且错误原因留下来了",
           rows[0]["ok"] is False and "400" in (rows[0]["error"] or ""),
           "%s / %s" % (rows[0]["ok"], (rows[0]["error"] or "")[:40]))
        ck("失败的成本记 0（没白花钱就不该记钱）", float(rows[0]["cost_cny"]) == 0.0)

    # ---------------------------------------------------------------- 3
    print("\n[3] 缓存命中：记 cached=true / 0 成本")
    gateway.record_cache_hit(cfg, trace_id="t-cache",
                             ctx={"user_id": "u1", "role": "data_analyst"},
                             key="abcdef1234567890")
    rows = _rows("t-cache")
    ck("命中落一条台账", len(rows) == 1)
    if rows:
        ck("cached=true 且成本 0",
           rows[0]["cached"] is True and float(rows[0]["cost_cny"]) == 0.0)
        ck("note 说清是缓存省下的", "缓存" in (rows[0]["note"] or ""), rows[0]["note"])

    # ---------------------------------------------------------------- 4
    print("\n[4] 单次预算：超了不发请求（省的是真钱）")
    SRV.update(mode="ok", hits=0)
    small = _cfg(base)
    refused = None
    try:
        gateway.chat([{"role": "user", "content": "x"}], small, max_tokens=2000,
                     ctx={"budget_tokens": 50}, trace_id="t-budget")
    except gateway.GatewayRefused as e:
        refused = e
    ck("按预算拒绝", refused is not None and refused.reason == "budget",
       getattr(refused, "reason", ""))
    ck("拒绝时一次请求都没发", SRV["hits"] == 0, "hits=%d" % SRV["hits"])
    ck("拒绝也是一种 LLMError（上层不用改捕获）", isinstance(refused, llm.LLMError))

    # ---------------------------------------------------------------- 5
    print("\n[5] 每日成本上限：花到线就停")
    daily = _cfg(base, daily_cost_cny=0.0005)
    store.insert_cost({"trace_id": "t-prev", "user_id": "u-daily", "role": "pm",
                       "model": "fake-model", "caller": "generate", "tokens_in": 1,
                       "tokens_out": 1, "cost_cny": 0.0006, "cached": False,
                       "ok": True, "latency_ms": 1, "attempts": 1, "error": "",
                       "prompt_hash": "", "prompt_chars": 0, "prompt_head": "",
                       "answer_head": "", "note": "占位"}, daily)
    refused = None
    try:
        gateway.chat([{"role": "user", "content": "x"}], daily,
                     ctx={"user_id": "u-daily"}, trace_id="t-daily")
    except gateway.GatewayRefused as e:
        refused = e
    ck("按每日上限拒绝", refused is not None and refused.reason == "daily_budget",
       getattr(refused, "reason", ""))

    # ---------------------------------------------------------------- 6
    print("\n[6] 频次限流：一分钟内的第二次被挡")
    limiter().reset()
    rate = _cfg(base, max_calls_per_min=1)
    gateway.chat([{"role": "user", "content": "x"}], rate,
                 ctx={"user_id": "u-rate"}, trace_id="t-rate1")
    refused = None
    try:
        gateway.chat([{"role": "user", "content": "x"}], rate,
                     ctx={"user_id": "u-rate"}, trace_id="t-rate2")
    except gateway.GatewayRefused as e:
        refused = e
    ck("超频次被拒", refused is not None and refused.reason == "rate_limit",
       getattr(refused, "reason", ""))
    ck("给了 retry_after（调用方不用猜）", getattr(refused, "retry_after", 0) > 0,
       str(getattr(refused, "retry_after", 0)))
    limiter().reset()

    # ---------------------------------------------------------------- 7
    print("\n[7] 台账写失败：不阻断问答，但必须看得见")
    dead = _cfg(base)
    dead["governance"] = {"dsn": "postgresql://postgres@127.0.0.1:1/nope"}
    SRV.update(mode="ok", hits=0)
    out = gateway.chat([{"role": "user", "content": "x"}], dead, trace_id="t-dead")
    ck("模型结果照常返回", "SELECT 1" in (out.get("content") or ""))
    ck("ledger_ok=false", out.get("ledger_ok") is False)
    ck("ledger_error 带原因（不是空字符串）", bool(out.get("ledger_error")),
       (out.get("ledger_error") or "")[:60])

    # ---------------------------------------------------------------- 8
    print("\n[8] 直连守卫：不经网关调传输层会被拒")
    code = ("import _bootstrap, llm;"
            "from conf import load_config;"
            "llm._transport_chat([{'role':'user','content':'hi'}], load_config())")
    p = subprocess.run([sys.executable, "-c", code], cwd=str(HERE),
                       capture_output=True, text=True, timeout=60)
    blob = (p.stdout or "") + (p.stderr or "")
    ck("子进程直连被拒", "直连模型被拒" in blob, blob.strip().splitlines()[-1:] or "")
    ck("拒绝发生在发请求之前（不是网络错误）", "ConnectError" not in blob)

    # ---------------------------------------------------------------- 9
    print("\n[9] 出网闸：内网放行，公网只认网关 + allowlist")
    pub = _cfg("https://api.deepseek.com")
    egress.install(pub)
    egress.reset_stats()
    for host, port in (("127.0.0.1", 55432), ("192.168.128.163", 9030),
                       ("10.1.2.3", 3306)):
        try:
            egress.check(host, port)
            ck("内网放行 %s:%d" % (host, port), True)
        except egress.EgressDenied as e:
            ck("内网放行 %s:%d" % (host, port), False, str(e)[:60])
    try:
        egress.check("api.deepseek.com", 443)
        ck("公网非网关发起被拒", False, "居然放行了")
    except egress.EgressDenied as e:
        ck("公网非网关发起被拒", "非网关" in str(e), str(e)[:60])
    try:
        egress.check("evil.example.com", 443)
        ck("allowlist 外的目标被拒", False, "居然放行了")
    except egress.EgressDenied as e:
        ck("allowlist 外的目标被拒", "allowlist" in str(e), str(e)[:60])
    try:
        egress.assert_target("evil.example.com")
        ck("意图层也拦 allowlist 外", False)
    except egress.EgressDenied:
        ck("意图层也拦 allowlist 外", True)
    egress.assert_target("api.deepseek.com")
    ck("意图层放行网关域名", True)
    st = egress.stats()
    ck("拒绝有计数（不是静默）", st["blocked"] >= 2, "blocked=%d" % st["blocked"])
    ck("拒绝记录带调用方", bool(st["denied"]) and bool(st["denied"][-1]["caller"]),
       str(st["denied"][-1] if st["denied"] else {}))

    # ---------------------------------------------------------------- 10
    print("\n[10] 代理环境：回环代理不能让闸失灵（第一版就是这么翻的）")
    proxy_srv, proxy_base = _serve()
    proxy_port = proxy_srv.server_address[1]
    ck("代理端点就是 127.0.0.1（正是骗过闸的那种）", proxy_base.startswith("http://127.0.0.1:"))
    egress.install(pub)
    # 这台机器的代理端点来自环境变量（实际是 127.0.0.1:50401）。闸必须把它
    # 当出网点看：它虽然长着回环的地址，做的却是出网的事。
    env_proxies = egress.stats()["proxies"]
    ck("环境里的代理端点被识别", bool(env_proxies), str(env_proxies))
    if env_proxies:
        p = env_proxies[0]
        try:
            egress.check(p["host"], p["port"])
            ck("回环代理端点按出网判（非网关→拒绝）", False, "被当成内网放行了")
        except egress.EgressDenied as e:
            ck("回环代理端点按出网判（非网关→拒绝）",
               "代理" in str(e) or "非网关" in str(e), str(e)[:60])

    old = {k: os.environ.get(k) for k in ("HTTP_PROXY", "HTTPS_PROXY",
                                          "http_proxy", "https_proxy")}
    os.environ["HTTP_PROXY"] = proxy_base
    os.environ["http_proxy"] = proxy_base
    for k in ("HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy"):
        os.environ.pop(k, None)
    try:
        proxy_cfg = _cfg("http://api.deepseek.com")   # 公网域名，但经代理出
        egress.install(proxy_cfg)
        ck("闸认识到了代理", bool(egress.stats()["proxies"]), str(egress.stats()["proxies"]))
        try:
            egress.check("127.0.0.1", proxy_port)
            ck("代理端点（非网关发起）被拒", False, "放行了")
        except egress.EgressDenied:
            ck("代理端点（非网关发起）被拒", True)
        SRV.update(mode="ok", hits=0)
        denied = False
        try:
            httpx.get("http://api.deepseek.com/", timeout=5)
        except Exception:
            denied = True
        ck("代理环境下：非网关直连仍被拒", denied)
        out = gateway.chat([{"role": "user", "content": "x"}], proxy_cfg,
                           trace_id="t-proxy")
        ck("代理环境下：经网关可以出去", "SELECT 1" in (out.get("content") or ""))
        ck("经代理的出网被单独计数", egress.stats()["via_proxy"] >= 1,
           "via_proxy=%d" % egress.stats()["via_proxy"])
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        os.environ["NO_PROXY"] = os.environ["no_proxy"] = "127.0.0.1,localhost"
        proxy_srv.shutdown()

    # ---------------------------------------------------------------- 11
    print("\n[11] 静态检查：生产代码里没有第二处直连")
    offenders = []
    for p in sorted(HERE.glob("*.py")):
        if p.name in ("llm.py", "gateway.py", "egress.py") or p.name.startswith("selftest_"):
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        if "_transport_chat" in text:
            offenders.append(p.name + "(_transport_chat)")
        if "import httpx" in text:
            offenders.append(p.name + "(httpx)")
    ck("只有 gateway 调传输层 / 只有 llm 用 httpx", not offenders, "、".join(offenders))
    ck("llm 已不再暴露 chat()", not hasattr(llm, "chat"))

    srv.shutdown()
    print("\n%d 通过 / %d 失败" % (OK, FAILED))
    return 0 if FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
