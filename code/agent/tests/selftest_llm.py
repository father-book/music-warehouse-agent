"""llm 传输层的墙钟预算自测：本地起一个"慢速吐字节"的假服务端。

为什么这条必须测
----------------
`read` 超时算的是两次读之间的间隔，服务端每 30 秒吐一个字节就能合法地
把一次调用拖成十分钟——实测撞到过一次 607 秒的 generate，日志里没有任何
超时记录，只有"卡住了"。修法是自己算 deadline，而 deadline 这种代码
**不测就等于没写**：正常路径永远不会碰到它。

这里用 http.server 起一个假接口，先每 0.2s 吐一个空格拖住连接，
再看 chat() 是不是在预算内主动断开。

用法：python code/agent/tests/selftest_llm.py
"""
from __future__ import annotations

import http.server
import json
import os
import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import _bootstrap  # noqa: F401
import llm

PASS = FAIL = 0


def ck(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        print("  ✗ %s   %s" % (name, extra))


class _Trickle(http.server.BaseHTTPRequestHandler):
    """收到请求后每 0.2 秒吐一个空格，拖住连接不结束。"""

    def do_POST(self):                                   # noqa: N802
        self.rfile.read(int(self.headers.get("content-length") or 0))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        try:
            for _ in range(120):                         # 最长 24 秒
                self.wfile.write(b" ")
                self.wfile.flush()
                time.sleep(0.2)
        except (BrokenPipeError, ConnectionAbortedError, OSError):
            pass                                         # 客户端断开是预期行为

    def log_message(self, *a):                           # 别刷屏
        pass


class _Fast(http.server.BaseHTTPRequestHandler):
    def do_POST(self):                                   # noqa: N802
        self.rfile.read(int(self.headers.get("content-length") or 0))
        body = json.dumps({"model": "fake", "choices": [{"message": {"content": "嗯"}}],
                           "usage": {"prompt_tokens": 3, "completion_tokens": 1}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def _serve(handler) -> tuple[str, http.server.HTTPServer]:
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return "http://127.0.0.1:%d" % srv.server_address[1], srv


def _cfg(base: str, budget: float, timeout: float = 2.0) -> dict:
    return {"generator": {
        "mode": "llm", "model": "fake", "base_url": base,
        "api_key_env": "LLM_API_KEY", "temperature": 0, "max_tokens": 16,
        "timeout_sec": timeout, "total_budget_sec": budget,
    }}


def main() -> int:
    os.environ.setdefault("LLM_API_KEY", "test-key")

    print("\n[1] 正常响应照常返回")
    base, srv = _serve(_Fast)
    try:
        r = llm._transport_chat([{"role": "user", "content": "hi"}], _cfg(base, 10))
        ck("拿到 content", r.get("content") == "嗯", str(r.get("content")))
        ck("拿到 usage", (r.get("usage") or {}).get("prompt_tokens") == 3)
    except Exception as e:
        ck("正常响应不报错", False, "%s: %s" % (type(e).__name__, e))
    srv.shutdown()

    print("\n[2] 慢速吐字节：必须在总预算内断开")
    base, srv = _serve(_Trickle)
    t0 = time.perf_counter()
    err = ""
    try:
        llm._transport_chat([{"role": "user", "content": "hi"}], _cfg(base, 2.5))
    except llm._Deadline as e:
        err = "deadline"
        print("    （断开原因：%s）" % str(e)[:70])
    except Exception as e:
        err = "%s: %s" % (type(e).__name__, str(e)[:60])
    dt = time.perf_counter() - t0
    ck("抛的是 _Deadline 而不是别的异常", err == "deadline", err)
    # 允许 2 秒余量：断开时机取决于 chunk 到达节奏，不该要求毫秒级精确
    ck("在预算附近就断开（%.1fs ≤ 4.5s）" % dt, dt <= 4.5, "%.1fs" % dt)
    ck("没有把 24 秒的慢响应等完", dt < 20, "%.1fs" % dt)
    srv.shutdown()

    print("\n[3] 总预算更小的时候，重试也不会把时间翻倍")
    base, srv = _serve(_Trickle)
    t0 = time.perf_counter()
    try:
        llm._transport_chat([{"role": "user", "content": "hi"}], _cfg(base, 1.5))
    except llm.LLMError:
        pass
    dt = time.perf_counter() - t0
    ck("三次重试总计不超过预算太多（%.1fs ≤ 4.5s）" % dt, dt <= 4.5, "%.1fs" % dt)
    srv.shutdown()

    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
