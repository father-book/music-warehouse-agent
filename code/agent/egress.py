"""出网闸（AC-22）：数据留在自有集群，出网只走受控模型网关。

规则只有一条，但分两半：
  · **内网/回环一律放行** —— Doris(192.168.128.163)、PG(127.0.0.1:55432)、
    Redis(127.0.0.1:6379) 都在这条线上，拦它们等于把应用拦死；
  · **公网只允许调用栈里有 gateway 的连接** —— 出了内网，唯一合法目的地是
    配置里的模型网关域名，而且必须是从 gateway.py 发起的。其他模块（某个
    节点、某个脚本）想直连外部主机，会在 socket 层被拒并留痕。

为什么用 socket 钩子而不是防火墙
--------------------------------
  1. 演示环境没有改 iptables / 安全组 的权限，而验收线要能自己证明；
  2. 钩子能拿到**调用栈**，所以能回答"这次外连是谁发的、走没走网关"，
     防火墙只知道 IP、不知道发起方；
  3. 它能挡住"顺手 import httpx 直连"这类无意绕过——这是真实发生过的
     退化方式，而不是假想攻击。

它不是安全边界
--------------
  Python 里能 uninstall、能换 socket 实现，所以这只是**围栏**，真正的边界
  在防火墙/安全组。文档里必须这么写，不能把围栏吹成边界。

只 patch 两处，是因为它们覆盖了本项目全部外连路径：
  · httpx/httpcore 走 `socket.create_connection`；
  · 其他库自己解析域名走 `socket.getaddrinfo`。
`bind()` 不动——uvicorn 监听端口是入站，不属出网。

**代理是这套规则唯一的真窟窿，必须单独说**
本机跑在 `HTTP_PROXY=http://127.0.0.1:50401` 后面（实测：httpx 连的是回环代理，
不是 api.deepseek.com）。如果只按"回环放行"判，所有出网都会被当成内网放掉——
闸装了跟没装一样，而且**在页面上看起来还是"0 次外连"**，属于最坏的一种假绿。
所以判据分两层：
  1. **意图层**（`assert_target`，在 llm 里、拿到 URL 时调）：目标域名必须在
     allowlist 里，否则连代理都不许走；
  2. **socket 层**（`_judge`）：目标 IP 是公网，**或者目标是环境变量里配的
     代理端点**，都算出网，必须有 gateway 在调用栈上。
只有两层同时成立，出网才真的收口。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import ipaddress
import os
import socket
import sys
import threading
import time
from urllib.parse import urlparse

# 内网 + 回环 + 链路本地。数据的家在这里，不该被"出网收口"误伤。
_INTERNAL_NETS = tuple(ipaddress.ip_network(n) for n in (
    "127.0.0.0/8",        # 本机 PG / Redis
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",     # Doris FE 192.168.128.163
    "169.254.0.0/16",
    "::1/128",
    "fc00::/7",
    "fe80::/10",
))

_state_lock = threading.Lock()
_tls = threading.local()

_state: dict = {
    "installed": False,
    "enabled": True,
    "allowed": 0,          # 放行的出网连接数（内网不计）
    "blocked": 0,          # 拒绝的连接数
    "denied": [],          # 最近 20 条拒绝记录，供 /api/gateway 展示
    "allow_hosts": [],     # 允许出网的域名（来自 config.generator.base_url）
    "proxies": [],         # 环境变量里的代理端点，按"出网"对待
    "via_proxy": 0,        # 其中经代理出去的次数
    "since": "",
    "reason": "",          # 未安装 / 被关掉时的原因
}

_orig_getaddrinfo = socket.getaddrinfo
_orig_create_connection = socket.create_connection

_MAX_DENIED_KEPT = 20

# 这些环境变量一旦配了，httpx 会把**所有**请求发给代理，socket 层再也看不到
# 真实目标域名。所以代理端点必须被当成出网点来判，否则规则形同虚设。
_PROXY_ENV = ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy",
              "ALL_PROXY", "all_proxy")


class EgressDenied(ConnectionError):
    """外连被拒。继承 ConnectionError，让 httpx 当成网络错误处理而不是崩栈。"""


# ------------------------------------------------------------------ 判定

def _is_internal_ip(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip.split("%", 1)[0])   # 去掉 IPv6 zone id
    except ValueError:
        return False
    return any(addr in net for net in _INTERNAL_NETS)


def _is_gateway_frame(g) -> bool:
    """这一帧是不是网关。

    比模块名 + 文件名两个都要判：`python gateway.py` 时模块名是 __main__，
    只比名字会把网关自己当成"非网关"拦掉（第一版就是这么挂的）。
    """
    name = g.get("__name__") or ""
    if name == "gateway":
        return True
    fname = (g.get("__file__") or "").replace("\\", "/").rsplit("/", 1)[-1]
    return fname == "gateway.py"


def _gateway_in_stack() -> bool:
    """从当前帧往上找 gateway。

    只走 40 层：httpx 的调用栈大约 15~20 层，给一倍余量就够，再深就是
    别人的框架在转圈，没必要为它无限遍历。
    """
    try:
        f = sys._getframe(1)
    except ValueError:
        return False
    depth = 0
    while f is not None and depth < 40:
        if _is_gateway_frame(f.f_globals):
            return True
        f = f.f_back
        depth += 1
    return False


def _caller_name() -> str:
    """报错时告诉人"是谁想连"，否则日志里只有目标地址。"""
    try:
        f = sys._getframe(2)
    except ValueError:
        return "?"
    depth = 0
    while f is not None and depth < 40:
        name = f.f_globals.get("__name__") or ""
        # httpcore._backends.sync / contextlib 这类中转帧要跳过，
        # 否则报出来的"调用方"是中转层，看日志的人找不到真正发请求的那一行。
        if name and name != "egress" and not name.startswith(
                ("socket", "httpcore", "httpx", "contextlib", "ssl")):
            return name
        f = f.f_back
        depth += 1
    return "?"


def _host_allowed(host: str) -> bool:
    h = (host or "").strip().lower()
    return any(h == a or h.endswith("." + a) for a in _state["allow_hosts"])


def _is_proxy(host: str, port: int) -> bool:
    """这个 (host, port) 是不是环境变量里配的代理端点。"""
    h = (host or "").strip().lower()
    for p in _state["proxies"]:
        if p["host"] != h:
            continue
        if not p["port"] or not port or int(p["port"]) == int(port):
            return True
    return False


def _deny(host: str, port: int, why: str) -> None:
    with _state_lock:
        _state["blocked"] += 1
        _state["denied"].append({
            "at": time.strftime("%H:%M:%S"),
            "host": host, "port": port,
            "caller": _caller_name(),
            "reason": why,
        })
        del _state["denied"][:-_MAX_DENIED_KEPT]
    raise EgressDenied("出网被拒（%s）：%s:%s 未经模型网关，调用方 %s"
                       % (why, host, port, _caller_name()))


def _judge(host: str, port: int) -> None:
    """连接建立前的唯一判据。

    注意顺序：**先判代理，再判内网**。代理端点通常就是 127.0.0.1，
    先判内网会把出网全部放过去（这就是第一版踩的坑）。
    """
    h = (host or "").strip()
    if not h:
        return

    if _is_proxy(h, port):
        # 走到这里说明请求是发给代理的：真实目标域名在这一层已经看不到，
        # 只能靠意图层（assert_target）把关，这里只保证"是谁发起的"。
        if not _gateway_in_stack():
            _deny(h, port, "经代理出网但非网关发起")
        with _state_lock:
            _state["allowed"] += 1
            _state["via_proxy"] += 1
        return

    if _is_internal_ip(h):
        return                                  # 内网/回环：数据的家，放行
    if not _host_allowed(h):
        _deny(h, port, "目标不在 allowlist")
    if not _gateway_in_stack():
        _deny(h, port, "非网关发起")
    with _state_lock:
        _state["allowed"] += 1


def assert_target(host: str, port: int = 443) -> None:
    """意图层放行检查：拿到 URL 之后、发出请求之前调。

    经代理时 socket 层看不到真实域名，所以这一层是**唯一**能判"你要去哪"的
    地方。少了它，代理环境下的 allowlist 等于没写。
    """
    h = (host or "").strip().lower()
    if not h or _is_internal_ip(h):
        return
    if not _host_allowed(h):
        _deny(h, port, "目标不在 allowlist")


def check(host: str, port: int = 443) -> None:
    """给自测用的探针：不真连，只判。通过返回 None，拒绝抛 EgressDenied。"""
    _judge(host, port)


# ------------------------------------------------------------------ 钩子

def _wrapped_getaddrinfo(host, port, *args, **kwargs):
    # create_connection 内部会再调一次 getaddrinfo；外层已经判过，
    # 这里跳过以免同一次连接被计两次。
    if not getattr(_tls, "judging", False):
        _tls.judging = True
        try:
            _judge(str(host), int(port or 0))
            return _orig_getaddrinfo(host, port, *args, **kwargs)
        finally:
            _tls.judging = False
    return _orig_getaddrinfo(host, port, *args, **kwargs)


def _wrapped_create_connection(address, *args, **kwargs):
    host, port = (address[0], address[1]) if isinstance(address, tuple) else (address, 0)
    # judging 要连同 _orig 一起罩住：create_connection 内部还会调一次
    # getaddrinfo，不罩住就会把同一次连接判两遍、计两次数。
    _tls.judging = True
    try:
        _judge(str(host), int(port or 0))
        return _orig_create_connection(address, *args, **kwargs)
    finally:
        _tls.judging = False


# ------------------------------------------------------------------ 安装

def _allow_hosts_from(cfg: dict | None) -> list[str]:
    hosts: list[str] = []
    base = ((cfg or {}).get("generator") or {}).get("base_url") or ""
    h = urlparse(base).hostname
    if h:
        hosts.append(h.lower())
    extra = (os.getenv("EGRESS_ALLOW_HOSTS") or "").strip()
    for one in extra.split(","):
        one = one.strip().lower()
        if one and one not in hosts:
            hosts.append(one)
    return hosts


def _proxies_from_env() -> list[dict]:
    """把 HTTPS_PROXY 之类的端点解析出来。配了代理 = 出网多了个门，得看着它。"""
    seen: list[dict] = []
    default_port = {"http": 80, "https": 443, "socks5": 1080, "socks5h": 1080}
    for name in _PROXY_ENV:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            continue
        u = urlparse(raw if "://" in raw else "http://" + raw)
        host = (u.hostname or "").lower()
        if not host:
            continue
        port = u.port or default_port.get((u.scheme or "http").lower(), 0)
        item = {"host": host, "port": int(port or 0), "from": name}
        if not any(x["host"] == item["host"] and x["port"] == item["port"] for x in seen):
            seen.append(item)
    return seen


def install(cfg: dict | None = None) -> dict:
    """幂等安装。启动时调一次即可，重复调用只刷新 allowlist / 代理端点。"""
    if (os.getenv("AGENT_EGRESS") or "").strip().lower() == "off":
        with _state_lock:
            _state["enabled"] = False
            _state["reason"] = "AGENT_EGRESS=off"
        return stats()

    proxies = _proxies_from_env()
    with _state_lock:
        _state["allow_hosts"] = _allow_hosts_from(cfg)
        _state["proxies"] = proxies
        need_install = not _state["installed"]
        if need_install:
            socket.getaddrinfo = _wrapped_getaddrinfo
            socket.create_connection = _wrapped_create_connection
            _state["installed"] = True
            _state["since"] = time.strftime("%Y-%m-%d %H:%M:%S")
        no_hosts = not _state["allow_hosts"]

    # 注意：stats() 也要拿同一把锁，所以**必须**在锁外调。
    # 第一版在 with 里面 return stats()，第二次调用直接死锁——自测抓到的。
    if need_install and no_hosts:
        # 没配网关域名时仍然安装：内网照常，公网全拒。
        # 这比"配置缺失就放行"安全——缺配置不该等于开闸。
        with _state_lock:
            _state["reason"] = "未配置 generator.base_url，公网一律拒绝"
    return stats()


def uninstall() -> None:
    """只给自测用。生产代码里没有理由卸闸。"""
    with _state_lock:
        if not _state["installed"]:
            return
        socket.getaddrinfo = _orig_getaddrinfo
        socket.create_connection = _orig_create_connection
        _state["installed"] = False


def stats() -> dict:
    with _state_lock:
        return {
            "installed": _state["installed"],
            "enabled": _state["enabled"],
            "allowed": _state["allowed"],
            "blocked": _state["blocked"],
            "allow_hosts": list(_state["allow_hosts"]),
            "proxies": list(_state["proxies"]),
            "via_proxy": _state["via_proxy"],
            "denied": list(_state["denied"]),
            "since": _state["since"],
            "reason": _state["reason"],
        }


def reset_stats() -> None:
    """自测里清计数，不清钩子。"""
    with _state_lock:
        _state["allowed"] = 0
        _state["blocked"] = 0
        _state["via_proxy"] = 0
        _state["denied"].clear()


if __name__ == "__main__":
    from conf import load_config
    install(load_config())
    st = stats()
    print("allow_hosts:", st["allow_hosts"])
    print("proxies   :", st["proxies"])
    for probe in ("127.0.0.1", "192.168.128.163", "api.deepseek.com", "evil.example.com"):
        try:
            check(probe, 443)
            print("  放行", probe)
        except EgressDenied as e:
            print("  拒绝", probe, "→", str(e)[:70])
