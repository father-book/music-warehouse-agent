"""极简 Redis 客户端：RESP 协议走裸 socket，**零第三方依赖**。

为什么不用 redis-py
-------------------
1. 本机的 pip 连不上 PyPI（见 requirements.txt 里那段说明），`_wheels` 里也没备
   redis 包；为缓存后端再加一个需要联网的依赖，会让"换台机器能不能跑起来"
   重新变成一个问号。
2. 缓存链路只用 7 个命令（AUTH/SELECT/PING/GET/SET/DEL/EXISTS/SCAN/DBSIZE/INFO）。
   为这 7 个命令引一个通用客户端不划算。
3. 客户端只有两百行，**出问题时能读得懂**——多实例共享缓存这种东西，
   故障往往表现为"偶尔不命中"，不能靠猜。

兼容性
------
按 Redis 2.6+ 的协议写，已在本机 Redis 3.2.100（Windows 版）实测。
避开了 3.2 之后才有的命令（UNLINK / SET EXAT / HELLO 等），
所以接 5.x / 6.x / 7.x 也不需要改。

约定
----
`cmd()` 原样返回：simple string → str，integer → int，bulk → bytes，
不存在（nil）→ None，array → list。**不做自动解码**——缓存值可能是
gzip 二进制，自动解码会把它弄坏。
"""
from __future__ import annotations

import socket
import threading


class RedisError(Exception):
    """连接层错误（网络断、超时、连接被关）。可以重试/降级。"""


class RedisReplyError(RedisError):
    """服务端明确报错（-ERR / -WRONGTYPE / -NOAUTH）。重试没有意义。"""


def _encode(args) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode("utf-8")
        out.append(b"$%d\r\n" % len(b))
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


def _read_reply(fp):
    line = fp.readline()
    if not line:
        raise RedisError("连接被对端关闭")
    if not line.endswith(b"\r\n"):
        raise RedisError("协议错误：回复未以 CRLF 结尾")
    t, body = line[:1], line[1:-2]
    if t == b"+":
        return body.decode("utf-8", "replace")
    if t == b"-":
        raise RedisReplyError(body.decode("utf-8", "replace"))
    if t == b":":
        return int(body)
    if t == b"$":
        n = int(body)
        if n < 0:
            return None
        buf = fp.read(n + 2)
        if len(buf) < n:
            raise RedisError("bulk 读取不完整")
        return buf[:n]
    if t == b"*":
        n = int(body)
        if n < 0:
            return None
        return [_read_reply(fp) for _ in range(n)]
    raise RedisError("未知回复类型 %r" % t)


class _Conn:
    __slots__ = ("sock", "fp", "born")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.fp = sock.makefile("rb")

    def close(self) -> None:
        for x in (self.fp, self.sock):
            try:
                x.close()
            except Exception:
                pass


class Redis:
    """连接池 + 断线不粘锅。线程安全。"""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379,
                 password: str | None = None, db: int = 0,
                 timeout: float = 2.0, pool: int = 4):
        self.host, self.port = host, int(port)
        self.password = password or None
        self.db = int(db or 0)
        self.timeout = float(timeout)
        self.pool_size = max(1, int(pool))
        self._idle: list[_Conn] = []
        self._lock = threading.Lock()
        self._closed = False

    # ---------------------------------------------------------------- 连接
    def _connect(self) -> _Conn:
        """建连。**所有** OSError 都必须转成 RedisError。

        这一层不转的话，socket 超时会以裸 TimeoutError 冒到调用方，
        而缓存层写的是 `except RedisError` —— 抓不到。后果不是"缓存不可用"，
        而是**整个问数请求失败**：明明有进程内降级可走，用户却看到报错。

        真实发生过：Redis 停止后 webapp 每个请求都返回
        `{"error":"TimeoutError: timed out"}`，只用了 2 秒（正是 Redis 的超时值）。
        根因就是 `_borrow()` 在 `cmd()` 的 try 之外，超时没人接。
        """
        try:
            s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        except OSError as e:
            raise RedisError("连接 Redis 失败 %s:%s（%s）"
                             % (self.host, self.port, type(e).__name__)) from e
        s.settimeout(self.timeout)
        c = _Conn(s)
        try:
            try:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            if self.password:
                s.sendall(_encode(("AUTH", self.password)))
                _read_reply(c.fp)
            if self.db:
                s.sendall(_encode(("SELECT", self.db)))
                _read_reply(c.fp)
        except RedisError:
            c.close()                      # AUTH 被拒等：连接不可用，别留在池里
            raise
        except OSError as e:
            c.close()
            raise RedisError("Redis 握手失败 %s:%s（%s）"
                             % (self.host, self.port, type(e).__name__)) from e
        return c

    def _borrow(self) -> _Conn:
        with self._lock:
            while self._idle:
                return self._idle.pop()
        return self._connect()

    def _give(self, c: _Conn) -> None:
        with self._lock:
            if not self._closed and len(self._idle) < self.pool_size:
                self._idle.append(c)
                return
        c.close()

    # ---------------------------------------------------------------- 命令
    def cmd(self, *args, _retry: bool = True):
        if self._closed:
            raise RedisError("客户端已关闭")
        c = self._borrow()
        try:
            c.sock.sendall(_encode(args))
            v = _read_reply(c.fp)
        except RedisReplyError:
            # 服务端说"你这条命令有问题"，连接本身是好的，放回池里继续用
            self._give(c)
            raise
        except (OSError, ValueError, RedisError):
            # 连接级故障：这条连接的状态未知，绝不能放回池——
            # 池里混进一条"回复错位"的连接，之后所有请求都会读到别人的回复。
            c.close()
            # 重试一次：绝大多数连接级故障是"池里那条连接的对端已经不在了"
            #（Redis 重启、空闲被踢），换条新连接立刻就好。不重试的话，
            # 每次 Redis 重启都会让头几个请求白白失败一次，缓存状态还被翻成降级。
            # 代价：INCR 这类非幂等命令可能多计一次——本项目里 INCR 只用来数
            # 缓存命中次数，多算一次不影响任何判断。
            if _retry:
                return self.cmd(*args, _retry=False)
            raise RedisError("Redis 连接异常：%s:%s" % (self.host, self.port))
        self._give(c)
        return v

    def ping(self) -> bool:
        try:
            return self.cmd("PING") == "PONG"
        except RedisError:
            return False

    def get(self, key: str):
        return self.cmd("GET", key)

    def set(self, key: str, val, ex: int | None = None, px: int | None = None,
            nx: bool = False):
        a = ["SET", key, val]
        if ex:
            a += ["EX", int(ex)]
        elif px:
            a += ["PX", int(px)]
        if nx:
            a.append("NX")
        return self.cmd(*a)

    def delete(self, *keys) -> int:
        return int(self.cmd("DEL", *keys) or 0)

    def incr(self, key: str, by: int = 1) -> int:
        return int(self.cmd("INCRBY", key, int(by)) or 0)

    def mget(self, *keys) -> list:
        vals = self.cmd("MGET", *keys)
        return list(vals) if isinstance(vals, list) else [None] * len(keys)

    def exists(self, key: str) -> bool:
        return bool(int(self.cmd("EXISTS", key) or 0))

    def dbsize(self) -> int:
        return int(self.cmd("DBSIZE") or 0)

    def scan(self, match: str, count: int = 200) -> list[str]:
        out, cursor = [], "0"
        while True:
            cur, keys = self.cmd("SCAN", cursor, "MATCH", match, "COUNT", count)
            out += [k.decode("utf-8", "replace") if isinstance(k, bytes) else k
                    for k in keys or []]
            cursor = cur.decode() if isinstance(cur, bytes) else str(cur)
            if cursor == "0":
                return out

    def info(self, section: str = "") -> dict:
        raw = self.cmd("INFO", section) if section else self.cmd("INFO")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        out = {}
        for line in (raw or "").splitlines():
            if ":" in line and not line.startswith("#"):
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip()
        return out

    def close(self) -> None:
        with self._lock:
            self._closed = True
            idle, self._idle = self._idle, []
        for c in idle:
            c.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
