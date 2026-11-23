"""进程内限流（阶段 1）：问数配额 + 登录失败锁定。

为什么不用 Redis：单实例演示部署，引入外部依赖换不来收益；
接口按 `Limiter` 抽象，阶段 5 上多副本时换 Redis 实现即可，调用方不动。

两条曲线分开，因为它们的攻击面不同：
  · `ask`     —— 防成本滥用（每次问答都烧 token + 打满 Doris FE）；
  · `login`   —— 防密码爆破（按 IP + 用户名双维度计，撞库和定向爆破都盖住）。

滑动窗口而不是固定窗口：固定窗口在边界处能放过 2 倍流量
（59 秒来 20 次、01 秒再来 20 次），答辩演示时容易被一眼看穿。
"""
from __future__ import annotations

import threading
import time
from collections import deque


class Limiter:
    """滑动窗口计数器。线程安全——FastAPI 的 def 端点跑在线程池里。"""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str, limit: int, window_sec: float = 60.0) -> tuple[bool, int]:
        """返回 (是否放行, 本窗口剩余次数)。放行时才记账。"""
        if limit <= 0:                     # 0 = 不限（内部通道 / 关闭限流）
            return True, -1
        now = time.monotonic()
        cutoff = now - window_sec
        with self._lock:
            q = self._hits.get(key)
            if q is None:
                q = self._hits[key] = deque()
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= limit:
                return False, 0
            q.append(now)
            return True, limit - len(q)

    def remaining(self, key: str, limit: int, window_sec: float = 60.0) -> int:
        if limit <= 0:
            return -1
        cutoff = time.monotonic() - window_sec
        with self._lock:
            q = self._hits.get(key) or deque()
            return max(0, limit - sum(1 for t in q if t >= cutoff))

    def reset(self, key: str | None = None) -> None:
        """登录成功后清掉失败计数；测试里用 reset() 清场。"""
        with self._lock:
            if key is None:
                self._hits.clear()
            else:
                self._hits.pop(key, None)

    def retry_after(self, key: str, limit: int, window_sec: float = 60.0) -> int:
        """还要等几秒才能再试——放进 429 响应头，客户端不用猜。"""
        cutoff = time.monotonic() - window_sec
        with self._lock:
            q = self._hits.get(key) or deque()
            live = [t for t in q if t >= cutoff]
            if len(live) < limit:
                return 0
            return max(1, int(window_sec - (time.monotonic() - live[0])) + 1)


_LIMITER = Limiter()


def limiter() -> Limiter:
    return _LIMITER
