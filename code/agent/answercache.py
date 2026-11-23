"""答案缓存（AC-15）：同一个问题、同一个身份、同一份语义层，第二次问 0 token。

三条不能省的安全边界
--------------------
1. **key 必须含 policy_hash**。两个同为数据分析师但地域范围不同的用户，
   可见行不同。若 key 只按问题算，受限用户会拿到全量结果——这是越权，
   而且是"缓存顺带把权限绕了"这种最难查的越权。
2. **key 必须含语义层指纹**。改了 metrics.yaml 的口径后，旧答案必须失效，
   否则改完还是旧数字，人会以为是改动没生效。
3. **失败与安全拦截不进缓存**。被 R21/R22/R23 拦下的请求缓存下来，
   等于把一次拒绝变成长期拒绝；执行报错缓存下来，等于把偶发故障变成常态。

另外：`source=eval` 不读不写缓存。评测要测的是"每次真跑"的成本与稳定性，
命中缓存会让 token 统计变成 0，成本曲线直接失真。

阶段 5：跨实例共享（AC-17）
---------------------------
进程内字典在多实例下是**假的缓存**：三个 worker 各自命中率 1/3，重启即清零，
"缓存生效了"只对当前进程成立。这里拆成后端：

    local  进程内 OrderedDict + TTL + LRU（默认，零依赖，单机演示够用）
    redis  跨进程/跨机器共享，key 带前缀与 schema 版本，值 gzip 压缩

三条工程约束，比"换成 Redis"本身重要：

· **降级必须可见**。Redis 连不上就回落 local，但 `/api/cache` 会显式报
  `degraded=true` 与原因。静默降级会让"以为共享了，其实每个实例各存一份"——
  这种问题在演示时完全看不出来，在生产上是缓存穿透。
· **单飞要跨实例**。进程内 `threading.Lock` 只能压住同一进程的并发；多实例
  同时冷启动仍会各调一次模型。这里用 `SET lock NX PX` 抢锁，没抢到的轮询等
  结果，**超时后自己算**——宁可重复算一次，也不能因为某个实例崩了把请求挂死。
· **schema 变更必须让旧值失效**。`_KEEP` 增删字段时缓存里的 JSON 会缺键，
  回放出来就是"答案少了对比段"这种诡异 bug。`SCHEMA_V` 进 key，改字段就换命名空间。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import gzip
import hashlib
import json
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager

import vault

# 进缓存的字段白名单。只存"答案本身 + 复算依据"，
# 不存 ctx / trace_id / policy_decision —— 这些每次请求都要重新生成。
_KEEP = (
    "question", "answer", "sql", "applied_sql", "result",
    "metrics", "metric_hits", "tables", "dimensions", "dimension_cautions",
    "time_window", "time_start", "time_end", "freshness",
    "verdict", "findings", "attempts", "shape", "shape_issues",
    "shape_retries", "shape_unresolved", "compare_result",
    "refused", "refuse_reason", "meta_intent", "meta_answer",
    "unsupported", "lane", "followup",
)

# 白名单结构版本。**改 _KEEP 就要 +1**，否则老值回放会缺字段。
# v4：compare_result 里多了 consistency（口径一致性结论）。旧值里没有它，
# 回放出来就是"这个差值按哪套规则算的"无处可查——而那套规则刚刚变严了。
# v5：加了 followup（多轮追问沿用了上一轮的什么）。旧值里没有它，
# 缓存命中的追问答案就丢了"指标/时间是沿用上一轮的"这个说明。
SCHEMA_V = 5

# 放锁要认主人：锁过期后可能已被别人抢到，直接 DEL 会删掉别人的锁。
_UNLOCK_LUA = ("if redis.call('get',KEYS[1])==ARGV[1] "
               "then return redis.call('del',KEYS[1]) else return 0 end")

_PUNCT = re.compile(r"[\s，。？！、；：\"'“”‘’（）()\[\]【】!?.,;:]+")
_FP_LOCK = threading.Lock()
_FP: str | None = None


def _norm(q: str) -> str:
    """归一化：去掉全部空白与中英文标点，英文小写。

    「11-08 的播放量？」和「11-08的播放量」在业务上是同一个问题，
    但对字符串是两个。归一化只做这一层，不做同义词改写——
    改写属于语义层的事，缓存不该悄悄改变问句。
    """
    return _PUNCT.sub("", (q or "")).lower()


def semantic_fingerprint(cfg: dict) -> str:
    """语义层 + 配置的指纹。文件一改，旧缓存自动失效。"""
    global _FP
    with _FP_LOCK:
        if _FP:
            return _FP
        paths = cfg.get("_paths") or {}
        items = []
        for k in sorted(paths):
            p = str(paths[k])
            if not os.path.exists(p):
                continue
            st = os.stat(p)
            items.append([k, int(st.st_mtime), st.st_size])
        _FP = hashlib.sha256(
            json.dumps(items, sort_keys=True).encode("utf-8")).hexdigest()[:16]
        return _FP


def features_fingerprint(cfg: dict) -> str:
    """影响生成结果的能力开关指纹。

    为什么必须进缓存键：缓存原本只看「问句 × 策略 × 语义层 × 模型参数」。
    于是打开 CoT / few-shot / Planning / 混合检索之后，同一个问题仍然命中
    关闭时那份答案——A/B 会得出"开了等于没开"的假结论（实测踩到过：开了
    Planning，节点确实进链了，但返回的 state 里连 plan 字段都没有，因为整条
    结果是从旧缓存回放的）。

    代价是这批开关一改，旧缓存全部失效。这是对的：它们确实会改变答案。
    """
    pc = cfg.get("prompt") or {}
    rc = cfg.get("retrieval") or {}
    payload = {
        "cot": bool(pc.get("cot")),
        "fewshot": bool((pc.get("fewshot") or {}).get("enabled")),
        "fewshot_top": (pc.get("fewshot") or {}).get("top"),
        "fewshot_min": (pc.get("fewshot") or {}).get("min_score"),
        "sc": bool((pc.get("self_consistency") or {}).get("enabled")),
        "sc_n": (pc.get("self_consistency") or {}).get("samples"),
        "plan": bool((pc.get("plan") or {}).get("enabled")),
        "react": bool((pc.get("react") or {}).get("enabled")),
        "backend": rc.get("backend"),
        "hybrid": bool(rc.get("hybrid_enabled")),
        "dense_backend": rc.get("dense_backend"),
        "neural_model_dir": rc.get("neural_model_dir"),
        "alpha": rc.get("dense_alpha"),
        "dense_dim": rc.get("dense_dim"),
        "failcase": bool((cfg.get("failcase") or {}).get("enabled")),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str)
                          .encode("utf-8")).hexdigest()[:12]


def make_key(question: str, cfg: dict, decision, extra: str = "") -> str:
    """缓存键：归一化问句 × 策略版本 × 语义层指纹 × 生成器配置 × 能力开关。"""
    g = cfg.get("generator") or {}
    off = (cfg.get("lane") or {}).get("offline") or {}
    payload = {
        "q": _norm(question),
        "policy": getattr(decision, "policy_hash", "") or "",
        "sem": semantic_fingerprint(cfg),
        "gen": [g.get("mode"), g.get("model"), g.get("temperature")],
        "guard": [(cfg.get("guard") or {}).get("mode"),
                  (cfg.get("guard") or {}).get("strict_warn")],
        "data_until": off.get("data_until"),
        "clock": (cfg.get("clock") or {}).get("fixed_today"),
        "feat": features_fingerprint(cfg),
        "extra": extra,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def in_scope(ctx) -> bool:
    """只有真实用户入口（web / api）参与缓存。

    读和写都必须过这一关。只卡写不卡读的话，评测会命中网页留下的答案：
    token 变 0、成本曲线失真，更要紧的是——**评测不再真的生成 SQL**，
    它会开始给一个坏掉的系统打满分。
    """
    return (getattr(ctx, "source", "") or "") in ("web", "api")


def cacheable(state: dict, ctx) -> bool:
    """只有「真跑通且答案成形」的结果值得缓存。"""
    if not in_scope(ctx):
        return False                 # eval / cli 不进缓存：会污染成本统计
    if state.get("security_block") or state.get("cache_hit"):
        return False
    if state.get("unsupported"):
        return False                 # 能力缺口随实现变化，缓存它会掩盖进展
    if state.get("generate_error"):
        return False
    # 2026-11-14：形态返工用尽上限、答案里如实写着「形状可能不符」的结果不能缓存。
    # 否则同一个问题在 TTL（6h）内固定拿到那个"没解决"的版本，用户会以为系统一直这样答，
    # 而后来修好的版本永远出不来。SNAPSHOT_KEYS 里本来就存了 shape_unresolved，
    # 说明这条状态是有意义的，只是之前没参与「值不值得缓存」的判断。
    if state.get("shape_unresolved"):
        return False
    res = state.get("result") or {}
    if res.get("error"):
        return False
    if not (state.get("answer") or "").strip():
        return False
    if state.get("meta_intent"):
        return True
    return bool((state.get("sql") or "").strip())


# ---------------------------------------------------------------- 序列化
# 大答案（榜单 + 复算 SQL）JSON 后常见 20~60KB。gzip 一般能压到 1/5，
# 一次网络往返省下的时间比解压成本高。小值不压——省下的字节还不够 magic 头。
def _dump(val: dict) -> bytes:
    raw = json.dumps(val, ensure_ascii=False, default=str).encode("utf-8")
    if len(raw) >= 1024:
        z = gzip.compress(raw, 5)
        if len(z) < len(raw) * 0.9:
            return b"\x1fZ" + z
    return b"J" + raw


def _load(raw: bytes) -> dict | None:
    try:
        if raw[:2] == b"\x1fZ":
            return json.loads(gzip.decompress(raw[2:]))
        if raw[:1] == b"J":
            return json.loads(raw[1:])
        return json.loads(raw)              # 兼容无前缀的旧值
    except Exception:
        return None                          # 解不开就当作未命中，别让坏值反复报错


# ---------------------------------------------------------------- 进程内后端
class LocalCache:
    """带 TTL + LRU 的进程内缓存。线程安全，读路径无锁竞争（GIL 保证 dict 原子）。"""

    name = "local"

    def __init__(self, ttl_sec: float = 21600, maxsize: int = 512):
        self.ttl = float(ttl_sec)
        self.maxsize = int(maxsize)
        self._d: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self._lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._stat = {"hit": 0, "miss": 0, "put": 0, "evict": 0, "expired": 0}

    # ---- 读写 ----
    def get(self, key: str) -> dict | None:
        with self._lock:
            item = self._d.get(key)
            if not item:
                self._stat["miss"] += 1
                return None
            born, val = item
            if time.time() - born > self.ttl:
                self._d.pop(key, None)
                self._stat["expired"] += 1
                self._stat["miss"] += 1
                return None
            self._d.move_to_end(key)
            self._stat["hit"] += 1
        return json.loads(json.dumps(val))      # 深拷贝：调用方改了不能污染缓存

    def put(self, key: str, state: dict) -> None:
        val = {k: state.get(k) for k in _KEEP if k in state}
        with self._lock:
            self._d[key] = (time.time(), val)
            self._d.move_to_end(key)
            self._stat["put"] += 1
            while len(self._d) > self.maxsize:
                self._d.popitem(last=False)
                self._stat["evict"] += 1

    def clear(self) -> None:
        with self._lock:
            self._d.clear()

    # ---- 单飞：同一问题并发只跑一次 ----
    def key_lock(self, key: str) -> threading.Lock:
        with self._lock:
            lk = self._locks.get(key)
            if lk is None:
                if len(self._locks) > 4096:      # 兜底，正常远达不到
                    self._locks.clear()
                lk = threading.Lock()
                self._locks[key] = lk
            return lk

    @contextmanager
    def single_flight(self, key: str):
        with self.key_lock(key):
            yield

    def stats(self) -> dict:
        with self._lock:
            s = dict(self._stat)
            n = s["hit"] + s["miss"]
            s["size"] = len(self._d)
            s["maxsize"] = self.maxsize
            s["ttl_sec"] = self.ttl
            s["hit_rate"] = round(100.0 * s["hit"] / n, 1) if n else 0.0
            s["backend"] = "local"
            s["degraded"] = False
            return s


# 老名字，别处还有引用
Cache = LocalCache


# ---------------------------------------------------------------- Redis 后端
class RedisCache:
    """跨实例共享。值 gzip + JSON，TTL 交给 Redis，容量交给 maxmemory 策略。"""

    name = "redis"

    def __init__(self, ttl_sec: float, maxsize: int, rcfg: dict):
        from redisclient import Redis, RedisError  # 延迟导入：local 后端不需要它

        self._RedisError = RedisError
        self.ttl = int(ttl_sec)
        self.prefix = str(rcfg.get("prefix") or "agc")
        self.env = str(rcfg.get("env") or "dev")
        self.wait_sec = float(rcfg.get("single_flight_wait_sec", 25))
        self.poll = max(0.02, float(rcfg.get("single_flight_poll_ms", 100)) / 1000.0)
        self.lock_ttl_ms = int(rcfg.get("lock_ttl_ms", 40000))
        pw = rcfg.get("password") or ""
        penv = rcfg.get("password_env") or ""
        # 走 vault 而不是 os.getenv：这里原来是一处"直读"，密钥从哪来
        # 除了它自己没人知道，/api/secrets 也看不见它取没取过。
        if penv and not pw:
            pw = vault.get(penv).reveal()
        # 环境变量优先于配置文件：同一份 config.yaml 要能在「自带 Redis 的开发机」
        # 和「别人机器上 6379 已被占用 / 走远程 Redis」两种场景下都用，不必改文件。
        # 配置文件是默认值，环境变量是部署时的覆盖层——这个优先级不能反。
        host = os.getenv("REDIS_HOST") or rcfg.get("host", "127.0.0.1")
        port = int(os.getenv("REDIS_PORT") or rcfg.get("port", 6379))
        db = int(os.getenv("REDIS_DB") or rcfg.get("db", 0))
        self.addr = "%s:%s/%s" % (host, port, db)
        self.r = Redis(
            host=host, port=port,
            password=pw or None, db=db,
            timeout=float(rcfg.get("timeout", 2.0)),
            pool=int(rcfg.get("pool", 4)),
        )
        # 降级容器：Redis 抖动时服务不中断，读写在本地顶一下。
        # 不这样做的话，缓存故障会变成"问数失败"——缓存是加速器，不该成为单点。
        self._fallback = LocalCache(ttl_sec, maxsize)
        self._lock = threading.Lock()
        self._down = False
        self._reason = ""
        self._retry_at = 0.0
        self._stat = {"hit": 0, "miss": 0, "put": 0, "evict": 0,
                      "error": 0, "waited": 0, "owner": 0}

    # ---- key 布局 ----
    def _key(self, key: str) -> str:
        return "%s:%s:q:%s:v%d" % (self.prefix, self.env, key, SCHEMA_V)

    def _lock_key(self, key: str) -> str:
        return "%s:%s:lock:%s" % (self.prefix, self.env, key)

    def _skey(self, name: str) -> str:
        return "%s:%s:stat:%s" % (self.prefix, self.env, name)

    def _bump(self, name: str) -> None:
        """全局计数器。多实例下，"命中率"必须跨实例聚合。

        只用本地计数的话：8000 生成、8001 命中，8000 那边永远显示 0%——
        运维看到的是"缓存没生效"，其实是生效在别的实例上。
        """
        try:
            self.r.incr(self._skey(name))
        except self._RedisError:
            pass

    # ---- 故障标记 ----
    def _note_ok(self) -> None:
        if self._down:
            self._down = False
            self._reason = ""

    def _note_err(self, e: Exception) -> None:
        self._down = True
        self._reason = str(e)[:160]
        self._retry_at = time.time() + 30.0     # 30 秒后再试，避免每条请求都去撞墙
        self._stat["error"] += 1

    def ping(self) -> bool:
        return self.r.ping()

    # ---- 读写 ----
    def get(self, key: str) -> dict | None:
        if self._down and time.time() < self._retry_at:
            return self._fallback.get(key)
        try:
            raw = self.r.get(self._key(key))
            self._note_ok()
        except self._RedisError as e:
            self._note_err(e)
            return self._fallback.get(key)
        if raw is None:
            self._stat["miss"] += 1
            self._bump("miss")
            return None
        val = _load(raw)
        if val is None:
            # 解不开（schema 变更 / 值损坏）→ 删掉，当未命中。留着会每次都白读一遍。
            try:
                self.r.delete(self._key(key))
            except self._RedisError:
                pass
            self._stat["miss"] += 1
            self._bump("miss")
            return None
        self._stat["hit"] += 1
        self._bump("hit")
        return val

    def put(self, key: str, state: dict) -> None:
        val = {k: state.get(k) for k in _KEEP if k in state}
        self._fallback.put(key, state)
        if self._down and time.time() < self._retry_at:
            return
        try:
            self.r.set(self._key(key), _dump(val), ex=self.ttl)
            self._note_ok()
            self._stat["put"] += 1
            self._bump("put")
        except self._RedisError as e:
            self._note_err(e)

    def clear(self) -> None:
        self._fallback.clear()
        try:
            for pat in (self._key("*"), "%s:%s:lock:*" % (self.prefix, self.env)):
                keys = self.r.scan(pat)
                for i in range(0, len(keys), 200):
                    self.r.delete(*keys[i:i + 200])
        except self._RedisError as e:
            self._note_err(e)

    # ---- 跨实例单飞 ----
    @contextmanager
    def single_flight(self, key: str):
        if self._down and time.time() < self._retry_at:
            with self._fallback.single_flight(key):
                yield
            return

        lk = self._lock_key(key)
        token = uuid.uuid4().hex
        try:
            got = self.r.set(lk, token, px=self.lock_ttl_ms, nx=True) == "OK"
            self._note_ok()
        except self._RedisError as e:
            # 锁服务不可用：不加锁直接跑。用缓存不该因为抢不到锁就报错。
            self._note_err(e)
            yield
            return

        if not got:
            # 别人在算——等结果。等到锁释放（owner 先 put 再解锁，顺序保证
            # follower 醒来时数据已在），或超时后自己算。
            self._stat["waited"] += 1
            deadline = time.time() + self.wait_sec
            while time.time() < deadline:
                try:
                    if not self.r.exists(lk):
                        break
                except self._RedisError as e:
                    self._note_err(e)
                    break
                time.sleep(self.poll)
            yield
            return

        self._stat["owner"] += 1
        try:
            yield
        finally:
            try:
                self.r.cmd("EVAL", _UNLOCK_LUA, 1, lk, token)
            except self._RedisError:
                pass                         # 放不掉就让它过期，不能影响主流程

    def stats(self) -> dict:
        s = dict(self._stat)
        s["backend"] = "redis"
        s["addr"] = self.addr          # 连的是哪个 Redis，排障第一问
        s["degraded"] = self._down
        s["reason"] = self._reason
        s["ttl_sec"] = self.ttl
        s["wait_sec"] = self.wait_sec
        try:
            # 对外报的是**跨实例**的命中率——这才是"缓存到底省了多少"的答案。
            h, m, p = self.r.mget(self._skey("hit"), self._skey("miss"),
                                  self._skey("put"))
            s["hit"], s["miss"], s["put"] = int(h or 0), int(m or 0), int(p or 0)
            s["local_hit"], s["local_miss"] = self._stat["hit"], self._stat["miss"]
            n = s["hit"] + s["miss"]
            s["hit_rate"] = round(100.0 * s["hit"] / n, 1) if n else 0.0
            s["size"] = self.r.dbsize()      # 整个 db 的 key 数，含锁与计数器
            s["redis_version"] = self.r.info("server").get("redis_version", "")
            self._note_ok()
        except self._RedisError as e:
            self._note_err(e)
            n = s["hit"] + s["miss"]
            s["hit_rate"] = round(100.0 * s["hit"] / n, 1) if n else 0.0
            s["size"] = -1
        return s


# ---------------------------------------------------------------- 工厂
_ONE = None
_ONE_LOCK = threading.Lock()
_INIT_WARNING = ""


def get_cache(cfg: dict | None = None):
    """全局单例。TTL / 容量 / 后端来自 config 的 `cache` 段。"""
    global _ONE, _INIT_WARNING
    with _ONE_LOCK:
        if _ONE is None:
            c = (cfg or {}).get("cache") or {}
            ttl = float(c.get("ttl_sec", 21600))
            size = int(c.get("maxsize", 512))
            backend = str(c.get("backend") or "local").lower()
            if backend == "redis":
                rcfg = c.get("redis") or {}
                try:
                    rc = RedisCache(ttl, size, rcfg)
                    if not rc.ping():
                        raise RuntimeError("PING 无响应")
                    _ONE = rc
                except Exception as e:       # 连不上就回落，但把原因留着给人看
                    _INIT_WARNING = "Redis 不可用（%s: %s），已回落到进程内缓存" % (
                        type(e).__name__, str(e)[:120])
                    _ONE = LocalCache(ttl, size)
            else:
                _ONE = LocalCache(ttl, size)
        return _ONE


def reset_cache() -> None:
    """测试用：清掉单例（换后端 / 换库）。"""
    global _ONE, _INIT_WARNING
    with _ONE_LOCK:
        _ONE = None
        _INIT_WARNING = ""


def stats() -> dict:
    if not _ONE:
        return {"size": 0, "hit": 0, "miss": 0, "hit_rate": 0.0,
                "backend": "none"}
    s = _ONE.stats()
    if _INIT_WARNING:
        s["init_warning"] = _INIT_WARNING
    return s
