"""护栏调用封装。

护栏是唯一的规则实现，Agent 侧不再实现第二份——规则改了只改一个地方。

调用方式有两种，都指向**同一个** `agent/guards/guard.py`：

  · 常驻工作进程（默认）：`guard.py --serve` 起来后，一行 JSON 进、一行 JSON 出。
    sqlglot 的 import 成本（约 0.2s/次）只付一次，之后每条 SQL 只剩解析成本。
    并发压测（AC-16）里这是最大的一块：三路并发时 subprocess 逐次调用
    把 P95 顶到了 3s 以上，而护栏本身只要 30ms。
  · 一次性 subprocess（兜底）：进程池满了、工作进程挂了、或者配置显式关掉
    常驻模式时走这条。慢，但行为与以前完全一致——降级不能改变判定结果。

一条硬规则：**护栏没给出结果时不能当通过**。曾经因为 stdout 为空被当成
"没有 findings"而静默放行过 16 条 SQL，所以这里所有异常路径都返回 REJECT。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import atexit
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

from conf import load_config

# ---------------------------------------------------------------- 常驻进程

class _Worker:
    """一个常驻 guard 进程。读用独立线程，避免 Windows 上 select 不支持管道。"""

    def __init__(self, script: Path, mode: str):
        env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
        self.p = subprocess.Popen(
            [sys.executable, str(script), "--serve", "--mode", mode],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", errors="replace",
            bufsize=1, env=env)
        self.q: queue.Queue = queue.Queue()
        self.dead = False
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        try:
            for line in self.p.stdout:
                self.q.put(line)
        except (ValueError, OSError):
            pass
        finally:
            self.q.put(None)        # EOF 哨兵：调用方据此判死

    def call(self, payload: dict, timeout: float) -> list[dict]:
        if self.dead or self.p.poll() is not None:
            raise RuntimeError("guard worker 已退出")
        self.p.stdin.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self.p.stdin.flush()
        line = self.q.get(timeout=timeout)
        if line is None:
            raise RuntimeError("guard worker 管道关闭")
        out = json.loads(line)
        if out.get("error"):
            raise RuntimeError(out["error"])
        return out.get("findings") or []

    def kill(self):
        self.dead = True
        try:
            self.p.kill()
        except OSError:
            pass


class _Pool:
    def __init__(self, script: Path, mode: str, size: int):
        self.script, self.mode, self.size = script, mode, size
        self.idle: queue.Queue = queue.Queue()
        self.made = 0
        self.lock = threading.Lock()
        for _ in range(size):
            self.idle.put(self._spawn())

    def _spawn(self) -> _Worker:
        with self.lock:
            self.made += 1
        return _Worker(self.script, self.mode)

    def borrow(self, timeout: float) -> _Worker | None:
        try:
            w = self.idle.get(timeout=timeout)
        except queue.Empty:
            return None
        if w.dead:
            w = self._spawn()
        return w

    def give_back(self, w: _Worker, ok: bool):
        if ok and not w.dead:
            self.idle.put(w)
        else:
            w.kill()
            self.idle.put(self._spawn())

    def close(self):
        while True:
            try:
                self.idle.get_nowait().kill()
            except queue.Empty:
                break


_POOLS: dict[tuple[str, str], _Pool] = {}
_POOL_LOCK = threading.Lock()


def _pool_for(script: Path, mode: str, size: int) -> _Pool:
    key = (str(script), mode)
    with _POOL_LOCK:
        p = _POOLS.get(key)
        if p is None:
            p = _Pool(script, mode, max(1, size))
            _POOLS[key] = p
        return p


@atexit.register
def _shutdown():
    for p in list(_POOLS.values()):
        p.close()


# ---------------------------------------------------------------- 一次性调用

def _oneshot(script: Path, sql: str, mode: str, context: dict | None,
             decision: dict | None, question: str) -> tuple[list[dict], str]:
    """降级路径：与阶段 1 完全一致的一次性 subprocess。"""
    # 走临时文件而不是命令行参数：SQL 可能上千字符，Windows 命令行有长度上限
    fd, tmp = tempfile.mkstemp(suffix=".sql", prefix="guard_")
    ctx_tmp = None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(sql)
        cmd = [sys.executable, str(script), "--json", "--mode", mode]
        if context is not None or decision is not None:
            pol = decision.to_dict() if hasattr(decision, "to_dict") else (decision or {})
            c = {
                **(context or {}),
                "question": question or (context or {}).get("question") or "",
                "policy": pol,
            }
            cfd, ctx_tmp = tempfile.mkstemp(suffix=".json", prefix="guard_ctx_")
            with os.fdopen(cfd, "w", encoding="utf-8") as cf:
                json.dump(c, cf, ensure_ascii=False)
            cmd += ["--context", ctx_tmp]
        cmd.append(tmp)
        proc = subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
    finally:
        for p in (tmp, ctx_tmp):
            if p:
                try:
                    os.unlink(p)
                except OSError:
                    pass

    raw = (proc.stdout or "").strip()
    if not raw:
        return [], ("护栏未产出结果（退出码 %d）：%s"
                    % (proc.returncode, (proc.stderr or "")[-300:]))
    try:
        return json.loads(raw), ""
    except json.JSONDecodeError:
        return [], "护栏输出不是合法 JSON：%s" % raw[:200]


# ---------------------------------------------------------------- 对外接口

def check(sql: str, cfg: dict | None = None, *, context: dict | None = None,
          decision: dict | None = None,
          question: str = "") -> tuple[str, list[dict], str]:
    """跑 27 条护栏（R21~R23 是阶段 1 新增的安全规则，R24 与成本闸配合；
    R25 是口径机检，规则在 metrics.yaml.caliber_guard；
    R26 是实时快照的时间窗与混查禁令，白名单在 realtime.yaml；
    R27 是 JOIN 维表必须按主键，防止用维度属性关联导致整表放大）。

    返回 (verdict, findings, stderr)。
      verdict: pass / warn / reject
      findings: [{level, rule, message, hint, fix}]

    `context` / `decision` 只用于把身份与策略摘要传给 guard.py 的
    `--context`；CI 的 test_cases.sh 直接调 guard.py 时不传，规则退化为
    "只拦元数据探测/PII 列/SQL 里的注入"，保持旧用例语义。
    """
    cfg = cfg or load_config()
    g = cfg["guard"]
    script: Path = cfg["_paths"]["guard_script"]

    if not sql.strip():
        return "reject", [{
            "level": "REJECT", "rule": "EMPTY",
            "message": "未生成 SQL", "hint": "", "fix": {},
        }], ""

    q = question or (context or {}).get("question") or ""
    payload = {
        "sql": sql, "mode": g["mode"],
        "context": {
            **(context or {}),
            "question": q,
            "policy": (decision.to_dict() if hasattr(decision, "to_dict")
                       else (decision or {})),
        },
    }

    findings: list[dict] = []
    err = ""
    used_pool = False

    if g.get("serve", True):
        try:
            pool = _pool_for(script, g["mode"], int(g.get("workers", 3)))
            w = pool.borrow(float(g.get("acquire_timeout", 10)))
            if w is not None:
                used_pool = True
                try:
                    findings = w.call(payload, float(g.get("call_timeout", 60)))
                    pool.give_back(w, ok=True)
                except Exception as e:      # 进程挂了/超时：换一个，本请求降级
                    pool.give_back(w, ok=False)
                    used_pool = False
                    err = "常驻护栏不可用：%s" % e
        except Exception as e:              # 连池都建不起来（比如脚本路径不对）
            used_pool = False
            err = "常驻护栏启动失败：%s" % e

    if not used_pool:
        findings, oerr = _oneshot(script, sql, g["mode"], context, decision, q)
        if oerr:
            # 两条路都没结果：绝不能当通过
            return "reject", [{
                "level": "REJECT", "rule": "GUARD_CRASH",
                "message": oerr, "hint": "这条 SQL 不放行；先确认 agent/guards/guard.py 能独立跑通",
                "fix": {},
            }], err or oerr

    levels = {f.get("level") for f in findings}
    if "REJECT" in levels or (g.get("strict_warn") and "WARN" in levels):
        verdict = "reject"
    elif "WARN" in levels:
        verdict = "warn"
    else:
        verdict = "pass"
    return verdict, findings, err


def fix_hints(findings: list[dict]) -> list[dict]:
    """只挑出可机械执行的那类 fix（{op, target, replacement}）。"""
    out = []
    for f in findings:
        fx = f.get("fix") or {}
        if fx.get("op") in ("replace", "restructure") and fx.get("target"):
            out.append({
                "rule": f.get("rule"),
                "op": fx["op"],
                "target": fx["target"],
                "replacement": fx.get("replacement"),
                "message": f.get("message"),
            })
    return out


def warm(cfg: dict | None = None, n: int | None = None) -> int:
    """预热进程池（服务启动时调用），把首问的 0.2s×N 提前付掉。"""
    cfg = cfg or load_config()
    g = cfg["guard"]
    if not g.get("serve", True):
        return 0
    size = n or int(g.get("workers", 3))
    _pool_for(cfg["_paths"]["guard_script"], g["mode"], size)
    return size


if __name__ == "__main__":
    sql = sys.argv[1] if len(sys.argv) > 1 else "SELECT 1"
    v, fs, _ = check(sql)
    print(v, json.dumps(fs, ensure_ascii=False, indent=1))
