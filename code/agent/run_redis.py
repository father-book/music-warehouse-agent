"""启动 / 停止 / 体检项目自带的 Redis（third_party/redis-win64）。

为什么要在仓库里带二进制和这个脚本
----------------------------------
答案缓存的后端是 Redis，但 Redis 官方从 3.2 之后就不再发布 Windows 版，
GitHub 上只有微软的移植包。这意味着：谁把项目 clone 到一台干净的 Windows 上，
`redis-server` 一定不存在，缓存会**静默降级成本地字典**——
功能看着正常，跨实例共享其实没有生效。

所以「clone 下来就能跑」这件事，靠的不是文档里写一句"请先安装 Redis"，
而是把 2.4MB 的二进制、配置、启停脚本一起放进仓库。

用法
----
    python code/agent/run_redis.py            # 前台语义：没有就起，有就复用
    python code/agent/run_redis.py --status   # 只体检，不启动
    python code/agent/run_redis.py --stop     # 停掉（SHUTDOWN NOSAVE）

密码
----
配置文件里**不写密码**（那会随仓库公开）。密码从环境变量 `REDIS_PASSWORD`
（code/agent/.env，已 gitignore）读，有就作为 `--requirepass` 传给服务端，
没有就以「仅监听 127.0.0.1 的无密码模式」启动——本机开发够用，
真要暴露到网络，请自己加 TLS 或走内网。

数据与日志
----------
落在 `code/agent/.run/redis/`（已 gitignore）：dump.rdb 不会污染仓库。
配置里 `save ""` 关掉了持久化，这个目录通常只有一份日志。
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

AGENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = AGENT_DIR.parent.parent
REDIS_HOME = PROJECT_ROOT / "third_party" / "redis-win64"
RUN_DIR = AGENT_DIR / ".run" / "redis"
EXE = REDIS_HOME / ("redis-server.exe" if os.name == "nt" else "redis-server")
CONF = REDIS_HOME / "redis.conf"

sys.path.insert(0, str(AGENT_DIR))
import env  # noqa: E402  （顺带加载 .env，REDIS_PASSWORD 才拿得到）
import vault  # noqa: E402  （密钥统一从 vault 取，不直读 os.environ）
import redisclient  # noqa: E402


def _cfg() -> dict:
    """连接参数与 answercache 保持一致：config.yaml 里 cache.redis 为准。"""
    host, port, pw = "127.0.0.1", 6379, (vault.get("REDIS_PASSWORD").reveal() or None)
    try:
        from conf import load_config
        r = ((load_config().get("cache") or {}).get("redis") or {})
        host = os.getenv("REDIS_HOST") or r.get("host") or host
        port = int(os.getenv("REDIS_PORT") or r.get("port") or port)
    except Exception:
        pass                      # 配置读不到就用默认值，脚本不该因此起不来
    return {"host": host, "port": port, "password": pw}


def _port_open(host: str, port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def probe(c: dict | None = None) -> dict:
    """体检：端口通不通、要不要密码、版本多少、key 数量。"""
    c = c or _cfg()
    out = {"running": False, "host": c["host"], "port": c["port"],
           "auth": None, "version": "", "keys": -1, "error": ""}
    if not _port_open(c["host"], c["port"]):
        return out
    out["running"] = True
    # 先不带密码试：服务端没设密码时，发 AUTH 反而会被它当错误回。
    for pw in ([None, c["password"]] if c["password"] else [None]):
        try:
            r = redisclient.Redis(host=c["host"], port=c["port"], password=pw,
                                  timeout=1.0, pool=1)
            if r.ping():
                out["auth"] = bool(pw)
                try:
                    out["version"] = r.info("server").get("redis_version", "")
                    out["keys"] = r.dbsize()
                except Exception as e:          # NOAUTH：能连但不让看，说明有密码
                    out["error"] = str(e)[:80]
                r.close()
                return out
            r.close()
        except redisclient.RedisReplyError as e:
            if "NOAUTH" in str(e).upper() or "AUTH" in str(e).upper():
                out["error"] = "需要密码（REDIS_PASSWORD 未设置或不匹配）"
                out["auth"] = True
        except Exception as e:
            out["error"] = str(e)[:80]
    return out


def start(quiet: bool = False) -> tuple[bool, str]:
    """确保有一个可用的 Redis。已经有人占了端口就复用，不重复起。"""
    c = _cfg()
    st = probe(c)
    if st["running"] and not st["error"]:
        return True, "已有 Redis 在 %s:%s 运行（v%s，%s 个 key），直接复用" % (
            c["host"], c["port"], st["version"] or "?", st["keys"])
    if st["running"]:
        return False, "端口 %s 已被占用但不是可用的 Redis：%s" % (c["port"], st["error"])

    if not EXE.exists():
        return False, ("找不到 %s。仓库里应该带了这个文件；"
                       "若从别处下载，请解压到该目录。" % EXE)
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUN_DIR / "redis.log"

    args = [str(EXE), str(CONF)]
    if c["password"]:
        args += ["--requirepass", c["password"]]
    # 有些人机器上 6379 被别的 Redis 占了但要连本项目的：允许临时改端口
    if int(c["port"]) != 6379:
        args += ["--port", str(int(c["port"]))]

    log = open(log_path, "ab", buffering=0)
    log.write(("\n==== %s 启动 %s:%s ====\n"
               % (time.strftime("%Y-%m-%d %H:%M:%S"), c["host"], c["port"])).encode())
    kw = {}
    if os.name == "nt":
        # 别弹黑窗口，也别继承我们的 Ctrl+C：webapp 退出时不该顺手带走 Redis
        kw["creationflags"] = 0x00000008 | 0x00000200   # DETACHED_PROCESS|CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(args, cwd=str(RUN_DIR), stdout=log, stderr=subprocess.STDOUT,
                     stdin=subprocess.DEVNULL, close_fds=True, **kw)

    for _ in range(60):                       # 最多等 6 秒
        time.sleep(0.1)
        st = probe(c)
        if st["running"] and not st["error"]:
            if not quiet:
                print("Redis 已启动：%s:%s（v%s，日志 %s）"
                      % (c["host"], c["port"], st["version"] or "?", log_path))
            return True, "已启动 v%s" % (st["version"] or "?")
        if st["error"]:
            break
    tail = ""
    try:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-400:]
    except Exception:
        pass
    return False, "启动失败：%s\n%s" % (st["error"] or "进程未监听端口", tail)


def stop() -> tuple[bool, str]:
    c = _cfg()
    st = probe(c)
    if not st["running"]:
        return True, "本来就没在跑"
    for pw in ([c["password"], None] if c["password"] else [None]):
        try:
            r = redisclient.Redis(host=c["host"], port=c["port"], password=pw,
                                  timeout=1.0, pool=1)
            try:
                r.cmd("SHUTDOWN", "NOSAVE")     # 正常的 Redis 会直接断连、不回包
            except Exception:
                pass
            r.close()
        except Exception:
            continue
        for _ in range(30):
            time.sleep(0.1)
            if not _port_open(c["host"], c["port"]):
                return True, "已停止"
    if not _port_open(c["host"], c["port"]):
        return True, "已停止"
    return False, "停不掉：可能不是本项目的 Redis（无权限执行 SHUTDOWN）"


def ensure_running(cfg: dict | None = None) -> tuple[bool, str]:
    """给 webapp 启动时调用：能自动拉起就拉，拉不起来只返回原因，不抛异常。"""
    return start(quiet=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="项目自带 Redis 的启停脚本")
    ap.add_argument("--status", action="store_true", help="只体检，不启动")
    ap.add_argument("--stop", action="store_true", help="停止 Redis")
    a = ap.parse_args()

    if a.status:
        st = probe()
        print("Redis  %s:%s  %s" % (st["host"], st["port"],
                                    "运行中" if st["running"] else "未运行"))
        if st["running"]:
            print("  版本  %s" % (st["version"] or "?"))
            print("  鉴权  %s" % ("已启用" if st["auth"] else "无密码（仅本机）"))
            print("  key   %s" % st["keys"])
            if st["error"]:
                print("  异常  %s" % st["error"])
        print("  二进制 %s %s" % (EXE, "存在" if EXE.exists() else "缺失"))
        return 0 if st["running"] and not st["error"] else 1

    ok, msg = (stop() if a.stop else start())
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
