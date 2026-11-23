"""run_redis.py 自测：仓库自带 Redis 的启停与配置是否真的生效。

为什么这段值得测
----------------
它是"跨实例缓存能不能用"的唯一前置。一旦 `third_party/redis-win64/redis.conf`
里某个指令写错（比如 3.x 不认某个新参数），redis-server 会**启动失败但很快退出**，
而 answercache 的处理是"连不上就回落 local"——于是整套系统照常运行，
只有"跨实例共享"这条验收线悄悄失效了。演示时完全看不出来。

所以这里不测"函数返回了什么"，而是测**真实端口上的行为**：
起得来、连得上、配置项确实是我们要的、重复调用不重复起、停得掉。

用法：python code/agent/tests/selftest_redis_boot.py
注：会短暂停掉本机 6379 上的 Redis，**结束时重新拉起**——
一个把开发环境弄停的自测，下次就没人愿意跑了。
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import env  # noqa: F401
import redisclient
import run_redis as R

PASS = FAIL = 0
FAILED: list[str] = []


def ck(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s  %s" % (name, extra))


def _gw() -> redisclient.Redis:
    c = R._cfg()
    return redisclient.Redis(host=c["host"], port=c["port"],
                             password=c["password"], timeout=1.0, pool=1)


def build_ok() -> None:
    print("\n[1] 二进制与配置都在仓库里")
    ck("redis-server.exe 存在", R.EXE.exists(), str(R.EXE))
    ck("redis.conf 存在", R.CONF.exists(), str(R.CONF))
    txt = R.CONF.read_text(encoding="utf-8")
    # 只认真实生效的行，注释里提到不算
    lines = [ln.strip() for ln in txt.splitlines()
             if ln.strip() and not ln.strip().startswith("#")]
    # 密码一旦写进配置文件，就会随仓库公开——这条必须由代码卡住，不能靠自觉
    ck("配置里没有明文密码", not any(ln.startswith("requirepass")
                                     for ln in lines))
    ck("绑定回环地址", any(ln == "bind 127.0.0.1" for ln in lines), str(lines[:4]))
    ck("关闭 RDB 持久化", any(ln.replace(" ", "") == 'save""' for ln in lines))
    ck("有内存上限（否则一直被写到 OOM）",
       any(ln.startswith("maxmemory ") for ln in lines))


def start_once() -> None:
    print("\n[2] 停→起→真正在监听")
    R.stop()
    ck("已停下", not R.probe()["running"])
    ok, msg = R.start(quiet=True)
    ck("启动成功", ok, msg)
    st = R.probe()
    ck("端口在监听", st["running"])
    ck("版本是 3.2.100（仓库里那一份）", st["version"] == "3.2.100", st["version"])
    ck("密码生效（无密码连不上）",
       _noauth_rejected(),
       "服务端接受了无密码连接——requirepass 没传进去")


def _noauth_rejected() -> bool:
    # 注意 ping() 是「吞掉所有 RedisError 返回 False」的语义，不抛异常。
    # 所以"被拒绝"的判据是 ping 返回 False，而不是 try/except 分支。
    try:
        r = redisclient.Redis(host=R._cfg()["host"], port=R._cfg()["port"],
                              timeout=1.0, pool=1)
        ok = r.ping()
        r.close()
        return not ok
    except Exception:
        return True


def _s(v) -> str:
    """CONFIG GET 回来的是 bytes，直接和 str 比会永远不等。"""
    return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v)


def conf_applied() -> None:
    print("\n[3] 配置项确实生效（不是写了没读）")
    r = _gw()
    save = r.cmd("CONFIG", "GET", "save")
    ck("save 为空（不落盘）", _s(save[1]) == "", str(save))
    mm = r.cmd("CONFIG", "GET", "maxmemory")
    ck("maxmemory 已设置", int(_s(mm[1])) > 0, str(mm))
    pol = r.cmd("CONFIG", "GET", "maxmemory-policy")
    ck("淘汰策略 allkeys-lru", _s(pol[1]) == "allkeys-lru", str(pol))
    r.close()


def no_double_start() -> None:
    print("\n[4] 重复调用不会起第二个")
    ok1, m1 = R.start(quiet=True)
    ok2, m2 = R.start(quiet=True)
    ck("第一次复用成功", ok1 and "复用" in m1, m1)
    ck("第二次仍复用（不叠加进程）", ok2 and "复用" in m2, m2)


def data_visible() -> None:
    print("\n[5] 写进去的 key 能被另一个连接读到（共享的前提）")
    r1, r2 = _gw(), _gw()
    r1.set("selftest:boot", "hello", ex=30)
    ck("跨连接可见", r2.get("selftest:boot") == b"hello", str(r2.get("selftest:boot")))
    r1.delete("selftest:boot")
    r1.close()
    r2.close()


def stop_works() -> None:
    print("\n[6] 停得掉，且再停一次不报错")
    ok, msg = R.stop()
    ck("停止成功", ok, msg)
    ck("端口已释放", not R.probe()["running"])
    ok2, msg2 = R.stop()
    ck("幂等：再停一次也说清楚", ok2 and "没在跑" in msg2, msg2)


def main() -> int:
    print("自带 Redis 自测 —— %s" % R.EXE)
    for fn in (build_ok, start_once, conf_applied, no_double_start,
               data_visible, stop_works):
        try:
            fn()
        except Exception as e:                    # 单段异常不该掩盖其它段
            global FAIL
            FAIL += 1
            FAILED.append("%s 抛异常" % fn.__name__)
            print("  ✗ %s 抛异常：%s: %s" % (fn.__name__, type(e).__name__, e))

    # 恢复现场：把 Redis 重新拉起来，别让跑一次自测把 webapp 变成降级状态
    ok, msg = R.start(quiet=True)
    print("\n[7] 恢复现场")
    ck("Redis 已重新运行", ok and R.probe()["running"], msg)

    print("\n%d 通过 / %d 失败" % (PASS, FAIL))
    if FAILED:
        print("失败项：" + "；".join(FAILED))
    # 退出码写反过（`0 if FAIL else 1`）：全过反而退 1，CI 里就成了"自测挂了"。
    # 其余 28 个 selftest 都是 `1 if FAIL else 0`，这里对齐。
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
