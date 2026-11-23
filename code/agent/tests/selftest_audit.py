"""审计闭环（AC-14）自测：脱敏、访问留痕、保留期。

这一套测的是三个"看不见"的东西：

  1. **脱敏到底脱没脱**：审计表和用户数据是两份拷贝，问句里带手机号就该被替换。
     最容易的失败不是"没脱"，而是"脱过头"——把 `2026-11-08` 也变成 `[PHONE]`，
     那张表就成了废纸。所以正向和反向用例一起写。
  2. **看审计这件事有没有被记**：漏记不会报错，只会让"谁查过谁的审计"
     永远查不出来。只能靠读一次、回头数一行来验。
  3. **保留期清理没把关触发器拆了**：最危险的状态是"触发器被关掉忘了开"。
     所以清理完必须验两件事：老行归档了；直接 DELETE 依然报错。

跑在独立库 `agent_gov_audit` 上（每次重建）。为什么不复用正式库：
qa_logs 有不可变触发器，测试数据写进去**删不掉**，只能污染正式审计。

    python code/agent/tests/selftest_audit.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import os
import sys
import uuid
from pathlib import Path

from _bootstrap import HERE  # noqa: E402
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import psycopg  # noqa: E402

from governance import pii, retention, store  # noqa: E402
from governance.contracts import AuditEvent, RequestContext  # noqa: E402

OK = FAILED = 0
PG_ADMIN = "postgresql://postgres@127.0.0.1:55432/postgres"
TEST_DB = "agent_gov_audit"
TEST_DSN = "postgresql://postgres@127.0.0.1:55432/" + TEST_DB


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAILED
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAILED += 1
        print("  ✗ %s %s" % (name, extra))


def _cfg() -> dict:
    return {"governance": {"dsn": TEST_DSN, "retention_days": 180}}


def _fresh_db() -> tuple[bool, str]:
    try:
        admin = psycopg.connect(PG_ADMIN, autocommit=True, connect_timeout=5)
        with admin.cursor() as cur:
            cur.execute("DROP DATABASE IF EXISTS %s WITH (FORCE)" % TEST_DB)
            cur.execute("CREATE DATABASE %s" % TEST_DB)
        admin.close()
        return store.init_schema(_cfg())
    except Exception as e:
        return False, "%s: %s" % (type(e).__name__, str(e)[:200])


def _q(sql: str, args: tuple = ()) -> list[tuple]:
    with psycopg.connect(TEST_DSN, connect_timeout=5) as c:
        with c.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()


def _event(question: str, trace_id: str, source: str = "web",
           answer: str = "手机号 13800001111 的记录如下……") -> AuditEvent:
    ctx = RequestContext(trace_id=trace_id, user_id="u-test", tenant_id="default",
                         role="data_analyst", source=source)
    state = {"question": question, "answer": answer,
             "result": {"row_count": 1}, "verdict": "pass",
             "elapsed_ms": 12, "tables": ["dwd_play_start"]}
    return AuditEvent.from_state(state, ctx, None, None)


def main() -> int:
    print("=" * 70)
    print("审计闭环自测（AC-14：脱敏 / 访问留痕 / 保留期）")
    print("=" * 70)

    # ---------------------------------------------------------------- [1] 脱敏
    print("\n[1] 脱敏：该脱的脱掉")
    cases = [
        ("13800001111", "手机号"),
        ("110101199003071234", "身份证"),
        ("zhang.san@example.com", "邮箱"),
        ("6222 0202 0000 1234", "银行卡（带空格）"),
        ("192.168.128.163", "IPv4"),
    ]
    for raw, label in cases:
        out, n = pii.redact("联系 %s 的记录" % raw)
        ck("%s 被替换" % label, n == 1 and raw not in out, "%s / %d" % (out, n))

    print("\n[2] 脱敏：不该动的绝不误伤（脱过头比不脱更糟）")
    keep = ["2026-11-08 的 DAU 是多少", "S000014 这首歌的播放量",
            "4994396 行数据", "第 3 名", "近 7 天播放量", "dt >= '2026-11-01'"]
    for s in keep:
        out, n = pii.redact(s)
        ck("原样保留：%s" % s[:26], out == s and n == 0, "%s / %d" % (out, n))

    out1, n1 = pii.redact("手机 13800001111")
    out2, n2 = pii.redact(out1)
    ck("幂等：重复脱敏不再变化", out1 == out2 and n2 == 0, "%s / %d" % (out2, n2))

    # ---------------------------------------------------------------- [3] 事件
    print("\n[3] 审计事件：落库前就脱敏")
    ok, err = _fresh_db()
    ck("测试库可用（agent_gov_audit）", ok, err)
    if not ok:
        print("\n建库失败，后续用例无法可信执行，中止。")
        return 1

    tid = "T-PII-" + uuid.uuid4().hex[:8]
    ev = _event("手机 13800001111 的用户昨天听了多少歌", tid)
    ck("事件 question 已脱敏", "13800001111" not in ev.question and "[PHONE]" in ev.question,
       ev.question)
    ck("事件 answer_excerpt 也脱敏", "13800001111" not in ev.answer_excerpt,
       ev.answer_excerpt)
    ck("pii_hits = 2（问句 + 答案各一处）", ev.pii_hits == 2, str(ev.pii_hits))

    ok, err = store.insert_audit(ev, _cfg())
    ck("写入成功", ok, err)
    rows = _q("SELECT question, pii_hits FROM qa_logs WHERE trace_id = %s", (tid,))
    ck("库里存的是脱敏后的问句", rows and "13800001111" not in rows[0][0],
       str(rows))
    ck("库里存了 pii_hits", rows and rows[0][1] == 2, str(rows))

    tid2 = "T-CLEAN-" + uuid.uuid4().hex[:8]
    ev2 = _event("2026-11-08 的 DAU 是多少", tid2, answer="DAU 是 1。")
    store.insert_audit(ev2, _cfg())
    rows = _q("SELECT pii_hits FROM qa_logs WHERE trace_id = %s", (tid2,))
    ck("干净问句 pii_hits = 0（不虚报）", rows and rows[0][0] == 0, str(rows))

    # ---------------------------------------------------------------- [4] 访问留痕
    print("\n[4] 审计访问留痕：看审计这件事本身也被记录")
    ok, err = store.insert_access("analyst_zhang", "data_analyst", "read",
                                  target=tid, filters={"limit": 30}, rows=1, cfg=_cfg())
    ck("访问记录写入成功", ok, err)
    acc = store.fetch_access(_cfg(), limit=10)
    ck("能读回访问记录", bool(acc) and acc[0]["user_id"] == "analyst_zhang", str(acc)[:80])
    ck("记录了动作与目标", acc and acc[0]["action"] == "read" and acc[0]["target"] == tid,
       str(acc[:1]))
    ck("记录了返回行数", acc and acc[0]["rows"] == 1, str(acc[:1]))

    ok2, _ = store.insert_access("pm_li", "data_pm", "read", target="", cfg=_cfg())
    acc = store.fetch_access(_cfg(), limit=10, user_id="pm_li")
    ck("按用户过滤访问记录", len(acc) == 1 and acc[0]["user_id"] == "pm_li", str(len(acc)))

    # ---------------------------------------------------------------- [5] 保留期
    print("\n[5] 保留期：归档在前，受控删除在后")
    old = "T-OLD-" + uuid.uuid4().hex[:8]
    ctx_old = RequestContext(trace_id=old, user_id="u-old", tenant_id="default",
                             role="data_analyst", source="web")
    ev_old = AuditEvent.from_state({"question": "2026-01-01 的 DAU", "result": {}}, ctx_old)
    ev_old.occurred_at = "2026-01-01T00:00:00+00:00"     # 200 天前，早于 180 天保留期
    ok, err = store.insert_audit(ev_old, _cfg())
    ck("造一条 200 天前的审计", ok, err)

    dry = retention.run(180, _cfg(), dry_run=True)
    ck("dry-run 只报数不删", dry["rows"] >= 1 and dry["dry_run"] is True, str(dry))
    ck("dry-run 之后行还在",
       bool(_q("SELECT 1 FROM qa_logs WHERE trace_id = %s", (old,))))

    trig_before = retention.trigger_enabled(_cfg())
    ck("清理前触发器在岗", trig_before is True, str(trig_before))

    res = retention.run(180, _cfg(), dry_run=False, actor="selftest")
    ck("清理执行成功", not res["error"], res["error"])
    ck("删掉了 1 行", res["deleted"] >= 1, str(res))
    ck("归档了 1 行", res["archived"] >= 1, str(res))
    ck("老行已不在 qa_logs",
       not _q("SELECT 1 FROM qa_logs WHERE trace_id = %s", (old,)))
    arch = _q("SELECT payload FROM qa_logs_archive WHERE trace_id = %s", (old,))
    ck("老行整行进了归档表", bool(arch), str(arch)[:60])
    ck("归档里保留了原始内容",
       bool(arch) and "2026-01-01" in str(arch[0][0]), str(arch)[:80])
    ck("清理动作记进了审计访问表",
       any(a["action"] == "purge" for a in store.fetch_access(_cfg(), limit=20)),
       str([a["action"] for a in store.fetch_access(_cfg(), limit=20)]))

    # ---------------------------------------------------------------- [6] 触发器还在
    print("\n[6] 最危险的状态：触发器被关掉忘了开")
    ck("清理后触发器仍自动恢复", retention.trigger_enabled(_cfg()) is True,
       str(retention.trigger_enabled(_cfg())))
    blocked = False
    try:
        _q("DELETE FROM qa_logs WHERE trace_id = %s", (tid2,))
    except Exception as e:
        blocked = "不可变" in str(e) or "immutable" in str(e).lower()
    ck("直接 DELETE 仍被触发器拒绝（清理只走受控通道）", blocked)
    ck("被拒绝后行没少",
       bool(_q("SELECT 1 FROM qa_logs WHERE trace_id = %s", (tid2,))))

    # ---------------------------------------------------------------- [7] 失败安全
    print("\n[7] 清理失败时不能把触发器留在关闭状态")
    bad = {"governance": {"dsn": "postgresql://postgres@127.0.0.1:55432/no_such_db"}}
    r = retention.run(180, bad, dry_run=False)
    ck("连不上库时返回错误而不是抛异常", bool(r["error"]), str(r)[:80])
    ck("错误路径不动正式库触发器", retention.trigger_enabled(_cfg()) is True)

    print("\n" + "=" * 70)
    print("%d 通过 / %d 失败" % (OK, FAILED))
    print("=" * 70)
    return 1 if FAILED else 0


if __name__ == "__main__":
    raise SystemExit(main())
