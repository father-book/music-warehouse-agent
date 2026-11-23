"""会话与多轮记忆自测。

    python code/agent/tests/selftest_conversation.py    # 全跑，不调模型、不连 Doris

覆盖五件事：
  1. store 会话 CRUD：创建 / 列表 / 改名 / 软删，以及**用户隔离**
     （别人的会话就是查不到——这是产品边界，不是性能优化）。
  2. recent_turns 的配对规则：半轮（问了没答）不进上下文，
     否则生成节点会拿"只有问题没有答案"的轮次去脑补。
  3. prompt 层：无历史时 prompt 逐字不变（保证评测集零回归）；
     有历史时出现对话块与追问说明。
  4. route 的追问兜底：指标 / 时间没提就沿用上一轮，且 followup 必须留痕；
     本轮明确给了时间就绝不沿用。
  5. 缓存盐与快照：同问句不同上下文必须拿到不同缓存键；
     会话快照裁剪掉大字段但保留渲染所需。

为什么不用模型：以上全部是"骨架行为"，模型一抖它们就该照常成立；
真跑通多轮的端到端验证在 smoke / 手工流程里做。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import sys
import datetime as _dt

from conf import load_config
from governance import store
from governance.contracts import RequestContext
from governance.policy import engine
import answercache
import node_route
import promptlib
import timeparse

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
rows: list[tuple[str, str, str]] = []


def rec(state: str, name: str, detail: str) -> None:
    rows.append((state, name, detail))
    print("  %-4s %-30s %s" % (state, name, detail))


def expect(ok: bool, name: str, detail: str = "") -> None:
    rec(PASS if ok else FAIL, name, detail)


# ---------------------------------------------------------------- store
def t_store() -> None:
    cfg = load_config()
    cid, err = store.create_conversation("u_selftest_conv", "default", "自测会话", cfg)
    expect(bool(cid) and not err, "建会话", err[:80] if err else cid)
    if not cid:
        return

    ok, _ = store.append_message(cid, "user", "11-08 的 DAU", cfg=cfg)
    expect(ok, "写用户消息")
    snap = {"answer": "DAU 3,237", "sql": "select 1", "metrics": ["dau"],
            "time_start": "2026-11-08", "time_end": "2026-11-08",
            "time_window": {"label": "2026-11-08"}}
    ok, err = store.append_message(cid, "assistant", "DAU 3,237",
                                   payload=snap, trace_id="t1", cfg=cfg)
    expect(ok, "写答案快照", err[:80])

    rows_ = store.list_conversations("u_selftest_conv", cfg=cfg)
    expect(any(r["conv_id"] == cid for r in rows_), "出现在列表")
    expect(not any(r["conv_id"] == cid
                   for r in store.list_conversations("someone_else", cfg=cfg)),
           "别人列表里没有")

    conv = store.get_conversation(cid, "u_selftest_conv", cfg)
    expect(conv is not None, "本人可读")
    expect(store.get_conversation(cid, "someone_else", cfg) is None,
           "越权读返回 None")

    turns = store.recent_turns(cid, "u_selftest_conv", cfg=cfg)
    expect(len(turns) == 1 and turns[0]["metrics"] == ["dau"]
           and turns[0]["time_start"] == "2026-11-08",
           "recent_turns 配出完整一轮", json_hint(turns))

    # 半轮：只问不答，不能进上下文
    ok, _ = store.append_message(cid, "user", "半轮消息", cfg=cfg)
    turns = store.recent_turns(cid, "u_selftest_conv", cfg=cfg)
    expect(len(turns) == 1, "半轮不进上下文", "%d 轮" % len(turns))

    expect(store.rename_conversation(cid, "u_selftest_conv", "改名", cfg=cfg),
           "改名")
    expect(store.rename_conversation(cid, "someone_else", "x", cfg=cfg) is False,
           "越权改名无效")

    expect(store.delete_conversation(cid, "u_selftest_conv", cfg=cfg), "软删")
    expect(store.list_conversations("u_selftest_conv", cfg=cfg) == [], "删后列表为空")
    expect(store.load_messages(cid, "u_selftest_conv", cfg=cfg) is None,
           "删后读消息为 None")


def json_hint(v) -> str:
    import json
    return json.dumps(v, ensure_ascii=False)[:80]


# ---------------------------------------------------------------- prompt
def t_prompt() -> None:
    cfg = load_config()
    base = {"question": "11-08 的 DAU 是多少", "metrics": ["dau"],
            "time_start": "2026-11-08", "time_end": "2026-11-08",
            "time_window": {"label": "2026-11-08"}, "tables": ["dwd_session"]}
    u1 = promptlib.build_generate_messages(base, cfg)[-1]["content"]
    expect("前几轮对话" not in u1 and "本轮是追问" not in u1,
           "单轮 prompt 无记忆块")

    hist = [{"question": "上周播放量", "answer": "…", "metrics": ["play_cnt"],
             "time_window": {"label": "2026-11-02 ~ 2026-11-08"},
             "time_start": "2026-11-02", "time_end": "2026-11-08"}]
    st2 = dict(base, history_turns=hist,
               followup={"metrics": ["dau"], "time": {"start": "2026-11-08",
                                                      "end": "2026-11-08"}})
    u2 = promptlib.build_generate_messages(st2, cfg)[-1]["content"]
    expect("前几轮对话" in u2 and "上周播放量" in u2, "多轮 prompt 含历史")
    expect("本轮是追问" in u2 and "指标 dau" in u2, "追问说明进 prompt")
    expect(promptlib.dialogue_block({}) == "", "空历史空串")
    # 只保留最近 3 轮，防止长会话把 prompt 撑爆
    many = [{"question": "q%d" % i, "answer": "a"} for i in range(8)]
    blk = promptlib.dialogue_block({"history_turns": many})
    expect("q7" in blk and "q0" not in blk, "只取最近 3 轮")


# ---------------------------------------------------------------- route 兜底
def t_followup() -> None:
    cfg = load_config()
    ctx = RequestContext.system(source="selftest_conversation")
    decision = engine(cfg).decide(ctx)
    prev = {"question": "2026-11-08 的 DAU 是多少", "answer": "DAU 3,237",
            "metrics": ["dau"], "time_start": "2026-11-08",
            "time_end": "2026-11-08",
            "time_window": {"label": "2026-11-08", "kind": "single"}}

    def run(q, hist):
        st = {"question": q, "policy_decision": decision.to_dict(),
              "ctx": ctx.to_dict(), "trace_id": ctx.trace_id}
        if hist:
            st["history_turns"] = hist
        out = node_route.route(st, cfg)
        return out

    # 单轮：不沿用
    o = run("再算一遍", None)
    expect(o.get("followup") in (None, {}), "单轮不沿用")
    expect(o.get("metrics") == [], "单轮指标仍为空")

    # 追问：指标 + 时间都沿用，且留痕
    o = run("再算一遍", [prev])
    fu = o.get("followup") or {}
    expect(o.get("metrics") == ["dau"] and fu.get("metrics") == ["dau"],
           "追问沿用指标")
    expect(o.get("time_start") == "2026-11-08", "追问沿用时间窗")
    expect(bool(fu.get("time")), "时间沿用留痕")
    detail = " ".join(t.get("detail", "") for t in (o.get("trace") or []))
    expect("追问" in detail and "dau" in detail, "trace 里写明沿用", detail[:100])

    # 本轮明确给了时间：绝不沿用
    o = run("11-01 的 DAU", [prev])
    fu = o.get("followup") or {}
    expect(not fu.get("time"), "明确时间不沿用",
           "start=%s" % o.get("time_start"))
    expect(o.get("time_start") == "2026-11-01", "明确时间生效")


# ---------------------------------------------------------------- 缓存盐
def t_cache_salt() -> None:
    from graph import _history_salt
    expect(_history_salt(None) == "", "无历史盐为空")
    h1 = [{"question": "上周播放量"}]
    h2 = [{"question": "别的会话的问句"}]
    s1 = _history_salt(h1)
    expect(s1 != "" and s1 == _history_salt(h1), "同历史同盐")
    expect(s1 != _history_salt(h2), "不同历史不同盐")
    # 盐只取最近两轮：3 轮历史与"去掉最老一轮"的 2 轮应得到同一个键，
    # 也就是说更早的轮次不该影响缓存命中
    long3 = _history_salt([{"question": "p"}, {"question": "q2"}] + h1)
    two2 = _history_salt([{"question": "q2"}] + h1)
    expect(long3 == two2, "盐只看最近两轮")


# ---------------------------------------------------------------- 快照
def t_snapshot() -> None:
    import webapp
    st = {"answer": "x", "sql": "select 1", "result": {"columns": ["a"],
          "rows": [[i] for i in range(120)]},
          "trace": [{"node": "route", "detail": "d" * 900,
                     "data": {"big": "x" * 900}}],
          "ctx": {"user_id": "u"}, "policy_decision": {"role": "r"}}
    snap = webapp._snapshot(st)
    rows_ = (snap.get("result") or {}).get("rows") or []
    expect(len(rows_) == 50 and snap["result"].get("rows_truncated"),
           "快照行数裁剪到 50", "%d 行" % len(rows_))
    expect(len(snap["trace"][0]["detail"]) <= 501, "trace 详情截断")
    expect("ctx" not in snap and "policy_decision" not in snap,
           "权限内部结构不进快照")
    small = webapp._snapshot({"answer": "ok"})
    expect(small.get("answer") == "ok", "正常字段保留")


def main() -> int:
    print("== 会话与多轮自测 ==")
    t_store()
    t_prompt()
    t_followup()
    t_cache_salt()
    t_snapshot()
    bad = [r for r in rows if r[0] == FAIL]
    print("\n%d/%d 通过" % (len(rows) - len(bad), len(rows)))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
