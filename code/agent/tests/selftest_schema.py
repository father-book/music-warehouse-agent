"""结构化输出契约（P1-1）的自测。

为什么要单独一套
----------------
`json_mode=True` 只保证"回的是 JSON 语法"，不保证**回的是我们要的那张表**。
真见过五种坏法，每一种以前都能一路走到 validate：

    ① 缺 sql            —— 模型只回了 reason；
    ② reason 是对象      —— detail 里 `[:80]` 直接 TypeError；
    ③ 多一个字段         —— 模型自造 `confidence: 0.9`，没人读它，静默忽略；
    ④ used_tables 是字符串 —— 下游 `[t.split(".")[-1] for t in ...]` 遍历到字符；
    ⑤ sql 是一句自然语言  —— 护栏解析抛异常，看起来像我们的 bug。

这里测的不是"pydantic 会不会报错"（那是它的事），而是三条边界：

    · 坏输出**只重试一次**，第二次仍坏就交空 SQL 走拒答；
    · 坏 SQL 不能流到 validate / execute（可以流到 validate 的只有空串）；
    · 脏但可救的输出（围栏、前后废话、结尾分号）不能被误杀——收紧口径
      不等于把能救的也拒掉。

做法：全部打桩，不调真模型。
  · [1]~[3]、[6] 直接调 schemas，纯函数，零外部依赖；
  · [4]、[5] 把 `gateway.chat` 换成回放器，数"到底调了几次"；
  · [7] 走完整 graph.ask，验证十个节点里没有哪个会把坏 SQL 捞回来执行。

    python code/agent/tests/selftest_schema.py
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import copy
import json
import sys

import node_generate
import node_generate_llm as gen
import schemas
from conf import load_config

OK = 0
FAIL = 0
FAILED: list[str] = []


def ck(name: str, cond: bool, extra: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        FAILED.append(name)
        print("  ✗ %s %s" % (name, extra))


GOOD_SQL = "SELECT dau FROM dws_user_daily WHERE dt = '2026-11-08'"


def _draft(**kw) -> str:
    obj = {"sql": GOOD_SQL, "used_tables": ["dws_user_daily"], "reason": "原子指标直取"}
    obj.update(kw)
    return json.dumps(obj, ensure_ascii=False)


def _err(raw: str) -> str:
    """解析失败时的可读原因（一条）。成功时返回带 SUCCESS 前缀的标识。"""
    try:
        d = schemas.parse_sql_draft(raw)
    except Exception as e:
        return " | ".join(schemas.brief_errors(e))
    return "SUCCESS:" + d.sql


# ---------------------------------------------------------------- [1] 五坏

def t_bad_outputs() -> None:
    print("\n[1] 五种坏输出必须被拦下，且原因能贴给人看")
    cases = [
        ("缺 sql", {"used_tables": [], "reason": "r"}, "required"),
        ("reason 是对象", {"sql": GOOD_SQL, "reason": {"tier": "atomic"}}, "string"),
        ("多一个字段", {"sql": GOOD_SQL, "confidence": 0.9}, "Extra inputs"),
        ("used_tables 是字符串", {"sql": GOOD_SQL, "used_tables": "dws_user_daily"}, "list"),
        ("sql 空串", {"sql": "", "reason": "r"}, "为空"),
        ("sql 只有代码围栏", {"sql": "```sql\n```"}, "为空"),
        ("自然语言冒充 SQL", {"sql": "查询 2026-11-08 的 DAU"}, "SELECT"),
        ("sql 不是字符串", {"sql": 3237}, "字符串"),
    ]
    for name, obj, want in cases:
        got = _err(json.dumps(obj, ensure_ascii=False))
        ck("%s → 拒（%s）" % (name, want), want in got and not got.startswith("SUCCESS"),
           "实得 %r" % got[:90])

    # 报错是给人看的：不能把 pydantic 的文档 URL / 版本头原样抛给用户
    msg = _err('{"sql": 1}')
    ck("报错里没有 pydantic 文档链接", "http" not in msg and "pydantic.dev" not in msg, msg)


# ---------------------------------------------------------------- [2] 可救

def t_salvage() -> None:
    print("\n[2] 脏但可救的输出不能被误杀")
    ck("纯 JSON 收", _err(_draft()) == "SUCCESS:" + GOOD_SQL)
    ck("```json 围栏收",
       _err("```json\n" + _draft() + "\n```") == "SUCCESS:" + GOOD_SQL)
    ck("前面一段话 + JSON 收",
       _err("好的，我先确认口径：\n" + _draft() + "\n以上。") == "SUCCESS:" + GOOD_SQL)
    ck("结尾分号被去掉", _err(_draft(sql=GOOD_SQL + ";")) == "SUCCESS:" + GOOD_SQL)

    braces = "SELECT '{a}' AS tag, COUNT(*) AS c FROM dwd_session WHERE dt = '2026-11-08'"
    got = _err(_draft(sql=braces))
    ck("字符串里的花括号不误判成第二个对象", got == "SUCCESS:" + braces, got[:80])

    cot = _draft(plan={"metric": "dau", "source_table": "dws_user_daily",
                       "joins": [], "filters": [], "time_condition": "dt='2026-11-08'",
                       "shape": "scalar"})
    try:
        d = schemas.parse_sql_draft(cot)
        ck("CoT 的 plan 字段透传（不被 extra=forbid 拦）",
           (d.plan or {}).get("source_table") == "dws_user_daily", str(d.plan)[:80])
    except Exception as e:
        ck("CoT 的 plan 字段透传（不被 extra=forbid 拦）", False, str(e)[:120])

    ck("used_tables / reason 缺省不影响成单",
       _err(json.dumps({"sql": GOOD_SQL})) == "SUCCESS:" + GOOD_SQL)


# ---------------------------------------------------------------- [3] 多 JSON

def t_multi_json() -> None:
    print("\n[3] 多个 JSON 对象：宁可重试，也不要掷硬币")
    raw = _draft() + json.dumps({"sql": "DROP TABLE dws_user_daily"}, ensure_ascii=False)
    got = _err(raw)
    ck("两个对象 → 拒", "2 个 JSON 对象" in got, got[:90])
    ck("拒的时候没有返回任何 sql（第二个对象不会被执行）",
       not got.startswith("SUCCESS"), got[:60])

    ck("顶层是数组 → 拒", "list" in _err("[" + _draft() + "]"))
    ck("括号不闭合 → 拒且说明没闭合", "没有闭合" in _err('{"sql": "SELECT 1"'))
    ck("空 content → 拒", "为空" in _err(""))
    ck("整段不是 JSON → 拒", "没有 JSON 对象" in _err("我不知道该怎么回答"))


# ---------------------------------------------------------------- 桩

_SENT: list[list[dict]] = []


def _install(replies: list[str]):
    """把 gateway.chat 换成回放器；返回调用计数。"""
    box = {"n": 0}

    def fake_chat(messages, cfg, **kw):
        _SENT.append(copy.deepcopy(messages))
        i = min(box["n"], len(replies) - 1)
        box["n"] += 1
        return {"content": replies[i], "model": "stub", "elapsed_ms": 1,
                "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                          "total_tokens": 15},
                "cost_cny": 0.0, "ledger_ok": True, "tool_calls": None}

    real = gen.gateway.chat
    gen.gateway.chat = fake_chat
    return box, real


# ---------------------------------------------------------------- [4] 重试

def t_call_once(cfg) -> None:
    print("\n[4] _call_once：坏输出只重试一次，且带着错因重试")
    msgs = [{"role": "user", "content": "2026-11-08 的 DAU 是多少"}]
    st = {"ctx": None}

    _SENT.clear()
    box, real = _install([json.dumps({"used_tables": [], "reason": "r"}), _draft()])
    try:
        got, err = gen._call_once(msgs, cfg, st, "generate", None, 100)
    finally:
        gen.gateway.chat = real
    ck("坏一次 + 好一次 → 调 2 次", box["n"] == 2, "实得 %d" % box["n"])
    ck("第二次拿到了可用 SQL", bool(got) and got[0] == GOOD_SQL, str(err)[:80])
    tail = json.dumps(_SENT[-1], ensure_ascii=False) if _SENT else ""
    ck("重试请求里带上了上一轮的错因（缺 sql）",
       "sql" in tail and "上一次输出不合格" in tail, tail[-140:])
    ck("重试请求里带回了上一轮的坏输出",
       "used_tables" in tail and "reason" in tail, tail[-140:])

    _SENT.clear()
    box, real = _install([json.dumps({"used_tables": [], "reason": "r"})])
    try:
        got, err = gen._call_once(msgs, cfg, st, "generate", None, 100)
    finally:
        gen.gateway.chat = real
    ck("两次都坏 → 调 2 次（不是 3 次、也不会无限重试）", box["n"] == 2,
       "实得 %d" % box["n"])
    ck("两次都坏 → 不返回 SQL", got is None)
    ck("两次都坏 → 打出 schema_error 标记", bool(err.get("schema_error")), str(err)[:90])
    ck("两次都坏 → detail 里写明已重试过一次",
       "已重试一次" in str(err.get("detail") or ""), str(err.get("detail"))[:100])

    # 一条合法 SQL 不该被多调一次
    _SENT.clear()
    box, real = _install([_draft()])
    try:
        got, _ = gen._call_once(msgs, cfg, st, "generate", None, 100)
    finally:
        gen.gateway.chat = real
    ck("合规输出只调 1 次", box["n"] == 1 and bool(got), "实得 %d" % box["n"])


# ---------------------------------------------------------------- [5] 短路

def t_generate_abort(cfg) -> None:
    print("\n[5] generate 节点：契约失败后不再问第二遍模型")
    box, real = _install(['{"used_tables": [], "reason": "r"}'])
    try:
        out = node_generate.generate(
            {"attempts": 1, "sql": "",
             "generate_schema_failed": True,
             "generate_error": "模型输出不合规（已重试一次）：sql: Field required",
             "time_end": "2026-11-08", "ctx": None}, cfg)
    finally:
        gen.gateway.chat = real
    ck("短路：一次模型都没调", box["n"] == 0, "实得 %d" % box["n"])
    ck("短路：交回空 SQL（validate 才能判 EMPTY）", out.get("sql") == "", repr(out.get("sql")))
    ck("短路：把原始错因原样留给 refuse",
       "已重试一次" in str(out.get("generate_error") or ""), str(out.get("generate_error"))[:80])
    ck("短路：trace 里留下了 schema_error",
       bool((out.get("trace") or [{}])[0].get("data", {}).get("schema_error")))

    # 反向：没有 schema 失败标记时必须照常生成，别把正常重试也短路掉
    box, real = _install([_draft()])
    try:
        out2 = node_generate.generate(
            {"attempts": 0, "sql": "", "generate_schema_failed": False,
             "time_end": "2026-11-08", "ctx": None}, cfg)
    finally:
        gen.gateway.chat = real
    ck("正常首轮不受影响（调 1 次、拿到 SQL）",
       box["n"] == 1 and out2.get("sql") == GOOD_SQL, "n=%d sql=%r" % (box["n"], out2.get("sql")))


# ---------------------------------------------------------------- [6] QueryPlan

def t_plan() -> None:
    print("\n[6] QueryPlan：字段合同对齐 planner.PLAN_SPEC")
    good = {"intent": "查 DAU", "metrics": ["uv"], "dimensions": [],
            "tables": ["dws_user_daily"], "filters": [],
            "time_condition": "dt = '2026-11-08'", "shape": "scalar",
            "need_clarify": False, "clarify_question": ""}
    try:
        p = schemas.parse_query_plan(json.dumps(good, ensure_ascii=False)).model_dump()
        ck("合规计划收", p["metrics"] == ["uv"] and p["need_clarify"] is False)
    except Exception as e:
        ck("合规计划收", False, str(e)[:120])

    ck("多余字段 → 拒",
       "Extra inputs" in " ".join(schemas.brief_errors(
           _plan_err({**good, "confidence": 0.5}))))
    ck("metrics 是字符串 → 拒",
       "list" in " ".join(schemas.brief_errors(
           _plan_err({**good, "metrics": "uv"}))))
    # shape 不在枚举里只是"可疑"，不归 schema 判——否则会把 validate_plan 的
    # WARN 偷偷升级成硬失败，等于改口径。
    try:
        schemas.parse_query_plan(json.dumps({**good, "shape": "weird"}))
        ck("shape 非法不拦（留给 validate_plan 给 WARN）", True)
    except Exception as e:
        ck("shape 非法不拦（留给 validate_plan 给 WARN）", False, str(e)[:100])


def _plan_err(obj) -> Exception:
    try:
        schemas.parse_query_plan(json.dumps(obj, ensure_ascii=False))
    except Exception as e:
        return e
    return ValueError("没有报错")


# ---------------------------------------------------------------- [7] 端到端

def t_end_to_end(cfg) -> None:
    print("\n[7] 端到端：模型一直吐不合规 JSON，坏 SQL 不能进执行")
    from governance.contracts import RequestContext
    from graph import ask

    box, real = _install(['{"reason": "我查一下"}'])     # 每次都缺 sql
    try:
        c = copy.deepcopy(cfg)
        st = ask("2026-11-08 的 DAU 是多少", c,
                 ctx=RequestContext.system(source="eval"))
    finally:
        gen.gateway.chat = real

    ck("模型总共只调 2 次（首轮 1 + 契约重试 1）", box["n"] == 2, "实得 %d" % box["n"])
    ck("最终走拒答", bool(st.get("refused")), "refused=%r" % st.get("refused"))
    ck("execute 没跑（没有 result）", not (st.get("result") or {}).get("rows")
       and not (st.get("result") or {}).get("columns"), str(st.get("result"))[:80])
    ck("拒答原因能读懂（提到不合规/缺字段）",
       any(k in (st.get("refuse_reason") or "") for k in ("不合规", "sql", "可用 SQL")),
       (st.get("refuse_reason") or "")[:120])
    ck("图层只判了两轮（attempts=2）", (st.get("attempts") or 0) == 2,
       "attempts=%r" % st.get("attempts"))
    stages = [h.get("stage") for h in (st.get("history") or [])]
    ck("history 里留下了 schema_abort（不是静默跳过）",
       "schema_abort" in stages, str(stages))


def main() -> int:
    cfg = copy.deepcopy(load_config())
    print("=" * 60)
    print("schemas（P1-1 结构化输出契约）自测")
    print("=" * 60)

    t_bad_outputs()
    t_salvage()
    t_multi_json()
    t_call_once(cfg)
    t_generate_abort(cfg)
    t_plan()
    t_end_to_end(cfg)

    print("\n" + "-" * 60)
    print("%d 通过 / %d 失败" % (OK, FAIL))
    if FAILED:
        print("失败项：")
        for f in FAILED:
            print("  · %s" % f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
