"""B 阶段：用 DeepSeek 真生成 SQL。

和 stub 的唯一区别是"SQL 从哪来"，产出的 state 结构完全一样，
所以 graph.py 一行都不用改——这是 A 阶段刻意留的接缝。

失败策略：任何异常都不向上抛，转成 sql="" + generate_error，
交给 validate 判 EMPTY、refuse 说明原因。整张图永远不因为模型抽风而崩。

输出先过 `schemas.SQLDraft`（P1-1）：字段/类型/形态不合合同就带着错因重试一次，
仍不合规才落成空 SQL。所以到达 validate 的只有两种东西——合规的 SQL，或者空串。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import re

import gateway
import llm
import promptlib
import schemas
from pydantic import ValidationError
from state import AgentState

# clean_sql 搬去 schemas.py 了：sql 字段的规范化属于交付合同的一部分
# （校验器要拿它先洗掉代码围栏/分号，再判是不是以 SELECT 开头）。
# 这里保留同名导入，是因为 node_react 一直在用 `gen.clean_sql`——
# 为了搬一个函数去改三个调用方，不划算。
from schemas import clean_sql  # noqa: F401

_RE_WS = re.compile(r"\s+")


def sql_fingerprint(sql: str) -> str:
    """自洽投票用的"同一条 SQL"判据。

    直接比较字符串几乎永远不等——换个别名、多个空格就算两条。用 sqlglot 重新
    序列化后比较结构：它会规范大小写和空白，但不会把 `a.id` 和 `b.id` 变一样，
    所以别名差异仍然算不同答案。这是刻意的：别名不同的两版可能真是两种写法，
    让它们分票、再由第一次生成兜底，比强行合并更安全。
    """
    s = (sql or "").strip().rstrip(";")
    if not s:
        return ""
    try:
        import sqlglot
        tree = sqlglot.parse_one(s, read="hive")
        return tree.sql(dialect="hive", normalize=True).lower()
    except Exception:
        return _RE_WS.sub(" ", s.lower()).strip()


def generate_with_llm(state: AgentState, cfg: dict,
                      findings: list[dict] | None = None,
                      prev_sql: str = "") -> tuple[str, str, dict]:
    """返回 (sql, detail, meta)。

    meta 里有 usage / model / elapsed_ms，供 trace 和成本统计用。
    findings 非空 = C 阶段的修复请求。
    """
    repair = bool(findings and prev_sql)
    messages = promptlib.build_repair_messages(state, cfg, findings, prev_sql) \
        if repair else promptlib.build_generate_messages(state, cfg)

    est = promptlib.estimate_tokens(messages)
    pc = cfg.get("prompt") or {}
    sc = pc.get("self_consistency") or {}
    # 自洽采样只在首次生成做。修复路径已经有护栏 fix 指明了改法，
    # 这里再采 3 次等于把同一份 fix 读 3 遍，成本翻倍而分歧极小。
    n = int(sc.get("samples", 3)) if (sc.get("enabled") and not repair) else 1

    cands: list[tuple[str, schemas.SQLDraft, dict]] = []
    errors: list[str] = []
    schema_fail = False
    for i in range(max(1, n)):
        # 多采样时必须抬温度，否则 3 次结果几乎一样，投票没有意义
        temp = float(sc.get("temperature", 0.7)) if n > 1 else None
        got, err = _call_once(messages, cfg, state,
                             "repair" if repair else "generate",
                             temperature=temp, est=est)
        if got:
            cands.append(got)
        else:
            errors.append(str(err.get("error") or err.get("detail") or "?"))
            schema_fail = schema_fail or bool(err.get("schema_error"))
    if not cands:
        return "", "模型调用失败：%s" % (errors[0] if errors else "?")[:200], {
            "error": "; ".join(errors)[:400], "est_in": est,
            "samples": max(1, n), "schema_error": schema_fail}

    sql, obj, out = _pick(cands)
    usage = out.get("usage") or {}
    meta = {
        "model": out.get("model"),
        "elapsed_ms": out.get("elapsed_ms"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        # 网关给的账：和 cost_ledger 里的那一行是同一份数，便于对账
        "cost_cny": out.get("cost_cny"),
        "ledger_ok": out.get("ledger_ok"),
        "ledger_error": out.get("ledger_error"),
        "est_in": est,
        # obj 现在是 schemas.SQLDraft（校验过的），不是裸 dict：
        # 下面这些值能取到，就说明它们至少**类型是对的**。
        "schema": "SQLDraft",
        "schema_error": False,
        "used_tables": list(obj.used_tables),
        "reason": obj.reason,
        "metric_code": obj.metric_code,
        "assumptions": list(obj.assumptions),
        # CoT 产出的计划：validate 会拿 plan.source_table 去核 SQL 有没有真的用它
        "plan": obj.plan,
    }
    if n > 1:
        fps = [sql_fingerprint(c[0]) for c in cands]
        meta["samples"] = len(cands)
        meta["distinct"] = len(set(fps))
        meta["agreement"] = round(fps.count(sql_fingerprint(sql)) / max(1, len(fps)), 2)

    head = "llm 修复" if repair else "llm 生成"
    extra = ""
    if n > 1:
        extra = " ｜ 自洽 %d/%d 一致" % (int(meta["agreement"] * meta["samples"]),
                                        meta["samples"])
    detail = "%s：%s ｜ %s tok / %sms ｜ 口径：%s%s" % (
        head, meta.get("model") or "?",
        usage.get("total_tokens") or "?",
        meta.get("elapsed_ms") or "?",
        (obj.reason or "-")[:80], extra)

    return sql, detail, meta


_RETRY_HINT = """\
【上一次输出不合格】
%s

只输出一个 JSON 对象，字段与上面【输出格式】完全一致；不要多写字段，不要多写文字。"""


def _retry_messages(messages: list[dict], bad: str, errors: list[str]) -> list[dict]:
    """把上一轮的坏输出和具体错因一起带回去，让它改而不是重写一遍。

    `bad` 为空时补一个占位符：空 content 的 assistant 消息会被部分 OpenAI 兼容
    接口判成 400，那时候我们会从"格式不对"直接掉到"调用失败"，反而更难排查。
    """
    return list(messages) + [
        {"role": "assistant", "content": (bad or "(空)")[:2000]},
        {"role": "user", "content": _RETRY_HINT % "\n".join("- " + e for e in errors)},
    ]


def _call_once(messages: list[dict], cfg: dict, state: dict, caller: str,
               temperature: float | None, est: int):
    """一次生成；输出不合规时**只重试一次**。

    返回 ((sql, SQLDraft, out), {}) 或 (None, 错误信息)。

    为什么重试放在这一层：图层的 `after_validate` 本来就有一条"generate_error
    再给一次机会"的边。两处都重试 = 同一条坏输出花 4 次调用，而且第二次是
    **原样再问一遍**，模型没有任何新信息，改对的概率远低于"告诉它缺了什么"。
    所以这里带着上一轮的错因重试一次；两次都不合规就把 `schema_error` 打出去，
    由 node_generate 短路，不再进图层那次重试。
    """
    msgs = messages
    errors: list[str] = []
    for attempt in range(2):
        try:
            # 走网关而不是直连：台账要能回答"这次生成花了多少、是谁调的"。
            # ctx 传 state 里那份 dict，网关据此拿到 trace_id / user_id / role / 预算。
            out = gateway.chat(msgs, cfg, json_mode=True, caller=caller,
                               temperature=temperature, ctx=state.get("ctx"))
        except llm.LLMError as e:
            return None, {"error": str(e), "est_in": est}
        try:
            draft = schemas.parse_sql_draft(out.get("content") or "")
        except (schemas.SchemaError, ValidationError) as e:
            errors = schemas.brief_errors(e)
            if attempt == 0:
                msgs = _retry_messages(messages, out.get("content"), errors)
                continue
            return None, {
                "detail": "模型输出不合规（已重试一次）：%s" % "；".join(errors),
                "error": "schema: " + "；".join(errors),
                "schema_error": True, "est_in": est,
                "raw": (out.get("content") or "")[:300],
            }
        return (draft.sql, draft, out), {}
    return None, {"error": "schema 重试已用尽", "schema_error": True, "est_in": est}


def _pick(cands: list[tuple[str, schemas.SQLDraft, dict]]
          ) -> tuple[str, schemas.SQLDraft, dict]:
    """自洽投票。按结构指纹计票，平票取第一个（即第一次生成的）。

    平票取第一次而不是"最长的那个"：第一次生成没有受到前几轮措辞的污染，
    并且这样在只有 1 个候选时行为与关掉自洽完全一致。
    """
    if len(cands) == 1:
        return cands[0]
    votes: dict[str, int] = {}
    for c in cands:
        fp = sql_fingerprint(c[0])
        votes[fp] = votes.get(fp, 0) + 1
    best = max(votes.values())
    for c in cands:
        if votes[sql_fingerprint(c[0])] == best:
            return c
    return cands[0]
