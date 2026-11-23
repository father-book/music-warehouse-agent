"""critique 节点（阶段 3）：执行完之后，回头问一句「这答案的形状对吗」。

放在 execute 之后而不是 validate 之前，是因为「形状」必须拿**真实结果**判断：
SQL 合法不代表结果是对的形态——`SELECT SUM(play_start_cnt)` 完全合法，
跑出来一行 12710，护栏全过，但它答不了「排行榜」。

触发条件是「形态明显崩塌」，只返工一次：
  · 排行榜只回 1 行、趋势只回 1 个点、分布只回 1 组
  · 对比题回了多行明细（要的是差值）
  · 占比题的值超出 100
  · 结果整列全 NULL

刻意不做的事
------------
1. 不判断「数值对不对」——那是口径问题，靠语义层和评测集，不靠猜。
2. 不在返工失败后回滚：第二版即使仍不符，也通常比第一版更接近问题意图；
   回滚需要额外保存上一版结果与 SQL，收益不抵复杂度。
3. 不对安全拦截/拒答做二次判断——身份边界不允许"再试一次"。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import shape
from state import AgentState


def _findings(issues: list[str]) -> list[dict]:
    """把形态不符包装成护栏同构的 REJECT，直接复用 C 阶段的修复通道。"""
    return [{
        "level": "REJECT",
        "rule": "SHAPE",
        "message": msg,
        # op=rewrite 而不是 deny：这是可修的，不是身份边界
        "fix": {"op": "rewrite"},
    } for msg in issues]


def critique(state: AgentState, cfg: dict) -> dict:
    contract = shape.classify(state.get("question") or "")
    # `shape_retry_now` 必须在**每一次**返回里显式写死。
    # 踩过的坑：只有发起返工的分支写了 True，其余分支不写，
    # 而 LangGraph 的 state 是"合并"而不是"覆盖"——上一轮的 True 会留到下一轮，
    # 于是 after_critique 一直判"回 generate"，图上无限循环，
    # 直到 RecursionError。122 题评测全绿也发现不了这个 bug，
    # 因为那一轮没有任何一道题触发返工（返工次数 = 0）。
    out: dict = {"shape": contract.as_dict(), "shape_retry_now": False}

    # 这几类不走形态检查：拒答、执行失败、调试直传 SQL、stub 模式
    if state.get("refused") or state.get("security_block"):
        return out
    res = state.get("result") or {}
    if res.get("error") or cfg["generator"]["mode"] != "llm":
        return out
    if state.get("sql") and not state.get("metrics") and not state.get("time_window"):
        # 调试入口（直接给 SQL）没有真实问句语义，形态校验无意义
        return out

    issues = shape.check_result(contract, res, state.get("sql") or "")
    if not issues:
        return {
            **out,
            "trace": [{"node": "critique",
                       "detail": "形态 ✓ %s（%d 行 × %d 列）"
                                 % (contract.label, res.get("row_count") or 0,
                                    len(res.get("columns") or [])),
                       "data": {"shape": contract.as_dict(), "issues": []}}],
        }

    tried = int(state.get("shape_retries") or 0)
    left = (int(cfg["limits"]["max_attempts"]) - int(state.get("attempts") or 0)) > 0
    can_retry = tried < int((cfg.get("limits") or {}).get("shape_retries", 1)) and left

    base = {
        **out,
        "shape_issues": issues,
        "trace": [{"node": "critique",
                   "detail": "形态 ✗ %s：%s%s" % (
                       contract.label, issues[0][:80],
                       "（返工）" if can_retry else "（已达返工上限，如实输出）"),
                   "data": {"shape": contract.as_dict(), "issues": issues,
                            "retry": can_retry}}],
    }
    if not can_retry:
        return {**base, "shape_unresolved": True}

    return {
        **base,
        "shape_retries": tried + 1,
        "shape_retry_now": True,
        "findings": _findings(issues),
        "history": [{"attempt": int(state.get("attempts") or 0),
                     "stage": "shape_rewrite", "rejects": ["SHAPE"]}],
    }
