"""refuse 节点：答不了就说清为什么，不硬凑。

拒答的理由只有两类：需求本身不可答（超出数据范围），或 SQL 没能通过校验。
两类都要给出可操作的信息——前者说明数据截止到哪天，后者列出卡在哪条规则。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

from state import AgentState


def refuse(state: AgentState, cfg: dict) -> dict:
    reason = state.get("unsupported") or ""

    # P1-2：用户在澄清环节选择"不查了"。这时默认措辞（"无法生成可执行的查询"）
    # 会让人以为是系统坏了，必须说清这是用户的主动选择，并告诉他下次怎么问。
    if not reason and state.get("clarify_answer") and state.get("refused"):
        reason = str(state.get("refuse_reason") or "")

    # 契约失败要排在最前面解释。这时 findings 里只会有一条 EMPTY（因为交上来的
    # sql 是空串），按护栏口径念给用户就成了"SQL 没通过校验"——技术上是真的，
    # 但对用户毫无信息量：真实原因是模型没按格式交货，重问一次很可能就好了。
    # 把 generate_error 里的具体错因（"缺 sql 字段"）摆出来，用户才知道下一步
    # 该做什么（重问 / 拆细），而不是以为自己的问题不合法。
    if not reason and state.get("generate_schema_failed"):
        detail = str(state.get("generate_error") or "").strip()
        reason = ("模型这次没按约定的 JSON 格式输出（已自动重试一次仍不合规）：%s"
                  "\n\n这不是你的问题不合法——同一个问题重问一次，"
                  "或者把问题拆细一点，通常就能过。" % (detail or "输出里缺 SQL 字段"))

    if not reason:
        rejects = [f for f in (state.get("findings") or []) if f.get("level") == "REJECT"]
        if rejects:
            bits = []
            for f in rejects[:3]:
                line = "%s %s" % (f.get("rule"), f.get("message"))
                fx = f.get("fix") or {}
                if fx.get("op") == "replace" and fx.get("replacement"):
                    line += "（可改为：%s）" % fx.get("replacement")
                bits.append(line)
            reason = "SQL 未通过护栏校验：\n  - " + "\n  - ".join(bits)
            tries = state.get("attempts", 0)
            if tries > 1:
                reason += "\n\n已自动修正 %d 轮仍未通过，停止重试——" \
                          "继续改下去只会是在猜，不如把问题交回给你。" % (tries - 1)
        elif state.get("generate_error"):
            reason = "模型没能产出可用 SQL：%s" % state["generate_error"]
        else:
            reason = "无法生成可执行的查询"

    return {
        "refused": True,
        "refuse_reason": reason,
        "trace": [{"node": "refuse", "detail": reason.split("\n")[0][:120], "data": {}}],
    }
