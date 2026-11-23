"""把一次问答落成不可变审计事件（ADR-006）。

统一在 `graph.ask()` 结尾调用，因此 CLI / eval / web / 元问题 / 拒答 /
执行失败六条入口都覆盖，不需要每个调用方各写一遍。
"""
from __future__ import annotations

from . import store
from .contracts import AuditEvent, PolicyDecision, RequestContext


def record(state: dict, ctx: RequestContext,
           decision: PolicyDecision | None = None,
           cfg: dict | None = None) -> tuple[bool, str]:
    ev = AuditEvent.from_state(state, ctx, decision, cfg)
    return store.insert_audit(ev, cfg)
