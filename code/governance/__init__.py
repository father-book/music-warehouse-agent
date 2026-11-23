"""治理层（阶段 1）。

目录名为什么不是 `platform`：Python 标准库里有 `platform` 模块，
把 `code/` 加进 sys.path 后建同名包会遮蔽它，langgraph / uvicorn 等依赖
都会 import platform，出问题时极难排查。所以包名定为 `governance`，
`docs/35` 第七节里写的 `code/platform/contracts.py` 以本目录为准。

对外只暴露四个核心契约；具体实现（identity / policy / store）按需导入。
"""
from .contracts import (  # noqa: F401
    AuditEvent,
    PolicyDecision,
    QueryPlan,
    RequestContext,
    RowRule,
    new_trace_id,
)

__all__ = [
    "RequestContext", "AuditEvent", "QueryPlan", "PolicyDecision", "RowRule",
    "new_trace_id",
]
