"""命令行入口。

    python code/agent/run.py "11-08 的 DAU 是多少"
    python code/agent/run.py "查 DAU" --trace
    python code/agent/run.py --sql "SELECT 1"           # 调试：跳过生成，只验护栏和执行
    python code/agent/run.py "查 DAU" --json            # 给程序消费
"""
from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
import sys


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("question", nargs="?", default="2026-11-08 的 DAU 是多少")
    ap.add_argument("--sql", default=None, help="直接给 SQL，跳过生成（调试用）")
    ap.add_argument("--trace", action="store_true", help="打印每个节点的执行轨迹")
    ap.add_argument("--json", action="store_true", help="输出完整 state 的 JSON")
    a = ap.parse_args()

    from conf import load_config
    from graph import ask
    import vault

    cfg = load_config()
    # 与 webapp 同一个闸：密钥不齐就当场退出，别跑到一半才发现。
    try:
        vault.warmup(cfg)
        vault.assert_ready(cfg)
    except vault.SecretMissing as e:
        print("启动中止：%s" % e)
        return 2

    state = ask(a.question, cfg, a.sql)

    if a.json:
        print(json.dumps({
            "question": state.get("question"),
            # --- 阶段 1 治理字段 ---
            "ctx": state.get("ctx"),
            "trace_id": state.get("trace_id"),
            "policy_decision": state.get("policy_decision"),
            "applied_sql": state.get("applied_sql"),
            "security_block": state.get("security_block"),
            "audit_error": state.get("audit_error"),
            "elapsed_ms": state.get("elapsed_ms"),
            "lane": state.get("lane"),
            "time": [state.get("time_start"), state.get("time_end")],
            "time_window": state.get("time_window"),
            "dimensions": state.get("dimensions"),
            "dimension_cautions": state.get("dimension_cautions"),
            "retrieved_tables": state.get("retrieved_tables"),
            "freshness": state.get("freshness"),
            "tables": state.get("tables"),
            "metrics": state.get("metrics"),
            "sql": state.get("sql"),
            "verdict": state.get("verdict"),
            "attempts": state.get("attempts"),
            "exec_retries": state.get("exec_retries"),
            "meta_intent": state.get("meta_intent"),
            "meta_answer": state.get("meta_answer"),
            "findings": state.get("findings"),
            "result": state.get("result"),
            "answer": state.get("answer"),
            "refused": state.get("refused"),
            "refuse_reason": state.get("refuse_reason"),
            "generate_error": state.get("generate_error"),
            "history": state.get("history"),
            "fix_log": state.get("fix_log"),
            "trace": state.get("trace"),
        }, ensure_ascii=False, indent=2))
        return 1 if state.get("refused") or (state.get("result") or {}).get("error") else 0

    print(state.get("answer", "（无输出）"))

    if a.trace:
        print("\n" + "-" * 60)
        for i, t in enumerate(state.get("trace") or [], 1):
            print("%d. %-10s %s" % (i, t.get("node"), t.get("detail")))

        # C 阶段的核心可观测性：每版 SQL 都留痕，答辩展示"被拦 → 改 → 过"用它
        sqls = [h for h in (state.get("history") or []) if h.get("sql")]
        if len(sqls) > 1:
            print("\n" + "=" * 60)
            print("SQL 演化（共 %d 版）" % len(sqls))
            for h in sqls:
                print("\n--- 第 %s 轮 · %s ---" % (h.get("attempt"), h.get("stage")))
                print((h.get("sql") or "").strip())
            if state.get("fix_log"):
                print("\n机械修复动作：")
                for x in state["fix_log"]:
                    print("  · " + x)

    if state.get("refused") or (state.get("result") or {}).get("error"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
