#!/usr/bin/env bash
# =============================================================================
# 一键回归门禁（阶段 2）
#
#   bash eval/ci.sh            # 全量：护栏 → 语义层 → 覆盖矩阵 → 评测 → 门禁
#
# 公开版说明：开发期 33 个 selftest 不在公开仓库内，相关步骤已移除。
# 保留的是任何 clone 都能跑的检查：护栏用例、语义层一致性、覆盖矩阵、
# 评测执行与三点退化门禁。
#   bash eval/ci.sh --quick    # 只跑零 LLM 部分（约 1 分钟，提交前用）
#   bash eval/ci.sh --update   # 把本次评测结果定为新基线（**只在确认变更是预期时用**）
#
# 退出码：0 = 全部通过；1 = 有失败或门禁不通过；2 = 环境问题（连不上集群等）
#
# 为什么要有这个脚本：
#   门禁如果只是"一个可以手动跑的文件"，它就一定不会被执行。
#   这里把 4 道检查串成一条命令，任何一道红就中断——
#   本地改完直接 `bash eval/ci.sh`，比记 5 条命令可靠。
#
# 挂到 git 的 pre-commit（仓库初始化后执行一次即可）：
#   git config core.hooksPath .githooks
# =============================================================================
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 2
PY="${PY:-python}"

MODE="full"
UPDATE=""
for a in "$@"; do
  case "$a" in
    --quick)  MODE="quick" ;;
    --update) UPDATE="--update-baseline" ;;
    *) echo "未知参数：$a"; exit 2 ;;
  esac
done

FAILED=0
step() { printf "\n\033[1m▶ %s\033[0m\n" "$1"; }
ok()   { printf "  \033[32m✓ %s\033[0m\n" "$1"; }
bad()  { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAILED=1; }

# ---- 0. 环境：集群能不能连上 ------------------------------------------------
step "环境自检"
if "$PY" -c "
import sys; sys.path.insert(0,'code'); sys.path.insert(0,'code/agent')
import _bootstrap
from db import run_sql
r = run_sql('SELECT 1')
sys.exit(0 if not r.get('error') else 1)
" 2>/dev/null; then
  ok "Doris 可连接"
else
  bad "Doris 不可连接（163:9030），后续步骤会大量失败——先起集群"
  exit 2
fi

if "$PY" -c "
import sys; sys.path.insert(0,'code'); sys.path.insert(0,'code/agent')
from conf import load_config
from governance import store
store.connect(load_config()).close()
" 2>/dev/null; then
  ok "治理库可连接"
else
  bad "PostgreSQL 治理库不可连接（55432）"
  exit 2
fi

# ---- 1. 护栏 ----------------------------------------------------------------
step "护栏回归（agent/guards/test_cases.sh）"
if (cd agent/guards && PY="$PY" bash test_cases.sh >/tmp/ci_guard.log 2>&1); then
  ok "$(grep -E '全部 .* 条用例符合预期' /tmp/ci_guard.log | tail -1)"
else
  bad "护栏用例失败，详见 /tmp/ci_guard.log"
  grep -E "FAIL|ERR" /tmp/ci_guard.log | head -10
fi

# ---- 2. 自测（零 LLM 的部分总是跑；模型相关的只在全量模式跑）----------------
step "语义层一致性"
if "$PY" agent/semantic/sem_check.py >/tmp/ci_sem.log 2>&1; then
  ok "$(tail -1 /tmp/ci_sem.log)"
else
  bad "sem_check 失败，详见 /tmp/ci_sem.log"
  tail -5 /tmp/ci_sem.log
fi



step "覆盖矩阵"
# strict：有缺口就红。覆盖不是"差不多就行"——缺一格就意味着有一类问法
# 从没被验证过，而这种事在通过率上看不出来。
if "$PY" eval/matrix.py --strict --json eval/matrix.json >/tmp/ci_matrix.log 2>&1; then
  ok "$(grep -E '格达标' /tmp/ci_matrix.log | tail -1 | sed 's/^ *//')"
else
  bad "覆盖矩阵有缺口"
  sed -n '/缺口/,$p' /tmp/ci_matrix.log | head -12
fi








step "一键部署冒烟（AC-20）"
# 判据是"依赖被真的问过一次"，不是"进程在不在"。
# CI 里通常没有起 webapp，所以跳过 Web 两项；PG / Redis / 表结构 / 触发器照查。
# Redis 那条尤其重要：配置要 redis 却回落 local 时，必须红。
if "$PY" code/agent/deploy_smoke.py --skip-web >/tmp/ci_deploy.log 2>&1; then
  ok "$(grep -E '通过 /' /tmp/ci_deploy.log | tail -1)"
else
  bad "deploy_smoke 失败，详见 /tmp/ci_deploy.log"
  grep -E "\[FAIL\]" /tmp/ci_deploy.log | head -6
fi

step "缓存后端"
# 配置说要用 redis，实际却回落了 local —— 这是 CI 该发现的问题，不能算通过。
# 反过来，配置本来就是 local 的机器上，这里安静地显示 local 即可。
if "$PY" -c "
import sys; sys.path.insert(0,'code'); sys.path.insert(0,'code/agent')
from conf import load_config
import answercache as A
cfg = load_config(); s = A.get_cache(cfg).stats()
want = (cfg.get('cache') or {}).get('backend', 'local')
if want == 'redis' and s.get('backend') != 'redis':
    print('配置要求 redis，实际回落 %s：%s' % (s.get('backend'), s.get('init_warning') or s.get('reason')))
    sys.exit(1)
if s.get('degraded'):
    print('后端 %s 已降级：%s' % (s.get('backend'), s.get('reason')))
    sys.exit(1)
print('%s ｜ %s 键 ｜ TTL %ss' % (s.get('backend'), s.get('size'), s.get('ttl_sec')))
" > /tmp/ci_backend.log 2>&1; then
  ok "$(tail -1 /tmp/ci_backend.log)"
else
  bad "$(tail -1 /tmp/ci_backend.log)"
fi

step "答案层判据"
# 尺子本身也要校准：判据换成"看答案"以后，全绿只说明答案里有那串字。
if "$PY" eval/selftest_answer.py >/tmp/ci_ans.log 2>&1; then
  ok "$(tail -1 /tmp/ci_ans.log)"
else
  bad "selftest_answer 失败，详见 /tmp/ci_ans.log"
  grep -E "^  ✗" /tmp/ci_ans.log | head -6
fi


step "护栏开销"
# 只测延迟，不发问答。常驻模式如果没生效（比如退回了一次性 subprocess），
# 这里 P50 会从 ~1ms 跳到 ~300ms，一眼就能看见——比在读代码时发现要早。
if "$PY" eval/loadtest.py --guard-bench --n 0 >/tmp/ci_bench.log 2>&1; then
  ok "$(grep -E '常驻进程' /tmp/ci_bench.log | sed 's/^ *//')"
else
  bad "护栏基准跑不通"
  tail -5 /tmp/ci_bench.log
fi

# ---- 3. 评测 ----------------------------------------------------------------
step "评测（$MODE）"
EVAL_ARGS=(--json eval/last.json)
# quick 模式的结果不完整，必须写另一个文件——否则会把上次全量结果覆盖掉，
# 下一次 gate 就会拿 12 题的结果去比 90 题的基线，报一堆"分域消失"的假警报。
# --json 和 --md 都要分流：run.py 现在对未指定 --md 的 partial 跑会自动落到
# report_partial.md，这里再显式指定一份，双保险且不污染 ci.sh 的 quick 产物。
QUICK_ARGS=(--quick --json eval/last_quick.json --md eval/report_quick.md)
if [ "$MODE" = "full" ]; then
  if "$PY" eval/run.py "${EVAL_ARGS[@]}" >/tmp/ci_eval.log 2>&1; then
    ok "$(grep -E '^[0-9]+/[0-9]+ 通过' /tmp/ci_eval.log | tail -1)"
  else
    bad "评测有失败用例，详见 /tmp/ci_eval.log"
    grep -E "^  - " /tmp/ci_eval.log | head -8
  fi
else
  if "$PY" eval/run.py "${QUICK_ARGS[@]}" >/tmp/ci_eval.log 2>&1; then
    ok "$(grep -E '^[0-9]+/[0-9]+ 通过' /tmp/ci_eval.log | tail -1)"
  else
    bad "零 LLM 用例有失败，详见 /tmp/ci_eval.log"
  fi
fi

# ---- 4. 门禁 -----------------------------------------------------------------
# quick 模式的结果不完整，不能拿它覆盖基线，也不能拿它做"分域消失"判定
if [ "$MODE" = "full" ]; then
  step "门禁比对"
  if "$PY" eval/gate.py $UPDATE > /tmp/ci_gate.log 2>&1; then
    ok "$(tail -1 /tmp/ci_gate.log)"
    sed -n '/改善/,$p' /tmp/ci_gate.log | head -12
  else
    bad "门禁不通过"
    sed -n '/门禁/,$p' /tmp/ci_gate.log | head -25
  fi
else
  step "门禁比对"
  echo "  跳过（quick 模式不做门禁，避免用不完整结果覆盖基线）"
fi

printf "\n"
# 历史看板每次重渲染：它是从 history.jsonl 现算的，不缓存中间状态，
# 所以"CI 过了但看板上没有这一轮"这种情况不会发生。
if "$PY" eval/history.py --html >/tmp/ci_hist.log 2>&1; then
  echo "$(tail -1 /tmp/ci_hist.log)"
fi

if [ "$FAILED" -eq 0 ]; then
  printf "\033[32m全部通过\033[0m\n"
else
  printf "\033[31m有失败项，见 /tmp/ci_*.log\033[0m\n"
fi
exit "$FAILED"
