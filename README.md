# 音乐业务数仓与可信问数 Agent

把自然语言业务问题翻译成 **Doris SQL**，经 27 条 AST 级护栏 + EXPLAIN 成本闸校验后执行，输出「结论 + 数字 + 出处 + 口径版本 + 可复核 SQL」。

一句话定位：**一个会拒绝、会复算、有权限边界的 Text-to-SQL 问数系统**，不是“调个大模型写 SQL”的演示。

---

## 它解决什么问题

| 普通 Text-to-SQL 的硬伤 | 本项目的做法 |
|---|---|
| 模型写错 SQL 就给错数 | 生成 → 校验 → 带 fix 定向重写 → 结果形态复核；校验不过转拒答，不硬凑答案 |
| 数对了但口径错了没人知道 | 语义层作唯一事实源（59 指标 / 16 条口径陷阱）；口径变更带版本号；跨期不可平均、UV 不可跨日相加写成 AST 规则 |
| 一条 SQL 拖垮集群 / 越权看数 | EXPLAIN 成本闸 + 岗位×数据范围两维 RBAC（行级/列级编译期下推）+ 不可变审计 |

**自建 128 题评测集连续 3 轮 128/128，L1/L2/L3 全 100%**（`eval/last.json` 可复核）。期望值独立重算，不拿 Agent 自己的输出自证。

---

## 架构

```
问题 → route（时间窗 / 指标 / 权限）→ generate（SQL + 语义检索）
     → validate（27 护栏 + EXPLAIN 成本闸）→ execute（策略下推 + 基线复算）
     → critique（答案形态）→ narrate（结论 + 出处 + 口径）
     侧路：meta 元问题 / refuse 拒答 / plan 歧义澄清（LangGraph interrupt）
```

十节点固定图 + 条件边，LangGraph 1.1.2 编排；模型走 DeepSeek OpenAI 兼容接口，**全项目只有 `llm.py` 碰模型传输层**。

```
code/agent/        图编排、节点、网关、缓存、长期偏好、Web 服务
code/governance/   RBAC、审计、PII、密钥、成本台账（PostgreSQL）
agent/guards/      guard.py（27 条 AST 护栏，线上与 CI 共用一份实现）
agent/semantic/    指标/维度/口径陷阱 YAML（生成 tables.json）
sql/               ODS→DWD→DWS→ADS 分层加工 SQL（71 个）
eval/              128 题评测集 + 运行器 + 门禁 + 最新结果
dashboard/         Flask + ECharts 看板
```

---

## 快速开始

环境：Python 3.10+，PostgreSQL（治理库）、Doris（查询面）、Redis（可选，答案缓存）。**问数和评测前须先起 Doris 与 PG**，否则 execute 节点会连不上库。

```bash
pip install -r code/agent/requirements.txt

# 先启动 Doris / PostgreSQL / Redis（Windows 用 start.ps1，Linux 见 eval/ci.sh 的环境检查）

# 初始化治理库（不可变审计表、RBAC、会话表）
python -c "import sys; sys.path[:0]=['code','code/agent']; from governance import store; from conf import load_config; print(store.init_schema(load_config()))"

python code/agent/run.py "2026-11-08 的 DAU 是多少"   # 命令行问一句
python code/agent/webapp.py --port 8000               # Web 界面
python eval/run.py --repeat 3 --json eval/last.json   # 128 题 ×3 轮评测
bash eval/ci.sh --quick                               # 提交前门禁（约 1 分钟）
```

Windows 另提供 `setup.ps1` / `start.ps1` / `stop.ps1` 一键脚本。

> 数据是**自造模拟音乐埋点**（曲目元数据来自 MusicBrainz，行为为模拟生成），不含真实用户隐私。指标结论均可在 `eval/` 复现。

---

## 可信度机制

- **护栏 27 条**：SQLGlot AST 静态检查；只读、多语句、越权、PII、注入均为不可重试的硬拒绝。
- **成本闸**：EXPLAIN 估算分区与扫描量，全分区扫大表直接拒，不重试。
- **口径机检**：记录一次真实事故——只算 `order_pay` 漏掉 `vip_subscribe`，收入低估 5.24 倍；四项静态检查全绿仍算错，最终固化为机器规则（见 `docs/52`）。
- **答案层断言**：66 题连答案文本一起校验（含“拒答理由必须命中哪些关键词”）。
- **长期偏好**：只存用户显式说出口的口径偏好，不存历史数字；语义召回复用本地 BGE-small ONNX（见 `docs/64`）。

---

## 技术栈

`LangGraph` · `FastAPI` · `SQLGlot` · `jieba + BM25 + BGE(ONNX)` · `Pydantic` · `PyMySQL / psycopg` · `Redis` · `PostgreSQL` · `Doris` · `Hive on Spark` · `Flink` · `Kafka` · `HBase`

---

## 文档

本仓库只保留与运行和答辩直接相关的文档：`docs/20`（指标字典）、`docs/22`（架构图）、`docs/52`（口径机检）、`docs/57`（面试宝典）、`docs/61`（复测与 A/B）、`docs/64`（长期偏好）、`docs/00`（导航）。

评测结果以 `eval/last.json` 与 `eval/report.md` 为准。
