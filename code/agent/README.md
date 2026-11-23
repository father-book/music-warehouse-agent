# Data Agent · 问数智能体

把业务问题翻译成 Doris SQL，经 27 条护栏校验后执行，输出「结论 + 数字 + 出处 + 口径 + 复算 SQL」。

基于 **LangGraph 1.1.2** 编排，模型走 **DeepSeek**（OpenAI 兼容接口）。

---

## 一、用 PyCharm 打开

### 第一步：打开目录

`File → Open`，选这个目录：

```
<项目根>/code/agent
```

> **不要打开项目根 `毕业设计`**。那样 PyCharm 会把 42 张表的 SQL、文档、Python
> 全混在一个工程里，`import semantic` 这类同级导入会标红。
> 本目录已经带了 `.idea/` 配置，直接把 `code/agent` 当项目根打开即可。

### 第二步：配解释器（**这一步最容易踩坑**）

本项目**自带一个 `venv/`**，它已经配好了，不要再指向别的 Python。

`File → Settings → Project: agent → Python Interpreter → Add Interpreter →
Add Local Interpreter → Existing`，路径填：

```
<项目根>/code/agent/venv/Scripts/python.exe
```

确认解释器版本显示 **3.13.x**，且包列表里能看到 `PyYAML`、`pymysql`、`langgraph`。

> **为什么自带 venv 还带 `--system-site-packages`？**
> 本机 `pip install` 直连 PyPI 和清华镜像都失败（SSL 握手超时），
> 而 langgraph 的依赖树有 20 多个包。于是 venv 建在系统 Python 3.13 之上并继承它的
> site-packages，缺的那几个（`idna`/`certifi`/`urllib3`/`packaging`/`jsonpatch`）
> 由 `_vendor/` 提供，`_bootstrap.py` 在启动时注入 `sys.path`。
>
> 代价：这个 venv 依赖 `C:\Users\<你>\.workbuddy\binaries\python\versions\3.13.12`
> 这个基础解释器。换机器时重建：

```bash
# 换机器时的完整重建步骤
<任意 Python 3.13> -m venv --system-site-packages venv
venv/Scripts/pip install --no-index --find-links=_wheels -r requirements.txt
```

`_wheels/` 里有 17 个 wheel，覆盖 langgraph 那棵依赖树里装不上的部分。

### 常见报错对照

| 报错 | 原因 | 修法 |
|---|---|---|
| `No module named 'yaml'` | 解释器指向了一个空 venv（常见于别的项目的 `.venv`） | 照第二步指到 `code/agent/venv/Scripts/python.exe` |
| `No module named 'idna'` / `'packaging'` | 绕过 `run.py` 直接 import 了 httpx/langgraph | 正常现象：只有经 `_bootstrap` 启动才有 `_vendor` 兜底 |
| `No module named 'langgraph'` 但命令行能跑 | PyCharm 索引没刷新 | `File → Invalidate Caches → Restart` |

### 第三步：确认 `.env`

`code/agent/.env` 已存在且已加入 `.gitignore`，内容形如：

```ini
LLM_API_KEY=sk-...
DORIS_RO_PASSWORD=...
```

没有的话照这个格式新建一份。**不要把 key 提交到 git。**

### 第四步：跑

打开右上角运行配置下拉框，四个配置已经建好：

| 配置 | 作用 | 预期 |
|---|---|---|
| `01 问数 · DAU` | 完整问答链路 | 输出 DAU=3,237 |
| `02 问数 · 各省份活跃用户` | 需要 GROUP BY 的问题 | 31 行 |
| `03 冒烟 · A 阶段` | 骨架/护栏/执行面 6 条 | `6/6 通过` |
| `04 自测 · B/C 阶段` | 生成与修复 14 条 | `14/14 通过` |
| `05 自测 · D 阶段` | 时间/检索/新鲜度 48 条 | `48/48 通过` |
| `06 前端 · Web UI` | 启动问数页面 | 浏览器打开 `http://127.0.0.1:8000` |

也可以在 Terminal 里直接跑：

```bash
python run.py "2026-11-08 的 DAU 是多少" --trace
```

---

## 二、目录结构

```
code/agent/
├── config.yaml            所有可调项（执行面、限制、模型、链路开关）
├── .env                   LLM key + Doris 只读密码（gitignored）
│
├── graph.py               LangGraph 编排：route → generate → validate ⟲ → execute → narrate
├── state.py               唯一的状态类型 AgentState
│
├── node_route.py          问题 → 时间窗 / 指标 / 维度 / 候选表
├── timeparse.py           D 阶段：相对时间、区间、裁剪、新鲜度
├── retrieval.py           D 阶段：jieba + BM25 倒排 + 维度/明细意图
├── node_generate.py       SQL 来源三选一：stub / 首轮生成 / 失败修复
├── node_generate_llm.py   DeepSeek 生成（B 阶段）
├── node_validate.py       调护栏（subprocess）
├── node_execute.py        唯一真正跑 SQL 的节点
├── node_refuse.py         答不了就说清为什么
├── node_narrate.py        包成「结论 + 出处 + 口径 + 复算」
├── trust.py               答案可信度：四项结果自检 + 链路信号 → 高/中/低
│
├── promptlib.py           prompt 构建（铁律 / 上下文 / 重写边界 / 输出格式）
├── llm.py                 DeepSeek 客户端（全项目唯一碰模型的地方）
├── fixer.py               C 阶段：机械修复 + 别名转换
├── semantic.py            语义层读取与检索
├── guardtool.py           护栏调用封装（subprocess 调 guard.py）
├── db.py                  执行面（全项目唯一 import pymysql 的地方）
├── conf.py / env.py      配置与环境变量
├── _bootstrap.py          把本目录和 _vendor 注入 sys.path
│
├── run.py                 命令行入口
├── webapp.py              F 阶段：FastAPI 前端服务（/api/ask）
├── web/index.html         单页界面（内联 CSS/JS，零前端构建）
├── smoke.py               A 阶段冒烟（6 条，零 LLM 依赖）
│
├── tests/                 【33 个 selftest_* 自测脚本 + _bootstrap.py 路径适配层】
│   _bootstrap.py          暴露 HERE=code/agent、ROOT=项目根；脚本仍从项目根运行
│   selftest_bc.py         B/C 阶段自测（14 条）
│   selftest_d.py          D 阶段自测（48 条，--quick 可跳过模型）
│   selftest_trust.py      可信度自测（37 条，零 LLM；量级复算走打桩）
│
├── _vendor/               内置兜底：jieba 分词 / jsonpatch / idna 等
├── _wheels/               离线 wheel，换机器可直接 --find-links 安装
└── venv/                  项目自带解释器（PyCharm 指向这个，gitignored）

# 项目根下、与本目录平级
../eval/cases.yaml         E 阶段评测集（128 题 / 8 域）
../eval/run.py             评测运行器
../eval/report.md          最近一次评测报告
../docs/32-双引擎重算结果.md  35 天窗口基线（expected 的来源之一）

```

---

## 三、三处「只此一处」

这不是代码风格，是结构性约束——它们让某些错误**在类型上就不可能发生**。

**1. 只有 `db.py` 能 `import pymysql`。**
别的模块想查数只能调 `run_sql()`。模型拿不到连接，也就不存在"它偷偷执行一条 DELETE"。

**2. 只有 `llm.py` 能发 LLM 请求。**
调用方拿不到 key。换模型只改 `config.yaml` 的 `base_url` / `model`。

**3. 护栏不在 Agent 里实现第二份。**
`guardtool.check()` 用 subprocess 调 `agent/guards/guard.py --json`，每次多花 0.3 秒，
换来的是「Agent 的判断」和「CI 里那 34 条回归用例」永远是同一套规则。
护栏改了只改一个地方，不会出现"线上放行、CI 拦下"。

---

## 四、生成与修复是怎么走的

```
问题
  ↓
route        解析时间区间（裁剪后）／指标／维度／候选表；
             实时词和完全无数据的区间直接判不可答
  ↓
generate     ① stub：固定 SQL（只在 A 阶段用）
             ② 首轮：DeepSeek 生成，输出 JSON {sql, used_tables, reason}
             ③ 重试：先机械修复，再模型重写
  ↓
validate     27 条护栏，返回 pass / warn / reject + 每条的结构化 fix
  ↓
  ├─ pass/warn → execute → narrate
  └─ reject    → 还有余量就回 generate，否则 refuse
```

### 修复的两级

| 级别 | 触发条件 | 成本 | 覆盖 |
|---|---|---|---|
| **L1 机械修复** | 护栏的 fix 带 `op=replace` 或 `add_condition` | 0 token，<10ms | R17 跨 catalog、R18 扇出 |
| **L2 模型重写** | L1 修不好，或 fix 是一句人话 | 一次调用，约 3 秒 | R9 SELECT *、R5 缺 LIMIT、语义错 |

L1 有个容易漏的细节：护栏给的 fix 是**逻辑表名**（`dim_user.is_current = 'Y'`），
而 SQL 里可能写成 `JOIN dim_user u`。这时直接把条件拼进去，Doris 会报
`Unknown column 'is_current' in 'dim_user'`——护栏放行了，执行才炸。
所以 `fixer.rewrite_alias()` 会把条件里的表名换成实际别名。这一条有专门的回归用例。

---

## 五、常用命令

```bash
# 正常问答
python run.py "2026-11-08 的 DAU 是多少" --trace

# 看完整 state（含每轮 SQL、护栏 findings、token 用量）
python run.py "查 DAU" --json

# 调试护栏：直接给 SQL，跳过生成
python run.py "x" --sql "SELECT * FROM dwd_session WHERE dt='2026-11-08' LIMIT 5"

# 换回 stub 模式（不花 token，只验管道）：改 config.yaml 的 generator.mode
```

`--trace` 会额外打印 SQL 演化，这是答辩要展示的那一段：

```
SQL 演化（共 2 版）

--- 第 2 轮 · llm_repair ---
SELECT DISTINCT user_id FROM dwd_session WHERE dt = '2026-11-08'

--- 第 3 轮 · llm_repair ---
SELECT DISTINCT user_id FROM dwd_session WHERE dt = '2026-11-08' LIMIT 1000
```

---

## 六、已知限制

- **只走离线**。`lane.realtime.enabled = false`，最新分区 2026-11-08。
  问"今天""最近一小时"会被拒答，理由写清是实时链路停摆，不是不会查。
- **时间基准默认固定**。`config.yaml → clock.mode: fixed` + `fixed_today: "2026-11-09"`。
  数据是冻结的造数数据，用真实时钟会让「昨天」逐日漂出范围、越跑越不可用。
  接上实时链路后切 `real`，`timeparse.clocks()` 两种都支持。
  `fixed_today` 与 `lane.offline.data_until` 要一起改，别只改一个。
- **`data_since` / `data_until` 是硬边界**。实测只有 35 个分区（10-05 ~ 11-08），
  请求窗口超出会被裁剪并在答案里写明少了几天；完全落在范围外直接拒答。
- **SQL 里没有 `dt` 条件的表会被 R6 拦**。`ads_user_retention` 用 `cohort_date` 分区，
  护栏已放行这一种。
- **tab 补全/语法高亮依赖 PyCharm 的解释器配置**。解释器没配好时
  `import langgraph` 会标红，但命令行跑得动——是索引问题，不是装的包有问题。

---

## 七、改这个项目前先看

| 想改什么 | 改哪儿 | 注意 |
|---|---|---|
| 换模型 | `config.yaml → generator` | `temperature` 必须为 0，同一个问题要能复现同一版 SQL |
| 加护栏规则 | `agent/guards/guard.py` | 改完必须跑 `agent/guards/test_cases.sh`（34 条，27 条规则各有正反例） |
| 改指标口径 | `agent/semantic/metrics.yaml` | 改完跑 `python agent/semantic/gen_tables.py` 再跑 `sem_check.py` |
| 改执行面（换库） | `config.yaml → execution.init_sql` | 同时要改护栏的 `BAD_CATALOG` / `BAD_DB`，否则 R17 形同虚设 |
| 调 prompt | `promptlib.py` 的 `RULES` | 每条铁律后面标了对应的护栏编号，删之前先确认那条护栏还在 |

---

## 八、D 阶段：时间 / 检索 / 新鲜度

### 8.1 时间不再是「一天」

A 阶段只认显式日期和「昨天/前天」，问「上个月」会**静默**答成 11-08 一天。
现在 `timeparse.py` 输出一个完整区间，并做三件事：

| 能力 | 例子 | 实际窗口 |
|---|---|---|
| 相对日 | 昨天 / 前天 | 相对**今天**算（11-08 / 11-07），不是相对数据截止日 |
| 区间 | 最近 7 天 / 上周 / 上个月 / 10 月 / 10-05 到 10-12 | 见下 |
| 裁剪 | 上个月（10-01~10-31） | **10-05~10-31**，答案写明「少了 4 天」 |
| 裁剪 | 最近 7 天（11-03~11-09） | **11-03~11-08**，写明「少了 1 天」 |
| 拒答 | 今天 / 2026-12-01 | 实时词判 `realtime`，超范围判 `empty` |

双基准：`today`（可固定）与 `data_until` / `data_since`（实测 10-05~11-08）。
**相对词相对 today 算**——A 阶段把「昨天」相对 `data_until` 算成 11-07，是错的。

`lag_days = today - data_until`。超过 1 天时，
`freshness_note()` 会输出「⚠ 数据滞后 N 天」，这是 P1-10 的验收点。

### 8.2 检索：jieba + BM25 倒排

42 张表、58 个指标各自编成一篇「文档」，按字段加权（表名/中文名 ×3、
caveats/trap ×2、列名/公式 ×1），用标准 Okapi BM25 排序。
（检索语料含实时层 9 张表，所以比离线语义层的 33 张表多；稠密/混合召回见 `vecindex.py`。）

三条关键设计：

1. **领域词典只注册 2~6 字短词**。把整句中文名（「会话与启动退出事件」）
   注册成 jieba 用户词，会让文档里不再出现「启动」「会话」，
   实测「有哪些用户启动了 App」就召不到 `dwd_session`。
2. **软/硬加权分开**。维度命中和明细意图的 boost 只作用于**本来就有词面匹配**
   的文档；否则给所有 `dwd_*` 表无条件加分，会凭空冒出一堆无关的
   `dwd_skip` / `ods_user_change`。精确点名表名才用 hard_boost。
3. **该不该 JOIN 由 `dimensions.yaml` 判定**，不是手写关键词。
   纯指标问题（「11-08 的 DAU」）不命中任何分组维度，就只查指标 source；
   命中 `province` 才补维表，并把 `caution`（分省 DAU 求和 > 全站 DAU）送进 prompt。

jieba 放在 `_vendor/jieba`（精简掉 30MB 词性/关键词模型，只留分词必需的
7.4MB），不依赖 pip；缺失时自动退化成中文二元切分。

### 8.3 这轮抓到的两个真 bug

| bug | 症状 | 根因 |
|---|---|---|
| **无指标时 prompt 里一张表都没有** | 模型修 R9 时编出 `dws_user_daily.user_id` / `is_active` / `app_launch_cnt`，被 R4 连拦两轮耗尽重试 | 一条指标、一个维度都没命中时，`route` 返回空表清单；D 阶段删掉 A 阶段的手写关键词后漏了兜底 |
| **长术语注册进分词词典破坏召回** | 「有哪些用户启动了 App」召不到 `dwd_session` | `jieba.add_word("会话与启动退出事件")` 把「启动」吞掉了 |

修法：无指标时启用 BM25 兜底召回；`add_word` 限长；
修复请求里新增【重写边界】块，把「只能用哪些表、哪些列」贴在输出要求前面。
两者都有专门的回归用例（`t_no_metric_recall` / `t_repair_boundary`）。

### 8.4 验收

```
tests/selftest_d.py        48/48（46 条确定性 + 2 条端到端）
smoke.py                   6/6
tests/selftest_bc.py      14/14
```

三份自测刻意分开：骨架管道 / 模型生成与修复 / 语义检索与时间，
混在一起时任何一环抖动都会让整片变红，反而看不出哪坏了。

真实问答：

```
最近7天的播放量
→ 窗口 2026-11-03 ~ 2026-11-08（truncated，写明少了 1 天）
→ SQL: ... WHERE dt BETWEEN '2026-11-03' AND '2026-11-08'
→ 76,630

上个月各省份的活跃用户数
→ 窗口 2026-10-05 ~ 10-31（truncated，写明少了 4 天）
→ 命中 province 维度，带 caution
→ SQL: SELECT province, COUNT(DISTINCT ...) FROM dwd_session
       WHERE dt BETWEEN ... GROUP BY province
```

---

## 九、E / F 阶段：评测集与前端

### 9.1 E 阶段评测集（首版 42 题 → 现 128 题）

> **2026-11-15 补**：评测集已扩到 **128 题 / 8 域**（`eval/cases.yaml` 96 题
> + `eval/adversarial.yaml` 32 题），命令与结构仍适用；下面的 42 题是 E 阶段
> 首版的规模，只作沿革记录。
> 之后又加了三个工具，改口径/改 prompt 时都要用：
>
> ```bash
> python eval/run.py --repeat 3 --json eval/last.json  # 多数投票（2/3 才算 PASS）
> python eval/caliber_impact.py --save                 # 改口径**之前**存语义层快照
> python eval/caliber_impact.py --gate                 # 改完看影响面，顺带跑门禁
> ```
>
> `caliber_impact` 按 route 的真实召回算影响面（不是关键词匹配），
> 改一条指标会列出"哪几道评测题会被它影响"，不用盲跑全量 5 分钟。

```bash
python eval/run.py                 # 全量 128 题（约 300 秒）
python eval/run.py --quick         # 只跑 46 条零 LLM 的用例（约 5 秒）
python eval/run.py --tag 多日窗口   # 按标签过滤
python eval/run.py --id V21        # 单题复现
```

`eval/cases.yaml` + `eval/adversarial.yaml`：**81 条数值题 + 44 条拒答题 + 3 条元问题**，
每题带 `expected` / `tolerance`（或 `expect_reason` / `expect_text`）/ `tags`。

期望值不是拍脑袋写的，也不是 Agent 自己的输出：

- 单日 11-08 → `code/ground_truth_independent.py --only doris --window 2026-11-08 2026-11-08`
- 多日窗口与维度基数 → 在 `hive_catalog.music_dw` 上独立 SQL 实测
- **`docs/32` 那 47 个值是 35 天窗口，不能直接拿来当单日 expected**（这是最容易踩的坑）

拒答题覆盖 D 阶段的 `realtime` / `empty` 两类边界，
以及 stub 模式下「护栏确实拦得住」的三类：R17 跨 catalog、R18 SCD2 扇出、写操作。
这 10 条全部不调模型，2 秒跑完，可以当 CI 用。

### 9.2 评测抓到的三个真 bug

第一次全量是 **34/36**，两条失败都不是模型乱写，是口径漂移——
而且语义层早就预言了这两个错值：

| 题 | 模型算出 | 正确 | 语义层里已有的话 |
|---|---|---|---|
| V17 人均播放次数 | 5.0414 | 3.9265 | `avg_play_per_user.note`：「换成 play_uv 会得到 5.05」 |
| V18 ARPU | 26.6921 | 7.4708 | `arpu.trap`：「`dws_revenue_daily.arpu` 实为 ARPPU」 |

逐个查下去，是三个不同的洞：

1. **glossary 缺同义词**：「人均播放次数」没映射到 `avg_play_per_user`
   （语义层中文名是「人均起播次数」），那条精准的 note 根本没进 prompt。
2. **derived 指标的 `sources`（复数）没被消费**：`_collect_tables` / `_pick_tables`
   只认 `source` / `ads`，于是 ARPU 的候选表为空、BM25 兜底召回
   `dws_revenue_daily`，模型顺着就用了它的 `net_revenue / pay_user_cnt`。
3. **trap 是叙述性的，模型会当背景略过**：给 `additivity=none` 的指标补了
   「formula 逐字照用」的硬约束，并把 formula 引用的指标定义**就地展开**
   （分子分母各自的公式、来源表、过滤条件一次给全）。

修完 **36/36**。第 2 条最值得记：**元数据字段定义了却没被消费，
比字段缺失更隐蔽**——`sem_check` 查的是"字段齐不齐"，查不出"有没有人读它"。

### 9.3 F 阶段：问数前端

```bash
python code/agent/webapp.py                 # http://127.0.0.1:8000
python code/agent/webapp.py --port 8080
```

只绑定 `127.0.0.1`——这个页面能查生产数仓，别暴露到局域网。

三段式界面：

- **左 · 答案**：结构化数字卡片 / 结果表格、时间窗口与新鲜度、维度陷阱提示、
  带复制按钮的复算 SQL。
- **右 · Trace**：节点步骤树、时间解析摘要、命中维度与候选表、
  护栏 findings 徽章、**SQL 演化逐版对照**、机械修复动作。
  每一步可展开看原始 JSON。
- **顶栏**：执行面 `hive_catalog.music_dw`、数据截止日、27 条护栏、当前模型。

快捷问题里特意放了两个拒答 demo（「今天 DAU 多少」「2026-12-01 的播放量」），
以及一个会触发 R18 修复闭环的「11-08 各省份活跃用户数」——
答辩时点一下就能看到「被拦 → 改 → 过」。

**为什么不是 Chainlit**：本机离线 PyPI 镜像里 chainlit 2.12.0 的硬依赖
`literalai==0.1.201` 不存在（最高 0.0.102）、`syncer` 没有任何 wheel；
而自动"取最新版本"抓包会抓到 `pydantic 1.8.2` / `PyJWT 1.6.0`，
装上去直接摧毁已跑通的 A~D 阶段。已装的 FastAPI 0.135 + Uvicorn 0.46
零依赖提供同样能力，trace 面板还能完全按答辩需要设计。
接口只有 `GET /`、`GET /api/meta`、`POST /api/ask`，方便以后换成别的壳。

### 9.4 验收

```
eval/run.py       128/128（81 数值 + 44 拒答 + 3 元问题）
smoke.py            6/6      tests/selftest_bc.py  14/14      tests/selftest_d.py  48/48
sem_check.py     175/0      护栏 test_cases.sh 34/34
webapp.py        /api/meta 200 ｜ / 200 ｜ /api/ask 返回完整 state
```

### 9.5 前端实测反馈的两处修正

用户截图里出现两件事：「昨天一共有多少人退款了」连续 3 轮 `REJECT 2` 后拒答；
答案卡片的「数据源」把**指标 code** 当成表名显示（`…· revenue_sum`）。

- **前端**：数据源改为从 SQL 里 `FROM/JOIN` 解析真实表名，另加「命中指标」一行。
- **C 阶段**：新增 `_attempt_history_block()`，把前几轮被拦的 SQL 与规则编号
  一起摆进修复 prompt。原来每轮只看得见「上一版 SQL + 本轮 findings」，
  于是模型换个姿势又撞同一条规则——**修复闭环缺的不是再试一次，是记忆**。
- **评测**：新增 `V27` 固化这一问（expected=26，正确链路是 R16 游客实体 →
  机械修复 → pass），并加 `t_repair_history` 确定性用例。

本地连续复现 6 次均为 2 轮通过。

> **改完 `promptlib.py` 后必须重启 `webapp.py`**：Python 模块在进程启动时导入，
> 文件改动不会热加载。页面一直开着、服务没重启时，用的还是旧 prompt——
> 这也是「本地命令行跑得通、页面却在拒答」最常见的原因。

### 9.6 「消费」不是「播歌」：一次语义歧义修正

用户前端实测：「昨天消费最多的用户是谁」被答成 `dwd_play_start` 的
**播放次数 TOP1**（2,360），护栏 20 条全放行——SQL 语法合法、表和字段都存在，
错的是语义。这类错护栏抓不到，只能靠词典 + 评测集。

查下去是两个洞：

1. **glossary 里根本没有「消费」词条** → 指标没命中，模型只能按字面理解。
2. **更隐蔽：词典的 note 从来没进过 prompt**。`find_metrics` 只返回
   `matched_by: "glossary:xxx"`，而 `_metric_block` 输出的是**指标**的 note，
   同义词自己的口径说明一直被丢掉。就算补了词条，模型也看不到解释。

修法：

- 商业化段补 6 个词条（`消费` / `消费最多` / `消费金额` / `付费金额` /
  `花了多少钱` / `高消费用户`）→ `gross_revenue`（**毛额**，用户付出去的钱；
  平台净收入仍是 `revenue_sum`）。
- 新增 `promptlib._glossary_hits_block()`，`node_route` 把 `metric_hits` 存进 state，
  prompt 里输出「「消费最多」→ gross_revenue；口径：…不是播歌最多的用户」。
  前端 Trace 也同步显示这条映射。
- 词典里写明**用户主体口径**：`COALESCE(NULLIF(user_id,''), device_id)`
  或排除 `user_id=''`，否则匿名支付会被聚成一个金额 5558 的假「用户」。

修完实测：`U0024613 / 173`，SQL 走 `dwd_pay` + `SUM(amount)` + 用户主体归并。
评测新增 `V28`（断言 SQL 含 `dwd_pay`、不含 `dwd_play_start`）与
`V29`（「昨天消费金额」= 25,582，毛额；净收入是 24,183）。

> 同样是「改了 YAML 要重启 `webapp.py`」——`semantic._CACHE` 是进程级缓存。

### 9.7 「不智能」的批评，与这轮的边界改造

用户实测反馈：「只会按照特定的逻辑回答问题，超出就什么也回答不了，一点也不智能」。
截图里问「你都能进行什么开发吗」，系统仍然硬走 SQL 生成链路，
模型编出 `dws_user_daily.user_id`（那张表全站一行、没有这列），
执行报 `Unknown column` 后把原始异常直接甩到界面上。

**先说清楚定位**：这是「可信问数助手」，刻意选择「宁可拒答，也不编一个数」。
每个答案都要能给出处、口径和复算 SQL——这是它和通用聊天机器人的区别，
也是护栏和评测集存在的理由。这个底线不会为了「显得聪明」而放弃。

**但用户批评的是对的**：保守不等于生硬。原来缺三样东西，这轮都补上了：

| 缺口 | 原来的表现 | 现在 |
|---|---|---|
| 能力类问题也走 SQL | 为了回答「你是谁」编一条 SELECT，再报错 | `node_meta.detect_meta()` 在生成 SQL **之前**分流 |
| 执行失败不自愈 | 只有护栏 reject 会重试，Doris 报错直接显示 | `EXEC-ERROR` 当作一条证据回灌 generate，最多 2 次自愈 |
| 拒答/报错不引导 | 「查询执行失败」+ 一坨异常 | 说明改了几轮、可能原因、建议问法，附「你能做什么」入口 |

#### 三层元路由（都不查数仓）

- **help 能力自述**：从语义层**动态**统计表数 / 指标数 / 维度数 / 数据范围，
  列出示例问法和拒答边界。不硬编码数字，语义层改了说明会跟着变。
- **explain 口径解释**：「DAU 是什么」「完播率怎么算」→ 直接返回语义层定义
  （公式、来源表、过滤、陷阱），并提示"要算数字请带日期"。
- **offscope 通用回答**：「帮我用 tensorflow」→ 交给 LLM 正常回答，
  但答案开头显式写 **「通用回答 · 未查询数仓」**，末尾引回问数能力。
  绝不把通用知识伪装成本项目的数据结论。开关是
  `config.yaml → meta.allow_general_chat`，关掉就退回纯能力说明。

判定优先级保证不误伤数据题：**出现时间表达（日期 / 昨天 / 最近 / 上月 …）
一律走问数链路**，所以「11-08 的 DAU 是什么」仍是算数，不是口径解释。

#### 执行失败自愈

`node_execute` 失败时记一条 `EXEC-ERROR` 到 history 并递增 `exec_retries`，
`graph.after_execute` 把它送回 generate；`node_generate._repair` 跳过机械修复
（护栏的 fix 对 Unknown column 没用），把数据库错误当作证据让模型重写。
实测：`SELECT user_id FROM dws_user_daily` 报错 → 自动改成
`SELECT ... FROM dim_user WHERE is_current='Y' LIMIT 100` → 通过护栏并返回 100 行。

前端同步加了「你能做什么」快捷入口，元答案用独立卡片渲染并标注来源。

> 如果你要的是**能自由聊任何话题的通用助手**，那是另一个产品定位：
> 需要在 route 之前再放一个不设边界的对话节点。代价是「答案可复算」这个
> 卖点会被稀释，答辩时也会更难解释护栏为什么存在。当前实现选择的是
> 「守可信的底线，但把边界体验做像个人」，取舍可以通过 `meta` 配置调整。
