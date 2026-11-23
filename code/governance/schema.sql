-- ============================================================================
-- 治理面 schema（PostgreSQL 16）
-- 依据 docs/36-ADR.md：ADR-002 内置 RBAC、ADR-003 PG、ADR-006 不可变审计事件。
--
-- 幂等：全部 IF NOT EXISTS，可重复执行。
-- qa_logs 在数据库层强制不可变（触发器禁 UPDATE/DELETE），
-- 不依赖"应用自觉遵守"——这是审计能不能过审的分界线。
-- ============================================================================

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   text PRIMARY KEY,
    name        text NOT NULL,
    status      text NOT NULL DEFAULT 'active',
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- 岗位（role）：只描述"这个岗位能看什么"，不描述"这个人管哪个片区"。
--   read_only  所有岗位一律只读（写能力由 DBA 角色在数仓侧持有，不在问数系统里）
--   max_rows   单次查询结果上限，按岗位的取数习惯设
--   layers     可访问的数仓分层，逗号分隔；空串=不限。用数据而非代码表达分层权限
CREATE TABLE IF NOT EXISTS roles (
    role        text PRIMARY KEY,
    name_cn     text NOT NULL,
    description text NOT NULL DEFAULT '',
    read_only   boolean NOT NULL DEFAULT true,
    max_rows    int NOT NULL DEFAULT 5000,
    layers      text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE roles ADD COLUMN IF NOT EXISTS read_only boolean NOT NULL DEFAULT true;
ALTER TABLE roles ADD COLUMN IF NOT EXISTS max_rows  int NOT NULL DEFAULT 5000;
ALTER TABLE roles ADD COLUMN IF NOT EXISTS layers    text NOT NULL DEFAULT '';

-- 数据范围（scope）是用户属性，不是角色：华东区的数据分析师 = data_analyst + region=east。
-- 这样新增一个片区只要加一行用户，不用新增角色，也不会让角色数按 岗位×片区 相乘。
CREATE TABLE IF NOT EXISTS users (
    user_id       text PRIMARY KEY,
    tenant_id     text NOT NULL REFERENCES tenants(tenant_id),
    username      text NOT NULL UNIQUE,
    display_name  text NOT NULL DEFAULT '',
    password_hash text NOT NULL,           -- pbkdf2$iterations$salt$digest，绝不存明文
    role          text NOT NULL REFERENCES roles(role),
    scope_region  text NOT NULL DEFAULT 'all',   -- all | east | ...
    status        text NOT NULL DEFAULT 'active',
    created_at    timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS scope_region text NOT NULL DEFAULT 'all';
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- policies.kind: table | row | column | mask
--   table  → table_name 填表名或不填（* = 全部），effect=allow/deny
--   row    → table_name + column_name + op + values_jsonb
--   column → column_name，effect=deny 时命中即拒答
--   mask   → column_name，结果层脱敏（R22 兜底）
CREATE TABLE IF NOT EXISTS policies (
    policy_id    bigserial PRIMARY KEY,
    tenant_id    text NOT NULL DEFAULT '*',
    role         text NOT NULL REFERENCES roles(role),
    kind         text NOT NULL CHECK (kind IN ('table','row','column','mask')),
    effect       text NOT NULL DEFAULT 'deny' CHECK (effect IN ('allow','deny')),
    table_name   text,
    column_name  text,
    op           text,
    values_jsonb jsonb NOT NULL DEFAULT '[]'::jsonb,
    version      int  NOT NULL DEFAULT 1,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    note         text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_policies_role ON policies(role, kind);

-- 范围策略：与岗位策略正交存放。
--   scope_kind=region, scope_value=east → 对含 province 列的表注入 province IN (华东 7 省市)
-- 放在独立的表里，是为了让"谁在哪个片区"变成可增删的数据行，
-- 而不是每次都要往 policies 里塞一批 role 名——那正是角色爆炸的来源。
CREATE TABLE IF NOT EXISTS scope_policies (
    policy_id    bigserial PRIMARY KEY,
    scope_kind   text NOT NULL,            -- region
    scope_value  text NOT NULL,            -- east | ...
    kind         text NOT NULL DEFAULT 'row' CHECK (kind IN ('row','table','column','mask')),
    effect       text NOT NULL DEFAULT 'deny' CHECK (effect IN ('allow','deny')),
    table_name   text,
    column_name  text,
    op           text,
    values_jsonb jsonb NOT NULL DEFAULT '[]'::jsonb,
    note         text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_scope_policies ON scope_policies(scope_kind, scope_value);

CREATE TABLE IF NOT EXISTS qa_logs (
    trace_id       text PRIMARY KEY,
    occurred_at    timestamptz NOT NULL,
    tenant_id      text NOT NULL,
    user_id        text NOT NULL,
    role           text NOT NULL,
    source         text NOT NULL,
    intent         text NOT NULL,          -- query | meta | refuse | error
    question       text NOT NULL,
    time_start     date,
    time_end       date,
    sql_versions   jsonb NOT NULL DEFAULT '[]'::jsonb,
    final_sql      text,
    applied_sql    text,                   -- 策略注入后真正执行的 SQL
    tables         jsonb NOT NULL DEFAULT '[]'::jsonb,
    row_count      int  NOT NULL DEFAULT 0,
    elapsed_ms     int  NOT NULL DEFAULT 0,
    tokens_in      int  NOT NULL DEFAULT 0,
    tokens_out     int  NOT NULL DEFAULT 0,
    cost_cny       numeric(12,6) NOT NULL DEFAULT 0,
    verdict        text,
    refused        boolean NOT NULL DEFAULT false,
    refuse_reason  text,
    answer_excerpt text,
    policy_hash    text,
    trace_json     jsonb NOT NULL DEFAULT '{}'::jsonb,
    error          text
);
CREATE INDEX IF NOT EXISTS idx_qa_logs_time ON qa_logs(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_qa_logs_user ON qa_logs(user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_qa_logs_intent ON qa_logs(intent, occurred_at DESC);

-- 阶段 8（AC-14）：问句与答案摘录落盘前脱敏，命中个数记在这里。
-- 用 ALTER ... IF NOT EXISTS：CREATE TABLE IF NOT EXISTS 不会给存量表加列，
-- 而这张表里已经有真实审计数据，重建等于毁档。
ALTER TABLE qa_logs ADD COLUMN IF NOT EXISTS pii_hits int NOT NULL DEFAULT 0;

-- 审计访问记录（AC-14）：**看审计这件事本身也要被审计**。
-- 单独建表而不是塞进 qa_logs，是因为两者语义不同：qa_logs 是"谁问了什么数"，
-- 这张表是"谁看了谁的审计"。混在一张表里，两边的统计口径都会被污染。
CREATE TABLE IF NOT EXISTS audit_access (
    id          bigserial PRIMARY KEY,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    user_id     text NOT NULL DEFAULT '',
    role        text NOT NULL DEFAULT '',
    action      text NOT NULL DEFAULT 'read',   -- read | export | purge
    target      text NOT NULL DEFAULT '',       -- 目标 trace_id（空 = 列表查询）
    filters     jsonb NOT NULL DEFAULT '{}'::jsonb,
    rows        int  NOT NULL DEFAULT 0,        -- 本次返回多少行
    source      text NOT NULL DEFAULT 'web',
    note        text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_access_time ON audit_access(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_access_user ON audit_access(user_id, occurred_at DESC);

-- 保留期归档（AC-14）：qa_logs 不可变，清理只能"先归档再受控删除"。
-- 归档用整行 jsonb 存：出问题时能原样还原，而不是只留几个关键字段再靠记忆。
CREATE TABLE IF NOT EXISTS qa_logs_archive (
    trace_id    text PRIMARY KEY,
    archived_at timestamptz NOT NULL DEFAULT now(),
    payload     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_archive_time ON qa_logs_archive(archived_at DESC);

CREATE OR REPLACE FUNCTION qa_logs_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'qa_logs 是不可变审计表，禁止 % 操作', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_qa_logs_immutable ON qa_logs;
CREATE TRIGGER trg_qa_logs_immutable
    BEFORE UPDATE OR DELETE ON qa_logs
    FOR EACH ROW EXECUTE FUNCTION qa_logs_immutable();

CREATE TABLE IF NOT EXISTS cost_ledger (
    id           bigserial PRIMARY KEY,
    trace_id     text NOT NULL,
    occurred_at  timestamptz NOT NULL DEFAULT now(),
    user_id      text NOT NULL DEFAULT '',
    model        text NOT NULL DEFAULT '',
    caller       text NOT NULL DEFAULT '',    -- generate | repair | meta
    tokens_in    int  NOT NULL DEFAULT 0,
    tokens_out  int  NOT NULL DEFAULT 0,
    cost_cny     numeric(12,6) NOT NULL DEFAULT 0,
    cached       boolean NOT NULL DEFAULT false,
    note         text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_cost_trace ON cost_ledger(trace_id);
CREATE INDEX IF NOT EXISTS idx_cost_time ON cost_ledger(occurred_at DESC);

-- 网关台账（AC-18）。上面那张表是最初的骨架，这里把「一次模型调用到底发生了什么」
-- 补齐：谁发起的、成没成、快不快、prompt 的指纹与首段、模型吐回来的开头。
-- 用 ALTER ... IF NOT EXISTS 而不是重建表：表里可能已有历史数据，重建等于毁账。
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS role         text NOT NULL DEFAULT '';
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS ok           boolean NOT NULL DEFAULT true;
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS latency_ms   int NOT NULL DEFAULT 0;
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS attempts     int NOT NULL DEFAULT 1;
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS error        text NOT NULL DEFAULT '';
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS prompt_hash  text NOT NULL DEFAULT '';
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS prompt_chars int NOT NULL DEFAULT 0;
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS prompt_head  text NOT NULL DEFAULT '';
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS answer_head  text NOT NULL DEFAULT '';

-- 台账只许追加、不许改：它和 qa_logs 是同一份证据链的两端。
-- 保留期不靠 DELETE 做（见 docs/45：按天归档 + 运维角色执行），
-- 应用角色连删的资格都没有。
CREATE OR REPLACE FUNCTION cost_ledger_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'cost_ledger 是只追加台账，禁止 UPDATE/DELETE';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cost_ledger_immutable ON cost_ledger;
CREATE TRIGGER trg_cost_ledger_immutable
    BEFORE UPDATE OR DELETE ON cost_ledger
    FOR EACH ROW EXECUTE FUNCTION cost_ledger_immutable();

CREATE INDEX IF NOT EXISTS idx_cost_user_time ON cost_ledger(user_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS sessions (
    token_hash   text PRIMARY KEY,          -- 只存 sha256，不存原始 token
    user_id      text NOT NULL REFERENCES users(user_id),
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at);

-- ============================================================================
-- 对话会话（产品功能，与审计刻意分开）
--
-- 为什么不直接拿 qa_logs 当历史：
--   1. qa_logs 在库层不可变（触发器禁 UPDATE/DELETE）。会话要能改名、能删、
--      能按人列出，这些在审计表上一条都做不到。
--   2. 两者的语义不同：审计问"谁在什么时候问了什么"（谁都不能改），
--      会话问"这个人的这一段对话里聊过什么"（本人可删）。
--      把可删的东西混进不可删的证据链，等于给审计开了后门。
--   关联靠 trace_id：一条会话消息能追到对应审计事件，反过来也能。
--
-- 软删而不是物理删除：`deleted` 置位后列表里不再出现，
-- 但审计侧完整保留。用户删的是"自己的聊天记录"，不是"审计证据"。
-- ============================================================================
CREATE TABLE IF NOT EXISTS conversations (
    conv_id     text PRIMARY KEY,
    user_id     text NOT NULL,
    tenant_id   text NOT NULL DEFAULT 'default',
    title       text NOT NULL DEFAULT '新对话',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    msg_count   int NOT NULL DEFAULT 0,
    deleted     boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_conv_user ON conversations(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conv_tenant ON conversations(tenant_id, updated_at DESC);

-- payload 是答案快照：会话恢复时要能原样画出答案卡（表格、复算 SQL、执行过程），
-- 只存 answer 文本的话，翻回历史只剩一段话，出处和口径全丢了。
-- 体积靠写入侧裁剪（trace 里每步 data 截断），不靠事后清理。
CREATE TABLE IF NOT EXISTS conversation_messages (
    msg_id     bigserial PRIMARY KEY,
    conv_id    text NOT NULL REFERENCES conversations(conv_id) ON DELETE CASCADE,
    role       text NOT NULL,              -- user | assistant
    content    text NOT NULL DEFAULT '',   -- user: 问题原文；assistant: 答案摘要
    payload    jsonb,                      -- assistant 的答案快照（user 行为 null）
    trace_id   text,                       -- 对得上 qa_logs 的那条事件
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_convmsg ON conversation_messages(conv_id, msg_id);

-- ============================================================
-- ETL 开发脚本（AC：数据开发工程师让 agent 生成开发表加工逻辑）
-- agent_ro 只读，真实执行在系统之外；这里登记"生成物 + 校验结论"，
-- 谁在什么时候让 agent 生成了什么，事后可追溯。
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_scripts (
    id           bigserial PRIMARY KEY,
    user_id      text NOT NULL,
    title        text NOT NULL DEFAULT '',
    target_table text NOT NULL,
    requirement  text NOT NULL,
    sql_text     text NOT NULL,
    checks       jsonb NOT NULL DEFAULT '{}'::jsonb,  -- etlguard.validate 的完整结果
    status       text NOT NULL DEFAULT 'generated',   -- generated / verified
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_etl_user ON etl_scripts(user_id, id DESC);

-- ============================================================
-- 统一数据质量体检（P2 / 论文第四章 L0~L5）
-- 与 qa_logs 的区别：审计日志是"谁问了什么"的不可变事实，
-- dq_results 是"规则今天过没过"的可重复度量，按 run_id 归档形成趋势。
-- 规则定义在 agent/semantic/dq_rules.yaml，代码不硬编码阈值。
-- ============================================================
CREATE TABLE IF NOT EXISTS dq_runs (
    run_id   bigserial PRIMARY KEY,
    run_at   timestamptz NOT NULL DEFAULT now(),
    total    int NOT NULL,
    passed   int NOT NULL,
    failed   int NOT NULL,
    alerts   int NOT NULL,
    detail   text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_dq_runs_at ON dq_runs(run_at DESC);

CREATE TABLE IF NOT EXISTS dq_results (
    id        bigserial PRIMARY KEY,
    run_id    bigint NOT NULL REFERENCES dq_runs(run_id) ON DELETE CASCADE,
    rule_id   text NOT NULL,
    layer     text,
    name_cn   text,
    severity  text,
    ok        boolean NOT NULL,
    value     text,
    threshold text,
    detail    text,
    alert     boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_dq_results_run ON dq_results(run_id, id);

-- ============================================================
-- 答案反馈回流（2026-11-15）
-- 为什么单独一张表而不是往 qa_logs 里加列：
--   qa_logs 有 BEFORE UPDATE OR DELETE 触发器（不可变审计事实），
--   而反馈会改判（先点"不准"、用户自己发现是理解错又改成"有用"）。
--   把会变的东西塞进不可变表，只能靠"再插一行"表达，查询时还得自己挑最新的。
-- 谁有权反馈：只能对自己问过的那条 trace 反馈；engineer 可代标（用于整理 badcase）。
-- ============================================================
CREATE TABLE IF NOT EXISTS answer_feedback (
    id         bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    trace_id   text NOT NULL,
    user_id    text NOT NULL,
    role       text,
    verdict    text NOT NULL,          -- useful / wrong / unclear
    reason     text,                   -- 预设原因码（口径错/数字错/答非所问/其他）
    comment    text NOT NULL DEFAULT '',
    question   text NOT NULL DEFAULT '',
    trust_level text,                  -- 提交时的可信度级别，用来校验规则准不准
    UNIQUE (trace_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_feedback_at ON answer_feedback(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_verdict ON answer_feedback(verdict, created_at DESC);

-- ============================================================================
-- 长期记忆（用户级，跨会话）：只存本人显式要求记住的口径偏好，不存历史数字。
-- 与 conversations 分开：会话删了就没了，偏好要能单条查看/删除。
-- 不写 qa_logs：那是不可变证据链，给它开 UPDATE/DELETE 等于开后门。
-- 每条带 caliber_version：口径一改，旧偏好自动不注入（5.24 倍事故的教训）。
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_memory (
    mem_id          bigserial PRIMARY KEY,
    user_id         text NOT NULL,
    tenant_id       text NOT NULL DEFAULT 'default',
    kind            text NOT NULL,      -- preference（只有这一类）
    mem_key         text NOT NULL,      -- 去重键：偏好文本指纹
    content         text NOT NULL,      -- 剥掉数字的口径偏好原文
    payload         jsonb,              -- {metrics, time_window}
    caliber_version text NOT NULL DEFAULT '',
    trace_id        text,               -- 溯源到 qa_logs 那条不可变事件
    hits            int NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    last_used_at    timestamptz,
    deleted         boolean NOT NULL DEFAULT false,
    UNIQUE (user_id, kind, mem_key)
);
CREATE INDEX IF NOT EXISTS idx_mem_user ON user_memory(user_id, kind, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_mem_tenant ON user_memory(tenant_id, updated_at DESC);
