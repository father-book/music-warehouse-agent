"""内置 RBAC + 可扩展 IdentityProvider（ADR-002 / ADR-009）。

分工：这里只回答「你是谁、你有哪些策略」。
把策略编译进 SQL 是 `policy.py` 的事，拦截越权是护栏 R21 的事，
三者解耦——将来接 OIDC / 企微 / 飞书，只需换一个 IdentityProvider 实现。

**角色模型（ADR-009）**
岗位与数据范围是两个正交维度，不再混成一个角色：

    role  = 岗位 → 决定能看哪些数仓分层、哪些列、单次取多少行
    scope = 数据范围 → 决定能看哪些行（如 region=east）

早期版本用 `analyst_full / region_east / report_readonly` 三个角色，
问题在于 `region_east` 把「华东」这个**用户属性**固化成了岗位：
每多一个片区就要多一个角色，岗位数 × 片区数 相乘，角色表必然失控。
拆开后，「华东的数据分析师」= data_analyst + region=east，加片区只是加用户。

**只读**
三个岗位全部 `read_only=true`。写能力不在问数系统里——数仓侧由 DBA 角色持有，
连接层用 agent_ro 只读账号，策略层显式声明只读，护栏 R10 拦 DML/DDL，三层冗余。

演示账号（本地开发用，文档公开，生产部署必须改）：
    analyst      / Demo@2026   数据分析师 · 全国
    analyst_east / Demo@2026   数据分析师 · 华东 7 省市（同岗位、不同范围）
    engineer     / Demo@2026   数据开发工程师 · 全国（唯一能看到 ODS 原始层）
    pm           / Demo@2026   数据产品经理 · 全国（仅汇总层）
"""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from abc import ABC, abstractmethod
from pathlib import Path

from psycopg.types.json import Jsonb

from . import store
from .contracts import PolicyDecision, RequestContext, RowRule, new_trace_id

DEMO_PASSWORD = "Demo@2026"
EAST_PROVINCES = ("上海市", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省")

# ---------------------------------------------------------------- 岗位定义
# 这是全系统唯一描述"岗位能干什么"的地方。改权限改这里，不要在节点里写 if role == ...
#   layers      可访问的数仓分层；seed 时展开成具体的表级 allow 行落库
#   deny_cols   整列不可出现（连 WHERE 里都不许）——用于产品经理这类"不该碰个体"的岗位
#   mask_cols   允许查，结果层打码——分析师/工程师需要按脱敏值分组，但不能拿到明文
#   audit_scope 审计可见范围：all=可查全员（排障需要）/ self=只看自己
ROLE_SPECS: tuple[dict, ...] = (
    {
        "role": "data_analyst",
        "name_cn": "数据分析师",
        "description": "查明细与汇总做归因分析；不含 ODS 原始事件层，PII 脱敏",
        "layers": ("dws", "dwd", "ads", "dim", "dev", "rt"),
        "max_rows": 5000,
        "audit_scope": "self",
        "deny_columns": (),
        # 分析师要按脱敏后的手机号做去重/分群，所以是 mask 而不是 deny
        "mask_columns": ("phone", "id_card", "email", "ip", "ip_addr"),
    },
    {
        "role": "data_engineer",
        "name_cn": "数据开发工程师",
        "description": "全层可查（含 ODS 原始事件），用于排查埋点与 ETL 口径问题；PII 同样脱敏",
        "layers": (),            # 空 = 不按分层裁剪，全部表可见
        "max_rows": 20000,       # 抽数核对明细，上限高于分析岗
        "audit_scope": "all",    # 要能看别人的失败链路才能定位口径 bug
        "deny_columns": (),
        # 工程师也不例外：合规底线与岗位无关。真要看明文走脱敏接口 + 审批。
        "mask_columns": ("phone", "id_card", "email", "ip", "ip_addr"),
    },
    {
        "role": "data_pm",
        "name_cn": "数据产品经理",
        "description": "只看汇总层（ADS/DWS）与公开维度，看大盘与指标口径，不看个体行为",
        "layers": ("ads", "dws", "dim", "dev", "rt"),
        "max_rows": 1000,
        "audit_scope": "self",
        # 产品经理最常见的越权是"顺手捞一份用户明细"，所以列级也堵死
        "deny_columns": ("user_id", "device_id", "phone", "id_card", "email", "ip", "ip_addr"),
        "mask_columns": (),
    },
)

ROLE_BY_NAME = {s["role"]: s for s in ROLE_SPECS}
# 产品经理不该看 dim_user 这类个体画像维度，从可见表里排除
ROLE_TABLE_EXCLUDE = {"data_pm": ("dim_user",)}

AUDIT_ALL_ROLES = frozenset(s["role"] for s in ROLE_SPECS if s["audit_scope"] == "all")


def _meta_path(cfg: dict | None) -> Path:
    p = (cfg or {}).get("_paths", {}).get("tables_json")
    return Path(p) if p else Path(__file__).resolve().parents[2] / "agent" / "tables.json"


def _load_meta(cfg: dict | None) -> dict:
    return json.loads(_meta_path(cfg).read_text(encoding="utf-8"))["tables"]


def _tables_with_column(column: str, cfg: dict | None = None) -> list[str]:
    """从元数据动态生成，避免"新增一张含 province 的表却忘了配策略"。"""
    return sorted(n for n, t in _load_meta(cfg).items()
                  if column in [str(c).lower() for c in t.get("columns", [])])


def tables_for_role(role: str, cfg: dict | None = None) -> list[str]:
    """按岗位分层展开成具体表清单。数据驱动，新增表只要重跑 seed 就自动纳入。"""
    spec = ROLE_BY_NAME.get(role) or {}
    layers = set(spec.get("layers") or ())
    meta = _load_meta(cfg)
    exclude = set(ROLE_TABLE_EXCLUDE.get(role, ()))
    out = []
    for name, t in meta.items():
        if name in exclude:
            continue
        if layers:
            layer = str(t.get("layer") or name.split("_")[0]).lower()
            if layer not in layers:
                continue
        out.append(name)
    return sorted(out)


def hash_password(pw: str, iterations: int = 200_000, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"),
                             bytes.fromhex(salt), iterations).hex()
    return "pbkdf2$%d$%s$%s" % (iterations, salt, dk)


def verify_password(pw: str, stored: str) -> bool:
    try:
        _, iters, salt, digest = stored.split("$")
        dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"),
                                 bytes.fromhex(salt), int(iters)).hex()
        return hmac.compare_digest(dk, digest)
    except (ValueError, TypeError):
        return False


class IdentityProvider(ABC):
    """身份源适配层。将来接 OIDC 只换实现类，策略引擎/护栏/审计不动。"""

    @abstractmethod
    def authenticate(self, username: str, password: str,
                     source: str = "web") -> RequestContext | None: ...

    @abstractmethod
    def decision(self, ctx: RequestContext) -> PolicyDecision: ...

    @abstractmethod
    def profile(self, user_id: str) -> dict | None: ...


class BuiltinRBAC(IdentityProvider):
    def __init__(self, cfg: dict | None = None):
        self.cfg = cfg

    # ---------------------------------------------------------------- 账号
    def authenticate(self, username: str, password: str,
                      source: str = "web") -> RequestContext | None:
        try:
            with store.connect(self.cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT user_id, tenant_id, role, password_hash, status, "
                        "scope_region FROM users WHERE username = %s", (username,))
                    row = cur.fetchone()
        except Exception:
            return None
        if not row or row[4] != "active" or not verify_password(password, row[3]):
            return None
        return RequestContext(
            trace_id=new_trace_id(), user_id=row[0], tenant_id=row[1],
            role=row[2], scope={"region": row[5] or "all"},
            source=source, unrestricted=False,
        )

    def profile(self, user_id: str) -> dict | None:
        try:
            with store.connect(self.cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT u.user_id, u.username, u.display_name, u.tenant_id, "
                        "u.role, r.name_cn, u.scope_region, r.max_rows, r.read_only, "
                        "r.description "
                        "FROM users u JOIN roles r ON u.role = r.role "
                        "WHERE u.user_id = %s", (user_id,))
                    r = cur.fetchone()
                    if not r:
                        return None
                    return {"user_id": r[0], "username": r[1], "display_name": r[2],
                            "tenant_id": r[3], "role": r[4], "role_name": r[5],
                            "scope_region": r[6], "max_rows": r[7],
                            "read_only": r[8], "role_description": r[9]}
        except Exception:
            return None

    def context_for_user(self, user_id: str,
                          source: str = "web") -> RequestContext | None:
        """已登录会话 → 带新 trace_id 的上下文。每次问答一个 trace_id。

        同时接受 user_id 和 username：会话里存的是前者，而评测 / 运维脚本
        写的是后者（`analyst_east` 比 `u-analyst-east` 好记），让调用方
        自己记映射迟早会写错一个。
        """
        try:
            with store.connect(self.cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT user_id, tenant_id, role, scope_region FROM users "
                        "WHERE (user_id = %s OR username = %s) AND status = 'active'",
                        (user_id, user_id))
                    r = cur.fetchone()
        except Exception:
            return None
        if not r:
            return None
        return RequestContext(trace_id=new_trace_id(), user_id=r[0],
                              tenant_id=r[1], role=r[2],
                              scope={"region": r[3] or "all"}, source=source)

    # ---------------------------------------------------------------- 策略
    def decision(self, ctx: RequestContext) -> PolicyDecision:
        if ctx.unrestricted:
            return PolicyDecision.unrestricted_for(ctx.role)
        try:
            with store.connect(self.cfg) as conn:
                with conn.cursor() as cur:
                    # 岗位能力（表/列/上限/只读）来自 roles 表，不在代码里硬编码
                    cur.execute(
                        "SELECT max_rows, read_only FROM roles WHERE role = %s",
                        (ctx.role,))
                    r = cur.fetchone()
                    if not r:
                        return PolicyDecision.build(
                            role=ctx.role, deny_tables={"*"}, max_rows=0,
                            reason="角色 %s 未定义，按最小权限拒绝" % ctx.role)
                    max_rows, read_only = int(r[0]), bool(r[1])

                    cur.execute(
                        "SELECT kind, effect, table_name, column_name, op, values_jsonb "
                        "FROM policies WHERE role = %s AND tenant_id IN (%s, '*') "
                        "ORDER BY policy_id", (ctx.role, ctx.tenant_id))
                    rows = cur.fetchall()

                    # 数据范围：按 ctx.scope 里的每个维度去取行级规则
                    scope_rows = []
                    for kind, val in (ctx.scope or {}).items():
                        if not val or val == "all":
                            continue
                        cur.execute(
                            "SELECT kind, effect, table_name, column_name, op, values_jsonb "
                            "FROM scope_policies WHERE scope_kind = %s AND scope_value = %s "
                            "ORDER BY policy_id", (kind, val))
                        scope_rows.extend(cur.fetchall())
        except Exception as e:
            # 策略读不到时必须**拒绝**而不是放行——fail closed。
            return PolicyDecision.build(
                role=ctx.role, deny_tables={"*"}, max_rows=0,
                reason="策略读取失败，按最小权限拒绝：%s" % str(e)[:120])

        allow, deny, row_rules, deny_cols, mask_cols = set(), set(), [], set(), set()
        for kind, effect, table, column, op, values in rows + scope_rows:
            vals = values if isinstance(values, list) else json.loads(values or "[]")
            if kind == "table" and table:
                (allow if effect == "allow" else deny).add(str(table).lower())
            elif kind == "row" and table and column:
                row_rules.append(RowRule(table=str(table).lower(),
                                        column=str(column).lower(),
                                        op=op or "in",
                                        values=tuple(str(v) for v in vals)))
            elif kind == "column" and column and effect == "deny":
                deny_cols.add(str(column).lower())
            elif kind == "mask" and column:
                mask_cols.add(str(column).lower())

        scope_txt = ",".join("%s=%s" % (k, v) for k, v in (ctx.scope or {}).items()) or "-"
        return PolicyDecision.build(
            role=ctx.role, allow_tables=allow, deny_tables=deny,
            row_rules=row_rules, deny_columns=deny_cols, mask_columns=mask_cols,
            max_rows=max_rows, read_only=read_only, scope=dict(ctx.scope or {}),
            reason="岗位=%s scope=%s 只读=%s：%d 张可见表 / %d 条行策略 / %d 列禁用"
                   % (ctx.role, scope_txt, "是" if read_only else "否",
                      len(allow), len(row_rules), len(deny_cols)),
        )

    # ---------------------------------------------------------------- 演示数据
    def seed(self, force: bool = False) -> dict:
        """幂等灌入 tenant / roles / users / policies / scope_policies。

        默认不覆盖；force=True 全量重建（演示库用）。
        """
        cfg = self.cfg
        try:
            with store.connect(cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM policies")
                    if cur.fetchone()[0] and not force:
                        cur.execute("SELECT count(*) FROM users")
                        return {"seeded": False, "users": cur.fetchone()[0],
                                "reason": "策略已存在，未覆盖（force=True 可重建）"}

                    if force:
                        # 顺序受 FK 约束：sessions → users → policies → roles。
                        # qa_logs 不动：审计是历史事实，触发器也不允许删。
                        cur.execute("DELETE FROM sessions")
                        cur.execute("DELETE FROM users")
                        cur.execute("DELETE FROM policies")
                        cur.execute("DELETE FROM scope_policies")
                        cur.execute("DELETE FROM roles")

                    cur.execute(
                        "INSERT INTO tenants(tenant_id,name) VALUES ('default','默认租户') "
                        "ON CONFLICT DO NOTHING")

                    for spec in ROLE_SPECS:
                        cur.execute(
                            "INSERT INTO roles(role,name_cn,description,read_only,max_rows,layers) "
                            "VALUES (%s,%s,%s,true,%s,%s) ON CONFLICT DO NOTHING",
                            (spec["role"], spec["name_cn"], spec["description"],
                             spec["max_rows"], ",".join(spec["layers"])))

                    # 同岗位不同范围、同范围不同岗位，各给一个账号——这是模型能否成立的实证
                    for uid, uname, cn, role, region in (
                        ("u-analyst", "analyst", "林析（数据分析师·全国）", "data_analyst", "all"),
                        ("u-analyst-east", "analyst_east", "周东（数据分析师·华东）", "data_analyst", "east"),
                        ("u-engineer", "engineer", "陈开（数据开发工程师）", "data_engineer", "all"),
                        ("u-pm", "pm", "苏产（数据产品经理）", "data_pm", "all"),
                    ):
                        cur.execute(
                            "INSERT INTO users(user_id,tenant_id,username,display_name,"
                            "password_hash,role,scope_region) "
                            "VALUES (%s,'default',%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (uid, uname, cn, hash_password(DEMO_PASSWORD), role, region))

                    def put(role, kind, effect="deny", table=None, column=None,
                            op=None, values=None, note=""):
                        cur.execute(
                            "INSERT INTO policies(role,kind,effect,table_name,column_name,"
                            "op,values_jsonb,note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (role, kind, effect, table, column, op,
                             Jsonb(values or []), note))

                    allow_cnt = {}
                    for spec in ROLE_SPECS:
                        role = spec["role"]
                        tbls = tables_for_role(role, cfg)
                        allow_cnt[role] = len(tbls)
                        for t in tbls:
                            put(role, "table", "allow", table=t,
                                note="%s 分层白名单"
                                     % (",".join(spec["layers"]) or "全部"))
                        for col in spec["deny_columns"]:
                            put(role, "column", "deny", column=col,
                                note="%s 不返回个体标识" % spec["name_cn"])
                        for col in spec["mask_columns"]:
                            put(role, "mask", "deny", column=col,
                                note="PII 结果层打码（R22 兜底）")

                    # 数据范围策略：华东 7 省市。列存在性由元数据驱动，
                    # 将来新增含 province 的表，重跑 seed 即自动覆盖。
                    prov_tables = _tables_with_column("province", cfg)
                    for t in prov_tables:
                        cur.execute(
                            "INSERT INTO scope_policies(scope_kind,scope_value,kind,effect,"
                            "table_name,column_name,op,values_jsonb,note) "
                            "VALUES ('region','east','row','deny',%s,'province','in',%s,%s)",
                            (t, Jsonb(list(EAST_PROVINCES)),
                             "华东 7 省市；与岗位正交，不带岗位语义"))
            return {"seeded": True, "roles": [s["role"] for s in ROLE_SPECS],
                    "tables_per_role": allow_cnt,
                    "scope_tables": len(prov_tables)}
        except Exception as e:
            return {"seeded": False, "error": "%s: %s" % (type(e).__name__, str(e)[:250])}


_PROVIDER: IdentityProvider | None = None


def grant_table(table: str, layer: str = "dev",
                cfg: dict | None = None) -> dict:
    """把新登记的表按分层授予各角色（只增不删），并补数据范围策略。

    为什么需要它：seed 是把 layers **展开成行**落库的，新增一张 dev 表后
    权限表里没有它——语义层登记了、guard 也认识，但策略引擎会判"不在角色
    可见范围"。登记流程必须把这一步带上，否则用户看到的是"表建好、登记好，
    就是查不了"（2026-11-11 实测踩到）。

    只增不删：授予是幂等的；不重建 policies 是因为 force seed 会清 sessions，
    把在线用户踢下线，代价与收益不成比例。
    """
    from . import store
    granted, skipped, scoped = [], [], []
    try:
        with store.connect(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT role, layers FROM roles")
            roles = cur.fetchall()
            for role, layers in roles:
                lay = set((layers or "").split(",")) - {""}
                if lay and layer not in lay:
                    skipped.append(role)
                    continue
                cur.execute(
                    "SELECT 1 FROM policies WHERE role=%s AND kind='table'"
                    " AND effect='allow' AND table_name=%s", (role, table))
                if cur.fetchone():
                    continue
                cur.execute(
                    "INSERT INTO policies(role,kind,effect,table_name,note)"
                    " VALUES (%s,'table','allow',%s,%s)",
                    (role, table, "新登记 %s 层表自动授权" % layer))
                granted.append(role)

            # 数据范围：含 province 列的表要补华东行级策略，否则范围约束在这张表上失效
            prov_cols = _tables_with_column("province", cfg)
            if table.lower() in {t.lower() for t in prov_cols}:
                cur.execute(
                    "SELECT 1 FROM scope_policies WHERE scope_kind='region'"
                    " AND scope_value='east' AND table_name=%s", (table,))
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO scope_policies(scope_kind,scope_value,kind,"
                        "effect,table_name,column_name,op,values_jsonb,note) "
                        "VALUES ('region','east','row','deny',%s,'province','in',%s,%s)",
                        (table, Jsonb(list(EAST_PROVINCES)),
                         "华东 7 省市（新登记表自动补）"))
                    scoped.append("east")
    except Exception as e:
        return {"granted": granted, "skipped": skipped, "scoped": scoped,
                "error": str(e)[:200]}
    return {"granted": granted, "skipped": skipped, "scoped": scoped,
            "error": ""}


def provider(cfg: dict | None = None) -> IdentityProvider:
    """全局单例；接 OIDC 时只改这里的工厂返回值。"""
    global _PROVIDER
    if _PROVIDER is None:
        _PROVIDER = BuiltinRBAC(cfg)
    return _PROVIDER
