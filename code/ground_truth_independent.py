#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""P1-9 双引擎重算：同一批 SQL，Doris MPP 与 Hive-on-Spark 各跑一遍，对比结果。

===============================================================================
【这个脚本能证明什么，不能证明什么】—— 答辩必须照这段说，否则会被问穿
===============================================================================
能证明：**同一份 SQL 逻辑在两个独立计算引擎上给出一致结果**。
        能抓的是 SQL 写错（口径、JOIN、去重、类型转换）——即"人写错了逻辑"。

不能证明：数据本身是对的。
        两个引擎读的是**同一份 HDFS 上的 ORC 文件**
        （/warehouse/music_dw/<table>/dt=*/），不是两个独立数据源。
        如果 ORC 文件里的数是错的（造数器 bug、ETL 写错），
        两边会**一致地错**，这个脚本一个字都喊不出来。

所以它的定位是"独立计算引擎校验"，不是"独立数据源校验"。
真正的独立数据源校验只能靠：埋点原始日志重放 / 业务方对账 / 第三方报表核对。

===============================================================================
【为什么 SQL 层只跑 atomic，派生指标在 Python 里算】
===============================================================================
如果为了"一条 SQL 出全部指标"而写 `SUM(CASE WHEN ... THEN 1 END)` 这类条件聚合，
改写本身就成了新的错误源：改写有 bug → 两边一起错 → 对比通过 → 假阴性。
所以这里坚持**原样执行 metrics.yaml 里的 formula**，一个字符都不改；
派生指标（比率/人均）的除法放到 Python 里，用两边各自的 atomic 值算。
这样唯一的变量就是引擎。

===============================================================================
【两端怎么连】
===============================================================================
Doris：pymysql 直连 163:9030 → SWITCH hive_catalog; USE music_dw;
       （不切 catalog 会落到 internal 实时库，行数差 6.2 倍且零报错）
Hive ：ssh 161 → hive -f，一次会话跑完全部 SQL
       （单次 hive 启动约 60 秒，逐条发会等到天荒地老）

用法：
    python code/ground_truth_independent.py                  # 全部可重算指标
    python code/ground_truth_independent.py --only doris     # 只跑 Doris 侧
    python code/ground_truth_independent.py --metrics uv,ctr,arpu
    python code/ground_truth_independent.py --md docs/32-双引擎重算结果.md

退出码：0 全部一致；1 存在超差；2 执行失败（连不上/指标不可展开）。
"""
import argparse
import io
import os
import re
import subprocess
import sys
from decimal import Decimal

import pymysql
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
METRICS_YAML = os.path.join(ROOT, "agent", "semantic", "metrics.yaml")
TABLES_JSON = os.path.join(ROOT, "agent", "tables.json")

DORIS = dict(host="192.168.128.163", port=9030, user="root", password="", charset="utf8mb4")
SSH_HOST = "root@192.168.128.161"
SSH_KEY = os.path.expanduser("~/.ssh/id_ed25519")
HIVE = "/opt/module/hive/bin/hive"

# Hive 侧 executor 规格：本集群 YARN 总量 3072/1024/1024，
# 沿用 hive-site 的 2g executor 会在 AM 落到 WWC160 时永久 pending（见 hq.sh）
HIVE_CONF = [
    "--hiveconf", "spark.executor.memory=640m",
    "--hiveconf", "spark.executor.memoryOverhead=384m",
    "--hiveconf", "spark.executor.instances=3",
    "--hiveconf", "spark.executor.cores=2",
]


def log(msg):
    print(msg)
    sys.stdout.flush()


# ============================================================== 指标展开
def load_metrics(only=None):
    d = yaml.safe_load(io.open(METRICS_YAML, encoding="utf-8"))
    m = d["metrics"]
    win = d.get("window") or ["2026-10-05", "2026-11-08"]
    if only:
        miss = [x for x in only if x not in m]
        if miss:
            log("!! 未知指标：%s" % miss)
            sys.exit(2)
        return {k: m[k] for k in only}, m, win
    return m, m, win


def atomic_sql(spec, lo, hi):
    """原样套用 metrics.yaml 里的 formula，只加时间窗和 filter。

    不加任何改写 —— 改写会变成第二个错误源，见文件头说明。
    """
    sql = "SELECT %s FROM %s WHERE dt BETWEEN '%s' AND '%s'" % (
        spec["formula"], spec["source"], lo, hi)
    if spec.get("filter"):
        sql += " AND (%s)" % spec["filter"]
    return sql


def uv_perday_sql(spec, lo, hi):
    """按日去重后求和 = 人日。ARP U 这类分母需要它（SUM(uv) ≠ 35 天去重 UV）。"""
    inner = "SELECT dt, %s AS d FROM %s WHERE dt BETWEEN '%s' AND '%s'" % (
        spec["formula"], spec["source"], lo, hi)
    if spec.get("filter"):
        inner += " AND (%s)" % spec["filter"]
    return "SELECT SUM(d) FROM (%s GROUP BY dt) t" % inner


def expandable_derived(name, spec, allm):
    """派生指标能不能机械算出：(分子键, 分母键)。

    只认三种写法，其余一律返回 None 并显式报告"未覆盖"：
      A / B                两个 atomic 指标
      A / SUM(B)           B 是 semi 指标，需要按日去重后再求和（人日）
      A / AVG(col)         暂不支持（要拿列级平均，不是指标）
    """
    f = (spec.get("formula") or "").strip()
    if "/" not in f:
        return None
    parts = [p.strip() for p in f.split("/")]
    if len(parts) != 2:
        return None
    num, den = parts
    out = []
    for p in (num, den):
        m = re.match(r"^SUM\(\s*([a-zA-Z_][a-zA-Z_0-9]*)\s*\)$", p, re.I)
        if m:
            b = m.group(1)
            if b not in allm or allm[b].get("tier") != "atomic":
                return None
            out.append((b, "perday"))
            continue
        if p in allm and allm[p].get("tier") == "atomic":
            out.append((p, "scalar"))
            continue
        return None
    return out[0], out[1]


# ============================================================== 两端执行
def run_doris(jobs, lo, hi):
    """jobs: [(key, sql)]；返回 {key: Decimal}。"""
    conn = pymysql.connect(autocommit=True, **DORIS)
    cur = conn.cursor()
    cur.execute("SWITCH hive_catalog")
    cur.execute("USE music_dw")
    out = {}
    for key, sql in jobs:
        cur.execute(sql)
        row = cur.fetchone()
        out[key] = _dec(row[0] if row else None)
    conn.close()
    return out


def run_hive(jobs, lo, hi):
    """把全部 SQL 拼成一个文件传上去，一次 hive -f 跑完。

    为什么要标记：hive -S 的输出里 Spark 的运行日志会混进 stdout
    （RemoteBytesRead / ShuffleBytesWritten 那些），没有标记无法定位结果行。
    """
    lines = ["SET hive.cli.print.header=false;", "USE music_dw;"]
    for key, sql in jobs:
        # 标记行 + 值行：SELECT '@@key', CAST(expr AS STRING)
        body = re.sub(r"^SELECT\s+", "SELECT '@@%s', CAST(" % key, sql, count=1, flags=re.I)
        # 在 FROM 之前补上右括号，把整个表达式 CAST 成字符串
        body = re.sub(r"\s+FROM\s+", " AS STRING) FROM ", body, count=1, flags=re.I)
        lines.append(body + ";")
    script = "\n".join(lines) + "\n"

    log("   Hive 侧：上传 %d 条 SQL，单次会话执行（约 1~3 分钟）" % len(jobs))
    p = subprocess.run(
        ["ssh", "-i", SSH_KEY, "-o", "BatchMode=yes",
         "-o", "StrictHostKeyChecking=accept-new", SSH_HOST,
         "cat > /tmp/gt_dual.sql"],
        input=script.encode("utf-8"), capture_output=True)
    if p.returncode != 0:
        log("!! 上传 SQL 失败：%s" % p.stderr.decode("utf-8", "ignore")[:300])
        return None
    cmd = "%s -S -f /tmp/gt_dual.sql %s" % (HIVE, " ".join(HIVE_CONF))
    p = subprocess.run(
        ["ssh", "-i", SSH_KEY, "-o", "BatchMode=yes",
         "-o", "StrictHostKeyChecking=accept-new", SSH_HOST, cmd],
        capture_output=True, text=True, timeout=1800)
    if p.returncode != 0:
        log("!! Hive 执行失败，stderr 末 500 字：\n%s" % p.stderr[-500:])
        return None

    out, cur = {}, None
    for raw in p.stdout.splitlines():
        line = raw.strip()
        if not line or re.search(r"INFO|WARN|SLF4J|log4j", line):
            continue
        if line.startswith("@@"):
            parts = line.split()
            cur = parts[0][2:].strip("'\"")
            val = parts[-1] if len(parts) > 1 else None
            if cur != val:
                out[cur] = _dec(val)
    return out


def _dec(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    s = str(v).strip().strip("'\"")
    if s.lower() in ("null", "none", ""):
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


# ============================================================== 比对
def rel_diff(a, b):
    if a is None or b is None:
        return None
    if a == b:
        return Decimal(0)
    den = max(abs(a), abs(b))
    if den == 0:
        return Decimal(0)
    return abs(a - b) / den


def judge(name, additivity, a, b):
    """计数类必须严格相等；比率/金额给 1e-6 相对容差（浮点最后一位）。

    为什么计数类不放宽：差 1 就说明逻辑不同（去重键、过滤条件、NULL 处理），
    不是精度问题。放宽到"允许差 0.1%"等于把要抓的错放过去。
    """
    if a is None or b is None:
        return "NULL", "一侧为 NULL"
    d = rel_diff(a, b)
    if d is None:
        return "ERR", "无法比较"
    if additivity == "full" and a == b:
        return "OK", ""
    if d == 0:
        return "OK", ""
    tol = Decimal("1e-6") if additivity != "full" else Decimal(0)
    if d <= tol:
        return "OK", "（浮点末位）"
    return "DIFF", "%.4f%%" % (d * 100)


def judge_baseline(add, dv, base):
    """与 YAML baseline 对照。

    这一列不是装饰。2026-11-09 就是靠它抓到 revenue_sum 定义写错：
    Doris 与 Hive 结果一致（都错），只有和 baseline 823,410 一比才露出
    实测 156,986 —— 差了 5.24 倍。

    baseline 可能过期（窗口不同、重造前后），所以差异只报 WARN 不判 FAIL；
    但必须显式打出来，绝不允许"两边一致"就当成对了。
    """
    if base is None or dv is None:
        return "-", ""
    d = rel_diff(dv, base)
    if d is None:
        return "-", ""
    if d == 0:
        return "OK", ""
    # 容差按 baseline 自己的小数位数推：7.28 只承诺到 0.01，差 0.0016 是正常的；
    # 823410 是整数，差 1 就算错。拍一个固定的 1e-4 会把 7.28 这种误判成超差。
    exp = Decimal(base).as_tuple().exponent
    abs_tol = (Decimal(1).scaleb(exp) if exp < 0 else Decimal("0.5")) / 2
    if abs(dv - base) <= abs_tol:
        return "OK", "（基线精度内）"
    return "BASE_DIFF", "基线 %s，偏差 %.2f%%" % (_fmt(base), d * 100)


# ============================================================== 主流程
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", default="", help="逗号分隔，默认全部可重算指标")
    ap.add_argument("--window", nargs=2, default=None, help="起止 dt")
    ap.add_argument("--only", choices=("doris", "hive", "both"), default="both")
    ap.add_argument("--md", default="", help="把结果写成 markdown 报告")
    a = ap.parse_args()

    only = [x.strip() for x in a.metrics.split(",") if x.strip()] or None
    allm, m_all, win = load_metrics(only)
    lo, hi = a.window if a.window else win

    log("=" * 78)
    log("双引擎重算  |  窗口 %s ~ %s  |  引擎 %s" % (lo, hi, a.only))
    log("=" * 78)
    log("【限制声明】两引擎读同一份 HDFS ORC 文件，只换计算引擎。")
    log("             能抓 SQL 逻辑错，抓不到数据本身的错。答辩口径必须带上这句。")
    log("")

    # ---- 1. 展开：哪些指标能算，哪些算不了（算不了的要说清楚为什么）----
    jobs, plan, skipped = [], [], []
    for k, v in allm.items():
        if v.get("tier") == "atomic":
            plan.append((k, "atomic", None))
            jobs.append(("A::" + k, atomic_sql(v, lo, hi)))
        elif v.get("tier") == "derived":
            ex = expandable_derived(k, v, m_all)
            if not ex:
                skipped.append((k, v.get("formula")))
                continue
            plan.append((k, "derived", ex))
            # 派生指标的分子分母必须一并下推，即使没在 --metrics 里点名。
            # 否则 `--metrics arpu` 只会拉 uv，revenue_sum 缺失 → arpu 永远算不出来。
            for base, mode in ex:
                if mode == "perday":
                    jobs.append(("D::" + base, uv_perday_sql(m_all[base], lo, hi)))
                else:
                    jobs.append(("A::" + base, atomic_sql(m_all[base], lo, hi)))
        else:
            # typed（衍生）：formula 带 GROUP BY / 窗口函数 / 分号串联多个子式，
            # 必须先在评测集里定死「按哪个维度」，否则没有唯一答案。
            # 不计入 skipped 就会出现 55 ≠ 46+2 的缺口，读者以为漏算了。
            skipped.append((k, v.get("formula")))

    # 去重（多个派生指标可能共用同一个 perday 分母）
    seen, uniq = set(), []
    for k, s in jobs:
        if k in seen:
            continue
        seen.add(k)
        uniq.append((k, s))
    jobs = uniq

    log("可重算 %d 项（atomic %d + derived %d），跳过 %d 项"
        % (len(plan), sum(1 for p in plan if p[1] == "atomic"),
           sum(1 for p in plan if p[1] == "derived"), len(skipped)))
    for k, f in skipped:
        log("   · 跳过 %-22s formula=`%s` 无法机械展开" % (k, f))
    log("   实际下推 SQL %d 条" % len(jobs))
    log("")

    # ---- 2. 两端执行 ----
    dor, hiv = {}, {}
    if a.only in ("doris", "both"):
        log("   跑 Doris（hive_catalog.music_dw）...")
        dor = run_doris(jobs, lo, hi) or {}
        log("   完成 %d 项" % len(dor))
    if a.only in ("hive", "both"):
        log("   跑 Hive（163/161 集群 spark 引擎）...")
        hiv = run_hive(jobs, lo, hi) or {}
        log("   完成 %d 项" % len(hiv))

    # ---- 3. 计算 + 比对 ----
    def val(store, key):
        return store.get(key)

    def derived_of(store, ex):
        (n1, m1), (n2, m2) = ex
        x = val(store, ("D::" if m1 == "perday" else "A::") + n1)
        y = val(store, ("D::" if m2 == "perday" else "A::") + n2)
        if x is None or y is None or y == 0:
            return None
        return (x / y).quantize(Decimal("0.00000001"))

    rows, bad = [], 0
    for k, kind, ex in plan:
        if kind == "atomic":
            add = (allm[k].get("additivity") or "full")
            dv = val(dor, "A::" + k) if dor else None
            hv = val(hiv, "A::" + k) if hiv else None
            base = allm[k].get("baseline")
        else:
            add = "none"
            dv = derived_of(dor, ex) if dor else None
            hv = derived_of(hiv, ex) if hiv else None
            base = allm[k].get("baseline")
        if a.only == "both" and dv is not None and hv is not None:
            status, note = judge(k, add, dv, hv)
            if status == "DIFF":
                bad += 1
        else:
            status, note = "-", ""
        bst, bnote = judge_baseline(add, dv if dv is not None else hv,
                                    _dec(base) if base is not None else None)
        rows.append((k, kind, add, dv, hv, _dec(base) if base is not None else None,
                     status, note, bst, bnote))

    # ---- 4. 打印 ----
    log("")
    log("%-22s %-8s %-6s %16s %16s %16s %9s %11s" % (
        "指标", "层级", "可加性", "Doris", "Hive", "baseline", "引擎判定", "vs基线"))
    log("-" * 112)
    for k, kind, add, dv, hv, base, st, note, bst, bnote in rows:
        log("%-22s %-8s %-6s %16s %16s %16s %9s %11s %s" % (
            k, kind, add, _fmt(dv), _fmt(hv), _fmt(base), st, bst, note or bnote))

    differ = [r for r in rows if r[6] == "DIFF"]
    basebad = [r for r in rows if r[8] == "BASE_DIFF"]
    okn = sum(1 for r in rows if r[6] == "OK")
    log("")
    log("=" * 78)
    if a.only == "both":
        log("引擎一致 %d 项 / 引擎超差 %d 项 / 只跑单侧 %d 项"
            % (okn, len(differ), sum(1 for r in rows if r[6] == "-")))
        if differ:
            log("\n引擎超差明细：")
            for r in differ:
                log("  - %-22s Doris=%s  Hive=%s  %s" % (r[0], _fmt(r[3]), _fmt(r[4]), r[7]))
    else:
        log("仅跑了 %s 侧，未做引擎比对" % a.only)
    if basebad:
        log("\n与 YAML baseline 不符 %d 项（两边一致但都可能错，这是最值得看的）：" % len(basebad))
        for r in basebad:
            log("  - %-22s 实测=%s  baseline=%s  %s"
                % (r[0], _fmt(r[3] if r[3] is not None else r[4]), _fmt(r[5]), r[9]))

    if a.md:
        write_md(a.md, rows, lo, hi, skipped, differ, basebad, a.only)
        log("\n报告已写入 %s" % a.md)

    log("\n【结论口径】上述一致性证明\"同一逻辑在两个独立引擎上结果相同\"，")
    log("            不证明\"数据正确\"——两者读的是同一份 HDFS 文件。")
    return 1 if differ else 0


def _fmt(v):
    if v is None:
        return "-"
    if isinstance(v, Decimal):
        if v == v.to_integral_value():
            return "{:,}".format(int(v))
        return "{:,.4f}".format(float(v))
    return str(v)


def write_md(path, rows, lo, hi, skipped, differ, basebad, only):
    L = []
    L.append("# 双引擎重算结果（Doris MPP × Hive-on-Spark）\n")
    L.append("窗口：%s ~ %s　｜　引擎：%s\n" % (lo, hi, only))
    L.append("> **限制**：两引擎读同一份 HDFS ORC 文件，只换计算引擎。\n")
    L.append("> 能抓 SQL 逻辑错（口径、JOIN、去重、类型转换），**抓不到数据本身的错**。\n")
    L.append("> 若造数器或 ETL 把数写错了，两边会一致地错，本表无法发现。\n")
    L.append("")
    L.append("| 指标 | 层级 | 可加性 | Doris | Hive | YAML baseline | 引擎判定 | vs 基线 |")
    L.append("|---|---|---|---|---|---|---|---|")
    for k, kind, add, dv, hv, base, st, note, bst, bnote in rows:
        L.append("| `%s` | %s | %s | %s | %s | %s | %s %s | %s %s |" % (
            k, kind, add, _fmt(dv), _fmt(hv), _fmt(base), st, note, bst, bnote))
    L.append("")
    if differ:
        L.append("## 引擎超差 %d 项\n" % len(differ))
        for r in differ:
            L.append("- `%s`：Doris %s vs Hive %s（%s）" % (r[0], _fmt(r[3]), _fmt(r[4]), r[7]))
    else:
        L.append("## 引擎一致性\n\n全部一致，未发现引擎相关差异。\n")
    if basebad:
        L.append("\n## 与 YAML baseline 不符 %d 项\n" % len(basebad))
        L.append("两边算出来的数一致，但都与语义层声明的基线对不上——"
                 "**这类差异比引擎超差更值得查**，因为一致地错不会自己暴露。\n")
        for r in basebad:
            L.append("- `%s`：实测 %s vs baseline %s（%s）"
                     % (r[0], _fmt(r[3] if r[3] is not None else r[4]), _fmt(r[5]), r[9]))
    if skipped:
        L.append("\n## 未覆盖（formula 无法机械展开）\n")
        for k, f in skipped:
            L.append("- `%s`：`%s`" % (k, f))
    io.open(path, "w", encoding="utf-8").write("\n".join(L) + "\n")


if __name__ == "__main__":
    sys.exit(main())
