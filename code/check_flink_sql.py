"""Flink SQL 静态自检：提交前抓出最常见的低级错误。

检查项：
    1. CREATE TABLE 的列数 vs 每个 INSERT INTO 该表的 SELECT 列数是否一致
    2. 括号是否配对
    3. EXECUTE STATEMENT SET 的 BEGIN / END 是否配对
    4. INSERT 的目标表是否在本文件中定义过

在虚拟机上调 Flink SQL 的成本很高（提交 → 报错 → 改 → 重提，一轮几分钟），
所以把这类「数逗号」的活交给脚本。

用法：
    python code/check_flink_sql.py                        # 检查 sql/realtime 下全部
    python code/check_flink_sql.py sql/realtime/job_b_dwd.sql
"""
import glob
import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def strip_comments(sql: str) -> str:
    """去掉 -- 行注释，注意不要误伤字符串里的 --（本项目 SQL 里没有，简单处理）。"""
    out = []
    for line in sql.split("\n"):
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        out.append(line)
    return "\n".join(out)


def split_top(s: str):
    """按顶层逗号切分，忽略括号内的逗号（ROUND(a,2)）与 ROW<...> 内的逗号。

    尖括号只在「ROW<」起始或已在 ROW<> 内部时才算嵌套，
    这样 SELECT 里的比较运算符 `>`（如 rn > 10）不会被误当成闭合括号。
    """
    parts, depth, angle, cur = [], 0, 0, ""
    for i, ch in enumerate(s):
        if ch == "<":
            if angle > 0 or s[max(0, i - 3):i].rstrip().upper().endswith("ROW"):
                angle += 1
        elif ch == ">" and angle > 0:
            angle -= 1
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0 and angle == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def parse_tables(sql: str):
    """提取 CREATE TABLE 名 -> (物理列名列表, WITH 参数 dict)。"""
    tables = {}
    for m in re.finditer(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)\)\s*WITH\s*\((.*?)\);",
        sql, re.S | re.I,
    ):
        name, body, props_raw = m.group(1), m.group(2), m.group(3)
        cols = []
        for item in split_top(body):
            head = item.strip()
            first = head.split()[0] if head.split() else ""
            # 跳过 watermark 声明行
            if re.match(r"(?i)^WATERMARK\b", head):
                continue
            # 跳过计算列（"ts AS TO_TIMESTAMP(x)"）——它们不是物理列
            if re.match(r"^\w+\s+AS\s+", head, re.I):
                continue
            # 跳过表约束行（HBase 维表要求 PRIMARY KEY (rowkey) NOT ENFORCED）
            if re.match(r"(?i)^(PRIMARY\s+KEY|UNIQUE|CONSTRAINT|CHECK|FOREIGN\s+KEY)\b", head):
                continue
            if first:
                cols.append(first.strip("`"))
        props = dict(re.findall(r"'([\w.\-]+)'\s*=\s*'([^']*)'", props_raw))
        tables[name.lower()] = (cols, props)
    return tables


def parse_inserts(sql: str):
    """提取 (目标表, SELECT 列数, SELECT 片段)。"""
    results = []
    for m in re.finditer(
        r"INSERT\s+INTO\s+(\w+)\s+SELECT\s+(.*?)\s+FROM\s",
        sql, re.S | re.I,
    ):
        target = m.group(1)
        select_body = m.group(2)
        # 去掉表达式里的换行与多余空格不影响计数
        cols = split_top(select_body)
        results.append((target.lower(), len(cols), select_body))
    return results


def check_file(path: str):
    raw = open(path, encoding="utf-8").read()
    sql = strip_comments(raw)
    file_name = os.path.basename(path)
    errors, warns = [], []

    # 括号配对
    depth = 0
    for i, ch in enumerate(sql):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                errors.append(f"括号在第 {i} 个字符处提前闭合")
                break
    if depth != 0:
        errors.append(f"括号未配对，差值 {depth}")

    # STATEMENT SET 配对
    n_begin = len(re.findall(r"(?i)EXECUTE\s+STATEMENT\s+SET\s+BEGIN", sql))
    n_end = len(re.findall(r"(?i)^\s*END\s*;", sql, re.M))
    if n_begin != n_end:
        errors.append(f"STATEMENT SET: BEGIN {n_begin} 个，END {n_end} 个，不配对")

    tables = parse_tables(sql)
    inserts = parse_inserts(sql)
    targets = {t for t, _, _ in inserts}

    for target, n_select, body in inserts:
        if target not in tables:
            errors.append(f"INSERT 目标表 {target} 在本文件里没有 CREATE TABLE")
            continue
        n_cols = len(tables[target][0])
        if n_cols != n_select:
            errors.append(
                f"{target}: 表 {n_cols} 列，SELECT {n_select} 列，差 {n_select - n_cols}"
            )
            # 列出 SELECT 列，方便对照
            cols = split_top(body)
            preview = [c.split("\n")[-1].strip()[:40] for c in cols]
            warns.append(f"    {target} 的 SELECT 列: {preview}")

    # 收集 kafka topic 表，供跨文件的层间列名比对
    kafka_tables = {}
    for name, (cols, props) in tables.items():
        if props.get("connector") != "kafka" or not props.get("topic"):
            continue
        role = "sink" if name in targets else "source"
        kafka_tables.setdefault(props["topic"], {"sink": [], "source": []})[role].append(
            (file_name, name, cols)
        )

    status = "OK" if not errors else "FAIL"
    print(f"[{status}] {file_name}  表 {len(tables)} 个，INSERT {len(inserts)} 条")
    for e in errors:
        print(f"    ✗ {e}")
    for w in warns:
        print(w)
    return len(errors) == 0, kafka_tables


def check_topics(all_kafka: dict):
    """层间 topic 的写入方与消费方，列名必须完全一致。

    json format 是按 key 匹配的，列名差一个字符就读成 NULL，
    而且不会报错——数据静默丢失，是这类管道最难查的 bug。
    """
    errors = []
    for topic, sides in sorted(all_kafka.items()):
        sinks, sources = sides["sink"], sides["source"]
        if not sinks or not sources:
            continue
        for _, s_name, s_cols in sinks:
            for _, o_name, o_cols in sources:
                if s_cols != o_cols:
                    only_s = [c for c in s_cols if c not in o_cols]
                    only_o = [c for c in o_cols if c not in s_cols]
                    errors.append(
                        f"topic {topic}: 写入方 {s_name} 与消费方 {o_name} 列名不一致"
                    )
                    if only_s:
                        errors.append(f"    仅写入方有: {only_s}")
                    if only_o:
                        errors.append(f"    仅消费方有: {only_o}")
                    if not only_s and not only_o:
                        errors.append("    列名相同但顺序不同")
    if errors:
        print("[FAIL] 层间 topic 列名比对")
        for e in errors:
            print(f"    ✗ {e}")
    else:
        n = sum(1 for s in all_kafka.values() if s["sink"] and s["source"])
        print(f"[OK] 层间 topic 列名比对    共 {n} 个 topic 有上下游")
    return len(errors) == 0


def main():
    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = sorted(glob.glob(os.path.join(BASE_DIR, "sql/realtime/job_*.sql")))

    if not files:
        print("没有找到待检查的文件")
        return 1

    ok = True
    all_kafka = {}
    for f in files:
        file_ok, kafka = check_file(f)
        ok &= file_ok
        for topic, sides in kafka.items():
            merged = all_kafka.setdefault(topic, {"sink": [], "source": []})
            merged["sink"].extend(sides["sink"])
            merged["source"].extend(sides["source"])

    print()
    ok &= check_topics(all_kafka)

    print()
    print("全部通过" if ok else "存在错误，先修好再提交")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
