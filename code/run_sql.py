"""执行 SQL 文件到 Doris（按分号拆分，逐条执行）。

用法：
    python code/run_sql.py sql/dwd/doris_dwd_tables.sql
    python code/run_sql.py sql/dwd/doris_dwd_play_end_clean.sql
"""
import sys
import os
import pymysql

HOST, PORT, USER, PWD = "192.168.128.163", 9030, "root", ""


def split_statements(sql: str):
    """去掉整行注释后按分号拆分。SQL 中不含字符串内的分号，拆分安全。"""
    lines = []
    for line in sql.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    body = "\n".join(lines)
    return [s.strip() for s in body.split(";") if s.strip()]


def main():
    if len(sys.argv) < 2:
        print("用法: python code/run_sql.py <sql文件>")
        sys.exit(1)

    path = sys.argv[1]
    sql = open(path, encoding="utf-8").read()
    stmts = split_statements(sql)
    print(f"文件: {os.path.basename(path)}  语句数: {len(stmts)}")

    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PWD,
                           charset="utf8mb4", autocommit=True)
    cur = conn.cursor()
    for i, stmt in enumerate(stmts, 1):
        head = " ".join(stmt.split())[:70]
        print(f"[{i}/{len(stmts)}] {head}")
        try:
            cur.execute(stmt)
            rows = cur.fetchall()
            if rows and len(rows) <= 20:
                for r in rows:
                    print("   ", r)
            elif rows:
                print(f"    ({len(rows)} 行)")
        except Exception as e:
            print(f"    !! 失败: {e}")
            conn.close()
            sys.exit(2)
    conn.close()
    print("全部执行完成")


if __name__ == "__main__":
    main()
