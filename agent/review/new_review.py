#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""审核包脚手架：从 review/_template 生成 review/<需求ID>/。

设计取向：**宁可留一堆 {{占位符}} 也不替人编内容**。
  · 能从参数确定的（需求 ID、目标表、字段名）直接填好
  · 填不了的一律保留 {{X}}，并在最后列出来
  · 不允许"先生成再补"含糊过去——未填占位符数就是评审前的待办数

用法：
  python3 new_review.py --id R001-ads-pay-rate \\
                        --title "ads_daily_overview 增加付费率" \\
                        --table ads_daily_overview --col pay_rate --col-type DOUBLE
"""
from __future__ import print_function

import argparse
import datetime
import io
import os
import re
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
TPL = os.path.join(ROOT, "review", "_template")
OUT_ROOT = os.path.join(ROOT, "review")

PH = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="需求 ID，如 R001-ads-pay-rate")
    ap.add_argument("--title", required=True, help="一句话标题")
    ap.add_argument("--table", default="{{T1}}", help="目标表")
    ap.add_argument("--col", default="{{COL}}", help="新增/变更字段")
    ap.add_argument("--col-type", default="{{COL_TYPE}}", help="字段类型")
    ap.add_argument("--force", action="store_true", help="覆盖已存在目录")
    a = ap.parse_args()

    out = os.path.join(OUT_ROOT, a.id)
    if os.path.exists(out) and not a.force:
        print("!! 目录已存在：%s\n   确认覆盖加 --force（覆盖会丢掉 05 的实跑记录）" % out)
        return 2
    if not os.path.isdir(TPL):
        print("!! 模板目录不存在：%s" % TPL)
        return 2

    if os.path.exists(out):
        shutil.rmtree(out)
    shutil.copytree(TPL, out)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fixed = {
        "REQ_ID": a.id,
        "REQ_TITLE": a.title,
        "REQ_TARGET_TABLE": a.table,
        "T1": a.table,
        "COL": a.col,
        "COL_TYPE": a.col_type,
        "GEN_AT": now,
        "RUN_AT": now,
    }

    total_ph = 0
    unfilled = []
    for fn in sorted(os.listdir(out)):
        p = os.path.join(out, fn)
        if not os.path.isfile(p):
            continue
        text = io.open(p, encoding="utf-8").read()
        for k, v in fixed.items():
            text = text.replace("{{%s}}" % k, v)
        io.open(p, "w", encoding="utf-8").write(text)
        left = sorted(set(PH.findall(text)))
        total_ph += len(left)
        if left:
            unfilled.append((fn, left))

    print("== 已生成 %s ==" % out)
    for fn in sorted(os.listdir(out)):
        p = os.path.join(out, fn)
        if os.path.isfile(p):
            print("   %-22s %6d B" % (fn, os.path.getsize(p)))

    if unfilled:
        print("\n待填占位符（%d 处，未填完不许进评审）：" % total_ph)
        for fn, ks in unfilled:
            print("   %-22s %s" % (fn, ", ".join(ks)))
    else:
        print("\n占位符已全部填好。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
