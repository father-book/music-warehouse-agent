"""配置加载。所有节点拿到的都是同一个 dict，改配置不用改代码。"""
from __future__ import annotations

import _bootstrap  # noqa: F401

from functools import lru_cache
import os
from pathlib import Path

import yaml

from env import AGENT_DIR, PROJECT_ROOT


@lru_cache(maxsize=4)
def load_config(path: str | None = None) -> dict:
    # `DA_CONFIG` 让同一份代码跑不同配置（压测、限流验证、多租户试跑），
    # 不用改文件再改回来——测试和线上共用一份 config.yaml 太容易互相污染。
    p = Path(path or os.environ.get("DA_CONFIG") or (AGENT_DIR / "config.yaml"))
    with open(p, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # 把语义层 / 护栏的相对路径解析成绝对路径，节点里就不用再拼
    cfg["_paths"] = {
        "tables_json": PROJECT_ROOT / cfg["semantic"]["tables_json"],
        "metrics_yaml": PROJECT_ROOT / cfg["semantic"]["metrics_yaml"],
        "dimensions_yaml": PROJECT_ROOT / cfg["semantic"]["dimensions_yaml"],
        "glossary_yaml": PROJECT_ROOT / cfg["semantic"]["glossary_yaml"],
        "guard_script": PROJECT_ROOT / cfg["guard"]["script"],
        "root": PROJECT_ROOT,
    }
    return cfg


def reload_config(path: str | None = None) -> dict:
    load_config.cache_clear()
    return load_config(path)
