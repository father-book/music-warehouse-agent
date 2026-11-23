"""tests/ 目录的路径适配层。

selftest 物理上搬进 tests/ 后，有两类隐藏依赖会断：
  1. `import _bootstrap` —— 原本靠同目录命中，现在不在同一层；
  2. `Path(__file__).parent` / `.parents[2]` —— 原本分别指 code/agent 与项目根，
     移动一层后全错位。有几个测试甚至用 `HERE.glob("*.py")` 扫描业务源码做
     静态断言（gateway / vault），指错目录会静默漏检。

所以这里统一提供：
  · 把 code/agent 与 code 注入 sys.path（与父版 _bootstrap 等价，但基于 tests 定位）；
  · HERE = code/agent 绝对路径；ROOT = 项目根绝对路径。
各 selftest 只要把 `HERE = Path(__file__).resolve().parent` 换成
`from _bootstrap import HERE, ROOT`，行为与搬之前逐字一致。
"""
from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent   # code/agent/tests
HERE = _TESTS.parent                       # code/agent
ROOT = HERE.parents[1]                     # 项目根（code/agent -> code -> 项目根）
_CODE = HERE.parent                        # code/
_VENDOR = HERE / "_vendor"

for p in (str(HERE), str(_CODE)):
    if p not in sys.path:
        sys.path.insert(0, p)
if str(_VENDOR) not in sys.path:
    sys.path.append(str(_VENDOR))

# 复用父版 bootstrap 的语义（它是一个模块，不是包）
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
