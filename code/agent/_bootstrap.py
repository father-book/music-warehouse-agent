"""把 code/agent 和 _vendor 放进 sys.path，让扁平模块之间能互相 import。

每个模块首行 `import _bootstrap` 即可，不用管从哪儿被调起。

_vendor 放 sys.path 末尾：真包（pip 装上的）优先命中，
本目录只是当前网络不可达时的兜底，详见 _vendor/annotated_types/__init__.py。
"""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_VENDOR = _HERE / "_vendor"
_CODE = _HERE.parent                 # code/governance 作为顶层包从这里导入

if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

if str(_CODE) not in sys.path:
    sys.path.append(str(_CODE))       # 放末尾：不抢 stdlib / 已装包的命中

if _VENDOR.exists() and str(_VENDOR) not in sys.path:
    sys.path.append(str(_VENDOR))
