"""jsonpatch 的最小实现（vendored），按 RFC 6902。

为什么需要：
    langchain_core 1.2 在 output_parsers 和 tracers.log_stream 里
    顶层 `import jsonpatch`，缺它则整个 runnable 链装不起来。
    离线环境装不上，只能自己实现。

覆盖范围（够 langchain_core 用）：
    apply_patch / make_patch / JsonPatch(.apply / .from_diff)
    op: add / remove / replace / move / copy / test

from_diff 采用"逐键递归 + 列表整体替换"的策略：
    语义正确，patch 不如官方精巧，但对状态同步场景完全够用。
"""
from __future__ import annotations

import copy
from typing import Any, Iterable

__all__ = [
    "JsonPatch",
    "JsonPatchConflict",
    "JsonPatchTestFailed",
    "apply_patch",
    "make_patch",
]


class JsonPatchException(Exception):
    pass


class JsonPatchConflict(JsonPatchException):
    pass


class JsonPatchTestFailed(JsonPatchException, AssertionError):
    pass


# ------------------------------------------------------------------ pointer

def _parse_pointer(pointer: str) -> list[str]:
    if pointer in ("", "/"):
        return [] if pointer == "" else [""]
    if not pointer.startswith("/"):
        raise JsonPatchException("路径必须以 / 开头：%r" % pointer)
    return [p.replace("~1", "/").replace("~0", "~") for p in pointer.split("/")[1:]]


def _walk(doc: Any, parts: list[str]) -> Any:
    cur = doc
    for p in parts:
        if isinstance(cur, dict):
            if p not in cur:
                raise JsonPatchConflict("路径不存在：%s" % "/".join(parts))
            cur = cur[p]
        elif isinstance(cur, list):
            cur = cur[int(p)]
        else:
            raise JsonPatchConflict("无法在 %s 上取 %r" % (type(cur).__name__, p))
    return cur


def _add(doc: Any, parts: list[str], value: Any) -> Any:
    if not parts:
        return value
    parent = _walk(doc, parts[:-1]) if len(parts) > 1 else doc
    key = parts[-1]
    if isinstance(parent, dict):
        parent[key] = value
    elif isinstance(parent, list):
        if key == "-":
            parent.append(value)
        else:
            parent.insert(int(key), value)
    else:
        raise JsonPatchConflict("无法在 %s 上新增 %r" % (type(parent).__name__, key))
    return doc


def _remove(doc: Any, parts: list[str]) -> Any:
    if not parts:
        return None
    parent = _walk(doc, parts[:-1]) if len(parts) > 1 else doc
    key = parts[-1]
    if isinstance(parent, dict):
        if key not in parent:
            raise JsonPatchConflict("删除不存在的键：%s" % key)
        del parent[key]
    elif isinstance(parent, list):
        parent.pop(int(key))
    else:
        raise JsonPatchConflict("无法从 %s 删除" % type(parent).__name__)
    return doc


# ------------------------------------------------------------------ patch

class JsonPatch:
    def __init__(self, patch: Iterable[dict] | None = None):
        self.patch: list[dict] = list(patch or [])

    def __repr__(self) -> str:
        return "JsonPatch(%r)" % (self.patch,)

    def apply(self, obj: Any, in_place: bool = False) -> Any:
        doc = obj if in_place else copy.deepcopy(obj)
        for op in self.patch:
            doc = _apply_one(doc, op)
        return doc

    @classmethod
    def from_diff(cls, src: Any, dst: Any, path: str = "") -> "JsonPatch":
        return cls(_diff(src, dst, path))


def _esc(k: str) -> str:
    return str(k).replace("~", "~0").replace("/", "~1")


def _diff(src: Any, dst: Any, path: str) -> list[dict]:
    if type(src) is not type(dst):
        return [{"op": "replace", "path": path or "", "value": dst}]

    if isinstance(src, dict):
        ops: list[dict] = []
        for k in src:
            if k not in dst:
                ops.append({"op": "remove", "path": "%s/%s" % (path, _esc(k))})
        for k, v in dst.items():
            if k not in src:
                ops.append({"op": "add", "path": "%s/%s" % (path, _esc(k)), "value": v})
            else:
                ops.extend(_diff(src[k], v, "%s/%s" % (path, _esc(k))))
        return ops

    if isinstance(src, list):
        if len(src) == len(dst):
            ops = []
            for i, (a, b) in enumerate(zip(src, dst)):
                ops.extend(_diff(a, b, "%s/%d" % (path, i)))
            return ops
        return [{"op": "replace", "path": path or "", "value": dst}]

    if src != dst:
        return [{"op": "replace", "path": path or "", "value": dst}]
    return []


def _apply_one(doc: Any, op: dict) -> Any:
    kind = op.get("op")
    parts = _parse_pointer(op.get("path", ""))

    if kind == "add":
        return _add(doc, parts, copy.deepcopy(op.get("value")))
    if kind == "remove":
        return _remove(doc, parts)
    if kind == "replace":
        if parts:
            _remove(doc, parts)
            return _add(doc, parts, copy.deepcopy(op.get("value")))
        return copy.deepcopy(op.get("value"))
    if kind == "move":
        val = copy.deepcopy(_walk(doc, _parse_pointer(op["from"])))
        doc = _remove(doc, _parse_pointer(op["from"]))
        return _add(doc, parts, val)
    if kind == "copy":
        val = copy.deepcopy(_walk(doc, _parse_pointer(op["from"])))
        return _add(doc, parts, val)
    if kind == "test":
        actual = _walk(doc, parts) if parts else doc
        if actual != op.get("value"):
            raise JsonPatchTestFailed("%s 的值不是 %r 而是 %r"
                                      % (op.get("path"), op.get("value"), actual))
        return doc
    raise JsonPatchException("未知操作：%r" % kind)


def apply_patch(doc: Any, patch: Iterable[dict], in_place: bool = False) -> Any:
    return JsonPatch(patch).apply(doc, in_place=in_place)


def make_patch(src: Any, dst: Any) -> JsonPatch:
    return JsonPatch(_diff(src, dst, ""))
