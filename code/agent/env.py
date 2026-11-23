"""极小的 .env 读取器。

不引 python-dotenv：只有几行，标准库够用，少一个依赖。
已存在的环境变量优先（.env 只做兜底，方便 CI / 容器里注入）。
"""
from __future__ import annotations

import _bootstrap  # noqa: F401  （确保 _vendor 在 sys.path 上，httpx 等依赖才能解析）

import os
from pathlib import Path

AGENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = AGENT_DIR.parent.parent


def load_dotenv(path: Path | None = None) -> None:
    p = path or (AGENT_DIR / ".env")
    if not p.exists():
        return
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, val = line.partition("=")
        if not sep:
            continue
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def require_env(name: str) -> str:
    """兼容层。新代码请用 `vault.require(name).reveal()`。

    保留它是因为"还有没有别处在用"这件事会随重构变化，直接删掉会让
    漏改的一处变成 ImportError——那是启动就炸，比悄悄用旧路更难查。
    这里转一手，行为一致（缺失即抛），但会留下调用痕迹。
    """
    import vault                    # 延迟导入：vault 依赖本模块加载 .env，顶层互导会成环
    return vault.require(name).reveal()


load_dotenv()
