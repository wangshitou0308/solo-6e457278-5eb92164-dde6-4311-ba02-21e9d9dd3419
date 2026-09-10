"""本地 HTTP 缓存仿真 API（仅依赖 Python 标准库）。"""

from .engine import Engine, new_state, DEFAULT_CONFIG
from .store import Store

__all__ = ["Engine", "Store", "new_state", "DEFAULT_CONFIG"]
__version__ = "1.0.0"
