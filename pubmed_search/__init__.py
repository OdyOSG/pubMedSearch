
from importlib.metadata import version as _v

from .client import search
from .query import rwdSearchTerms

__all__ = ["search"]

try:
    __version__ = _v(__name__)
except Exception:  # pragma: no cover
    __version__ = "0.3.0"
