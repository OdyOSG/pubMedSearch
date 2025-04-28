from __future__ import annotations
from importlib.metadata import version as _v

from .client import search
from .query import rwdSearchTerms

__all__ = ["search"]
__version__ = "0.1.0"
