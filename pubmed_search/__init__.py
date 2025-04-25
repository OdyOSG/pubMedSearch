
"""
pubmed_search
=============

High‑level wrapper around `searchpubmed` that adds:

* Opinionated Boolean query builder for observational real‑world‑data studies
* Tiny convenience client & CLI
* Spreadsheet export helpers

This keeps **all heavy lifting** (rate‑limited calls, full‑text scraping, XML parsing …) in
`searchpubmed`, focusing instead on reproducible query generation and downstream polishing.

Author: Refactored by ChatGPT – April 2025
License: Apache‐2.0
"""
from __future__ import annotations
from importlib.metadata import version as _v

from .query import build_query
from .client import search
from .utils import export_dataframe_to_excel
from .query import rwdSearchTerms

__all__ = ["build_query", "search", "export_dataframe_to_excel", "rwdSearchTerms"]
__version__ = "0.2.0"
