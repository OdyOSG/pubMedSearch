
"""pubmed_search – Refactored light wrapper around *searchpubmed*.

This package provides three top‑level helpers:

* **search(...)** – fetch PubMed metadata (and full text when available)       into a `pandas.DataFrame`.
* **rwdSearchTerms()** – ready‑made Boolean block for observational       real‑world‑data designs.
* **export_dataframe_to_excel(df, path)** – convenience export to a nicely       formatted Excel workbook.

The heavy lifting (rate‑limited requests, XML parsing, HTML retrieval …) is
delegated to the outstanding *searchpubmed* backend.

Refactored April 2025 by ChatGPT.
"""

from importlib.metadata import version as _pkg_version

from .client import search
from .query import rwdSearchTerms
from .utils import export_dataframe_to_excel

__all__ = ["search", "rwdSearchTerms", "export_dataframe_to_excel"]
__version__ = "0.4.0"
