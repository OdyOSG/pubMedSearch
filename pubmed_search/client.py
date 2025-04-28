from __future__ import annotations

import logging
import sys
import re
from typing import Any

import pandas as pd
from searchpubmed.pubmed import fetch_pubmed_fulltexts as _fetch

from datetime import date
from collections.abc import Sequence

from datetime import date

def append_calendar_year_range_to_query(
    query: str,
    *,
    year_from: int | None = None,
    year_to:   int | None = None,
) -> str:
    """
    Build a PubMed-style Boolean query.

    Examples
    --------
    >>> append_calendar_year_range_to_query("cancer", year_from=2010, year_to=2020)
    '((cancer) AND ("2010"[PDAT] : "2020"[PDAT]))'

    >>> append_calendar_year_range_to_query("machine learning")
    '(machine learning)'
    """
    if not query or not query.strip():
        raise ValueError("`query` cannot be empty")

    # always keep the literal query in its own parentheses
    query_block = f"({query})"

    if year_from or year_to:
        date_block = (
            f'("{year_from or 1900}"[PDAT] : '
            f'"{year_to or date.today().year}"[PDAT])'
        )
        # wrap the whole thing so the query and date window stay together
        return f"({query_block} AND {date_block})"

    return query_block


# ---------------------------------------------------------------------------
# Logger: ensure INFO messages reach the notebook/console if the host app has
# not configured logging yet (common in Databricks notebooks).
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setLevel(logging.INFO)
    _handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
from searchpubmed.query_builder import STRATEGY3_OPTS
def search(
    query: list[str],
    *,
    rwd_terms: str | None = build_query(STRATEGY3_OPTS),
    year_from: int | None = 2010,
    year_to: int | None = 3000,
    api_key: str | None = None,
    retmax: int = 2000,
    min_fulltext_chars: int = 2000,
    batch_size: int = 150,
    **fetch_kwargs: Any,
) -> pd.DataFrame:
    """Fetch PubMed metadata + full text for *query* with optional filters."""

    if not query:
        raise ValueError("'query' must be a non-empty list of strings")

    # 1) OR-join the user clause list
    base = " OR ".join(query)
    q = f"({base})"

    # 2) Optional RWD block
    if rwd_terms:
        q = f"({q} AND ({rwd_terms}))"

    # 3) Inject PDAT boundaries via append_calendar_year_range_to_query helper
    q = append_calendar_year_range_to_query(q, year_from=year_from, year_to=year_to)

    # 5) Delegate to the backend fetcher
    df: pd.DataFrame = _fetch(
        query=q,
        api_key=api_key,
        retmax=retmax,
        min_fulltext_chars=min_fulltext_chars,
        **fetch_kwargs,
    )

    # 6) Attach raw and pretty query for provenance / debugging
    df.attrs["query"] = q
    return df

__all__ = ["search"]
