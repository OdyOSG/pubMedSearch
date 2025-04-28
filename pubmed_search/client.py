
from __future__ import annotations

import logging
import sys
from typing import Any
import pandas as pd
from datetime import date
from ohdsi_utils.boolean_formatter import pretty_boolean

from searchpubmed.pubmed import fetch_pubmed_fulltexts as _fetch
from searchpubmed.query_builder import STRATEGY3_OPTS, STRATEGY2_OPTS, build_query


def append_calendar_year_range_to_query(
    query: str,
    *,
    year_from: int | None = None,
    year_to: int | None = None,
) -> str:
    """Build a PubMed-style Boolean query with optional publication date window."""
    if not query or not query.strip():
        raise ValueError("`query` cannot be empty")

    query_block = f"({query})"

    if year_from or year_to:
        date_block = (
            f'("{year_from or 1900}"[PDAT] : "{year_to or date.today().year}"[PDAT])'
        )
        return f"({query_block} AND {date_block})"

    return query_block


# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setLevel(logging.INFO)
    _handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------


def search(
    query: list[str],
    *,
    rwd_terms: str | None = build_query(STRATEGY2_OPTS),
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

    base = " OR ".join(query)
    q = f"({base})"

    if rwd_terms:
        q = f"({q} AND ({rwd_terms}))"

    q = append_calendar_year_range_to_query(q, year_from=year_from, year_to=year_to)

    df: pd.DataFrame = _fetch(
        query=q,
        api_key=api_key,
        retmax=retmax,
        min_fulltext_chars=min_fulltext_chars,
        **fetch_kwargs,
    )

    df.attrs["query"] = q
    df.attrs["query_pretty"] = pretty_boolean(q)
    return df


__all__ = ["search"]
