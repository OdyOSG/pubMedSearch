
"""High‑level `search()` convenience around *searchpubmed*.

Adds:
  * Human‑friendly Boolean assembly (OR‑joining + calendar boundaries)
  * Helpful pretty‑printing of the final query for reproducibility
  * Tiny dataframe wrapper that attaches the raw query in `df.attrs`
"""

from __future__ import annotations

import logging
import sys
import re
from datetime import date
from typing import Any

import pandas as pd
from collections.abc import Sequence

from searchpubmed.pubmed import fetch_pubmed_fulltexts as _fetch

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)


# -------------------------------------------------------------------------
# Query helpers
# -------------------------------------------------------------------------
_TOKEN_RE = re.compile(r'"[^"]+"|\(|\)|AND|OR|[^()\s]+')

def _beautify_query(q: str, *, indent: int = 2) -> str:
    """Pretty printer for PubMed queries (adds newlines / indentation)."""
    depth = 0
    out: list[str] = []
    for tok in _TOKEN_RE.findall(q):
        if tok == '(':
            out.append(' ' * (depth * indent) + '(')
            depth += 1
        elif tok == ')':
            depth -= 1
            out.append(' ' * (depth * indent) + ')')
        elif tok in {'AND', 'OR'}:
            out.append(' ' * (depth * indent) + tok)
        else:
            out.append(' ' * (depth * indent) + tok)
    return '\n'.join(out)


def _append_calendar_year_range(
    query: str,
    *,
    year_from: int | None = None,
    year_to: int | None = None,
) -> str:
    """Inject a PDAT calendar window into *query* if any bound is provided."""
    query_block = f"({query})"
    if year_from or year_to:
        year_from = year_from or 1900
        year_to = year_to or date.today().year
        date_block = f'("{year_from}"[PDAT] : "{year_to}"[PDAT])'
        return f'({query_block} AND {date_block})'
    return query_block


# -------------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------------
def search(
    user_queries: Sequence[str] | str,
    *,
    rwd_terms: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    api_key: str | None = None,
    retmax: int = 2000,
    min_fulltext_chars: int = 2000,
    batch_size: int = 150,
    **fetch_kwargs: Any,
) -> pd.DataFrame:
    """Return a dataframe of PubMed results for *user_queries*.

    Parameters
    ----------
    user_queries
        Either a single query string or an iterable of strings. They are             OR‑joined before being combined with other filters.
    rwd_terms
        Optional pre‑built Boolean block (e.g. from             :pyfunc:`pubmed_search.rwdSearchTerms`).
    year_from, year_to
        Inclusive publication year window. When not provided the window is             left unbounded.
    api_key
        Your personal NCBI API key for higher rate limits.
    retmax, min_fulltext_chars, batch_size
        Passed verbatim to *searchpubmed*'s :pyfunc:`fetch_pubmed_fulltexts`.
    fetch_kwargs
        Any other arguments accepted by *fetch_pubmed_fulltexts*.
    """
    if isinstance(user_queries, str):
        user_queries = [user_queries]
    user_queries = [q.strip() for q in user_queries if q and q.strip()]
    if not user_queries:
        raise ValueError("At least one non‑empty query string is required.")

    # Build the query string step by step
    q = " OR ".join(user_queries)
    if rwd_terms:
        q = f"({q}) AND ({rwd_terms})"
    q_final = _append_calendar_year_range(q, year_from=year_from, year_to=year_to)

    logger.info("PubMed query (pretty):\n%s", _beautify_query(q_final))

    df = _fetch(
        query=q_final,
        api_key=api_key,
        retmax=retmax,
        min_fulltext_chars=min_fulltext_chars,
        batch_size=batch_size,
        **fetch_kwargs,
    )

    # attach provenance
    df.attrs["query"] = q_final
    df.attrs["query_pretty"] = _beautify_query(q_final)

    return df
