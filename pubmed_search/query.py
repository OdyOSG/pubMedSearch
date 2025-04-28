
"""Utility helpers for common query building tasks."""

from searchpubmed.query_builder import build_query, STRATEGY1_OPTS, STRATEGY2_OPTS, STRATEGY3_OPTS, STRATEGY4_OPTS, STRATEGY5_OPTS, STRATEGY6_OPTS, build_query

def rwdSearchTerms(opts=STRATEGY2_OPTS):
    """Return a ready‑made Boolean block for observational RWD studies."""
    return build_query(opts)
