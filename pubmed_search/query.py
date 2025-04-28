
"""Utility helpers for common query building tasks."""

from searchpubmed.query_builder import build_query, STRATEGY3_OPTS


def rwdSearchTerms(opts=STRATEGY2_OPTS):
    """Return a ready‑made Boolean block for observational RWD studies."""
    return build_query(opts)
