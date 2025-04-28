
"""Ready‑made Boolean blocks for common PubMed study designs.

We keep the implementation tiny by delegating to *searchpubmed*'s
`query_builder.build_query` function.
"""

from __future__ import annotations

from searchpubmed.query_builder import build_query, STRATEGY3_OPTS

def rwdSearchTerms(opts: dict | None = None) -> str:
    """Return an observational real‑world‑data Boolean block.

    Parameters
    ----------
    opts
        Optional override for the `searchpubmed.query_builder.STRATEGY3_OPTS`
        dict. Pass your own tweaks if you need a narrower / broader filter.
    """
    return build_query(opts or STRATEGY3_OPTS)
