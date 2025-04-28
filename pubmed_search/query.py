# pubmed_search/query.py
from searchpubmed.query_builder import build_query, STRATEGY3_OPTS
def rwdSearchTerms(opts=STRATEGY3_OPTS):
    """Return a ready-made Boolean block for observational RWD studies."""
    return build_query(opts)
