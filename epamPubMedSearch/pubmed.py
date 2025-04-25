"""
pubmed.py - wrapper module integrating pubMedSearch for query generation
and searchpubmed for data retrieval and local post-processing.
"""

from __future__ import annotations

import importlib
import pandas as pd

# Load external pubMedSearch package (pubMedSearch must be installed)
try:
    # The external package provides pubmedSearchTerms() for RWD-focused query building
    pubmed_search_pkg = importlib.import_module("pubmed")
    pubmedSearchTerms = pubmed_search_pkg.pubmedSearchTerms
except ImportError:
    raise ImportError(
        "pubMedSearch dependency not found. Please install the pubMedSearch package."
    )

# Import the core searchpubmed pipeline
# Assumes that the original full-text retrieval logic has been refactored into a submodule named "fetcher"
try:
    from .fetcher import fetch_pubmed_fulltexts
except ImportError:
    raise ImportError(
        "searchpubmed fetcher not found. Make sure the pipeline code is available in fetcher.py."
    )


def search_pubmed_rwd(
    mesh_term: str,
    *,
    date_term: str | None = None,
    api_key: str | None = None,
    retmax: int = 2000,
    min_fulltext_chars: int = 2000,
) -> pd.DataFrame:
    """
    Search PubMed for observational real-world-data studies using a given MeSH term.

    This function generates an RWD-focused Boolean query via pubMedSearchTerms(),
    combines it with the provided MeSH term and optional date filter, then invokes
    the searchpubmed pipeline to fetch metadata and full-text variants. Results are
    post-processed locally (e.g., filtering out entries without full text).

    Parameters
    ----------
    mesh_term : str
        MeSH term for the target topic (e.g., "Ischemic Attack, Transient").
    date_term : str, optional
        Publication date filter, in PubMed syntax (e.g., '("2010"[PDAT] : "3000"[PDAT])').
    api_key : str, optional
        NCBI API key to increase rate limits.
    retmax : int, default 2000
        Maximum number of records to retrieve.
    min_fulltext_chars : int, default 2000
        Minimum number of characters for full-text; triggers fallback scraping if not met.

    Returns
    -------
    pd.DataFrame
        Tidy DataFrame with one row per (PMID, PMCID) pair, including metadata fields
        and full-text columns: xmlText, flatHtmlText, webScrapeText.
    """
    # Generate RWD filters
    rwd_terms = pubmedSearchTerms()

    # Build the combined query
    components = [f'"{mesh_term}"[MeSH Terms]', rwd_terms]
    if date_term:
        components.append(date_term)
    query = " AND ".join(components)

    # Fetch data through the core pipeline
    df = fetch_pubmed_fulltexts(
        query=query,
        api_key=api_key,
        retmax=retmax,
        min_fulltext_chars=min_fulltext_chars,
    )

    # Local post-processing: filter out entries without any full text
    if "xmlText" in df.columns:
        df = df[df["xmlText"] != "N/A"].reset_index(drop=True)

    return df
