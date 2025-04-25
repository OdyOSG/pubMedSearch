"""
searchpubmed package initializer
"""

# Expose the high-level RWD-focused search function
from .pubmed import search_pubmed_rwd

# Optionally expose the core pipeline for advanced use
from .fetcher import fetch_pubmed_fulltexts

__all__ = [
    "search_pubmed_rwd",
    "fetch_pubmed_fulltexts",
]
