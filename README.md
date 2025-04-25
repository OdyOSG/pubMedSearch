
# pubmed-search

This package wraps [OHDSI/searchpubmed](https://github.com/OHDSI/searchpubmed) with
a compact query builder tailored to **observational real‑world‑data studies**.
It originated as a cleanup of the experimental *pubMedSearch* notebook.

```
pip install pubmed-search                   # installs searchpubmed + deps
pubmed-search "Asthma" --year-from 2015
```
