
# pubmed-search (refactored)

A minimalist Python wrapper that turns one‑liner PubMed searches into tidy
`pandas` dataframes (plus an **Excel export** helper).

## Install (editable for dev)

```bash
git clone <this‑repo>
cd pubmed-search
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev,excel]
```

## Usage

**Python**

```python
from pubmed_search import search, rwdSearchTerms

df = search(
    ["Asthma"],          # one or several free‑text or MeSH queries
    year_from=2015,
    rwd_terms=rwdSearchTerms(),
)
```

**Command line**

```bash
pubmed-search "asthma" --year-from 2015 -o asthma.xlsx
```
