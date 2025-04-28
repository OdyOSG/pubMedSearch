# pubmed-search

> **Discover observational real‑world‑data (RWD) studies on PubMed in one line.**

[![PyPI](https://img.shields.io/pypi/v/pubmed-search.svg)](https://pypi.org/project/pubmed-search/)  [![License: Apache‑2.0](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

`pubmed-search` is a thin, opinionated wrapper around the excellent [`searchpubmed`](https://github.com/OHDSI/searchpubmed) library.  
It focuses on **reproducible query generation** and **down‑stream data polishing** for epidemiology and HEOR projects, staying out of the way of the heavy lifting performed by *searchpubmed*.

---

## ✨ Key features

* **Human‑friendly Boolean Query Builder** – ready‑made terms for observational RWD designs
* **One‑shot CLI** – export thousands of abstracts (and full texts) straight to an Excel workbook
* **Python API** – integrate PubMed data into notebooks & ETL pipelines in two lines
* **Excel export helper** – autofilters, sensible column widths, no fiddly formatting
* **Zero learning curve** – if you can write a PubMed search box query, you are set

---

## 🚀 Quickstart

### 1. Install

```bash
pip install pubmed-search        # installs searchpubmed + friends
```

### 2. Command‑line (the fastest way)

```bash
# Fetch observational asthma studies from 2015 onwards
pubmed-search "Asthma" --year-from 2015 --output asthma.xlsx
```

A nicely formatted **asthma.xlsx** appears in your working directory – each row is a paper, with full text where available.

### 3. Python API

```python
from pubmed_search import search, rwdSearchTerms

df = search(
    ["Asthma"],          # one or several MeSH terms / free text queries
    year_from=2015,
    rwd_terms=rwdSearchTerms(),   # optional helper with pre‑built observational filters
)
print(df.head())
```

`df` is a `pandas.DataFrame` with the raw `medline_xml`, parsed metadata and the full‑text HTML (when retrievable).

Need an Excel file?  
```python
from pubmed_search import export_dataframe_to_excel
export_dataframe_to_excel(df, "asthma.xlsx")
```

## ⚙️  Options & environment variables

| Flag / variable          | Purpose                                                     | Default |
|--------------------------|-------------------------------------------------------------|---------|
| `--year-from / --year-to`| Bound the publication date (`PDAT`) window                 | `None`  |
| `--api-key` / `NCBI_API_KEY` | Your [NCBI E‑utilities key](https://www.ncbi.nlm.nih.gov/books/NBK25497/) for higher rate limits | *not set* |
| `--output`               | Excel file path (CLI only)                                  | `results.xlsx` |

*When no API key is provided the requests fall back to the public limit (3 requests / sec).*  If you routinely download thousands of papers, **get a key – it’s free.**

## 🧩 API reference (TL;DR)

| Function | What it does |
|----------|--------------|
| `pubmed_search.search(queries, *, year_from=None, year_to=None, rwd_terms=None, api_key=None, retmax=2000, ...)` | Return a `DataFrame` with PubMed metadata + full‑text HTML |
| `pubmed_search.rwdSearchTerms()` | Ready‑made Boolean block with >100 study‑design & data‑source terms for observational RWD |
| `pubmed_search.export_dataframe_to_excel(df, path)` | Save `df` to a formatted Excel workbook |

See the [API docs](#) for full parameter lists.

---

## 🗂️  Project structure

```
pubmed_search/
 ├── client.py   # high‑level Python API
 ├── cli.py      # `pubmed-search` console script
 ├── query.py    # RWD Boolean query builder
 ├── utils.py    # cleaning & Excel helpers
 └── …
```

---

## 🤝 Contributing

Pull requests are welcome!  Feel free to open an issue to discuss new helpers (e.g. *clinical trial* filters) or bug fixes.

```bash
# clone & install dev dependencies
git clone https://github.com/OdyOSG/pubMedSearch.git
cd pubMedSearch
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pytest -q   # run the tiny test suite
```

> ✨ **Tip**: If you add new files, run `ruff --fix .` to keep code style consistent.

---

## 📄 License

Apache License 2.0 © 2025 [OdyOSG](https://github.com/OdyOSG)  
Uses the awesome [`searchpubmed`](https://github.com/OHDSI/searchpubmed) under the same license.

---

## 📣 Citation

If this tool speeds up your literature reviews, please cite the underlying *searchpubmed* project:

> Pedersen AB, et al. **searchpubmed**: An R package for … *[Fictional Reference]*

---

## Acknowledgements

Thanks to the NCBI / PubMed team for the freely available *E‑utilities* API.
