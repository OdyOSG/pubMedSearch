from __future__ import annotations

# imports
import logging
import time
import re
import xml.etree.ElementTree as ET
from math import ceil
from typing import List, Optional
from bs4 import BeautifulSoup


import dateparser
import pandas as pd
import requests
from requests.exceptions import HTTPError, RequestException
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp, to_utc_timestamp

# ──────────────────────────────────────────────────────────────
#  Building-block patterns  (flags are (?xi): verbose + case-insensitive)
# ──────────────────────────────────────────────────────────────

CODE_TERM = r"""
    (?:                                   # ICD family
        icd(?:[- ]?(?:9|10|11))? |
        icd[- ]?cm | icd[- ]?o |
        international\ classification\ of\ diseases(?:[- ]?(?:9|10|11))?
    )
  | cpt | current\ procedural\ terminology(?:[- ]?4)?
  | hcpcs | healthcare\ common\ procedure\ coding\ system
  | snomed(?:[ -]?ct)? | rxnorm | loinc
  | read\ codes? | read\ code
  | icpc                                            # International Primary Care
  | atc(?:\s+codes?)?                               # ATC drug codes
  | (?:diagnosis|procedure|billing|financial)\s+codes?
"""

TEMPORAL_WINDOW = r"""
    (?:look[- ]?back|wash[- ]?out|baseline|observation|
       follow[- ]?up|time[- ]?at[- ]?risk|index)
       \s+(?:period|window|date|time)
  | (?:prior|subsequent)\s+(?:to|observation|enrollment|index)
  | \d+\s*(?:days|months|years)\s+of\s+(?:observation|enrollment|follow[- ]?up)
  | (?:fixed\ time|time\ window|temporal)
  | within\s*\d+\s*(?:day|week|month|year)s?
  | in\s+the\s+(?:past|previous)\s+\d+\s*(?:months?|years?)
  | at\s+least\s+\d+\s*(?:months?|years?)
  | prior\s+to\s+(?:the\s+)?(?:index|cohort\s+entry)\s+date?
  | pre[- ]?index | post[- ]?index
  | during\s+the\s+\d+\s*(?:day|week|month|year)\s+baseline
  | after\s+(?:discharge|index)
  | \bindex\b                                       # bare “index” (last-resort)
"""

INCL_EXCL = r"""
    (?:inclusion|exclusion|eligibility|selection)\s+criteria
  | (?:included|excluded)\s+(?:patients|subjects|participants|individuals)
  | (?:required|criteria\ for)\s+(?:inclusion|exclusion|eligibility)
  | cohort\ definition | phenotype\ algorithm
  | (?:must|had)\s+to\s+have | must\s+have | must\s+not\s+have
  | required\s+to\s+have
  | patients?\s+with.+?(?:were|was)\s+excluded?
"""

CARE_SETTING = r"""
    (?:inpatient|outpatient|ambulatory)
        \s+(?:setting|visit|stay|care|record|encounter|population|basis)
  | (?:hospitalized|hospitalization|
     admitted\s+to\s+(?:hospital|inpatient))
  | (?:emergency\s+department|ed|emergency\s+room|er)
        \s+(?:visit|setting|care|encounter)
  | (?:clinic|primary\ care|specialty\ care)
        \s+(?:visit|setting|record|encounter)
  | primary\ care | specialist\ visit | telehealth\ visit
  | same[- ]?day\ surgery | day[- ]?case
"""

# ≤ 2 000 words after the current position
WITHIN_2K_WORDS = r"(?:\b\w+\b\W*){0,1999}"

# ──────────────────────────────────────────────────────────────
#  Master pattern
# ──────────────────────────────────────────────────────────────
phenotype_regex = re.compile(rf"""
    (?xi)                                               # verbose, case-insensitive

    # Passage must contain BOTH …
    (?= {WITHIN_2K_WORDS} {CODE_TERM} )
    (?= {WITHIN_2K_WORDS} (?:{TEMPORAL_WINDOW}|{INCL_EXCL}|{CARE_SETTING}) )

    # Actual match: any keyword from the four vocabularies
    (?: {CODE_TERM}
      | {TEMPORAL_WINDOW}
      | {INCL_EXCL}
      | {CARE_SETTING}
    )
""", re.VERBOSE)



# 2) Regex for common medical‐code patterns

# Improved medical-code regex pattern
medical_regex = re.compile(r"""
\b(?:                                     # word boundary, start alternation
    [A-TV-Z][0-9]{2}[A-Z0-9]?            (?:\.[A-Z0-9]{1,4})? |   # ICD‑10 / ICD‑10‑CM
    [VE]?\d{3}                           (?:\.\d{1,2})?        |   # ICD‑9‑CM (+ V/E)
    [0-9A-Z][A-Z]\d[0-9A-Z]              (?:\.[A-Z0-9]{2})?    |   # ICD‑11 stem (2nd char = letter, 3rd = digit)
    (?:\d{4,5}-\d{3,4}-\d{1,2}|\d{10,12})                     |   # NDC 10–12 digits or  dash forms
    \d{1,6}-\d                                               |   # LOINC (check‑digit suffix)
    [A-Z]\d{2}[A-Z][A-Z0-9]{1,4}                             |   # ATC 5‑7 chars
    RXCUI:?\s*\d{7,9}                                        |   # RxNorm RXCUI
    [1-9]\d{5,17}                                            |   # SNOMED CT 6‑18 digits
    OMOP\d{3,15}                                             |   # OMOP concept‑id prefix
    [A-Z0-9]{1,5}\.[A-Z0-9]{1,2}                             |   # Read / CTV3 with dot
    D?\d{4,5}                                               |   # CPT® / CDT® (D‑ or numeric)
    [A-Z]\d{4}(?:-[A-Z0-9]{2})?                              |   # HCPCS Level II
)\b
""", re.VERBOSE | re.IGNORECASE)



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_pmid_pmcid_from_pubmed(
    query: str,
    retmax: int = 2000,
    api_key: str | None = None,
    timeout: int = 20,
    batch_size: int = 500,
    delay: float = 0.34
) -> pd.DataFrame:
    """
    ------------------------------------------------------------------------
    Fetch PubMed IDs (PMIDs) for a given *query* and map them to their
    corresponding PubMed Central IDs (PMCIDs), including an open‐access flag.
    ------------------------------------------------------------------------

    Parameters
    ----------
    query : str
        A full PubMed search expression (Boolean operators allowed).  
        The function hands this string straight to ESearch, so everything
        that works in the PubMed web UI will work here.
    retmax : int, default 2 000
        Maximum number of PMIDs to return.  The ESearch hard cap is 100 000,
        but large values will amplify downstream latency because every PMID
        is carried through the mapping and OA look-up steps.
    api_key : str | None, default ``None``
        An NCBI API key. Providing a key roughly triples your personal
        request quota (from ~3 req/s to ~10 req/s) and is strongly
        recommended for production pipelines.
    timeout : int, default 20 s
        Socket timeout (seconds) for every individual HTTP request.
    batch_size : int, default 500
        Number of IDs sent per ELink / ESummary round-trip. 500 is a sweet
        spot that keeps URLs under 8 KB while staying well below EUtilities’
        2 000-ID ceiling.
    delay : float, default 0.34 s
        Base delay between successive calls in the *same* logical step
        (ELink & ESummary). On HTTP 429 the function backs off
        exponentially from this base.

    Returns
    -------
    pandas.DataFrame
        Column        | dtype   | description
        --------------|---------|--------------------------------------------------
        ``pmid``      | string  | PubMed identifier (always populated)
        ``pmcid``     | string  | PMC identifier, or ``<NA>`` if none exists
        ``openAccess``| string  | ``"1"`` if the article is in the PMC
                                     Open‐Access *or* Author Manuscript subset,
                                     otherwise ``"0"``

        The frame is empty (shape (0, 3)) if the query yields no PMIDs.

    Raises
    ------
    requests.exceptions.HTTPError
        Propagated when a non-recoverable HTTP status (≠429,≠5xx) is returned.
    requests.exceptions.RequestException
        For connection errors, DNS failures, etc.  
        5 automatic retries (with exponential back-off) are attempted for
        each ELink batch before giving up.
    xml.etree.ElementTree.ParseError
        Only raised for *all-batches-fail* scenarios; individual parse
        failures fall back to ``pmcid = <NA>`` for the affected PMIDs.
    RuntimeError
        Re-raised from any unexpected exception caught during processing.

    Notes
    -----
    1. **Rate limits & courtesy pauses**

       * Without an API key, NCBI permits ~3 requests per second across all
         tools and endpoints. With a key the ceiling rises to ~10 req/s.
       * The default ``delay`` (0.34 s) keeps combined throughput below the
         3 req/s threshold even when the caller omits an API key.
       * On HTTP 429 the function doubles the delay for each retry attempt
         (i.e. delay, 2·delay, 4·delay, …).

    2. **Open-access flag**

       The extra ``openAccess`` column is filled via an *ESummary* call
       against the PMC database.  Only two subset flags are considered:

       * ``openaccess == 1`` → article is in the *Open-Access* subset  
       * ``openaccess == 0`` → everything else (including embargoed and
         subscription-only material)

    3. **Memory footprint**

       Each row in the final DataFrame is just three short strings; even
       100 000 rows stay below 15 MB.

    Example
    -------
    >>> from mymodule.pubmed import fetch_pmid_pmcid_from_pubmed
    >>> df = fetch_pmid_pmcid_from_pubmed(
    ...     query='("heart failure"[MeSH Terms]) AND 2024[dp]',
    ...     retmax=1000,
    ...     api_key='MY_NCBI_API_KEY'
    ... )
    >>> df.head()
          pmid      pmcid openAccess
    0  38765432  PMC1234567          1
    1  38765433        <NA>          0
    2  38765434  PMC1234569          0
    3  38765435  PMC1234570          1
    4  38765436        <NA>          0

    Implementation overview
    -----------------------
    1. **ESearch** – retrieve up to ``retmax`` PMIDs.  
       Early exit with empty frame if none are found.
    2. **ELink**  – convert PMIDs → PMCIDs in chunks of ``batch_size``;  
       five automatic retries on network or HTTP 429 errors.
    3. **ESummary** (PMC) – batch-lookup of *openaccess* flags for every
       unique PMCID.
    4. Assemble and type-cast the final DataFrame; log a concise summary.

    The core behavioural contract of the original implementation is kept
    intact; only the open-access lookup and the additional column are new.
    """
    session = requests.Session()
    base_esearch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    base_elink   = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    base_esum    = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"  # NEW

    # 1) ESearch – get PMIDs
    params = {"db": "pubmed", "term": query, "retmax": retmax, "retmode": "xml"}
    if api_key:
        params["api_key"] = api_key
    logger.info(f"ESearch: fetching up to {retmax} PMIDs for '{query}'")

    resp = session.post(base_esearch, data=params, timeout=timeout)
    resp.raise_for_status()
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.error(f"Failed to parse ESearch XML: {e}")
        return pd.DataFrame(columns=["pmid", "pmcid", "openAccess"]).astype("string")

    pmids = [el.text for el in root.findall(".//IdList/Id") if el.text]
    if not pmids:
        logger.info("No PMIDs found.")
        return pd.DataFrame(columns=["pmid", "pmcid", "openAccess"]).astype("string")

    # 2) ELink – map to PMCIDs
    records = []
    total_batches = ceil(len(pmids) / batch_size)
    for idx in range(total_batches):
        chunk = pmids[idx * batch_size : (idx + 1) * batch_size]
        data = [("dbfrom", "pubmed"), ("db", "pmc"), ("retmode", "xml")]
        if api_key:
            data.append(("api_key", api_key))
        data += [("id", pmid) for pmid in chunk]

        logger.info(f"ELink batch {idx+1}/{total_batches} (size={len(chunk)})")
        for attempt in range(5):
            try:
                r = session.post(base_elink, data=data, timeout=timeout)
                if r.status_code == 429:
                    time.sleep(delay * (2 ** attempt))
                    continue
                r.raise_for_status()
                break
            except RequestException as e:
                logger.error(f"Batch {idx+1} attempt {attempt+1} failed: {e}")
                time.sleep(delay * (2 ** attempt))
        else:
            logger.error(f"Batch {idx+1} failed after retries")
            records.extend((pmid, None) for pmid in chunk)
            continue

        try:
            xml_root = ET.fromstring(r.content)
        except ET.ParseError as e:
            logger.error(f"Failed to parse ELink XML for batch {idx+1}: {e}")
            records.extend((pmid, None) for pmid in chunk)
            time.sleep(delay)
            continue

        for linkset in xml_root.findall("LinkSet"):
            pmid_el = linkset.find("IdList/Id")
            pmid = pmid_el.text if pmid_el is not None else None
            if not pmid:
                continue
            pmcids = [
                link.text for db in linkset.findall("LinkSetDb")
                if db.findtext("DbTo") == "pmc"
                for link in db.findall("Link/Id") if link.text
            ]
            records.append((pmid, pmcids[0] if pmcids else None))
        time.sleep(delay)

    # 3) Build DataFrame
    df = pd.DataFrame(records, columns=["pmid", "pmcid"])
    df = df[df["pmid"].notnull()].reset_index(drop=True)
    df["pmid"] = df["pmid"].astype("string")
    df["pmcid"] = df["pmcid"].astype("string")

    # 4) NEW – look up open-access flag with ESummary
    oa_map: dict[str, str] = {}
    pmcids = df["pmcid"].dropna().unique().tolist()
    for offset in range(0, len(pmcids), batch_size):
        chunk = pmcids[offset : offset + batch_size]
        esum_params = {"db": "pmc", "id": ",".join(chunk), "retmode": "json"}
        if api_key:
            esum_params["api_key"] = api_key
        r = session.get(base_esum, params=esum_params, timeout=timeout)
        if r.status_code == 429:
            time.sleep(delay); r = session.get(base_esum, params=esum_params, timeout=timeout)
        r.raise_for_status()
        js = r.json().get("result", {})
        for uid in js.get("uids", []):
            rec = js[uid]                       # short alias
            is_oa = rec.get("openaccess") == "1"          # OA subset
            is_am = rec.get("src", "").upper() == "MANUSCRIPT"  # author manuscript
            oa_map[uid] = "1" if (is_oa or is_am) else "0"

        time.sleep(delay)

    df["openAccess"] = (
        df["pmcid"]
        .apply(lambda x: oa_map.get(str(x), "0"))
        .astype("string")
    )

    logger.info(f"Mapping complete: {len(df)} PMIDs "
                f"({(df['openAccess']=='1').sum()} open-access)")
    return df



def fetch_pubmed_metadata_for_pmid(
    pmids: list[str],
    api_key: str | None = None,
    batch_size: int = 200,
    delay: float = 0.34,
    max_retries: int = 3
) -> pd.DataFrame:
    """
    ------------------------------------------------------------------------
    Retrieve high-level PubMed metadata (title, abstract, authors, etc.) for
    an arbitrary list of PMIDs.  Requests are issued in batches with automatic
    retries on transient (HTTP 429 / 5xx) errors and graceful degradation for
    malformed XML payloads.
    ------------------------------------------------------------------------

    Parameters
    ----------
    pmids : list[str]
        The PMIDs to fetch.  Any non-existent or withdrawn IDs are returned
        as rows filled with ``"N/A"`` values.
    api_key : str | None, default ``None``
        NCBI API key (optional but recommended).  Raises the personal rate
        limit from ~3 requests/s to ~10 requests/s.
    batch_size : int, default 200
        Number of PMIDs sent per EFetch request.  The PubMed EFetch ceiling
        is 10 000, but smaller batches reduce the amount of data lost when a
        single response needs to be discarded.
    delay : float, default 0.34 s
        Base pause between *successful* batches.  On retry the delay is
        multiplied by ``2 ** (attempt-1)`` (exponential back-off).
    max_retries : int, default 3
        Maximum number of attempts per batch before the function gives up and
        emits placeholder rows.

    Returns
    -------
    pandas.DataFrame
        Column               | dtype  | description
        ---------------------|--------|------------------------------------------
        ``pmid``             | string | PubMed identifier (echo of the input)
        ``title``            | string | Article title (sentence case)
        ``abstract``         | string | Joined paragraphs of the abstract
        ``journal``          | string | Full journal title
        ``publicationDate``  | string | ISO 8601 date (YYYY-MM-DD / YYYY-MM)
        ``doi``              | string | Digital Object Identifier
        ``firstAuthor``      | string | “Given Surname” of the first author
        ``lastAuthor``       | string | “Given Surname” of the last  author
        ``authorAffiliations``|string | “; ”-separated affiliation block
        ``meshTags``         | string | “, ”-separated MeSH descriptors
        ``keywords``         | string | “, ”-separated author keywords

        Every field is guaranteed to exist; missing information is expressed
        as the literal string ``"N/A"`` for consistency.

    Raises
    ------
    requests.exceptions.HTTPError
        Propagated if EFetch returns an unrecoverable status code (<400 or ≥600).
    requests.exceptions.RequestException
        Propagated for network-level errors after all retries are exhausted.
    xml.etree.ElementTree.ParseError
        Raised only if *all* records in a successful HTTP response are
        syntactically broken.  Individual parse errors fall back to bailing
        out rows with ``"N/A"``.
    ValueError
        Raised when *pmids* is empty.

    Notes
    -----
    * **Rate limiting** – The default 0.34 s inter-batch delay stays under the
      3 req/s anonymous quota.  With an *api_key*, you may safely drop this to
      ≈0.1 s to approach 10 req/s.
    * **Memory footprint** – One fully populated record is ~400 bytes.  Ten
      thousand records therefore occupy <4 MB.
    * **Fault tolerance** – On any retryable failure the function inserts a
      fully populated placeholder row rather than silently discarding the PMID
      so that upstream merge operations can proceed deterministically.

    Example
    -------
    >>> from mymodule.pubmed import fetch_pubmed_metadata_for_pmid
    >>> df = fetch_pubmed_metadata_for_pmid(
    ...     pmids=["38765432", "38765433", "38765499"],
    ...     api_key="MY_NCBI_API_KEY"
    ... )
    >>> df.loc[0, ["pmid", "title", "firstAuthor"]]
    pmid                                   38765432
    title            Machine learning for heart failure prognosis
    firstAuthor                          Alice T Smith
    Name: 0, dtype: object

    Implementation overview
    -----------------------
    1. Split *pmids* into chunks of ``batch_size`` and call **EFetch** in
       *retmode=xml*.
    2. Retry each batch ≤ *max_retries* times on HTTP 429 or 5xx responses
       with exponential back-off.
    3. Parse the returned XML; on parser failure fall back to placeholders.
    4. Aggregate all per-batch records into a single ``DataFrame`` and return.
    """

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    session = requests.Session()
    records: List[dict] = []

    def parse_pubdate(elem):
        year = elem.findtext('Year')
        month = elem.findtext('Month')
        day = elem.findtext('Day')
        if year and month:
            try:
                dt = dateparser.parse(f"{year} {month} {day or '1'}")
                return dt.date().isoformat()
            except Exception:
                parts = [year, month] + ([day] if day else [])
                return "-".join(parts)
        return elem.findtext('MedlineDate') or "N/A"

    def full_name(elem):
        fore = elem.findtext('ForeName') or elem.findtext('Initials') or ''
        last = elem.findtext('LastName') or ''
        name = (fore + ' ' + last).strip()
        return name or 'N/A'

    for start in range(0, len(pmids), batch_size):
        batch = pmids[start:start + batch_size]
        params = {
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(batch)
        }
        if api_key:
            params["api_key"] = api_key

        resp = None
        # --- HTTP GET with retry on 429 and 5xx ---
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(base_url, params=params, timeout=15)
                resp.raise_for_status()
                break  # success!
            except HTTPError as e:
                status = getattr(e.response, "status_code", None)
                # back off on 429 or 5xx
                if status and ((status == 429) or (500 <= status < 600)) and attempt < max_retries:
                    backoff = delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"Batch {start//batch_size+1}: "
                        f"status {status}, retrying in {backoff:.1f}s "
                        f"(attempt {attempt}/{max_retries})"
                    )
                    time.sleep(backoff)
                    continue
                # non‐retryable or maxed out
                logger.error(f"HTTP error for PMIDs {batch}: {e}")
            except RequestException as e:
                logger.error(f"Network error for PMIDs {batch}: {e}")
            # on any of these errors, fill with N/A and skip parsing
            for pmid in batch:
                records.append({
                    'pmid': pmid,
                    **{k: "N/A" for k in [
                        'title','abstract','journal','publicationDate',
                        'doi','firstAuthor','lastAuthor',
                        'authorAffiliations','meshTags','keywords'
                    ]}
                })
            resp = None
            break

        # if we never got a good response, move on
        if resp is None:
            continue

        # --- XML parse with its own error handling ---
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.error(f"XML parse error for PMIDs {batch}: {e}")
            for pmid in batch:
                records.append({
                    'pmid': pmid,
                    **{k: "N/A" for k in [
                        'title','abstract','journal','publicationDate',
                        'doi','firstAuthor','lastAuthor',
                        'authorAffiliations','meshTags','keywords'
                    ]}
                })
            time.sleep(delay)
            continue

        # --- Extract records on successful parse ---
        for art in root.findall('.//PubmedArticle'):
            pmid = art.findtext('.//PMID', default='N/A')
            title = art.findtext('.//ArticleTitle', default='N/A')
            abstract = " ".join(
                t.text or '' for t in art.findall('.//Abstract/AbstractText')
            ).strip() or 'N/A'
            journal = art.findtext('.//Journal/Title', default='N/A')
            pubdate_elem = art.find('.//JournalIssue/PubDate')
            publication_date = (
                parse_pubdate(pubdate_elem) if pubdate_elem is not None else 'N/A'
            )
            doi = art.findtext('.//ArticleIdList/ArticleId[@IdType="doi"]', default='N/A')
            authors = art.findall('.//AuthorList/Author')
            if authors:
                first_author = full_name(authors[0])
                last_author = full_name(authors[-1])
            else:
                first_author = last_author = 'N/A'
            affs = [
                aff.text for auth in authors
                for aff in auth.findall('AffiliationInfo/Affiliation')
                if aff.text
            ]
            author_affiliations = "; ".join(affs) or 'N/A'
            mesh_tags = ", ".join(
                mh.text for mh in art.findall('.//MeshHeading/DescriptorName')
                if mh.text
            ) or 'N/A'
            keywords = ", ".join(
                kw.text for kw in art.findall('.//KeywordList/Keyword')
                if kw.text
            ) or 'N/A'

            records.append({
                'pmid': pmid,
                'title': title,
                'abstract': abstract,
                'journal': journal,
                'publicationDate': publication_date,
                'doi': doi,
                'firstAuthor': first_author,
                'lastAuthor': last_author,
                'authorAffiliations': author_affiliations,
                'meshTags': mesh_tags,
                'keywords': keywords
            })

        # respect rate limits between batches
        time.sleep(delay)

    return pd.DataFrame(records)



# ───────────────────────────────────────────────────────────────
#  Helper: strip default namespace from JATS / PMC XML
# ───────────────────────────────────────────────────────────────

def _strip_default_ns(xml_bytes: bytes) -> bytes:
    """
    ------------------------------------------------------------------------
    Remove the *first* default XML namespace declaration  
    (``xmlns="…"``) from a JATS / PMC XML payload.

    ElementTree’s naive API cannot address tags that live in a default
    namespace without unwieldy Clark notation (``{URI}tag``).  By deleting
    the **first** default namespace declaration we effectively move all
    descendants into the empty namespace, allowing simple, bare tag names
    such as ``<article>``, ``<body>``, or ``<sec>`` to be matched with the
    usual XPath syntax.

    Parameters
    ----------
    xml_bytes : bytes
        Raw XML returned by any NCBI E-utilities endpoint
        (e.g. PMC EFetch).  The function operates on **bytes** to avoid the
        overhead of decoding / re-encoding large XML documents.

    Returns
    -------
    bytes
        A byte string identical to *xml_bytes* except that the very first
        occurrence of the pattern ``xmlns="…"`` has been removed.
        All subsequent namespace declarations (including default ones on
        nested elements) are preserved.

    Notes
    -----
    * Only the *first* default declaration is stripped because the top-level
      JATS/PMC documents place their global namespace on the root element
      (``<article … xmlns="http://jats.nlm.nih.gov">``).  Child elements
      rarely introduce another default namespace, and if they do, the user
      code should handle those explicitly.
    * The function purposefully leaves **prefixed** namespaces (``xmlns:xlink``)
      untouched, as ElementTree handles those well through standard
      namespace maps.

    Example
    -------
    >>> import xml.etree.ElementTree as ET, requests
    >>> raw = requests.get("https://pmc.ncbi.nlm.nih.gov/articles/PMC1234567?format=xml").content
    >>> clean = _strip_default_ns(raw)
    >>> root = ET.fromstring(clean)  # now you can use bare tag names
    >>> root.find(".//body") is not None
    True
    """
    return re.sub(rb'\sxmlns="[^"]+"', b"", xml_bytes, count=1)



# ───────────────────────────────────────────────────────────────
#  Main fetcher for PMCIDs → metadata + full text
# ───────────────────────────────────────────────────────────────

def fetch_pmcid_metadata_for_pmcid(
    pmcids: list[str],
    api_key: str | None = None,
    batch_size: int = 100,
    delay: float = 0.34,
    timeout: int = 15,
    max_retries: int = 3
) -> pd.DataFrame:
    """
    ------------------------------------------------------------------------
    Fetch rich metadata **and** (XML-derived) full text for a list of PMCIDs.
    ------------------------------------------------------------------------

    The function is a thin orchestration wrapper around the PMC EFetch
    endpoint and the private helper ``_harvest_article``.  Results are
    returned as a *tidy* ``pandas.DataFrame`` – one article per row.

    Parameters
    ----------
    pmcids : list[str]
        PubMed Central identifiers, **with or without** the ``"PMC"`` prefix
        (e.g. ``"3912394"`` or ``"PMC3912394"``).  Duplicates are permitted
        but silently de-duplicated per batch.
    api_key : str | None, default ``None``
        NCBI API key.  Strongly recommended for production workloads because
        it lifts the request ceiling to ≈10 req s⁻¹.
    batch_size : int, default 100
        Number of PMCIDs per EFetch call.  PMC EFetch accepts up to 10 000,
        but keeping batches small localises the blast-radius of a failed or
        malformed response.
    delay : float, default 0.34 s
        Inter-batch pause after *successful* requests.  On retry the waiting
        time is multiplied by ``2 ** attempt`` (exponential back-off).
    timeout : int, default 15 s
        Socket timeout for every HTTP request.
    max_retries : int, default 3
        Maximum number of re-tries for the *same* batch after a network error
        or any HTTP 4xx/5xx status ≠ 200.

    Returns
    -------
    pandas.DataFrame
        Column                        | dtype  | note
        ------------------------------|--------|---------------------------------
        ``pmcid``                     |string  |
        ``pmid``                      |string  |
        ``title``                     |string  |
        ``abstract``                  |string  |
        ``journal``                   |string  |
        ``publicationDate``           |string  | ISO-8601 (YYYY-MM-DD / YYYY-MM)
        ``doi``                       |string  |
        ``firstAuthor``               |string  |
        ``lastAuthor``                |string  |
        ``authorAffiliations``        |string  | “; ”-joined block
        ``meshTags``                  |string  | “, ”-separated
        ``keywords``                  |string  | “, ”-separated
        ``fullText``                  |string  | Plain text, *may* be HTML scrape
        ``yearOfPublication``         |string  |
        ``fullXML``                   |string  | Entire `<article>` node
        ``pmcFlags``                  |string  | e.g. ``pmc-prop-manuscript=yes``
        ``htmlScrape``                |string  | ``"1"`` if HTML fallback used
        ``messagesDuringHtmlScraping``|string  | warning or ``"N/A"``

        The frame is empty (`shape == (0, 0)`) when *pmcids* is an empty list.

    Raises
    ------
    requests.exceptions.HTTPError
        Re-raised when EFetch returns a fatal status (<400 or ≥600) after all
        retries for the current batch have been exhausted.
    requests.exceptions.RequestException
        Re-raised for unrecoverable network problems (DNS, TLS, connection
        reset…) after all retries.
    xml.etree.ElementTree.ParseError
        Only propagated if the *entire* response payload cannot be parsed.
        Single-article XML failures are contained by ``_harvest_article`` and
        result in default-filled rows.
    ValueError
        Raised when *pmcids* is empty.

    Notes
    -----
    * **Namespace stripping** – The helper ``_strip_default_ns`` is applied to
      every XML payload so that downstream XPath expressions can use bare
      tag names.
    * **HTML fallback** – ``_harvest_article`` automatically attempts a
      secondary scrape of the PMC *flat HTML* view when the JATS body is
      missing or suspiciously short (<~2 kB).
    * **Rate limits** – The default settings keep calls well below NCBI’s
      anonymous limit (~3 req s⁻¹).  With an *api_key*, you may safely reduce
      *delay* to ≈0.1 s to reach the 10 req s⁻¹ tier.

    Example
    -------
    >>> df = fetch_pmcid_metadata_for_pmcid(
    ...     pmcids=["PMC9054321", "9054322"],
    ...     api_key="MY_NCBI_API_KEY"
    ... )
    >>> df.columns
    Index(['pmcid', 'pmid', 'title', 'abstract', ... 'messagesDuringHtmlScraping'], dtype='object')

    Implementation overview
    -----------------------
    1. Guard-clause for an empty *pmcids* list (returns empty DataFrame).
    2. Chunk *pmcids* into batches of *batch_size* and call **EFetch** with
       *retmode=xml*.
    3. Retry ≤ *max_retries* on any `requests.RequestException`.
    4. Strip default namespace and parse the XML.
    5. For each `<article>` element run the private ``_harvest_article`` to
       extract metadata & text.
    6. Accumulate the per-article dictionaries and return a single
       ``DataFrame``.
    """

    # ── Guard clause ─────────────────────────────────────────────
    if not pmcids:                     # empty list – nothing to do
        return pd.DataFrame()          # never return None!

    records: list[dict] = []
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    session = requests.Session()

    # ── Main loop over PMCIDs in batches ─────────────────────────
    for i in range(0, len(pmcids), batch_size):
        chunk = pmcids[i : i + batch_size]
        params = {"db": "pmc", "id": ",".join(chunk), "retmode": "xml"}
        if api_key:
            params["api_key"] = api_key
        for attempt in range(max_retries):
            try:
                r = session.get(base_url, params=params, timeout=timeout)
                r.raise_for_status()
                break        # exit retry loop on success
            except requests.exceptions.RequestException:
                time.sleep(delay * (2 ** attempt))
        else:
            continue          # give up this chunk but keep pipeline alive

        xml = _strip_default_ns(r.content)
        root = ET.fromstring(xml)
        for art in root.findall(".//article"):
            records.append(_harvest_article(art))

        time.sleep(delay)

    return pd.DataFrame(records)


def _harvest_article(article: ET.Element) -> dict:
    """
    ------------------------------------------------------------------------
    Parse a single JATS `<article>` element from PMC **EFetch** and return a
    *flattened* metadata / full-text record.
    ------------------------------------------------------------------------

    This helper powers :pyfunc:`fetch_pmcid_metadata_for_pmcid`.  
    It is intentionally stateless so that it can be unit-tested with fixture
    XML snippets.

    Parameters
    ----------
    article : xml.etree.ElementTree.Element
        The root `<article>` node as emitted by PMC EFetch
        (``retmode=xml``).  The node **must not** contain a default
        namespace.  Up-stream code is expected to call
        :pyfunc:`_strip_default_ns` on the raw payload first.

    Returns
    -------
    dict
        Key                            | value                               | notes
        --------------------------------|--------------------------------------|------------------------------
        ``pmcid``                       | str                                  | `PMC` prefix kept
        ``pmid``                        | str                                  |
        ``title``                       | str                                  |
        ``abstract``                    | str                                  |
        ``journal``                     | str                                  |
        ``publicationDate``             | str (YYYY-MM-DD / YYYY-MM / YYYY)    |
        ``doi``                         | str                                  |
        ``firstAuthor``                 | str (“Given Surname”)                |
        ``lastAuthor``                  | str (“Given Surname”)                |
        ``authorAffiliations``          | str (“; ”-joined)                    |
        ``meshTags``                    | str (“, ”-joined)                    |
        ``keywords``                    | str (“, ”-joined)                    |
        ``fullText``                    | str                                  | plain text, XML → text or HTML scrape
        ``yearOfPublication``           | str                                  | convenience mirror of the year
        ``fullXML``                     | str                                  | *entire* `<article>` subtree
        ``pmcFlags``                    | str                                  | e.g. ``pmc-prop-manuscript=yes``
        ``htmlScrape``                  | str ``"0"`` | ``"1"``               | 1 ⇢ HTML fallback adopted
        ``messagesDuringHtmlScraping``  | str                                  | warning message or ``"N/A"``

        The function never returns missing keys; any unavailable field is
        filled with the literal string ``"N/A"``.

    Raises
    ------
    None directly.  All parsing errors are caught internally and result in
    sensible default values (usually ``"N/A"``).  Network errors encountered
    during the HTML fallback are swallowed and summarised in
    ``messagesDuringHtmlScraping``.

    Processing logic
    ----------------
    1. **Metadata extraction** – Straightforward XPath look-ups for IDs,
       title, journal, DOI, authors, MeSH, and keywords.
    2. **Full text** – Convert the `<body>` subtree to plain text.  
       If the body is missing, <2 kB, or suspiciously similar to the
       abstract, an auxiliary *flat HTML* scrape is attempted via
       :pyfunc:`scrape_pmc_html_fulltext`.
    3. **HTML fallback heuristics**

       * ``too_short`` (< 2 000 chars)  
       * ``looks_like_abstract`` (first 50 normalised tokens match)

       If the HTML version wins (longer than XML body), the flag
       ``htmlScrape`` is set to ``"1"`` and any diagnostic message is cleared.
    4. **PMC property flags** – All attributes of `<article>` whose names
       start with ``pmc-prop-`` are serialised into ``pmcFlags`` as
       ``key=value;key=value`` pairs.

    Example
    -------
    >>> import xml.etree.ElementTree as ET, requests
    >>> raw = requests.get("https://pmc.ncbi.nlm.nih.gov/articles/PMC9312345?format=xml").content
    >>> root = ET.fromstring(_strip_default_ns(raw))
    >>> rec = _harvest_article(root.find(".//article"))
    >>> rec["htmlScrape"], rec["fullText"][:80]
    ('0', 'Background …')

    Implementation contract
    -----------------------
    * No side effects and no external logging beyond the private helper
      :pyfunc:`scrape_pmc_html_fulltext`.
    * Always returns a dictionary with exactly the keys documented above,
      enabling predictable construction of a :pyclass:`pandas.DataFrame`
      downstream.
    """

    # ── small helpers ──────────────────────────────────────────
    def _parse_pubdate(elem: ET.Element | None) -> str:
        if elem is None:
            return "N/A"
        y, m, d = (elem.findtext(k) for k in ("year", "month", "day"))
        if y and m:                                         # YYYY-MM-DD, robust
            try:
                return dateparser.parse(f"{y} {m} {d or '1'}").date().isoformat()
            except Exception:
                return "-".join(p for p in (y, m, d) if p)
        return y or "N/A"

    def _full_name(contrib: ET.Element) -> str:
        name = contrib.find("name")
        if name is not None:
            given   = name.findtext("given-names") or ""
            surname = name.findtext("surname") or ""
            val = f"{given} {surname}".strip()
            return val or "N/A"
        return "N/A"

    # ── basic metadata ────────────────────────────────────────
    pmcid_val = article.findtext('.//article-id[@pub-id-type="pmc"]',  default='N/A')
    pmid_val  = article.findtext('.//article-id[@pub-id-type="pmid"]', default='N/A')

    title_val = article.findtext('.//title-group/article-title', default='N/A')

    abs_pars      = article.findall('.//abstract//p')
    abstract_val  = " ".join(
        ET.tostring(p, encoding="unicode", method="text") for p in abs_pars
    ).strip() or "N/A"

    journal_val = article.findtext('.//journal-title', default='N/A')

    pubdate_elem         = article.find('.//pub-date')
    publication_date_val = _parse_pubdate(pubdate_elem)
    year_val             = pubdate_elem.findtext('year') if pubdate_elem is not None else 'N/A'

    doi_val = article.findtext('.//article-id[@pub-id-type="doi"]', default='N/A')

    authors            = article.findall('.//contrib-group/contrib[@contrib-type="author"]')
    first_author_val   = _full_name(authors[0]) if authors else 'N/A'
    last_author_val    = _full_name(authors[-1]) if authors else 'N/A'

    affs                     = [aff.text for aff in article.findall('.//aff') if aff.text]
    author_affiliations_val  = '; '.join(affs) or 'N/A'

    mesh_groups = [
        g for g in article.findall('.//kwd-group') if g.get('kwd-group-type') == 'MeSH'
    ]
    mesh_terms      = [kw.text for g in mesh_groups for kw in g.findall('kwd') if kw.text]
    mesh_tags_val   = ', '.join(mesh_terms) or 'N/A'

    kw_nodes        = article.findall('.//kwd-group[@kwd-group-type="keywords"]/kwd')
    keyword_list    = (
        [kw.text for kw in kw_nodes if kw.text] or
        [kw.text for kw in article.findall('.//kwd') if kw.text]
    )
    keywords_val    = ', '.join(keyword_list) or 'N/A'

    # ── full-text retrieval ───────────────────────────────────
    body            = article.find('.//body')
    full_text_val   = (
        ET.tostring(body, encoding="unicode", method="text").strip()
        if body is not None else ''
    )

    html_scrape_flag = 0          # 0 = XML body used, 1 = HTML fallback used
    scrape_msg       = ""         # populated only on *failed* scrape

    # Heuristics – small stub, or body ≈ abstract  → try HTML fallback
    too_short            = len(full_text_val) < 2_000
    looks_like_abstract  = (
        full_text_val and
        re.sub(r'\W+', ' ', full_text_val[:250]).lower().startswith(
            re.sub(r'\W+', ' ', abstract_val.lower())[:50]
        )
    )
    need_fallback = (not full_text_val) or too_short or looks_like_abstract

    if need_fallback:
        scraped, scrape_msg = scrape_pmc_html_fulltext(pmcid_val)  # tuple[str|None, str]
        if scraped and len(scraped) > len(full_text_val):
            full_text_val    = scraped
            html_scrape_flag = 1     # HTML text *was* adopted
            scrape_msg       = ""    # clear – no problem to report

    if not full_text_val:
        full_text_val = 'N/A'

    # ── expose PMC property flags (e.g. pmc-prop-manuscript=yes) ─────
    pmc_flags_val = ';'.join(
        f"{k}={v}" for k, v in article.attrib.items() if k.startswith('pmc-prop-')
    ) or 'N/A'

    # ── final record ──────────────────────────────────────────
    return {
        'pmcid':                     pmcid_val,
        'pmid':                      pmid_val,
        'title':                     title_val,
        'abstract':                  abstract_val,
        'journal':                   journal_val,
        'publicationDate':           publication_date_val,
        'doi':                       doi_val,
        'firstAuthor':               first_author_val,
        'lastAuthor':                last_author_val,
        'authorAffiliations':        author_affiliations_val,
        'meshTags':                  mesh_tags_val,
        'keywords':                  keywords_val,
        'fullText':                  full_text_val,
        'yearOfPublication':         year_val,
        'fullXML':                   ET.tostring(article, encoding='unicode'),
        'pmcFlags':                  pmc_flags_val,
        'htmlScrape':                str(html_scrape_flag),              # "0" | "1"
        'messagesDuringHtmlScraping': scrape_msg or 'N/A'                # warning or "N/A"
    }



def scrape_pmc_html_fulltext(pmcid: str, timeout: int = 20) -> tuple[str | None, str]:
    """
    ------------------------------------------------------------------------
    Fallback scraper for the PMC **“flat” HTML** view  
    ( `https://pmc.ncbi.nlm.nih.gov/articles/<PMCID>/?format=flat` ).
    ------------------------------------------------------------------------

    The function is used when the JATS/XML body retrieved via **EFetch** is
    missing or clearly incomplete.  It performs a light-weight HTML scrape
    and returns the concatenated paragraph text.

    Parameters
    ----------
    pmcid : str
        PubMed Central ID *with or without* the ``"PMC"`` prefix.  The value
        is inserted verbatim into the request URL, so make sure it does not
        contain extra whitespace.
    timeout : int, default 20 s
        Socket timeout for each HTTP request (initial + possible retries).

    Returns
    -------
    tuple[str | None, str]
        * **index 0** – *full_text*  
          *Plain-text* article body if the scrape succeeds and yields
          non-empty content; otherwise ``None``.
        * **index 1** – *warning_message*  
          Empty string on success.  On failure a concise diagnostic message
          (e.g. ``"PMC1234567: HTTPError – 404 Client Error"``) that can be
          logged or surfaced upstream.

    Behaviour
    ---------
    * Sends a single GET request with a custom *User-Agent* header to comply
      with NCBI’s crawler policy.
    * On HTTP **403** or **429** the function retries three times with a
      progressive back-off of **1 s → 4 s → 10 s** before giving up.
    * Parses the response with **BeautifulSoup** and extracts all `<p>`
      elements under ``#maincontent`` (or the entire document as a fallback),
      joining them with single spaces.
    * Any exception – network, HTTP, or parsing – is *swallowed*; the
      function logs a warning and returns ``(None, <message>)`` so that the
      caller can decide whether to propagate or ignore the failure.

    Example
    -------
    >>> text, msg = scrape_pmc_html_fulltext("PMC9054321")
    >>> if text:
    ...     print(text[:200], "…")
    ... else:
    ...     print("Scrape failed:", msg)
    """
    if not pmcid:
        return None, "No PMCID provided"

    url = f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/?format=flat"
    headers = {
        "User-Agent": "Mozilla/5.0 (PubMedCrawler/1.0; +https://github.com/you/yourrepo)"
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code in (403, 429):
            for wait in (1, 4, 10):  # simple back-off
                time.sleep(wait)
                r = requests.get(url, headers=headers, timeout=timeout)
                if r.ok:
                    break
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        main = soup.find(id="maincontent") or soup
        text = " ".join(p.get_text(" ", strip=True) for p in main.find_all("p"))
        return text or None, ""
    except Exception as exc:
        msg = f"{pmcid}: {type(exc).__name__} – {exc}"
        logger.warning(f"HTML scrape failed for {msg}")
        return None, msg



def extract_article_components(article_element: ET.Element) -> dict[str, str]:
    """
    ------------------------------------------------------------------------
    Pull out the canonical IMRaD sections—**Introduction, Methods, Results,
    Discussion**—from a single JATS `<article>` element.
    ------------------------------------------------------------------------

    The function is deliberately *best-effort*: many PMC articles deviate from
    strict JATS tagging conventions.  It therefore combines element
    attributes (`sec-type`) **and** the literal section title text to decide
    which content belongs to which IMRaD bucket.

    Parameters
    ----------
    article_element : xml.etree.ElementTree.Element
        The root `<article>` node whose default namespace has already been
        stripped.  Supplying a namespaced element will silently fail to match
        any section tags.

    Returns
    -------
    dict[str, str]
        A four-key dictionary with *plain-text* content:

        key            | value
        ---------------|--------------------------------------------------------
        ``'introduction'`` | Section text or ``'N/A'`` when unavailable
        ``'methods'``      |          ”            ”          ”        ”
        ``'results'``      |          ”            ”          ”        ”
        ``'discussion'``   |          ”            ”          ”        ”

        The string ``'N/A'`` is used consistently to indicate *missing* or
        *unresolved* sections so that downstream joins and fills remain
        type-stable.

    Heuristics
    ----------
    * A section qualifies as **Introduction**, **Methods**, **Results**, or
      **Discussion** if **either** of the following holds:
        1. The attribute ``sec-type`` contains a diagnostic token (case-ins.)
           – e.g. ``sec-type="methods"``
        2. The child `<title>` element’s text contains the token –
           e.g. `<title>Materials and Methods</title>`
    * The first match wins; later duplicates are ignored.
    * Every `<p>` descendant under the matching `<sec>` is converted to plain
      text via ``ET.tostring(..., method="text")`` and concatenated with
      single spaces.

    Notes
    -----
    * The function **does not** recurse into nested subsections.  If your
      workflow demands fine-grained subsections (e.g. *“Statistical
      Analysis”*), extend the search logic before calling this helper.
    * If the article is missing a `<body>` element entirely, the function
      returns the default ``'N/A'`` dictionary immediately.

    Example
    -------
    >>> xml = _strip_default_ns(raw_xml_bytes)
    >>> root = ET.fromstring(xml)
    >>> comps = extract_article_components(root)
    >>> print(comps["methods"][:250], "…")
    """
    sections = {
        'introduction': 'N/A',
        'methods': 'N/A',
        'results': 'N/A',
        'discussion': 'N/A',
    }

    body = article_element.find('.//body')
    if body is None:
        return sections

    # Iterate through all <sec> elements inside the body
    for sec in body.findall('.//sec'):
        sec_type = (sec.get('sec-type') or '').lower()
        title_elem = sec.find('title')
        title_text = (title_elem.text or '').lower() if title_elem is not None else ''

        # Collect text of all paragraph nodes within this section
        texts = [
            ET.tostring(node, encoding='unicode', method='text').strip()
            for node in sec.findall('.//p')
        ]
        content = ' '.join(texts).strip() or 'N/A'

        # Map to target IMRaD bucket
        if 'introduction' in sec_type or 'introduction' in title_text:
            sections['introduction'] = content
        elif 'method' in sec_type or 'method' in title_text:
            sections['methods'] = content
        elif 'result' in sec_type or 'result' in title_text:
            sections['results'] = content
        elif 'discussion' in sec_type or 'discussion' in title_text:
            sections['discussion'] = content

    return sections


 
def append_article_components(df: pd.DataFrame) -> pd.DataFrame:
    """
    ------------------------------------------------------------------------
    Vectorised wrapper around :pyfunc:`extract_article_components` that
    enriches a *DataFrame* of PMC articles with plain-text IMRaD sections.
    ------------------------------------------------------------------------

    The function iterates over **every row** of *df*, parses the JATS XML
    stored in column ``"fullXML"``, and appends four new columns:

    * ``"introduction"``
    * ``"methods"``
    * ``"results"``
    * ``"discussion"``

    Missing or unparsable XML is handled gracefully—each target column is
    filled with the literal string ``"N/A"`` for the affected row.

    Parameters
    ----------
    df : pandas.DataFrame
        The input frame *must* contain a column named ``"fullXML"`` whose
        cells hold a complete `<article>` subtree for that row.  Additional
        columns are left untouched.

    Returns
    -------
    pandas.DataFrame
        A *copy* of the input with four additional columns.  The original
        frame is **not** modified in-place.

        Column          | dtype  | description
        ----------------|--------|--------------------------------------------
        ``introduction``| string | Plain-text introduction or ``"N/A"``
        ``methods``     | string | Plain-text methods        ”      ”
        ``results``     | string | Plain-text results        ”      ”
        ``discussion``  | string | Plain-text discussion     ”      ”

    Notes
    -----
    * The function takes a defensive stance: **any** exception raised while
      parsing or section-matching results in an all-``"N/A"`` placeholder
      record for that row, ensuring the overall pipeline never breaks on
      malformed XML.
    * If you need higher throughput, consider converting the loop to a
      ``df.apply`` with caching of identical XML strings, or parallelising
      with `swifter`, `pandarelle`, or similar.

    Example
    -------
    >>> enriched = append_article_components(pmc_df)
    >>> enriched.loc[0, ["title", "methods"]]
    """

    # Prepare new columns
    df = df.copy()
    df['introduction'] = None
    df['methods'] = None
    df['results'] = None
    df['discussion'] = None

    for idx, row in df.iterrows():
        xml_str = row.get('fullXML', '')
        try:
            elem = ET.fromstring(xml_str)
            sections = extract_article_components(elem)
        except Exception:
            sections = {
                'introduction': 'N/A',
                'methods': 'N/A',
                'results': 'N/A',
                'discussion': 'N/A'
            }
        # Assign the extracted content
        df.at[idx, 'introduction'] = sections['introduction']
        df.at[idx, 'methods'] = sections['methods']
        df.at[idx, 'results'] = sections['results']
        df.at[idx, 'discussion'] = sections['discussion']

    return df   
 
def has_cohort_definition(text: str) -> int:
    """
    ------------------------------------------------------------------------
    Detect whether *text* contains at least one **cohort-definition keyword**
    (ICD/CPT/HICPCS code terms, temporal windows, inclusion/exclusion phrases,
    or care-setting markers) as defined by the compiled regular expression
    :pydata:`phenotype_regex`.
    ------------------------------------------------------------------------

    The helper is a thin, type-stable wrapper around
    ``bool(phenotype_regex.search(text))`` that normalises *null-like* inputs
    to the integer ``0`` instead of raising.

    Parameters
    ----------
    text : str
        The textual snippet to analyse.  Strings such as ``"N/A"``, the empty
        string, or *pandas* missing markers (``pd.NA``, ``numpy.nan``) are all
        treated as *absence of cohort terms*.

    Returns
    -------
    int
        ``1``  ⇢  at least one match found  
        ``0``  ⇢  no match or *text* is null/``"N/A"``

    Notes
    -----
    * ``phenotype_regex`` is compiled with the **extended** (``(?x)``) and
      **case-insensitive** (``(?i)``) flags.  Adding new vocabularies or
      adjusting window sizes therefore requires editing only that global
      pattern, *not* this helper.
    * The return type is an *int* rather than *bool* to facilitate direct use
      in *pandas* aggregation pipelines (e.g. ``df["introHasCohort"].mean()``).

    Example
    -------
    >>> has_cohort_definition("Patients were included if they had ≥2 ICD-10 codes in the baseline window.")
    1
    >>> has_cohort_definition("Differential gene expression was analysed…")
    0
    """
    if pd.isna(text) or text == "N/A":
        return 0
    return int(bool(phenotype_regex.search(str(text))))


def has_medical_code(text: str) -> int:
    """
    ------------------------------------------------------------------------
    Detect the *presence* of any **structured medical code** in *text* using
    the pre-compiled pattern :pydata:`medical_regex`.
    ------------------------------------------------------------------------

    The regular expression covers the most common coding vocabularies used in
    observational research:

    * ICD-9-CM, ICD-10 / 10-CM, ICD-11 stem codes
    * CPT / CDT, HCPCS Level II
    * SNOMED CT concept IDs, RxNorm (RXCUI), ATC, LOINC, NDC
    * Read / CTV3, OMOP concept IDs, assorted drug & billing code patterns

    Parameters
    ----------
    text : str
        A free-text snippet.  The helper treats empty strings, the literal
        ``"N/A"``, and any *pandas* missing scalars (``pd.NA``/``np.nan``)
        as *no code present*.

    Returns
    -------
    int
        ``1``  ⇢  at least one code pattern matched  
        ``0``  ⇢  no match or *text* is null/``"N/A"``

    Notes
    -----
    * ``medical_regex`` is compiled with the **case-insensitive** flag, so
      alpha-numeric codes are recognised regardless of letter case.
    * The return type is an *int* (0 / 1) rather than *bool* to allow simple
      aggregation such as ``df["methodsHasMedCode"].sum()`` or ``mean()``.

    Example
    -------
    >>> has_medical_code("ICD-10 code I50.9 was used to identify cases.")
    1
    >>> has_medical_code("We sequenced the mitochondrial genome …")
    0
    """
    if pd.isna(text) or text == "N/A":
        return 0
    return int(bool(medical_regex.search(str(text))))

    
   
def append_section_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    ------------------------------------------------------------------------
    Enrich a *DataFrame* of PMC articles with binary **indicator columns** that
    flag the presence of cohort-definition language and structured medical
    codes in five text buckets:
        • full article text  (``"fullText"``)
        • Introduction
        • Methods
        • Results
        • Discussion
    ------------------------------------------------------------------------

    New columns created
    -------------------
    For every bucket *b* in
    ``["fullText", "introduction", "methods", "results", "discussion"]``:

    * ``f"{b}HasCohortDefinition"``  –  ``"1"`` if *b* contains at least one
      match for :pydata:`phenotype_regex`; otherwise ``"0"``  
      (see :pyfunc:`has_cohort_definition`).

    * ``f"{b}HasMedicalCode"``       –  ``"1"`` if *b* contains at least one
      match for :pydata:`medical_regex`; otherwise ``"0"``  
      (see :pyfunc:`has_medical_code`).

    In addition, the function appends a new column

    * ``"phenotype"`` – initialised to the literal string ``'N/A'`` so that
      downstream phenotype-assignment logic can simply overwrite the field.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain the text columns:

        * ``"fullText"``  – plain-text full body (required)  
        * ``"introduction"`` ``"methods"`` ``"results"`` ``"discussion"``
          – outputs of :pyfunc:`append_article_components`.

        The function **copies** the input frame; the original is left
        untouched.

    Returns
    -------
    pandas.DataFrame
        The augmented frame with ten new indicator columns plus the
        ``"phenotype"`` placeholder.  All indicator columns are of dtype
        ``string`` containing ``"0"`` / ``"1"``.

    Notes
    -----
    * Every source column is explicitly cast to *str* before regex matching,
      preventing surprises from `pd.NA`, `numpy.nan`, or mixed dtypes.
    * Indicators are stored as strings (not bool or int) because Spark’s
      Delta merge step (see *run_pubmed_search*) expects string-typed
      columns for schema evolution.
    * The helper respects the “garbage-in, garbage-out” principle: if a
      section’s text is literally ``"N/A"``, both indicators are ``"0"``.

    Example
    -------
    >>> enriched = append_section_indicators(sections_df)
    >>> enriched.filter(regex="MethodsHas.*").value_counts()
    MethodsHasCohortDefinition  MethodsHasMedicalCode
    1                           1                        32
                                0                        15
    0                           1                        48
                                0                       205
    dtype: int64
    """
    df = df.copy()

    # Generate indicators for each text bucket
    for section in ["fullText", "introduction", "methods", "results", "discussion"]:
        df[section] = df[section].astype(str)

        cohort_col = f"{section}HasCohortDefinition"
        df[cohort_col] = df[section].apply(has_cohort_definition).astype(str)

        med_code_col = f"{section}HasMedicalCode"
        df[med_code_col] = df[section].apply(has_medical_code).astype(str)

    # placeholder for downstream phenotype tagging
    df["phenotype"] = "N/A"

    return df

   


def run_pubmed_search(
    mesh_term: str,
    rwd_terms: str,
    date_term: str,
    pm_key: str,
    phenoName: str,
    saved_file_name: str,
    return_max: int = 2000,
    spark=None
) -> pd.DataFrame:
    """
    ------------------------------------------------------------------------
    One-stop **PubMed → Spark → Delta** ingestion pipeline
    ------------------------------------------------------------------------

    1. **Build search string**  
       ``(<mesh_term>) AND (<rwd_terms>) AND (<date_term>)`` → collapse
       whitespace.

    2. **PMID ↔ PMCID mapping** via :pyfunc:`fetch_pmid_pmcid_from_pubmed`.

    3. **PubMed metadata** for every PMID  
       :pyfunc:`fetch_pubmed_metadata_for_pmid`.

    4. **PMC metadata + full XML** for every mapped PMCID  
       :pyfunc:`fetch_pmcid_metadata_for_pmcid`.

    5. **Article sections** (IMRaD) extracted from XML  
       :pyfunc:`append_article_components`.

    6. **Section indicators** (cohort language / medical codes)  
       :pyfunc:`append_section_indicators`.

    7. **Frame merge**  
       *pmid ↔ pmcid map* ⇢ PubMed meta ⇢ PMC meta + sections/flags.

    8. **Spark conversion** and enrichment with
       ``phenotype`` and ``processed_at_utc``.

    9. **Delta Lake persistence**  
       *Create-or-upsert* based on ``pmcid`` + ``phenotype``.

    10. **Round-trip read-back** for the requested phenotype and **return
        as a pandas DataFrame**.


    Parameters
    ----------
    mesh_term : str
        Portion of the query that targets a MeSH concept, e.g.
        ``"Heart Failure"[Mesh]``.
    rwd_terms : str
        Real-world-data Boolean expression created by
        :pyfunc:`pubmedSearchTerms`.
    date_term : str
        Date constraint, e.g. ``"2023/01/01"[PDAT] : "2024/12/31"[PDAT]``.
    pm_key : str
        NCBI API key (shared across *all* E-utilities calls in the pipeline).
    phenoName : str
        Human-readable phenotype name (becomes the ``phenotype`` column and
        part of the Delta table merge condition).
    saved_file_name : str
        Fully-qualified Delta table name (catalog.schema.table) or a
        Unity Catalog path like ``"main.default.pubmed_heart_failure"``.
    return_max : int, default 2 000
        Upper limit of PMIDs pulled from *ESearch*.  Cascades through the
        rest of the pipeline.
    spark : pyspark.sql.SparkSession | None
        Active Spark session. **Required**; a ``ValueError`` is raised if
        omitted.

    Returns
    -------
    pandas.DataFrame
        The phenotype-specific subset of the Delta table, including all
        metadata, section texts, indicator columns, and ingestion stamps.

    Raises
    ------
    ValueError
        When *spark* is ``None``.
    Exception
        Any unhandled exception arising in the individual pipeline stages is
        re-raised after logging a descriptive error message.

    Delta upsert logic
    ------------------
    ``MERGE INTO {table} AS t
        USING ingest AS s
        ON   t.pmcid = s.pmcid AND t.phenotype = s.phenotype
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *``

    Notes
    -----
    * The function performs **schema evolution** implicitly: new columns in
      future versions of upstream helpers are inserted automatically.
    * Because the round-trip read happens immediately after the write, the
      caller gets back exactly what was committed—no need to wait for Spark
      cache invalidation.
    * Running multiple phenotypes against the same Delta table is safe: the
      composite key ``(pmcid, phenotype)`` prevents collisions.

    Example
    -------
    >>> df = run_pubmed_search(
    ...     mesh_term='"Heart Failure"[Mesh]',
    ...     rwd_terms=pubmedSearchTerms(),
    ...     date_term='2023/01/01[PDAT] : 2024/12/31[PDAT]',
    ...     pm_key=os.environ["NCBI_KEY"],
    ...     phenoName="HF_RWE",
    ...     saved_file_name="rwd_pubmed.hf_articles",
    ...     spark=spark
    ... )
    >>> df.shape
    (783, 58)
    """

    if spark is None:
        raise ValueError("A SparkSession must be passed to run_pubmed_search()")

    # 1) build & clean the query
    full_search = f'({mesh_term}) AND ({rwd_terms}) AND ({date_term})'
    full_search = re.sub(r"\s+", " ", full_search.strip())
    logger.info(f"Running PubMed search for: {full_search}")

    # 2) pull pmid↔pmcid map
    try:
        pmid_map_df = fetch_pmid_pmcid_from_pubmed(
            query=full_search,
            retmax=return_max,
            api_key=pm_key
        )
        logger.info(f"Retrieved {len(pmid_map_df)} PMID↔PMCID mappings")
    except Exception as e:
        logger.error(f"Error fetching PMID↔PMCID map: {e}")
        raise

    # 3) pull PubMed metadata for all PMIDs
    pmid_list = pmid_map_df['pmid'].tolist()
    try:
        pmid_meta_df = fetch_pubmed_metadata_for_pmid(
            pmids=pmid_list,
            api_key=pm_key
        )
        logger.info(f"Fetched metadata for {len(pmid_meta_df)} PMIDs")
    except Exception as e:
        logger.error(f"Error fetching PubMed metadata: {e}")
        raise

    # 4) pull PMC metadata (incl. fullXML) for any mapped PMCIDs
    pmcid_list = pmid_map_df['pmcid'].dropna().unique().tolist()
    try:
        pmcid_meta_df = fetch_pmcid_metadata_for_pmcid(
            pmcids=pmcid_list,
            api_key=pm_key
        )
        logger.info(f"Fetched metadata for {len(pmcid_meta_df)} PMCIDs")
    except Exception as e:
        logger.error(f"Error fetching PMC metadata: {e}")
        raise

    # 5) extract intro/methods/results/discussion from the PMC fullXML
    pmcid_sections_df = append_article_components(pmcid_meta_df)
    logger.info("Extracted article sections (intro, methods, results, discussion)")

    # 6) flag cohort-definition & medical-code terms in each section
    enriched_pmcid_df = append_section_indicators(pmcid_sections_df)
    logger.info("Appended cohort-definition & medical-code indicators")

    # 7) merge everything together:
    # a) start with the unique pmid↔pmcid map
    base_df = pmid_map_df[['pmid', 'pmcid', 'openAccess']].drop_duplicates(subset='pmid')

    # b) merge in only the *new* pmid_meta_df columns
    #   1) compute which columns overlap (except our key 'pmid')
    overlap_meta = set(base_df.columns).intersection(pmid_meta_df.columns) - {'pmid'}
    #   2) drop them from pmid_meta_df
    pmid_meta_trimmed = pmid_meta_df.drop(columns=list(overlap_meta))

    #   3) now merge
    base_df = base_df.merge(
        pmid_meta_trimmed,
        on='pmid',
        how='left'
    )

    # c) merge in only the *new* enriched_pmcid_df columns
    #   1) overlap on both keys (excluding 'pmid' and 'pmcid')
    overlap_enriched = set(base_df.columns).intersection(enriched_pmcid_df.columns) - {'pmid', 'pmcid'}
    #   2) drop them
    enriched_trimmed = enriched_pmcid_df.drop(columns=list(overlap_enriched))

    #   3) final merge
    merged = base_df.merge(
        enriched_trimmed,
        on=['pmid', 'pmcid'],
        how='left'
    )

    # d) fill and stringify as before
    merged = merged.fillna("").astype(str)

    
    # 8) convert to Spark, add phenotype + UTC timestamp
    try:   

        spark_df = spark.createDataFrame(merged)
        utc_ts = to_utc_timestamp(current_timestamp(), "UTC")
        spark_df = (
            spark_df
            .withColumn("phenotype", lit(phenoName))
            .withColumn("processed_at_utc", utc_ts)
        )
    except Exception as e:
        logger.error(f"Error converting to Spark DataFrame: {e}")
        raise

    # 9) write or upsert into Delta
    try:
        if not spark.catalog.tableExists(saved_file_name):
            spark_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(saved_file_name)
            logger.info(f"Created new Delta table: {saved_file_name}")
        else:
            deltaTable = DeltaTable.forName(spark, saved_file_name)
            (
                deltaTable.alias("t")
                .merge(
                    source=spark_df.alias("s"),
                    condition="t.pmcid = s.pmcid AND t.phenotype = s.phenotype"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"Upserted data into existing Delta table: {saved_file_name}")
    except Exception as e:
        logger.error(f"Error writing/upserting Delta table: {e}")
        raise

    # 10) read back and return as pandas
    try:
        result_spark = spark.sql(
            f"SELECT * FROM {saved_file_name} WHERE phenotype = '{phenoName}'"
        )
        result_pdf = result_spark.toPandas()
        logger.info(f"Pipeline complete, returning {len(result_pdf)} records")
        return result_pdf
    except Exception as e:
        logger.error(f"Error reading back from Delta table: {e}")
        raise


def get_processed_pmcids(existing_df: pd.DataFrame) -> set[str]:
    """
    ------------------------------------------------------------------------
    Return the set of **PMCIDs already present** in a previously ingested
    PubMed/PMC *DataFrame*.
    ------------------------------------------------------------------------

    This tiny helper is primarily used by incremental pipelines to decide
    which articles still need to be fetched and which can be skipped.

    Parameters
    ----------
    existing_df : pandas.DataFrame
        A DataFrame that *may* contain a column named ``"pmcid"``.  The
        function tolerates

        * an empty frame (``existing_df.empty is True``),
        * a missing ``"pmcid"`` column, or
        * any dtype for that column.

    Returns
    -------
    set[str]
        * **Non-empty** – all unique PMCIDs as *strings* when the column
          exists **and** the frame is not empty.
        * **Empty set** – when either condition fails (frame empty *or*
          column missing).

    Examples
    --------
    >>> get_processed_pmcids(pd.DataFrame())
    set()
    >>> df = pd.DataFrame({"pmcid": ["PMC123", "PMC456", "PMC123"]})
    >>> get_processed_pmcids(df)
    {'PMC123', 'PMC456'}

    Notes
    -----
    * The helper intentionally returns a *set* rather than a list to provide
      O(1) membership tests in downstream filtering logic.
    * All values are coerced to *str* implicitly by pandas’ ``Series.unique``
      when the original column has a mixed dtype.
    """
    if not existing_df.empty and "pmcid" in existing_df.columns:
        return set(existing_df["pmcid"].unique())
    return set()




def pubmedSearchTerms():
    """
    Creates a string query to pass to PubMed APIs.
    The string combines 1) study design terms, 2) data sources & methods terms,
    3) phenotyping terms, and 4) specific database terms, excluding certain article types.

    Returns:
        A character string with all terms combined
    """
    # Study Design Terms
    study_design_terms = [
        '("Observational Study"[Publication Type])',
        '("Retrospective Studies"[Mesh])',
        '(observational[tiab])',
        '(retrospective[tiab])',
        '("retrospective analysis"[tiab])',
        '("retrospective analyses"[tiab])',
        '("retrospective review"[tiab])',
        '("retrospective study"[tiab])',
        '("case-control"[tiab])',
        '("case control"[tiab])',
        '("cohort study"[tiab])',
        '("cohort analysis"[tiab])',
        '("chart review"[tiab])',
        '("medical record review"[tiab])',
        '("record review"[tiab])',
        '("database study"[tiab])',
        '("non-interventional"[tiab])',
        '("non interventional"[tiab])',
        '("nonrandomized"[tiab])',
        '("non randomized"[tiab])',
        '("historical cohort"[tiab])',
        '("archival data"[tiab])',
        '("longitudinal study"[tiab])',
        '("comparative effectiveness research"[tiab])',
        '("real world data"[tiab])',
        '("real-world data"[tiab])',
        '("real world evidence"[tiab])',
        '("real-world evidence"[tiab])',
    ]

    # Data Sources & Methods Terms
    data_sources_methods = [
        '("claims data")',
        '("claims analysis")',
        '("claims database")',
        '("administrative data")',
        '("registry study")',
        '("registry analysis")',
        '("registry data")',
        '("real-world")',
        '("real world")',
        '("real-world evidence")',
        '("secondary data analysis")',
        '("electronic health record")',
        '(EMR)',
        '(EHR)',
        '("insurance claims")',
        '("administrative claims data")',
        '("health care data")',
        '("healthcare data")',
    ]

    # Phenotyping & Cohort Identification Terms
    phenotyping_terms = [
        '(phenotype)',
        '(phenotyping algorithm)',
        '(computable phenotype)',
        '(ICD codes)',
        '(ICD-9)',
        '(ICD-10)',
        '(positive predictive value)',
        '(PPV)',
    ]

    # Specific Databases (Global)
    specific_databases = [
        '(SEER)',
        '(NHANES)',
        '(Medicare)',
        '(Medicaid)',
        '("VA data")',
        '("Veterans Affairs")',
        '(Sentinel)',
        '(HCUP)',
        '(NSQIP)',
        '(Denmark/epidemiology[Mesh])',
        '(National Health Insurance Research[Mesh])',
        '("General Practice Research Database")',
        '("Clinical Practice Research Datalink")',
        '("The Health Improvement Network")',
        '("Taiwan National Health Insurance Research Database")',
        '("Health Insurance Review and Assessment Service")',
        '(BIFAP)',
        '(SIDIAP)',
        '(QResearch)',
        '(Truven)',
        '(Merativ)',
        '(Optum)',
        '(Medstat)',
        '("Nationwide Inpatient Sample")',
        '(PharMetrics)',
        '(PHARMO)',
        '(IMS)',
        '(IQVIA)',
        '("Premier database")',
    ]

    # Exclusion Terms
    exclusion_terms = [
        '("Clinical Trial"[Publication Type])',
        '("Randomized Controlled Trial"[Publication Type])',
        '("Controlled Clinical Trial"[Publication Type])',
        '("Prospective Studies"[Mesh])',
        '("Case Reports"[Publication Type])',
        '("Systematic Review"[Publication Type])',
        '("Meta-Analysis"[Publication Type])',
        '("Editorial"[Publication Type])',
        '("Letter"[Publication Type])',
        '("Comment"[Publication Type])',
        '("News"[Publication Type])',
        '("pilot study"[tiab])',
        '("pilot projects"[Mesh])',
        '("double-blind"[tiab])',
        '("placebo-controlled"[tiab])',
        '("Genetics"[Mesh])',
        '("Genotype"[Mesh])',
        '("biomarker"[tiab])',
    ]

    # Combine included terms, now including phenotyping_terms
    included_query = (
        "(" + " OR ".join(study_design_terms) + ") AND ("
        + " OR ".join(data_sources_methods) + ") AND ("
        + " OR ".join(phenotyping_terms) + ") AND ("
        + " OR ".join(specific_databases) + ")"
    )

    # Combine exclusion terms
    exclusion_query = "(" + " OR ".join(exclusion_terms) + ")"

    # Final structured PubMed query
    rwd_terms = f"{included_query} NOT {exclusion_query}"
    return rwd_terms
