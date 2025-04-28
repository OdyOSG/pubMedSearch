# <add imports>





# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)



# def extract_article_components(article_element: ET.Element) -> dict[str, str]:
#     """
#     ------------------------------------------------------------------------
#     Pull out the canonical IMRaD sections—**Introduction, Methods, Results,
#     Discussion**—from a single JATS `<article>` element.
#     ------------------------------------------------------------------------

#     The function is deliberately *best-effort*: many PMC articles deviate from
#     strict JATS tagging conventions.  It therefore combines element
#     attributes (`sec-type`) **and** the literal section title text to decide
#     which content belongs to which IMRaD bucket.

#     Parameters
#     ----------
#     article_element : xml.etree.ElementTree.Element
#         The root `<article>` node whose default namespace has already been
#         stripped.  Supplying a namespaced element will silently fail to match
#         any section tags.

#     Returns
#     -------
#     dict[str, str]
#         A four-key dictionary with *plain-text* content:

#         key            | value
#         ---------------|--------------------------------------------------------
#         ``'introduction'`` | Section text or ``'N/A'`` when unavailable
#         ``'methods'``      |          ”            ”          ”        ”
#         ``'results'``      |          ”            ”          ”        ”
#         ``'discussion'``   |          ”            ”          ”        ”

#         The string ``'N/A'`` is used consistently to indicate *missing* or
#         *unresolved* sections so that downstream joins and fills remain
#         type-stable.

#     Heuristics
#     ----------
#     * A section qualifies as **Introduction**, **Methods**, **Results**, or
#       **Discussion** if **either** of the following holds:
#         1. The attribute ``sec-type`` contains a diagnostic token (case-ins.)
#            – e.g. ``sec-type="methods"``
#         2. The child `<title>` element’s text contains the token –
#            e.g. `<title>Materials and Methods</title>`
#     * The first match wins; later duplicates are ignored.
#     * Every `<p>` descendant under the matching `<sec>` is converted to plain
#       text via ``ET.tostring(..., method="text")`` and concatenated with
#       single spaces.

#     Notes
#     -----
#     * The function **does not** recurse into nested subsections.  If your
#       workflow demands fine-grained subsections (e.g. *“Statistical
#       Analysis”*), extend the search logic before calling this helper.
#     * If the article is missing a `<body>` element entirely, the function
#       returns the default ``'N/A'`` dictionary immediately.

#     Example
#     -------
#     >>> xml = _strip_default_ns(raw_xml_bytes)
#     >>> root = ET.fromstring(xml)
#     >>> comps = extract_article_components(root)
#     >>> print(comps["methods"][:250], "…")
#     """
#     sections = {
#         'introduction': 'N/A',
#         'methods': 'N/A',
#         'results': 'N/A',
#         'discussion': 'N/A',
#     }

#     body = article_element.find('.//body')
#     if body is None:
#         return sections

#     # Iterate through all <sec> elements inside the body
#     for sec in body.findall('.//sec'):
#         sec_type = (sec.get('sec-type') or '').lower()
#         title_elem = sec.find('title')
#         title_text = (title_elem.text or '').lower() if title_elem is not None else ''

#         # Collect text of all paragraph nodes within this section
#         texts = [
#             ET.tostring(node, encoding='unicode', method='text').strip()
#             for node in sec.findall('.//p')
#         ]
#         content = ' '.join(texts).strip() or 'N/A'

#         # Map to target IMRaD bucket
#         if 'introduction' in sec_type or 'introduction' in title_text:
#             sections['introduction'] = content
#         elif 'method' in sec_type or 'method' in title_text:
#             sections['methods'] = content
#         elif 'result' in sec_type or 'result' in title_text:
#             sections['results'] = content
#         elif 'discussion' in sec_type or 'discussion' in title_text:
#             sections['discussion'] = content

#     return sections


 
# def append_article_components(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     ------------------------------------------------------------------------
#     Vectorised wrapper around :pyfunc:`extract_article_components` that
#     enriches a *DataFrame* of PMC articles with plain-text IMRaD sections.
#     ------------------------------------------------------------------------

#     The function iterates over **every row** of *df*, parses the JATS XML
#     stored in column ``"fullXML"``, and appends four new columns:

#     * ``"introduction"``
#     * ``"methods"``
#     * ``"results"``
#     * ``"discussion"``

#     Missing or unparsable XML is handled gracefully—each target column is
#     filled with the literal string ``"N/A"`` for the affected row.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         The input frame *must* contain a column named ``"fullXML"`` whose
#         cells hold a complete `<article>` subtree for that row.  Additional
#         columns are left untouched.

#     Returns
#     -------
#     pandas.DataFrame
#         A *copy* of the input with four additional columns.  The original
#         frame is **not** modified in-place.

#         Column          | dtype  | description
#         ----------------|--------|--------------------------------------------
#         ``introduction``| string | Plain-text introduction or ``"N/A"``
#         ``methods``     | string | Plain-text methods        ”      ”
#         ``results``     | string | Plain-text results        ”      ”
#         ``discussion``  | string | Plain-text discussion     ”      ”

#     Notes
#     -----
#     * The function takes a defensive stance: **any** exception raised while
#       parsing or section-matching results in an all-``"N/A"`` placeholder
#       record for that row, ensuring the overall pipeline never breaks on
#       malformed XML.
#     * If you need higher throughput, consider converting the loop to a
#       ``df.apply`` with caching of identical XML strings, or parallelising
#       with `swifter`, `pandarelle`, or similar.

#     Example
#     -------
#     >>> enriched = append_article_components(pmc_df)
#     >>> enriched.loc[0, ["title", "methods"]]
#     """

#     # Prepare new columns
#     df = df.copy()
#     df['introduction'] = None
#     df['methods'] = None
#     df['results'] = None
#     df['discussion'] = None

#     for idx, row in df.iterrows():
#         xml_str = row.get('fullXML', '')
#         try:
#             elem = ET.fromstring(xml_str)
#             sections = extract_article_components(elem)
#         except Exception:
#             sections = {
#                 'introduction': 'N/A',
#                 'methods': 'N/A',
#                 'results': 'N/A',
#                 'discussion': 'N/A'
#             }
#         # Assign the extracted content
#         df.at[idx, 'introduction'] = sections['introduction']
#         df.at[idx, 'methods'] = sections['methods']
#         df.at[idx, 'results'] = sections['results']
#         df.at[idx, 'discussion'] = sections['discussion']

#     return df   
 
# def has_cohort_definition(text: str) -> int:
#     """
#     ------------------------------------------------------------------------
#     Detect whether *text* contains at least one **cohort-definition keyword**
#     (ICD/CPT/HICPCS code terms, temporal windows, inclusion/exclusion phrases,
#     or care-setting markers) as defined by the compiled regular expression
#     :pydata:`phenotype_regex`.
#     ------------------------------------------------------------------------

#     The helper is a thin, type-stable wrapper around
#     ``bool(phenotype_regex.search(text))`` that normalises *null-like* inputs
#     to the integer ``0`` instead of raising.

#     Parameters
#     ----------
#     text : str
#         The textual snippet to analyse.  Strings such as ``"N/A"``, the empty
#         string, or *pandas* missing markers (``pd.NA``, ``numpy.nan``) are all
#         treated as *absence of cohort terms*.

#     Returns
#     -------
#     int
#         ``1``  ⇢  at least one match found  
#         ``0``  ⇢  no match or *text* is null/``"N/A"``

#     Notes
#     -----
#     * ``phenotype_regex`` is compiled with the **extended** (``(?x)``) and
#       **case-insensitive** (``(?i)``) flags.  Adding new vocabularies or
#       adjusting window sizes therefore requires editing only that global
#       pattern, *not* this helper.
#     * The return type is an *int* rather than *bool* to facilitate direct use
#       in *pandas* aggregation pipelines (e.g. ``df["introHasCohort"].mean()``).

#     Example
#     -------
#     >>> has_cohort_definition("Patients were included if they had ≥2 ICD-10 codes in the baseline window.")
#     1
#     >>> has_cohort_definition("Differential gene expression was analysed…")
#     0
#     """
#     if pd.isna(text) or text == "N/A":
#         return 0
#     return int(bool(phenotype_regex.search(str(text))))


# def has_medical_code(text: str) -> int:
#     """
#     ------------------------------------------------------------------------
#     Detect the *presence* of any **structured medical code** in *text* using
#     the pre-compiled pattern :pydata:`medical_regex`.
#     ------------------------------------------------------------------------

#     The regular expression covers the most common coding vocabularies used in
#     observational research:

#     * ICD-9-CM, ICD-10 / 10-CM, ICD-11 stem codes
#     * CPT / CDT, HCPCS Level II
#     * SNOMED CT concept IDs, RxNorm (RXCUI), ATC, LOINC, NDC
#     * Read / CTV3, OMOP concept IDs, assorted drug & billing code patterns

#     Parameters
#     ----------
#     text : str
#         A free-text snippet.  The helper treats empty strings, the literal
#         ``"N/A"``, and any *pandas* missing scalars (``pd.NA``/``np.nan``)
#         as *no code present*.

#     Returns
#     -------
#     int
#         ``1``  ⇢  at least one code pattern matched  
#         ``0``  ⇢  no match or *text* is null/``"N/A"``

#     Notes
#     -----
#     * ``medical_regex`` is compiled with the **case-insensitive** flag, so
#       alpha-numeric codes are recognised regardless of letter case.
#     * The return type is an *int* (0 / 1) rather than *bool* to allow simple
#       aggregation such as ``df["methodsHasMedCode"].sum()`` or ``mean()``.

#     Example
#     -------
#     >>> has_medical_code("ICD-10 code I50.9 was used to identify cases.")
#     1
#     >>> has_medical_code("We sequenced the mitochondrial genome …")
#     0
#     """
#     if pd.isna(text) or text == "N/A":
#         return 0
#     return int(bool(medical_regex.search(str(text))))

    
   
# def append_section_indicators(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     ------------------------------------------------------------------------
#     Enrich a *DataFrame* of PMC articles with binary **indicator columns** that
#     flag the presence of cohort-definition language and structured medical
#     codes in five text buckets:
#         • full article text  (``"fullText"``)
#         • Introduction
#         • Methods
#         • Results
#         • Discussion
#     ------------------------------------------------------------------------

#     New columns created
#     -------------------
#     For every bucket *b* in
#     ``["fullText", "introduction", "methods", "results", "discussion"]``:

#     * ``f"{b}HasCohortDefinition"``  –  ``"1"`` if *b* contains at least one
#       match for :pydata:`phenotype_regex`; otherwise ``"0"``  
#       (see :pyfunc:`has_cohort_definition`).

#     * ``f"{b}HasMedicalCode"``       –  ``"1"`` if *b* contains at least one
#       match for :pydata:`medical_regex`; otherwise ``"0"``  
#       (see :pyfunc:`has_medical_code`).

#     In addition, the function appends a new column

#     * ``"phenotype"`` – initialised to the literal string ``'N/A'`` so that
#       downstream phenotype-assignment logic can simply overwrite the field.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Must contain the text columns:

#         * ``"fullText"``  – plain-text full body (required)  
#         * ``"introduction"`` ``"methods"`` ``"results"`` ``"discussion"``
#           – outputs of :pyfunc:`append_article_components`.

#         The function **copies** the input frame; the original is left
#         untouched.

#     Returns
#     -------
#     pandas.DataFrame
#         The augmented frame with ten new indicator columns plus the
#         ``"phenotype"`` placeholder.  All indicator columns are of dtype
#         ``string`` containing ``"0"`` / ``"1"``.

#     Notes
#     -----
#     * Every source column is explicitly cast to *str* before regex matching,
#       preventing surprises from `pd.NA`, `numpy.nan`, or mixed dtypes.
#     * Indicators are stored as strings (not bool or int) because Spark’s
#       Delta merge step (see *run_pubmed_search*) expects string-typed
#       columns for schema evolution.
#     * The helper respects the “garbage-in, garbage-out” principle: if a
#       section’s text is literally ``"N/A"``, both indicators are ``"0"``.

#     Example
#     -------
#     >>> enriched = append_section_indicators(sections_df)
#     >>> enriched.filter(regex="MethodsHas.*").value_counts()
#     MethodsHasCohortDefinition  MethodsHasMedicalCode
#     1                           1                        32
#                                 0                        15
#     0                           1                        48
#                                 0                       205
#     dtype: int64
#     """
#     df = df.copy()

#     # Generate indicators for each text bucket
#     for section in ["fullText", "introduction", "methods", "results", "discussion"]:
#         df[section] = df[section].astype(str)

#         cohort_col = f"{section}HasCohortDefinition"
#         df[cohort_col] = df[section].apply(has_cohort_definition).astype(str)

#         med_code_col = f"{section}HasMedicalCode"
#         df[med_code_col] = df[section].apply(has_medical_code).astype(str)

#     # placeholder for downstream phenotype tagging
#     df["phenotype"] = "N/A"

#     return df
