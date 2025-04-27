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
    (?:\d{4,5}-\d{3,4}-\d{1,2}|\d{10,12})                     |   # NDC 10–12 digits or  dash forms
    \d{1,6}-\d                                               |   # LOINC (check‑digit suffix)
    [A-Z]\d{2}[A-Z][A-Z0-9]{1,4}                             |   # ATC 5‑7 chars
    RXCUI:?\s*\d{7,9}                                        |   # RxNorm RXCUI
    [1-9]\d{5,17}                                            |   # SNOMED CT 6‑18 digits
    OMOP\d{3,15}                                             |   # OMOP concept‑id prefix
    [A-Z0-9]{1,5}\.[A-Z0-9]{1,2}                             |   # Read / CTV3 with dot
    D?\d{4,5}                                               |   # CPT® / CDT® (D‑ or numeric)
    [A-Z]\d{4}(?:-[A-Z0-9]{2})?                              |   # HCPCS Level II
)\b
""", re.VERBOSE | re.IGNORECASE)