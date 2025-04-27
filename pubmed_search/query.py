def rwdSearchTerms():
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