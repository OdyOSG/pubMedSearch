## EPAMPubMedSearch

The PubMedSearch package is designed to facilitate the identification of observational studies that utilize real-world healthcare data, particularly data derived from administrative claims and electronic health/medical records (EHR/EMR). This tool prioritizes studies from various international databases that elaborate on their methodologies for identifying patient cohorts and defining health conditions or outcomes, including the use of phenotyping algorithms and ICD coding systems.

The search query generated by PubMedSearch is specifically constructed to exclude clinical trials, genetic studies, and other publication types that do not focus on observational research using routinely collected healthcare data. Additionally, the search incorporates terms related to validation processes to ensure the relevance and accuracy of the studies retrieved.

By using PubMedSearch, researchers can efficiently gather comprehensive information on observational studies, enhancing their ability to analyze and utilize real-world healthcare data effectively.

### Table of Contents

- [Usage](#usage)
- [Query string](#query-string)
  - [mesh_term](#mesh_term)
  - [rwd_terms](#rwd_terms)
  - [date_term](#date_term)
- [Functions](#functions)
  - [pubmedSearchTerms](#pubmedsearchterms)
  - [clean_dataframe](#clean_dataframe)
  - [write_dataframe_to_excel](#write_dataframe_to_excel)
  - [copy_to_dbfs](#copy_to_dbfs)
  - [display_download_link](#display_download_link)
  - [export_df_to_excel](#export_df_to_excel)
  - [fetch_pmc_ids](#fetch_pmc_ids)
  - [fetch_pubmed_fulltext_and_metadata](#fetch_pubmed_fulltext_and_metadata)
  - [run_pubmed_search](#run_pubmed_search)

### Usage
Here is an example of how you can use PubMedSearch to fetch and export data from PubMed:
``` python
from pubmed import (
    pubmedSearchTerms, 
    export_df_to_excel, 
    run_pubmed_search
)

# Generate search terms
rwd_terms = pubmedSearchTerms()

# Fetch data and export to Excel
df = run_pubmed_search(
    mesh_term="Ischemic Attack, Transient",
    rwd_terms=rwd_terms,
    date_term="2020",
    pm_key="your_pubmed_api_key",
    saved_file_name="pubmed_search_results"
)

# Export the resulting DataFrame to an Excel file
export_df_to_excel(
    df=df,
    output_filename="PubMedSearchResults.xlsx",
    displayHTML=displayHTML  # Assuming this function is available in your environment
)
```

### Query string
Query string consists of three parts: **mesh_term**, **rwd_terms**, **date_term**.

#### mesh_term
The MeSH term for the search (e.g., "Ischemic Attack, Transient").

#### rwd_terms
Additional search terms. Can be retreived using `pubmedSearchTerms()` function call.
Let's break the rwd_terms down into its four major parts, which are connected by Boolean operators **AND** and **NOT**. 
Essentially, PubMed will return only those articles that satisfy all of the following:

---

###### 1. Must indicate some form of an observational or retrospective study
```
(("Observational Study"[Publication Type]) 
 OR ("Retrospective Studies"[Mesh]) 
 OR (observational[tiab]) 
 OR (retrospective[tiab]) 
 OR ("retrospective analysis"[tiab]) 
 OR ("retrospective analyses"[tiab]) 
 OR ("retrospective review"[tiab]) 
 OR ("retrospective study"[tiab]) 
 OR ("case-control"[tiab]) 
 OR ("case control"[tiab]) 
 OR ("cohort study"[tiab]) 
 OR ("cohort analysis"[tiab]) 
 OR ("chart review"[tiab]) 
 OR ("medical record review"[tiab]) 
 OR ("record review"[tiab]) 
 OR ("database study"[tiab]) 
 OR ("non-interventional"[tiab]) 
 OR ("non interventional"[tiab]) 
 OR ("nonrandomized"[tiab]) 
 OR ("non randomized"[tiab]) 
 OR ("historical cohort"[tiab]) 
 OR ("archival data"[tiab]) 
 OR ("longitudinal study"[tiab]) 
 OR ("comparative effectiveness research"[tiab]) 
 OR ("real world data"[tiab]) 
 OR ("real-world data"[tiab]) 
 OR ("real world evidence"[tiab]) 
 OR ("real-world evidence"[tiab]))
```

All those terms describe articles that are likely **observational**—including retrospective study designs, case-control, cohort, chart review, database study, and so on.

---

###### 2. Must involve real-world data sources like claims, registries, EHR
```
AND (("claims data") 
     OR ("claims analysis") 
     OR ("claims database") 
     OR ("administrative data") 
     OR ("registry study") 
     OR ("registry analysis") 
     OR ("registry data") 
     OR ("real-world") 
     OR ("real world") 
     OR ("real-world evidence") 
     OR ("secondary data analysis") 
     OR ("electronic health record") 
     OR (EMR) 
     OR (EHR) 
     OR ("insurance claims") 
     OR ("administrative claims data") 
     OR ("health care data") 
     OR ("healthcare data"))
```

This part ensures the article specifically refers to a real-world data environment: claims databases, electronic health records (EHR/EMR), registry data, etc.

---

###### 3. Must mention large population-level data sources / databases
```
AND ((SEER) 
     OR (NHANES) 
     OR (Medicare) 
     OR (Medicaid) 
     OR ("VA data") 
     OR ("Veterans Affairs") 
     OR (Sentinel) 
     OR (HCUP) 
     OR (NSQIP) 
     OR (Denmark/epidemiology[Mesh]) 
     OR (National Health Insurance Research[Mesh]) 
     OR ("General Practice Research Database") 
     OR ("Clinical Practice Research Datalink") 
     OR ("The Health Improvement Network") 
     OR ("Taiwan National Health Insurance Research Database") 
     OR ("Health Insurance Review and Assessment Service") 
     OR (BIFAP) 
     OR (SIDIAP) 
     OR (QResearch) 
     OR (Truven) 
     OR (Merativ) 
     OR (Optum) 
     OR (Medstat) 
     OR (Nationwide Inpatient Sample) 
     OR (PharMetrics) 
     OR (PHARMO) 
     OR (IMS) 
     OR (IQVIA) 
     OR ("Premier database"))
```

All of these are well-known sources or databases used in observational research (e.g., SEER, NHANES, Medicare, Medicaid, VA data, HCUP, major European databases, IQVIA, Optum, etc.).

---

###### 4. Must **exclude** (i.e., NOT) clinical trials, prospective designs, case reports, systematic reviews, editorials, pilot studies, genetics, etc.
```
AND NOT (("Clinical Trial"[Publication Type]) 
         OR ("Randomized Controlled Trial"[Publication Type]) 
         OR ("Controlled Clinical Trial"[Publication Type]) 
         OR ("Prospective Studies"[Mesh]) 
         OR ("Case Reports"[Publication Type]) 
         OR ("Systematic Review"[Publication Type]) 
         OR ("Meta-Analysis"[Publication Type]) 
         OR ("Editorial"[Publication Type]) 
         OR ("Letter"[Publication Type]) 
         OR ("Comment"[Publication Type]) 
         OR ("News"[Publication Type]) 
         OR ("pilot study"[tiab]) 
         OR ("pilot projects"[Mesh]) 
         OR ("double-blind"[tiab]) 
         OR ("placebo-controlled"[tiab]) 
         OR ("Genetics"[Mesh]) 
         OR ("Genotype"[Mesh]) 
         OR ("biomarker"[tiab]))
```

This final block removes articles that are explicitly clinical trials, prospective trials, systematic reviews, editorials, or otherwise not relevant to purely observational/retrospective secondary data analyses.

---

###### Putting It All Together
In plain language, the query does this:

1. **Find articles** that indicate an observational or retrospective design.
2. **AND** that mention real-world data sources (e.g., claims, registries, EHR).
3. **AND** that also reference major population databases (SEER, NHANES, Medicare, etc.).
4. **AND** **exclude** any that are prospective trials, case reports, systematic reviews, editorials, etc.

Effectively, it is a well-structured PubMed query for identifying **observational, retrospective, real-world data studies** using well-known databases, while excluding clinical trials and other article types that are not relevant to that purpose.

#### date_term
Date constraints for the search, e.g. `date_term = '("2010"[Publication Date] : "3000"[Publication Date])'`

### Functions

#### pubmedSearchTerms

Generates a string query to pass to PubMed APIs. [details](#rwd_terms)

```python
def pubmedSearchTerms():
    """
    Creates a string query to pass to PubMed APIs.
    The string combines 1) study design terms, 2) data sources & methods terms, 
    3) phenotyping terms and 4) database terms.

    Returns:
        A character string with all terms combined
    """
```

#### clean_dataframe

Replaces null values and cells with "N/A" with empty strings, and drops specified columns if they exist in the DataFrame.

```python
def clean_dataframe(df, columns_to_drop):
    """
    Replace null values and cells with "N/A" with empty strings,
    and drop specified columns if they exist in the DataFrame.
    
    Parameters:
        df (pd.DataFrame): A DataFrame to clean.
        columns_to_drop (list of str): Names of columns to drop from DataFrame.
    
    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
```

#### write_dataframe_to_excel

Writes the DataFrame to a temporary Excel file using XlsxWriter. Creates an Excel table over the data, sets a fixed row height, enables text wrapping, and adjusts column widths.

```python
def write_dataframe_to_excel(
    df, 
    sheet_name, 
    table_style, 
    row_height, 
    long_text_width, 
    short_text_max_width
):
    """
    Write the DataFrame to a temporary Excel file using XlsxWriter.
    Creates an Excel table over the data, sets a fixed row height,
    enables text wrapping, and adjusts column widths.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to export.
        sheet_name (str): The Excel sheet name.
        table_style (str): The Excel table style.
        long_text_width (int): Column width for columns with long text.
        short_text_max_width (int): Maximum width for columns with shorter text.
        row_height (int): Fixed row height for each row.
    
    Returns:
        The path to the temporary Excel file.
    """
```

#### copy_to_dbfs

Copies the Excel file from the temporary file location to the DBFS destination, and removes the local temporary file.

```python
def copy_to_dbfs(
    temp_file_path, 
    output_filename
):
    """
    Copies the Excel file from the temporary file location to the DBFS destination,
    and removes the local temporary file.
    
    Parameters:
        temp_file_path (str): Temporary path to save Excel file.
        output_filename (str): The name of the output file.
    """
```

#### display_download_link

Displays an HTML download link for the Excel file.

```python
def display_download_link(
    displayHTML, 
    output_filename
):
    """
    Displays an HTML download link for the Excel file.
    
    Parameters:
        displayHTML (function): Function to display HTML content in the notebook.
        output_filename (str): The name of the output file.
    """
```

#### export_df_to_excel

Exports a Pandas DataFrame to an Excel file in DBFS as an Excel table with text wrapping enabled for all cells. It replaces null or "N/A" values with empty strings, drops specified columns, and creates a downloadable link.

```python
def export_df_to_excel(
    df,
    output_filename,
    displayHTML,
    sheet_name="Sheet1",
    table_style="TableStyleMedium2",
    long_text_width=50,
    short_text_max_width=30,
    row_height=60,
    columns_to_drop=["fullXML"],
    download_link=True
):
    """
    Exports a Pandas DataFrame to an Excel file in DBFS as an Excel table with text wrapping enabled
    for all cells. It replaces null or "N/A" values with empty strings, drops specified columns, and
    creates a downloadable link.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to export.
        output_filename (str): The name of the Excel file.
        sheet_name (str): The Excel sheet name.
        table_style (str): The Excel table style.
        long_text_width (int): Column width for columns with long text.
        short_text_max_width (int): Maximum width for columns with shorter text.
        row_height (int): Fixed row height for each row.
        columns_to_drop (list): List of column names to drop from the DataFrame.
        download_link (bool): If True, displays an HTML download link.
    """
```

#### fetch_pmc_ids

Fetches PubMed Central (PMC) IDs based on a given search query using the Entrez API.

```python
def fetch_pmc_ids(
    query, 
    retmax=2000, 
    api_key=None, 
    timeout=20
):
    """
    Fetches PubMed Central (PMC) IDs based on a given search query using the Entrez API.

    Parameters:
        query (str): The search query string to execute against the PMC database.
        retmax (int, optional): The maximum number of PMC IDs to retrieve. Default is 2000.
        api_key (str, optional): An optional API key for higher rate limits with the NCBI API.
        timeout (int, optional): Maximum number of seconds to wait for a response from the API. Default is 20.

    Returns:
        list of str: A list containing PMC IDs matching the query.

    Raises:
        requests.exceptions.RequestException: If an HTTP request-related error occurs.
        xml.etree.ElementTree.ParseError: If an error occurs while parsing the XML response from the API.

    Logs:
        Informational logs on request status and number of PMC IDs fetched.
        Error logs detailing exceptions if they occur.
    """
```

#### fetch_pubmed_fulltext_and_metadata

Fetches metadata, full text/XML content, and article sections (excluding abstracts) for a list of PMCIDs from PubMed.

```python
def fetch_pubmed_fulltext_and_metadata(
    pmcids, 
    api_key
):
    """
    Fetches metadata, full text/XML content, and article sections (excluding abstracts)
    for a list of PMCIDs from PubMed.

    Parameters:
        pmcids (list): List of PMCIDs as strings.
        api_key (str): PubMed API key for increased request rate limits.

    Returns:
        pd.DataFrame: DataFrame containing metadata, full text, XML, non-abstract sections,
                      and log messages for each PMCID, with 'pmid' and 'pmcid' as the first two columns.
    """
```

#### run_pubmed_search

Runs the full pipeline to fetch PubMed data based on search terms, write the data to a Delta table in Databricks under the specified name, and read it back.

```python
def run_pubmed_search(
    mesh_term, 
    rwd_terms, 
    date_term, 
    pm_key, 
    saved_file_name, 
    return_max=2000,
    spark=None
):
    """
    Runs the full pipeline to fetch PubMed data based on search terms,
    write the data to a Delta table in Databricks under the specified name,
    and read it back.

    Parameters:
        mesh_term (str): The MeSH term for the search (e.g., "Ischemic Attack, Transient").
        rwd_terms (str): Additional search terms.
        date_term (str): Date constraints for the search.
        pm_key (str): API key for PubMed API access.
        saved_file_name (str): The name of the Delta table to write to and read from.
        return_max (int): Maximum number of results to fetch.
        spark (SparkSession, optional): Spark session to use for writing and reading Delta tables.

    Returns:
        pd.DataFrame or Spark DataFrame: The DataFrame read back from the Delta table.
    """
```