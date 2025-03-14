from pyspark.sql import SparkSession

def createSparkSession() -> SparkSession:
    """
    Creates and returns a Spark session.

    Returns:
        SparkSession: A Spark session object.
    """

    if spark is None:
     spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    return spark

def pubmedSearchTerms():
    """
    Creates a string query to pass to PubMed APIs.
    The string combines 1) study design terms, 2) data sources & methods terms, 3) phenotyping terms and 4) database terms.

    Returns:
        A character string with all terms combined
    """
    # Study Design Terms - is intentionally sensitive
    study_design_terms = [
        '("Observational Study"[Publication Type])',
        '("Retrospective Studies"[Mesh])',
        "(observational[tiab])",
        "(retrospective[tiab])",
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

    # Data Sources & Methods Terms -- adds specificity - anywhere
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
        "(EMR)",
        "(EHR)",
        '("insurance claims")',
        '("administrative claims data")',
        '("health care data")',
        '("healthcare data")',
    ]

    # Phenotyping & Cohort Identification Terms - anywhere
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

    # Specific Databases (Global) - anywhere
    specific_databases = [
        "(SEER)",
        "(NHANES)",
        "(Medicare)",
        "(Medicaid)",
        '("VA data")',
        '("Veterans Affairs")',
        "(Sentinel)",
        "(HCUP)",
        "(NSQIP)",
        "(Denmark/epidemiology[Mesh])",
        "(National Health Insurance Research[Mesh])",
        '("General Practice Research Database")',
        '("Clinical Practice Research Datalink")',
        '("The Health Improvement Network")',
        '("Taiwan National Health Insurance Research Database")',
        '("Health Insurance Review and Assessment Service")',
        "(BIFAP)",
        "(SIDIAP)",
        "(QResearch)",
        "(Truven)",
        "(Merativ)",
        "(Optum)",
        "(Medstat)",
        "(Nationwide Inpatient Sample)",
        "(PharMetrics)",
        "(PHARMO)",
        "(IMS)",
        "(IQVIA)",
        "(Premier database)",
    ]

    # Exclusion Terms (restricted type)
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

    # Combine included terms
    included_query = (
        "("
        + " OR ".join(study_design_terms)
        + ") AND ("
        + " OR ".join(data_sources_methods)
        + ") AND ("
        + " OR ".join(specific_databases)
        + ")"
    )

    # Combine revised exclusion terms
    exclusion_query = "(" + " OR ".join(exclusion_terms) + ")"

    # Final structured PubMed query
    rwd_terms = f"{included_query} NOT {exclusion_query}"
    return rwd_terms


def clean_dataframe(df, columns_to_drop):
    """
    Replace null values and cells with "N/A" with empty strings,
    and drop specified columns if they exist in the DataFrame.
    """
    # No external modules needed for cleaning
    df = df.fillna('').replace("N/A", "", regex=False)
    for col in columns_to_drop:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df
  

def write_dataframe_to_excel(df, sheet_name, table_style, row_height, long_text_width, short_text_max_width):
    """
    Write the DataFrame to a temporary Excel file using XlsxWriter.
    Creates an Excel table over the data, sets a fixed row height,
    enables text wrapping, and adjusts column widths.
    
    Returns:
      The path to the temporary Excel file.
    """
    # Encapsulate required imports
    import tempfile
    import pandas as pd
    
    # Create a temporary file to write the Excel file
    temp_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    temp_file_path = temp_file.name
    temp_file.close()
    
    with pd.ExcelWriter(temp_file_path, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        workbook  = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Determine dimensions: number of rows and columns
        nrows, ncols = df.shape
        
        # Add an Excel table over the data range with the specified style.
        worksheet.add_table(0, 0, nrows, ncols - 1, {
            'columns': [{'header': col} for col in df.columns],
            'style': table_style
        })
        
        # Set a fixed row height for all rows to support multi-line text.
        worksheet.set_default_row(row_height)
        
        # Create a format with text wrapping enabled.
        wrap_format = workbook.add_format({'text_wrap': True})
        
        # Adjust column widths and apply the wrap format to every column.
        for col_num, col_name in enumerate(df.columns):
            if df[col_name].dtype == 'object':
                if not df.empty:
                    max_len = df[col_name].astype(str).map(len).max()
                else:
                    max_len = len(col_name)
                # Use a wider width for very long text; otherwise, a shorter width.
                if max_len > 200:
                    width = long_text_width
                else:
                    width = min(max(max_len + 2, len(col_name) + 2), short_text_max_width)
            else:
                width = len(col_name) + 2
            worksheet.set_column(col_num, col_num, width, wrap_format)
    
    return temp_file_path
  
def copy_to_dbfs(temp_file_path, output_filename):
    """
    Copies the Excel file from the temporary file location to the DBFS destination,
    and removes the local temporary file.
    """
    # Import os locally
    import os
    
    dbfs_destination = f'/FileStore/{output_filename}'
    # Copy the temporary file to DBFS using Databricks dbutils (assumed to be available)
    dbutils.fs.cp("file:" + temp_file_path, "dbfs:" + dbfs_destination)
    os.remove(temp_file_path)


def display_download_link(output_filename):
    """
    Displays an HTML download link for the Excel file.
    """
    # displayHTML is assumed to be available in the environment (e.g. Databricks)
    displayHTML(f'<a href="/files/{output_filename}" download>Download Excel File</a>')
    
    
def export_df_to_excel(
    df,
    output_filename="odysseus.pubmed.ds0084_literature_search.xlsx",
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
    # Clean the DataFrame
    df = clean_dataframe(df, columns_to_drop)
    
    # Write the DataFrame to a temporary Excel file.
    temp_file_path = write_dataframe_to_excel(
        df, sheet_name, table_style, row_height, long_text_width, short_text_max_width
    )
    
    # Copy the temporary Excel file to DBFS and remove the local temporary file.
    copy_to_dbfs(temp_file_path, output_filename)
    
    # Display an HTML download link if required.
    if download_link:
        display_download_link(output_filename)


def fetch_pmc_ids(query, retmax=2000, api_key=None, timeout=20):
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

    Example:
        pmc_ids = fetch_pmc_ids("machine learning", retmax=50)
        print(pmc_ids)
    """
    import requests
    import logging
    import xml.etree.ElementTree as ET

    logger = logging.getLogger("fetch_pmc_ids")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    params = {
        "db": "pmc",
        "term": query,
        "retmax": retmax,
        "usehistory": "y",
        "retmode": "xml",
    }

    if api_key:
        params["api_key"] = api_key

    try:
        logger.info(
            f"Sending POST request to PubMed API with query length: {len(query)}"
        )
        response = requests.post(base_url, data=params, timeout=timeout)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        pmcids = [id_elem.text for id_elem in root.findall(".//Id")]

        logger.info(f"Fetched {len(pmcids)} PMC IDs successfully.")
        return pmcids

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error: {req_err}")
        raise

    except ET.ParseError as parse_err:
        logger.error(f"XML parsing error: {parse_err}")
        raise
    
    
def fetch_pubmed_fulltext_and_metadata(pmcids, api_key):
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
    import pandas as pd
    import logging
    import requests
    from requests.exceptions import HTTPError, ConnectionError, Timeout
    from xml.etree import ElementTree as ET
    import time

    # Configure logging inside the function
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger('pubmed_metadata_fetcher')

    base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
    articles_data = []

    for pmcid in pmcids:
        # Start a fresh list to collect log messages for this PMCID.
        log_messages = []
        
        # Initialize article_data with default values including pmcid
        article_data = {
            'title': 'N/A',
            'pmid': 'N/A',
            'pmcid': pmcid,
            'firstAuthor': 'N/A',
            'lastAuthor': 'N/A',
            'authorAffiliations': 'N/A',
            'abstract': 'N/A',
            'fullText': 'N/A',
            'meshTags': 'N/A',
            'keyWords': 'N/A',
            'journal': 'N/A',
            'yearOfPublication': 'N/A',
            'publicationDate': 'N/A',
            'doi': 'N/A',
            'fullXML': 'N/A',
            'introduction': 'N/A',
            'methods': 'N/A',
            'results': 'N/A',
            'discussion': 'N/A',
            'log': ''
        }
        max_retries = 5
        success_flag = False

        for attempt in range(max_retries):
            try:
                msg = f"Attempt {attempt+1} for PMCID: {pmcid}"
                logger.info(msg)
                log_messages.append(msg)

                params = {
                    'db': 'pmc',
                    'id': pmcid,
                    'retmode': 'xml',
                    'api_key': api_key
                }
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()

                full_xml = response.text
                article_data['fullXML'] = full_xml
                root = ET.fromstring(response.content)
                article = root.find('.//article')
                if article is None:
                    raise ValueError(f'XML parsing issue: No article found for PMCID {pmcid}')

                # Extract basic metadata
                title = article.findtext('.//article-title', default='N/A')
                abstract = ' '.join(article.findtext('.//abstract//p', default='N/A').split())
                journal = article.findtext('.//journal-title', default='N/A')
                year = article.findtext('.//pub-date/year', default='N/A')
                publication_date = '-'.join(filter(None, [
                    article.findtext('.//pub-date/year'),
                    article.findtext('.//pub-date/month'),
                    article.findtext('.//pub-date/day')
                ])) or 'N/A'
                doi = article.findtext('.//article-id[@pub-id-type="doi"]', default='N/A')

                # Extract author details
                authors = article.findall('.//contrib-group/contrib[@contrib-type="author"]')
                first_author = 'N/A'
                last_author = 'N/A'
                affiliations = []
                if authors:
                    first_elem = authors[0].find('name')
                    last_elem = authors[-1].find('name')
                    if first_elem is not None:
                        first_author = ' '.join([
                            first_elem.findtext('given-names', ''), 
                            first_elem.findtext('surname', '')
                        ]).strip()
                    if last_elem is not None:
                        last_author = ' '.join([
                            last_elem.findtext('given-names', ''), 
                            last_elem.findtext('surname', '')
                        ]).strip()
                    for author in authors:
                        aff = author.find('.//aff')
                        if aff is not None:
                            affiliations.append(' '.join(aff.itertext()).strip())
                author_affiliations = '; '.join(affiliations) if affiliations else 'N/A'

                # Mesh terms and keywords
                mesh_terms = [mesh.text for mesh in article.findall('.//mesh-heading/descriptor-name')]
                mesh_tags = ', '.join(mesh_terms) if mesh_terms else 'N/A'
                keywords_list = article.findall('.//kwd-group/kwd')
                keywords = ', '.join([kw.text for kw in keywords_list if kw.text]) or 'N/A'
                pmid = article.findtext('.//article-id[@pub-id-type="pmid"]', default='N/A')

                # Process full text and sections
                body = article.find('.//body')
                full_text = ''
                sections = {'introduction': '', 'methods': '', 'results': '', 'discussion': ''}
                if body is not None:
                    for sec in body.findall('.//sec'):
                        sec_title = sec.findtext('title', '').lower()
                        paragraphs = sec.findall('.//p')
                        sec_text = ' '.join([' '.join(p.itertext()).strip() for p in paragraphs if p.text])
                        full_text += sec_text + '\n\n'
                        if 'introduction' in sec_title or 'background' in sec_title:
                            sections['introduction'] += sec_text + '\n\n'
                        elif any(keyword in sec_title for keyword in ['method', 'materials', 'design', 'cohort', 'protocol', 'study variables']):
                            sections['methods'] += sec_text + '\n\n'
                        elif 'result' in sec_title or 'finding' in sec_title:
                            sections['results'] += sec_text + '\n\n'
                        elif 'discussion' in sec_title or 'conclusion' in sec_title or 'summary' in sec_title:
                            sections['discussion'] += sec_text + '\n\n'

                article_data.update({
                    'title': title,
                    'pmid': pmid,
                    'firstAuthor': first_author,
                    'lastAuthor': last_author,
                    'authorAffiliations': author_affiliations,
                    'abstract': abstract,
                    'fullText': full_text.strip(),
                    'meshTags': mesh_tags,
                    'keyWords': keywords,
                    'journal': journal,
                    'yearOfPublication': year,
                    'publicationDate': publication_date,
                    'doi': doi,
                    'introduction': sections['introduction'].strip(),
                    'methods': sections['methods'].strip(),
                    'results': sections['results'].strip(),
                    'discussion': sections['discussion'].strip(),
                })
                
                success_msg = f"Successfully processed PMCID: {pmcid} on attempt {attempt+1}"
                logger.info(success_msg)
                log_messages.append(success_msg)
                article_data['log'] = "\n".join(log_messages)
                success_flag = True
                break  # Exit retry loop on success

            except (HTTPError, ConnectionError, Timeout) as e:
                error_msg = f"Attempt {attempt+1} failed for PMCID {pmcid}: {e}"
                logger.error(error_msg)
                log_messages.append(error_msg)
                if attempt == max_retries - 1:
                    article_data['log'] = "\n".join(log_messages) + f"\nFailed after {max_retries} attempts: {e}"
                else:
                    time.sleep(0.7)  # Slightly longer delay between retries
            except Exception as e:
                unexpected_msg = f"Unexpected error for PMCID {pmcid}: {e}"
                logger.error(unexpected_msg)
                log_messages.append(unexpected_msg)
                article_data['log'] = "\n".join(log_messages)
                break

        articles_data.append(article_data)
        # Respect the rate limit even after retries
        time.sleep(0.35)

    df = pd.DataFrame(articles_data)
    
    # 1) If the DataFrame is empty, just return it
    if df.empty:
        return df

    # 2) If 'pmid' or 'pmcid' are missing, also return the DataFrame as-is
    required_cols = ['pmid', 'pmcid']
    if not set(required_cols).issubset(df.columns):
        return df

    # Reorder columns: 'pmid' and 'pmcid' first
    all_cols = df.columns.tolist()
    ordered_cols = ['pmid', 'pmcid'] + [col for col in all_cols if col not in ('pmid', 'pmcid')]
    return df[ordered_cols]
  

def run_pubmed_search(
    mesh_term, 
    rwd_terms, 
    date_term, 
    pm_key, 
    saved_file_name="searchOutputDf_table", 
    return_max=2000
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

    Returns:
        pd.DataFrame or Spark DataFrame: The DataFrame read back from the Delta table.
    """
    import re
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Set spark session
    spark = createSparkSession()

    # Construct and clean the full search query.
    full_search = f'"{mesh_term}"[Mesh] AND {rwd_terms} AND {date_term}'
    full_search = re.sub(r"\s+", " ", full_search.strip())
    print(f"Constructed full search query: {full_search}")

    # Fetch PMCIDs based on the full search string.
    pmcids = fetch_pmc_ids(query=full_search, retmax=return_max, api_key=pm_key)
    print(f"Fetched {len(pmcids)} PMCIDs.")

    # Retrieve full text and metadata from PubMed as a pandas DataFrame.
    searchOutputDf = fetch_pubmed_fulltext_and_metadata(pmcids=pmcids, api_key=pm_key)
    print(f"Fetched metadata for {len(searchOutputDf)} articles.")

    # Create a run-level log message.
    run_info = (
        f"Run Info: Search Query: {full_search}; "
        f"PMCIDs Fetched: {len(pmcids)}; "
        f"Articles Retrieved: {len(searchOutputDf)}."
    )

    # If the DataFrame is not empty, prepend run_info to each article's log and add a new column.
    if not searchOutputDf.empty:
        # Prepend run-level info to the existing 'log' column.
        searchOutputDf['log'] = run_info + "\n" + searchOutputDf['log'].astype(str)
        # Also add a separate column for the run-level log if desired.
        searchOutputDf['run_log'] = run_info

    # Enhance the DataFrame with indicator columns if it is not empty.
    if not searchOutputDf.empty:
        # Define a regex pattern for cohort definition related terms.
        cohort_regex = re.compile(
            r'(?i)\b(?:'
            r'age(?:\s*(?:at|group|restriction))?|'
            r'gender|'
            r'demographic(?:s)?|'
            r'male|'
            r'female|'
            r'race|'
            r'ethnicity|'
            r'calendar\s*year|'
            r'year(?:s)?|'
            r'index\s*date|'
            r'baseline|'
            r'washout|'
            r'lookback|'
            r'follow[-\s]?up|'
            r'inclusion|'
            r'exclusion|'
            r'eligibility|'
            r'ICD(?:-?10|(?:-?9))?|'
            r'CPT|'
            r'HCPCS|'
            r'SNOMED|'
            r'LOINC|'
            r'RxNorm|'
            r'NDC|'
            r'visit|'
            r'inpatient|'
            r'outpatient|'
            r'emergency'
            r')\b'
        )

        # Define a regex for common medical codes.
        medical_regex = re.compile(r'''
            (?xi)                       # Case-insensitive, free-spacing mode
            (?:
              [A-TV-Z][0-9A-Z]{2}(?:\.[0-9A-Z]{0,4})? |  # ICD10 / ICD10CM / OMOP RxExt-like codes
              \d{3}(?:\.\d{1,2})? |                     # ICD9 / ICD9CM codes
              (?:\d{10,11}|\d{4,5}-\d{3,4}-\d{1,2}) |    # NDC codes (plain or dashed)
              \d{5} |                                   # CPT codes (5 digits)
              [A-Z]\d{4} |                              # HCPCS codes (1 letter, 4 digits)
              [A-Za-z0-9]{1,6}(?:\.[A-Za-z0-9]{1,2})? |  # Read codes
              OMOP\d{3,15} |                            # OMOP RxExt codes
              (?<![.\d])\d{6,15}(?![.\d])                # “Raw” numeric codes (6-15 digits)
            )
        ''', re.VERBOSE)

        def has_cohort_definition(text):
            """
            Returns 1 if the text contains any cohort definition related terms, else 0.
            """
            if pd.isna(text) or text == "N/A":
                return 0
            return 1 if cohort_regex.search(str(text)) else 0

        def has_medical_code(text):
            """
            Returns 1 if the text contains any common medical code pattern, else 0.
            """
            if pd.isna(text) or text == "N/A":
                return 0
            return 1 if medical_regex.search(str(text)) else 0

        # Confirm DataFrame columns exist.
        print("DataFrame columns:", searchOutputDf.columns.tolist())

        # Add indicator columns for each section.
        for section in ['introduction', 'methods', 'results', 'discussion']:
            # Ensure the section is a string.
            searchOutputDf[section] = searchOutputDf[section].astype(str)
            # Indicator for cohort definition terms.
            cohort_col = f"{section}HasCohortDefinition"
            searchOutputDf[cohort_col] = searchOutputDf[section].apply(has_cohort_definition)
            # Indicator for medical code patterns.
            med_code_col = f"{section}HasMedicalCode"
            searchOutputDf[med_code_col] = searchOutputDf[section].apply(has_medical_code)

    # Convert to Spark DataFrame (or create an empty one with the correct schema).
    if searchOutputDf.empty:
        empty_schema = StructType([
            StructField("pmid", StringType(), True),
            StructField("pmcid", StringType(), True),
            StructField("title", StringType(), True),
            StructField("firstAuthor", StringType(), True),
            StructField("lastAuthor", StringType(), True),
            StructField("authorAffiliations", StringType(), True),
            StructField("abstract", StringType(), True),
            StructField("fullText", StringType(), True),
            StructField("meshTags", StringType(), True),
            StructField("keyWords", StringType(), True),
            StructField("journal", StringType(), True),
            StructField("yearOfPublication", StringType(), True),
            StructField("publicationDate", StringType(), True),
            StructField("doi", StringType(), True),
            StructField("fullXML", StringType(), True),
            StructField("introduction", StringType(), True),
            StructField("methods", StringType(), True),
            StructField("results", StringType(), True),
            StructField("discussion", StringType(), True),
            StructField("log", StringType(), True),
            StructField("run_log", StringType(), True)
        ])
        spark_df = spark.createDataFrame([], empty_schema)
    else:
        spark_df = spark.createDataFrame(searchOutputDf)

    # Write the Spark DataFrame to the Delta table with schema merging enabled.
    spark_df.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(saved_file_name)

    # Read back the Delta table into a Spark DataFrame and convert it to a pandas DataFrame.
    result_df = spark.sql("SELECT * FROM {}".format(saved_file_name))
    return result_df.toPandas()
  
  
