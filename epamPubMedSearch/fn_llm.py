#####################################################
#   Asynchronous Data Processing with LLMs Example  #
#                                                   #
# This code processes a DataFrame of data rows using #
# multiple Large Language Model (LLM) APIs concurrently.
# The code is organized into three sections:
#                                                   #
#   1. Logging and Data Management: Sets up logging  #
#      and manages reading/writing data from/to Spark#
#      Delta tables.                                #
#                                                   #
#   2. LLM Invocation and Helper Functions: Contains #
#      functions to call LLM APIs asynchronously,     #
#      parse their responses, and prepare prompts.   #
#                                                   #
#   3. Row and DataFrame Processing: Processes each   #
#      row concurrently by invoking the LLMs, groups   #
#      results, and writes the aggregated output back  #
#      to a Delta table immediately upon processing a #
#      PMCID, using a lock to prevent concurrency issues.
#####################################################

###############################################
# SECTION 1: Logging and Data Management
###############################################

from langchain_openai import AzureChatOpenAI

def initialize_logging():
    """
    Sets up logging configuration.
    
    This function configures the logger to:
      - Show messages with level INFO (or ERROR).
      - Include a custom filter to only allow messages that have
        a "[STATUS]" marker or include "PMCID" (for row-specific messages).
      - Suppress extra logs from libraries like Py4J and Spark.
    
    Imports are encapsulated inside the function.
    """
    import logging
    # Define a custom filter that passes only key messages.
    class ProgressAndErrorFilter(logging.Filter):
        def filter(self, record):
            # Always allow error messages.
            if record.levelno >= logging.ERROR:
                return True
            # Allow INFO messages only if they contain a specific marker.
            if record.levelno == logging.INFO and ("[STATUS]" in record.getMessage() or "PMCID" in record.getMessage()):
                return True
            return False

    # Configure the basic logging format.
    logging.basicConfig(
        level=logging.INFO,  # Set minimum log level to INFO.
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,  # Force reconfiguration of the logging system.
    )
    logger = logging.getLogger()
    logger.addFilter(ProgressAndErrorFilter())
    # Reduce verbosity for external libraries.
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark").setLevel(logging.ERROR)


def load_existing_delta_data(table_name, spark=None):
    """
    Loads an existing Delta table into a Pandas DataFrame.
    
    Parameters:
      table_name (str): The name of the Delta table.
      spark: A SparkSession instance.
    
    Returns:
      A Pandas DataFrame with the existing data or an empty DataFrame if the table does not exist.
    
    All required imports are inside this function.
    """
    import pandas as pd
    import logging
    
    try:
        # Check if the table exists in Spark's catalog.
        if spark.catalog.tableExists(table_name):
            existing_df = spark.table(table_name).toPandas()
            logging.debug("Existing table found. Loaded current data.")
        else:
            existing_df = pd.DataFrame()  # Return empty DataFrame if table is missing.
            logging.debug("No existing table found. Starting fresh.")
    except Exception as e:
        existing_df = pd.DataFrame()
        logging.error(f"Error loading existing data: {e}")
    return existing_df


def write_results_to_delta_table(merged_df, table_name, spark=None):
    """
    Writes a merged Pandas DataFrame to a Spark Delta table.
    
    Parameters:
      merged_df (DataFrame): The DataFrame to write.
      table_name (str): Name of the Delta table.
      spark: A SparkSession instance.
    
    Returns:
      The written table as a Pandas DataFrame (after saving).
    
    Imports are encapsulated inside the function.
    """
    import logging
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    try:
        
        # Create schema for the Delta table
        schema = StructType([
            StructField("pmcid", StringType(), True),
            StructField("llm", StringType(), True),
            StructField("output_type", StringType(), True),
            StructField("verbatim_output", StringType(), True),
            StructField("interpretation", StringType(), True),
            StructField("original_llm_output", StringType(), True),
            StructField("input_prompt", StringType(), True),
            StructField("llm_raw_llm_output", StringType(), True),
            StructField("error_log", StringType(), True)
        ])

        # Create spark DataFrame based on shcema
        df = spark.createDataFrame(merged_df, schema)

        # Create a Spark DataFrame from the Pandas DataFrame and write it as a Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        # Reload the table as a Pandas DataFrame
        merged_df = spark.sql(f"SELECT * FROM {table_name}").toPandas()

        logging.debug(f"Results written to Spark table: {table_name}")
    except Exception as e:
        logging.error(f"Error writing to Delta table: {e}")
    return merged_df


def get_processed_pmids(existing_df):
    """
    Extracts PMCID values from an existing DataFrame.
    
    Parameters:
      existing_df (DataFrame): DataFrame with previous results.
    
    Returns:
      A set of unique PMCIDs that have already been processed.
    
    No additional imports needed here.
    """
    if not existing_df.empty and "pmcid" in existing_df.columns:
        return set(existing_df["pmcid"].unique())
    return set()

###############################################
# SECTION 2: LLM Invocation and Helper Functions
###############################################

async def async_invoke_llm(llm_instance, prompt, pmcid, llm_name, logger, attempts=5, sleep_time=1):
    """
    Asynchronously calls an LLM instance with a given prompt and retries on error.
    
    Parameters:
      llm_instance: An object with a method `invoke` to call the LLM API.
      prompt (str): The text prompt sent to the LLM.
      pmcid: The identifier of the current data row.
      llm_name (str): The name of the LLM (for logging).
      logger: Logger instance for recording progress.
      attempts (int): Number of retry attempts.
      sleep_time (int): Seconds to sleep between attempts.
    
    Returns:
      A tuple of (raw_llm_output, error_log). If successful, error_log is None.
    
    Imports (time and asyncio) are done inside the function.
    """
    import time
    import asyncio

    for attempt in range(1, attempts + 1):
        logger.debug(f"PMCID {pmcid}: Attempt {attempt} for LLM '{llm_name}'.")
        try:
            start = time.time()
            # Asynchronously call the synchronous 'invoke' method using asyncio.to_thread.
            output_obj = await asyncio.to_thread(llm_instance.invoke, prompt)
            # Check if the response object has a 'content' attribute.
            raw_llm_output = output_obj.content if hasattr(output_obj, "content") else str(output_obj)
            elapsed = time.time() - start
            logger.debug(f"PMCID {pmcid}: LLM '{llm_name}' succeeded in {elapsed:.2f} sec on attempt {attempt}.")
            return raw_llm_output, None
        except Exception as e:
            error_msg = str(e)
            logger.error(f"PMCID {pmcid}: LLM '{llm_name}' error on attempt {attempt}: {error_msg}")
            await asyncio.sleep(sleep_time)
    return None, error_msg


def create_prompt(row, input_prompt, text_col):
    """
    Builds the prompt to be sent to an LLM by combining the base prompt with row-specific text.
    
    Parameters:
      row (dict-like): A row from the DataFrame containing at least a 'pmcid' key.
      input_prompt (str): The base prompt instructions.
      text_col (str): The name of the column containing additional text.
    
    Returns:
      A tuple (pmcid, prompt) where prompt includes the input prompt and the row's text.
    """
    pmcid = row["pmcid"]
    text = row.get(text_col, "")
    prompt = f"{input_prompt}\n\n{text}"
    return pmcid, prompt


async def call_llm(llm_instance, prompt, pmcid, llm_name, logger, attempts=5, sleep_time=1):
    """
    Helper function to asynchronously call an LLM.
    
    Simply wraps the async_invoke_llm function.
    
    Returns:
      The raw output from the LLM and any error log.
    """
    return await async_invoke_llm(llm_instance, prompt, pmcid, llm_name, logger, attempts, sleep_time)


def parse_llm_response(raw_llm_output, pmcid, llm_name, prompt, error_log, regex):
    results = []
    if raw_llm_output and not error_log:
        for line in raw_llm_output.splitlines():
            line = line.strip()
            if not line:
                continue  # Skip empty lines
            if "|" not in line:
                continue  # Skip lines that don't contain the delimiter
            
            # Expecting lines of the form:
            # | category | verbatim | interpretation |
            parts = line.split("|")
            # A proper markdown row should yield 5 parts:
            # parts[0] and parts[4] are empty (due to leading and trailing "|")
            # parts[1] = category, parts[2] = verbatim, parts[3] = interpretation.
            if len(parts) == 5:
                # Skip header row or divider row if present.
                header_keywords = ["category", "verbatim", "interpretation"]
                if any(keyword in parts[1].lower() for keyword in header_keywords):
                    continue
                # Also skip divider rows (e.g., a row containing only dashes)
                if set(parts[2].strip()) == {"-"} or set(parts[3].strip()) == {"-"}:
                    continue

                category = parts[1].strip()
                verbatim_output = parts[2].strip()
                interpretation = parts[3].strip()
                # Optionally, clean the category using your provided regex.
                cleaned_output_type = regex.sub("", category)
                
                results.append({
                    "pmcid": pmcid,
                    "llm": llm_name,
                    "output_type": cleaned_output_type,  # category name
                    "verbatim_output": verbatim_output,
                    "interpretation": interpretation,
                    "original_llm_output": raw_llm_output,
                    "input_prompt": prompt,
                    "llm_raw_llm_output": raw_llm_output,
                    "error_log": error_log,
                })
        # Fallback if no valid rows were parsed.
        if not results:
            results.append({
                "pmcid": pmcid,
                "llm": llm_name,
                "output_type": None,
                "verbatim_output": raw_llm_output.strip(),
                "interpretation": "",
                "original_llm_output": raw_llm_output,
                "input_prompt": prompt,
                "llm_raw_llm_output": raw_llm_output,
                "error_log": error_log,
            })
    else:
        results.append({
            "pmcid": pmcid,
            "llm": llm_name,
            "output_type": None,
            "verbatim_output": raw_llm_output.strip() if raw_llm_output else None,
            "interpretation": "",
            "original_llm_output": raw_llm_output,
            "input_prompt": prompt,
            "llm_raw_llm_output": raw_llm_output,
            "error_log": error_log,
        })
    return results


async def process_llm_for_pmcid(row, llm_name, llm_instance, input_prompt, text_col, logger, regex):
    """
    Processes a single row of data for one specific LLM.
    
    This function:
      - Builds the prompt for the row.
      - Calls the LLM asynchronously.
      - Parses the LLM response into a structured format.
    
    Parameters:
      row: A data row from the DataFrame.
      llm_name (str): The identifier for the LLM.
      llm_instance: The LLM object to call.
      input_prompt (str): The base prompt.
      text_col (str): Column name containing additional text.
      logger: Logger for debugging and info messages.
      regex: A regular expression to clean up output types.
    
    Returns:
      A list of result dictionaries with parsed LLM output.
    """
    pmcid, prompt = create_prompt(row, input_prompt, text_col)
    logger.info(f"Starting processing for PMCID: {pmcid} with LLM: {llm_name}")
    raw_llm_output, error_log = await call_llm(llm_instance, prompt, pmcid, llm_name, logger)
    results = parse_llm_response(raw_llm_output, pmcid, llm_name, prompt, error_log, regex)
    logger.info(f"Finished processing for PMCID: {pmcid} with LLM: {llm_name}")
    return results


async def process_pmcid_row_async(row, llm_dict, input_prompt, text_col, logger, regex):
    """
    Processes a single row asynchronously for all LLMs provided.
    
    The function:
      - Invokes each LLM concurrently for the given PMCID.
      - Groups and aggregates the responses.
    
    Parameters:
      row: A data row from the DataFrame.
      llm_dict (dict): Mapping of LLM names to their instances.
      input_prompt (str): Base prompt for processing.
      text_col (str): Column name for additional text.
      logger: Logger instance.
      regex: Compiled regex to clean output types.
    
    Returns:
      Aggregated results as a list of dictionaries.
    """
    import asyncio
    pmcid = row["pmcid"]
    # Step 1: Process each LLM concurrently.
    tasks = [
        process_llm_for_pmcid(row, llm_name, llm_instance, input_prompt, text_col, logger, regex)
        for llm_name, llm_instance in llm_dict.items()
    ]
    llm_results_lists = await asyncio.gather(*tasks)
    row_results = [item for sublist in llm_results_lists for item in sublist]

    # Step 2: Group results by (pmcid, llm, output_type) and collect fields separately.
    groups = {}
    for res in row_results:
        key = (res["pmcid"], res["llm"], res["output_type"])
        if key not in groups:
            groups[key] = {
                "verbatim_outputs": set(),      # For raw text returned from the LLM.
                "interpretations": set(),         # For the interpretation field from the LLM.
                "original_llm_outputs": set(),    # For the complete raw output.
                "input_prompt": res["input_prompt"],
            }
        groups[key]["verbatim_outputs"].add(res["verbatim_output"])
        groups[key]["interpretations"].add(res["interpretation"])
        groups[key]["original_llm_outputs"].add(res["original_llm_output"])

    # Step 3: For each group, aggregate separately for 'verbatim_output' and 'interpretation'.
    aggregated_results = []
    for (pmcid_val, llm_name, output_type), group_data in groups.items():
        # Filter out empty or placeholder strings.
        unique_verbatim = [v for v in group_data["verbatim_outputs"] if v and v != "None"]
        unique_interpretations = [v for v in group_data["interpretations"] if v and v != "None"]

        # Create aggregated strings.
        aggregated_verbatim = " | ".join(sorted(unique_verbatim)) if unique_verbatim else ""
        aggregated_interpretation = " | ".join(sorted(unique_interpretations)) if unique_interpretations else ""

        aggregated_results.append({
            "pmcid": pmcid_val,
            "llm": llm_name,
            "output_type": output_type,
            "verbatim_output": aggregated_verbatim,
            "interpretation": aggregated_interpretation,
            "original_llm_output": " | ".join(sorted(group_data["original_llm_outputs"])),
            "input_prompt": group_data["input_prompt"],
            "llm_raw_llm_output": aggregated_interpretation,  # Adjust field as needed.
            "error_log": None,
        })
    logger.info(f"Completed processing for PMCID: {pmcid}")
    return aggregated_results

###############################################
# SECTION 3: Row and DataFrame Processing
###############################################

async def process_df_with_llms_async(df, llm_dict, input_prompt, text_col, logger, saved_table_name, spark=None):
    """
    Processes an entire DataFrame asynchronously using the provided LLMs.
    
    This version updates the Delta table after each PMCID is processed by:
      - Loading the current Delta table.
      - Unioning the new PMCID results.
      - Writing the merged results back to the Delta table immediately.
      
    Concurrency in writing is controlled using an asynchronous lock so that
    only one PMCID result is written at a time.
    
    Parameters:
      df: Input DataFrame containing at least a 'pmcid' column.
      llm_dict (dict): Dictionary mapping LLM names to LLM instances.
      input_prompt (str): Base prompt for all rows.
      text_col (str): Column name containing additional text.
      saved_table_name (str): Name of the Delta table to store results.
      spark: SparkSession instance.
      logger: Logger instance.
    
    Returns:
      A cumulative Pandas DataFrame with all processed results.
    """
    import asyncio 
    import re
    import pandas as pd
    import threading
    from datetime import datetime

    # Create an asynchronous lock to serialize Delta table writes.
    lock = asyncio.Lock()

    # Load initial Delta table to filter out already processed PMCID values.
    existing_df_initial = load_existing_delta_data(saved_table_name, spark)
    processed_pmids = get_processed_pmids(existing_df_initial)
    # Filter out rows that have already been processed.
    new_df = df[~df["pmcid"].isin(processed_pmids)].copy()
    total_tasks = new_df.shape[0]
    logger.info(f"Skipping processing {processed_pmids} PMCID(s) that have already been processed.")
    logger.info(f"Processing {total_tasks} new PMCID(s) out of {df.shape[0]} total.")

    tasks = []
    regex = re.compile(r"^\d+\.\s*")  # Regex to remove leading numbers and dots.
    # Schedule processing tasks for each new row.
    for idx, row in new_df.iterrows():
        tasks.append(process_pmcid_row_async(row, llm_dict, input_prompt, text_col, logger, regex))
    
    completed_tasks = 0
    progress_bar_length = 20
    # Process tasks as they complete.
    for task in asyncio.as_completed(tasks):
        res = await task
        current_pmcid = res[0]['pmcid'] if res and 'pmcid' in res[0] else "Unknown"
        # Convert current PMCID result to a DataFrame.
        new_result_df = pd.DataFrame(res)
        # Use lock to perform the read–merge–write atomically.
        async with lock:
            existing_df = load_existing_delta_data(saved_table_name, spark)
            if not existing_df.empty:
                merged_df = pd.concat([existing_df, new_result_df], ignore_index=True)
            else:
                merged_df = new_result_df
            write_results_to_delta_table(merged_df, saved_table_name, spark)  
        
        completed_tasks += 1
        percent_complete = (completed_tasks / total_tasks) * 100
        num_hashes = int(percent_complete / (100 / progress_bar_length))
        progress_bar = "[" + "#" * num_hashes + " " * (progress_bar_length - num_hashes) + "]"
        active_threads = threading.active_count()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"{current_time} [STATUS] {progress_bar} {percent_complete:.0f}% | "
            f"Processed: {completed_tasks}/{total_tasks} | PMCID: {current_pmcid} | "
            f"Parallel Threads: {active_threads}"
        )
    
    # Return the final cumulative DataFrame from the Delta table.
    cumulative_df = load_existing_delta_data(saved_table_name, spark)  
    return cumulative_df


def process_df_with_llms(df, llm_dict, input_prompt, saved_table_name, text_col="methods", spark=None):
    """
    Synchronous entry point to process a DataFrame through multiple LLMs.
    
    This function:
      - Sets up logging.
      - Obtains a SparkSession (or creates one if not provided).
      - Applies nest_asyncio to allow nested event loops (important in interactive environments).
      - Calls the asynchronous processing function to process the DataFrame.
      - Returns the final cumulative results as a Pandas DataFrame.
    
    Parameters:
      df: Input DataFrame containing at least a 'pmcid' column.
      llm_dict (dict): Dictionary mapping LLM names to their instances.
      input_prompt (str): Base prompt for all rows.
      text_col (str): Column name for additional text (default is "methods").
      saved_table_name (str): Name of the Delta table to save results.
      spark: Optional SparkSession. If not provided, one will be created.
    
    Returns:
      A Pandas DataFrame with the processed and aggregated results.
    """
    import logging
    import asyncio 

    initialize_logging()
    logger = logging.getLogger(__name__)

    try:
        import nest_asyncio
        nest_asyncio.apply()  # Allow nested event loops if needed.
    except ImportError:
        logger.error("nest_asyncio not installed. Nested event loops may cause issues.")
    try:
        import asyncio
        result_df = asyncio.run(
            process_df_with_llms_async(df, llm_dict, input_prompt, text_col, saved_table_name, spark, logger)
        )
    except RuntimeError:
        # If an event loop is already running, get the current loop and run the task.
        import asyncio
        loop = asyncio.get_event_loop()
        result_df = loop.run_until_complete(
            process_df_with_llms_async(df, llm_dict, input_prompt, text_col, saved_table_name, spark, logger)
        )
    return result_df



def inputPrompt():
  
  # Define your input prompt.
  
  input_prompt = """
  **SYSTEM INSTRUCTIONS (FOLLOW EXACTLY)**
  
  1. You are given the Methods/Materials section of a scientific article describing real-world patient selection and cohort creation.
  
  2. You must extract information into exactly 27 specific categories (listed below). Each category must correspond to one row in **a single markdown table**.
  
  3. You may **not** add or remove any categories beyond those specified.
  
  4. For each category, do the following:
     - Search the provided text to retrieve relevant direct quotes (i.e., the exact wording).  
     - If multiple relevant pieces appear, combine them into one string **separated by line breaks** (e.g., using `\n`).  
     - Place that combined string in the **verbatim** column (still enclosed in double quotes if you wish).  
     - In the **interpretation** column, briefly interpret or clarify the content for that category.
  
  5. If a category is **mentioned** but does **not** include any direct quotes, place `""` in the **verbatim** column and briefly explain in the **interpretation** column.  
     If a category is **not mentioned at all**, place `""` in the **verbatim** column and write “Not mentioned.” in the **interpretation** column.
  
  6. **Special case for `medical_codes`**:
     - If ICD, SNOMED, CPT-4, HCPCS, or ATC codes are present, place them (in quotes) under **verbatim** and set **interpretation** to `codes_reported = Yes.`  
     - If none, set **verbatim** = `""` and **interpretation** = `codes_reported = No.`
  
  7. **OUTPUT FORMAT** — You must return **only** one markdown table with the following columns:
  
     - A header row exactly like this:  
       `| category | verbatim | interpretation |`
     - A divider row exactly like this:  
       `|----------|----------|----------------|`
     - One row for each of the 27 categories below.  
     - **No numbering, code blocks, or extra text** of any kind before or after the table.
  
  8. **CATEGORIES** (one row per item, in any order):
     - medical_codes  
     - demographic_restriction  
     - entry_event  
     - index_date_definition  
     - inclusion_rule  
     - exclusion_rule  
     - exit_criterion  
     - attrition_criteria  
     - washout_period  
     - follow_up_period  
     - exposure_definition  
     - treatment_definition  
     - outcome_definition  
     - severity_definition  
     - outcome_ascertainment  
     - study_period  
     - study_design  
     - comparator_cohort  
     - covariate_adjustment  
     - statistical_analysis  
     - sensitivity_analysis  
     - algorithm_validation  
     - data_provenance  
     - data_source_type  
     - healthcare_setting  
     - data_access  
     - ethics_approval  
  
  9. **PROHIBITED**:
     - Do not add any extra rows for categories not listed.
     - Do not produce any text outside the markdown table.
     - Do not include numbering, code fences, or extraneous markdown elements.
  
  **USER PROMPT**:
  Extract the relevant details from the following text based on the categories above. Provide each category as one row in the single markdown table described, following the rules exactly.
  
  """
  return input_prompt



def llmDict(llm_dict = [], llmModel=None, apiKey=None, temperature=0.0, azureEndpoint="https://ai-proxy.lab.epam.com"):
  
    # Example llm_dict; ensure your LLM objects are callable with the provided prompt.

    ###
    from langchain_openai import AzureChatOpenAI
    
    # Check if apiKey variable has been set
    if apiKey is None:
        print("API key has not been set. Please provide and run again.")
        
    # Check if llmModel variable has been set
    if llmModel is None:
        print("LLM model has not been set. Please provide and run again.")
    elif llmModel == "claude_sonnet":
        llm_dict = {
          "claude_sonnet": AzureChatOpenAI(
            api_key=apiKey,
            api_version="2024-08-01-preview",
            azure_endpoint=azureEndpoint,
            model="anthropic.claude-v3-sonnet",
            temperature=temperature,
         )
        }
    elif llmModel == "gpt-4o-full":  
        llm_dict = {
          "gpt-4o-full": AzureChatOpenAI(
            api_key=apiKey,
            api_version="2024-08-01-preview",
            azure_endpoint=azureEndpoint,
            model="gpt-4o",
            temperature=temperature,
         )
        }
    else:
      print("Invalid LLM model provided. Please provide valid LLM model and run again.")

    return llm_dict

# Helper function to process each DataFrame
def process_df(table_name):
  
    df = spark.table(table_name).toPandas()
    phenotype = table_name.replace('PubMedSearchDf', '')
    phenotype = re.sub(r'(?<!^)(?=[A-Z])', ' ', phenotype).title()
    df.insert(0, 'phenotype', phenotype)
    
    return df
