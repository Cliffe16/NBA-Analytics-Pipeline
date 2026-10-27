import os
import logging
from google.cloud import bigquery
from extraction.config import GCP_PROJECT_ID, STAGING_DATASET
from extraction import config
from airflow.hooks.base import BaseHook
import pandas as pd

# Set-up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bq_client() -> bigquery.Client:
    """Initializes the BigQuery client"""
    return bigquery.Client(project=GCP_PROJECT_ID)

def db_load(df: pd.DataFrame, table_name: str) -> None:
    """Bulk loads a pandas dataframe into BigQuery"""
    client = get_bq_client()

    # Construct the full table ID
    table_id = f"{GCP_PROJECT_ID}.{STAGING_DATASET}.{table_name}"

    # Overwrite staging tables daily to prevent bloat
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Uploading {len(df)} rows to {table_id}...")

    # Execute the asynchronous load job
    job = client.load_table_from_dataframe(
        df, 
        table_id,
        job_config=job_config
    )
    
    job.result() # Block until the job completes
    print(f"Success: Loaded {job.output_rows} rows to {table_id}.")

def get_column_data(query: str) -> list:
    """Executes a BigQuery SQL statement and returns the first column as a Python list"""
    client = get_bq_client()

    query_job = client.query(query)

    results = query_job.result()

    return [row[0] for row in results]
