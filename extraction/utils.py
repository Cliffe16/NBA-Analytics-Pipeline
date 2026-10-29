import os
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from extraction.config import GCP_PROJECT_ID, STAGING_DATASET, GCP_SERVICE_ACCOUNT_JSON
from extraction import config
from airflow.hooks.base import BaseHook
import pandas as pd
import json

# Set-up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bq_client() -> bigquery.Client:
    """Initializes the BigQuery client with in-memeory credentials"""
    if GCP_SERVICE_ACCOUNT_JSON:
        # Parse the raw string from Azure Key Vault into a Python dictionary
        creds_dict = json.loads(GCP_SERVICE_ACCOUNT_JSON)

        # Generate the authentication token in memory
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    else:
        # Fallback for local development using Application Default Credentials
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
