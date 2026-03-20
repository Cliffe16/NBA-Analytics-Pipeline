import os
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from extraction import config

# Set-up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_conn():
	"""Establishes connection to Snowflake"""
	logger.info("Establishing connection to Snowflake...")
	return snowflake.connector.connect(
		    user=config.DB_USER,
        	password=config.DB_PASSWORD,
        	account=config.DB_ACCOUNT,
        	warehouse=config.WAREHOUSE,
        	database=config.DB,
        	schema=config.DB_SCHEMA
		)

def db_load(df, table_name, conn):
	"""Bulk loads a pandas dataframe into Snowflake"""
	if df.empty:
		logger.warning(f"DataFrame for {table_name}  is empty. Operation skipped.")
		return

	# Capitalise column names to match Database DDL
	df.columns = [col.upper() for col in df.columns]

	logger.info(f"Initializing bulk load of {len(df)} rows into {table_name}...")
	success, num_chunks, num_rows, output = write_pandas(
		conn=conn,
		df=df,
		table_name=table_name.upper(),
		quote_identifiers=False
		)

	if success:
		logger.info(f"Successfully loaded {num_rows} rows into {table_name}.")
	else:
		logger.error(f"Failed to load data into {table_name}. Output: {output}")
