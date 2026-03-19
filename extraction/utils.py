import os
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# Set-up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_conn():
	"""Establishes connection to Snowflake"""
	logger.info("Establishing connection to Snowflake...")
	return snowflake.connector.connect(
		user=os.getenv('USER'),
        	password=os.getenv('PASS'),
        	account=os.getenv('SF_ACC'),
        	warehouse=getenv('WAREHOUSE'),
        	database=getenv('SF_DB'),
        	schema=getenv('SCHEMA')
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
