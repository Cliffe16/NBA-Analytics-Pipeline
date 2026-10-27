import os
import logging
from dotenv import load_dotenv
from extraction.utils import get_bq_client
from extraction.config import GCP_PROJECT_ID, STAGING_DATASET

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    logger.info("Loading environment variables...")
    load_dotenv()
    
    # Verify environment variables exist
    if not GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID is missing from .env")
        return
        
    logger.info(f"Targeting Project: {GCP_PROJECT_ID} | Dataset: {STAGING_DATASET}")
    
    try:
        # Test the Service Account JSON by initializing the client
        logger.info("Initializing BigQuery Client...")
        client = get_bq_client()
        
        # Tets Read/Write permissions
        query = "SELECT 'BigQuery Connection Successful!' as test_message"
        query_job = client.query(query)
        
        for row in query_job:
            logger.info(f"Result: {row['test_message']}")
            
        logger.info("SUCCESS: The Python environment is successfully authenticated with Google Cloud")
        
    except Exception as e:
        logger.error(f"FAILURE: Could not connect to BigQuery. Error: {str(e)}")

if __name__ == "__main__":
    test_connection()
