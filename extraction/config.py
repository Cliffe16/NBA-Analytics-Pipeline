import os
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger(__name__)

def get_azure_secret(vault_url: str, secret_name: str, fallback: str = None) -> str:
    """Fetches a secret from Azure Key Vault"""
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        
        logger.info(f"Fetching {secret_name} from Azure Key Vault...")
        return client.get_secret(secret_name).value
        
    except Exception as e:
        logger.warning(f"Failed to fetch {secret_name} from AKV: {str(e)}. Using fallback.")
        # Revert hyphens to underscores for local .env testing
        env_var_name = secret_name.replace("-", "_")
        return os.getenv(env_var_name, fallback)

VAULT_URL = "https://nba-kv-bqenv.vault.azure.net/"
ALERT_EMAIL = get_azure_secret(VAULT_URL, "ALERT-EMAIL")

# BigQuery routing variables
GCP_PROJECT_ID = get_azure_secret(VAULT_URL, "GCP-PROJECT-ID")
GCP_SERVICE_ACCOUNT_JSON = get_azure_secret(VAULT_URL, "GCP-SERVICE-ACCOUNT")
STAGING_DATASET = get_azure_secret(VAULT_URL, "STAGING-DATASET")

if not GCP_PROJECT_ID or not STAGING_DATASET:
    raise ValueError("Missing critical BigQuery environment variables in .env")

# API Configuration
API_DELAY = 0.6
SEASON = '2024-25'
SEASON_TYPE = 'Regular Season'
LEAGUE_ID = '00'

# Tables
PLAYER_STATS_TABLE = 'raw_player_game_logs'
GAME_STATS_TABLE = 'raw_team_game_results'
TEAM_ROSTERS_TABLE = 'raw_team_rosters'
PLAYER_INFO_TABLE = 'raw_player_info'
