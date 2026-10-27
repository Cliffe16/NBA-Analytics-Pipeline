import os
from dotenv import load_dotenv

load_dotenv()

# BigQuery routing variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
STAGING_DATASET = os.getenv("STAGING_DATASET")

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
