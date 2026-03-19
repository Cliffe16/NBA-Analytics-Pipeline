import os
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_ACCOUNT = os.getenv('SF_ACC')
DB_USER = os.getenv('USER')
DB_PASSWORD = os.getenv('PASS')
WAREHOUSE = os.getenv('WAREHOUSE')
DB = os.getenv('SF_DB')
DB_SCHEMA = os.getenv('SCHEMA')

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
