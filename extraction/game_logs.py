import os
import sys
import logging
from nba_api.stats.endpoints import playergamelogs, leaguegamefinder
from extraction.utils import get_snowflake_conn, db_load
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def extract_stats():
    """Extracts player and game stats from `playergamelogs` and `leaguegamefinder`"""
    conn = get_snowflake_conn()
    try:
        logger.info("Extracting Player Stats...")
        player_stats = playergamelogs.PlayerGameLogs(
            season_nullable='2024-25', 
            season_type_nullable='Regular Season')
        db_load(logs.get_data_frames()[0], 'raw_player_game_logs', conn)

        logger.info("Extracting Team Game Results...")
        game_stats = leaguegamefinder.LeagueGameFinder(
            season_nullable='22024', 
            season_type_nullable='Regular Season', 
            league_id_nullable='00')
        db_load(games.get_data_frames()[0], 'raw_team_game_results', conn)
        
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise
    finally:
        conn.close()
        logger.info("Snowflake connection closed.")
