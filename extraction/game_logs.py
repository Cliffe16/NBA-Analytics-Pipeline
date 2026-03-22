import os
import sys
import logging
from nba_api.stats.endpoints import playergamelogs, leaguegamefinder
from extraction.utils import get_snowflake_conn, db_load
from extraction import config

logger = logging.getLogger(__name__)

def extract_stats():
    """Extracts player and game stats from `playergamelogs` and `leaguegamefinder`"""
    conn = get_snowflake_conn()
    try:
        logger.info("Extracting Player Stats...")
        player_stats = playergamelogs.PlayerGameLogs(
            season_nullable=config.SEASON, 
            season_type_nullable=config.SEASON_TYPE)
        db_load(player_stats.get_data_frames()[0], config.PLAYER_STATS_TABLE, conn)

        logger.info("Extracting Team Game Results...")
        game_stats = leaguegamefinder.LeagueGameFinder(
            season_nullable=config.SEASON, 
            season_type_nullable=config.SEASON_TYPE, 
            league_id_nullable=config.LEAGUE_ID)
        db_load(game_stats.get_data_frames()[0], config.GAME_STATS_TABLE, conn)
        
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise
    finally:
        conn.close()
        logger.info("Snowflake connection closed.")
