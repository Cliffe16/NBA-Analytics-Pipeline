import os
import time
import logging
import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from extraction.utils import get_snowflake_conn, db_load
from extraction import config

logger = logging.getLogger(__name__)

def extract_player_info():
    """Extracts players' biographical information"""
    try:
        conn = get_snowflake_conn()
        logger.info("Fetching current active players...")
        cur = conn.cursor()

        # Fetch players who've been in a roster this season
        cur.execute("""
            SELECT DISTINCT PLAYER_ID FROM(
                -- Get anyone who has been on a roster this season
                SELECT PLAYER_ID 
                FROM NBA_ANALYTICS.RAW.RAW_TEAM_ROSTERS 
                
                UNION 
                
                -- Get anyone who has actually played in a game this season
                SELECT PLAYER_ID 
                FROM NBA_ANALYTICS.RAW.RAW_PLAYER_GAME_LOGS
            )
            WHERE PLAYER_ID IS NOT NULL
        """)
        active_player_ids = [row[0] for row in cur.fetchall()]
        
        logger.info(f"Extracting biographical info for {len(active_player_ids)} active players...")
        player_info_frames = []
        
        for count, player_id in enumerate(active_player_ids, 1):
            time.sleep(config.API_DELAY)
            if count & 50 == 0:
                logger.info(f"Processed {count}/{len(active_player_ids)} players...")
            
            info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            player_info_frames.append(info.get_data_frames()[0])

        if player_info_frames:
            player_info_df = pd.concat(player_info_frames, ignore_index=True)
            db_load(player_info_df, config.PLAYER_INFO_TABLE, conn)

    except Exception as e:
        logger.error(f"Error extracting player info: {str(e)}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")
           
        
    
