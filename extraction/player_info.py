import os
import time
import logging
import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from extraction.utils import get_snowflake_conn, db_load
from extraction import config
from requests.exceptions import ReadTimeout

logger = logging.getLogger(__name__)

def extract_player_info():
    """Extracts players' biographical information with state-saving"""
    conn = get_snowflake_conn()
    player_info_frames = []
    
    try:
        logger.info("Fetching current active players...")
        cur = conn.cursor()

        # Fetch players who've been in a roster this season
        cur.execute("""
            SELECT DISTINCT PLAYER_ID FROM(
                SELECT PLAYER_ID FROM NBA_ANALYTICS.RAW.RAW_TEAM_ROSTERS 
                UNION 
                SELECT PLAYER_ID FROM NBA_ANALYTICS.RAW.RAW_PLAYER_GAME_LOGS
            )
            WHERE PLAYER_ID IS NOT NULL
        """)
        all_target_ids = [row[0] for row in cur.fetchall()]
        
        # STATE-SAVING: Check who we already successfully loaded in previous failed runs
        try:
            cur.execute(f"SELECT DISTINCT PERSON_ID FROM NBA_ANALYTICS.RAW.{config.PLAYER_INFO_TABLE}")
            already_loaded_ids = [row[0] for row in cur.fetchall()]
        except Exception:
            already_loaded_ids = [] # Table might not exist yet on first run
            
        # Only process players we don't already have
        active_player_ids = [pid for pid in all_target_ids if pid not in already_loaded_ids]
        
        if not active_player_ids:
            logger.info("All player info already extracted. Skipping.")
            return

        logger.info(f"Extracting info for {len(active_player_ids)} remaining players...")
        
        for count, player_id in enumerate(active_player_ids, 1):
            if count % 50 == 0:
                logger.info(f"Processed {count}/{len(active_player_ids)} players...")
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Extended delay to appease the API
                    time.sleep(1.5) 
                    
                    info = commonplayerinfo.CommonPlayerInfo(
                        player_id=player_id,
                        timeout=60
                    )
                    player_info_frames.append(info.get_data_frames()[0])
                    break 
                    
                except (ReadTimeout, Exception) as e:
                    logger.warning(f"Attempt {attempt + 1} failed for player {player_id}: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(5)  
                    else:
                        logger.error(f"Failed to fetch player {player_id}. API likely severed connection.")
                        # We raise the error here to trigger the 'finally' block and alert Airflow
                        raise e 

    except Exception as e:
        logger.error(f"Extraction interrupted: {str(e)}")
        raise
        
    finally:
        # THE RESCUE BLOCK: Save whatever we managed to grab before the crash
        if player_info_frames:
            logger.info(f"Saving {len(player_info_frames)} players to Snowflake before exiting...")
            player_info_df = pd.concat(player_info_frames, ignore_index=True)
            db_load(player_info_df, config.PLAYER_INFO_TABLE, conn)
            
        conn.close()
        logging.info("Database connection closed.")
