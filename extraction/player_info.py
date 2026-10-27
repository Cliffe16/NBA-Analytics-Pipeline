import os
import time
import logging
import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from extraction.utils import db_load, get_column_data
from extraction import config
from requests.exceptions import ReadTimeout

logger = logging.getLogger(__name__)

def extract_player_info():
    """Extracts players' biographical information with state-saving"""
    player_info_frames = []
    
    try:
        logger.info("Fetching current active players...")
        # Construct BigQuery table references dynamically using backticks
        project_dataset = f"`{config.GCP_PROJECT_ID}.{config.STAGING_DATASET}"
        rosters_tbl = f"{project_dataset}.{config.TEAM_ROSTERS_TABLE}`"
        logs_tbl = f"{project_dataset}.{config.PLAYER_STATS_TABLE}`"
        info_tbl = f"{project_dataset}.{config.PLAYER_INFO_TABLE}`"

        target_query = f"""
            SELECT DISTINCT PLAYER_ID FROM(
                SELECT PLAYER_ID FROM {rosters_tbl}
                UNION DISTINCT 
                SELECT PLAYER_ID FROM {logs_tbl}
            )
            WHERE PLAYER_ID IS NOT NULL
        """
        all_target_ids = get_column_data(target_query)
        
        try:
            # Check who is already loaded
            loaded_query = f"SELECT DISTINCT PERSON_ID FROM {info_tbl}"
            already_loaded_ids = get_column_data(loaded_query)
        except Exception:
            already_loaded_ids = [] 
            
        active_player_ids = [pid for pid in all_target_ids if pid not in already_loaded_ids]
        
        if not active_player_ids:
            logger.info("All player info already extracted. Skipping.")
            return

        logger.info(f"Extracting info for {len(active_player_ids)} remaining players...")

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
                        # Trigger the 'finally' block and alert Airflow
                        raise e 

    except Exception as e:
        logger.error(f"Extraction interrupted: {str(e)}")
        raise
        
    finally:
        if player_info_frames:
            logger.info(f"Saving {len(player_info_frames)} players to BigQuery before exiting...")
            player_info_df = pd.concat(player_info_frames, ignore_index=True)
            db_load(player_info_df, config.PLAYER_INFO_TABLE)
