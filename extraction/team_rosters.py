import os 
import time
from datetime import datetime
import pandas as pd
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams
from extraction.utils import get_snowflake_conn, db_load
import logging
from dotenv import load_dotenv
from extraction import config

load_dotenv()
logger = logging.getLogger(__name__)

def extract_team_rosters():
    """Extracts team roster information"""
    conn = get_snowflake_conn()
    try:
        logger.info("Extracting Team Rosters...")
        nba_teams = teams.get_teams()
        roster_frames = []
        current_date = datetime.now().strftime('%Y-%m-%d')

        for index, team in enumerate(nba_teams, 1):
            time.sleep(config.API_DELAY) # Add delay for the rate limit
            if index % 10 == 0:  # Use as progress tracker for every tenth team extracted
                logger.info(f"Processed rosters for {index}/{len(nba_teams)} teams...")
            roster = commonteamroster.CommonTeamRoster(
                team_id=team['id'],
                season=config.SEASON
                )
            roster_frames.append(roster.get_data_frames()[0])
        
        roster_df = pd.concat(roster_frames, ignore_index=True)
        roster_df['SNAPSHOT_DATE'] = current_date

        db_load(roster_df, config.TEAM_ROSTERS_TABLE, conn)
    
    except Exception as e:
        logger.error(f"Error extracting team rosters: {str(e)}")
        raise
    finally:
        conn.close
        logger.info("Snowflake connection closed.")
