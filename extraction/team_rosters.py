import os 
import time
from datetime import datetime
import pandas as pd
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams
from extraction.utils import get_snowflake_conn, db_load
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


