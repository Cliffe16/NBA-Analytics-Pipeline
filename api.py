from fastapi import  FastAPI, HTTPException
import logging
from extraction import extract_stats, extract_team_rosters, extract_player_info

app = FastAPI()
logging.basicConfig(level=logging.info)
logger = logging.getLogger(__name__)

@app.post("/extract/game-logs")
def run_game_logs():
    try:
        logger.info("Player/Game stats exrtaction started...")
        extract_stats()
        return{"status": "success", "task": "game_lofs", "message": "Player/Game stats successfully extracted and loaded to database"}
    except Exception as e:
        logger.error(f"PLayer/Game stats extraction process failed: str{(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/extract/player-info")
def run_player_info():
    try:
        logger.info("Player Info extraction started...")
        player_info.extract_player_info()
        return {"status": "success", "task": "player_info", "message": "Player Info successfully extracted and  loaded to database" }
    except Exception as e:
        logger.error(f"Player info extraction process failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/extract/team-rosters")
def run_team_rosters():
    try:
        logger.info("Team Rosters extraction started...")
        team_rosters.extract_team_rosters()
        return {"status": "success", "task": "team_rosters", "message": "Team rosters successfully extracted and loaded to database"}
    except Exception as e:
        logger.error(f"Team Rosters extraction process failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

