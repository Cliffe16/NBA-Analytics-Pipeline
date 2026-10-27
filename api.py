from fastapi import  FastAPI, HTTPException, BackgroundTasks
import logging
from extraction import game_logs, team_rosters, player_info
import sys
import uuid 

app = FastAPI(title="NBA Local Extraction API")
logging.basicConfig(level=logging.info)
logger = logging.getLogger(__name__)

# INitialize empty dictionary as a task tracker
task_store = {}

def run_background_extraction(task_id: str, extract_func, task_name: str):
    """Executes the extraction and updates the global task store upon completion."""
    task_store[task_id] = "running"
    try:
        logger.info(f"Starting background task: {task_name}")
        extract_func()
        task_store[task_id] = "success"
        logger.info(f"Successfully completed: {task_name}")
    except Exception as e:
        task_store[task_id] = f"failed: {str(e)}"
        logger.error(f"Task {task_name} failed: {str(e)}")

@app.post("/extract/game-logs", status_code=202)
def trigger_game_logs(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    background_tasks.add_task(run_background_extraction, task_id, game_logs.extract_stats, "game_logs")
    return {"task_id": task_id, "status": "accepted", "message": "Extraction started in background."}

@app.post("/extract/player-info", status_code=202)
def trigger_player_info(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    background_tasks.add_task(run_background_extraction, task_id, player_info.extract_player_info, "player_info")
    return {"task_id": task_id, "status": "accepted", "message": "Extraction started in background."}

@app.post("/extract/team-rosters", status_code=202)
def trigger_team_rosters(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    background_tasks.add_task(run_background_extraction, task_id, team_rosters.extract_team_rosters, "team_rosters")
    return {"task_id": task_id, "status": "accepted", "message": "Extraction started in background."}

@app.get("/status/{task_id}")
def check_status(task_id: str):
    """Endpoint for Airflow to poll the status of a running extraction."""
    if task_id not in task_store:
        raise HTTPException(status_code=404, detail="Task not found")
    
    status = task_store[task_id]
    return {"task_id": task_id, "status": status}
