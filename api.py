from fastapi import FastAPI, HTTPException, BackgroundTasks
import logging
import sqlite3
import uuid
import sys
from contextlib import asynccontextmanager
from extraction import game_logs, team_rosters, player_info

# Initialize database on startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    with sqlite3.connect("extraction_tasks.db") as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                task_name TEXT,
                status TEXT
            )
        """)
    yield

app = FastAPI(title="NBA Local Extraction API", lifespan=lifesspan)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_task_status(task_id: str, task_name: str, status: str):
    """Writes the task state to the SQLite database."""
    with sqlite3.connect("extraction_tasks.db") as conn:
        conn.execute("""
            INSERT INTO tasks (task_id, task_name, status) 
            VALUES (?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET status=excluded.status
        """, (task_id, task_name, status))

def get_task_status(task_id: str):
    """Reads the task state from the SQLite database."""
    with sqlite3.connect("extraction_tasks.db") as conn:
        cursor = conn.execute("SELECT status FROM tasks WHERE task_id = ?", (task_id,))
        row = cursor.fetchone()
        return row[0] if row else None

def run_background_extraction(task_id: str, extract_func, task_name: str):
    """Executes the extraction and updates the database upon completion."""
    update_task_status(task_id, task_name, "running")
    try:
        logger.info(f"Starting background task: {task_name}")
        extract_func()
        update_task_status(task_id, task_name, "success")
        logger.info(f"Successfully completed: {task_name}")
    except Exception as e:
        error_msg = f"failed: {str(e)}"
        update_task_status(task_id, task_name, error_msg)
        logger.error(f"Task {task_name} failed: {error_msg}")

@app.post("/extract/game-logs", status_code=202)
def trigger_game_logs(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    update_task_status(task_id, "game_logs", "pending") # Pre-register to prevent Airflow race conditions
    background_tasks.add_task(run_background_extraction, task_id, game_logs.extract_stats, "game_logs")
    return {"task_id": task_id, "status": "accepted", "message": "Extraction started in background."}

@app.post("/extract/player-info", status_code=202)
def trigger_player_info(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    update_task_status(task_id, "player_info", "pending")
    background_tasks.add_task(run_background_extraction, task_id, player_info.extract_player_info, "player_info")
    return {"task_id": task_id, "status": "accepted", "message": "Extraction started in background."}

@app.post("/extract/team-rosters", status_code=202)
def trigger_team_rosters(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    update_task_status(task_id, "team_rosters", "pending")
    background_tasks.add_task(run_background_extraction, task_id, team_rosters.extract_team_rosters, "team_rosters")
    return {"task_id": task_id, "status": "accepted", "message": "Extraction started in background."}

@app.get("/status/{task_id}")
def check_status(task_id: str):
    """Endpoint for Airflow to poll the database for extraction status."""
    status = get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found in database")
    
    return {"task_id": task_id, "status": status}
