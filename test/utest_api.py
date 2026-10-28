import pytest
from fastapi.testclient import TestClient
import sqlite3
import os
from api import app 

client = TestClient(app)

def test_extraction_trigger_and_polling():
    """Tests the async extraction layer to ensure the 202 Accepted pattern"""
    # Trigger the background extraction
    trigger_response = client.post("/extract/game-logs")
    
    # Verify the API didn't block and returned immediately
    assert trigger_response.status_code == 202
    
    response_data = trigger_response.json()
    assert "task_id" in response_data
    assert response_data["status"] == "accepted"
    
    task_id = response_data["task_id"]
    
    # Poll the status endpoint to verify SQLite persistence
    status_response = client.get(f"/status/{task_id}")
    assert status_response.status_code == 200
    
    status_data = status_response.json()
    assert status_data["task_id"] == task_id
    # The task should immediately register as either pending or running
    assert status_data["status"] in ["pending", "running", "success"]

def test_invalid_task_polling():
    """
    Tests that the API correctly handles status requests for non-existent tasks.
    """
    status_response = client.get("/status/invalid-uuid-1234")
    assert status_response.status_code == 404
