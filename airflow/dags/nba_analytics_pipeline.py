from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.bigquery import GoogleCloudServiceAccountFileProfileMapping
from datetime import datetime, timedelta
import sys 
import os
import time
from dotenv import load_dotenv
# Main project directories
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from extraction.game_logs import extract_stats
from extraction.player_info import extract_player_info
from extraction.team_rosters import extract_team_rosters

load_dotenv()

# Define dbt directory and executable paths
DBT_PROJECT_PATH = os.path.join(project_root, os.getenv('DBT_ROOT'))
DBT_EXECUTABLE_PATH = os.path.join(project_root, os.getenv('DBT_BIN'))

# Map the Airflow snowflake connection to dbt
profile_config = ProfileConfig(
    profile_name="nba_pipeline",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": os.getenv("GCP_PROJECT_ID"),
            "dataset": "STAGING"
            }
        )
)

def trigger_and_poll(endpoint: str, **kwargs):
    """Triggers the asynchronous FastAPI endpoint and polls for completion"""
    # Trigger the background extraction
    trigger_hook = HttpHook(method='POST', http_conn_id='tailscale')
    response = trigger_hook.run(endpoint)
    
    # FastAPI returns 202 Accepted for background tasks
    if response.status_code != 202:
        raise AirflowException(f"Trigger failed. Status {response.status_code}: {response.text}")
        
    task_id = response.json().get('task_id')
    print(f"Successfully triggered extraction. Task ID: {task_id}")
    
    # Poll the status endpoint
    poll_hook = HttpHook(method='GET', http_conn_id='tailscale')
    while True:
        # Wait 10 seconds between checks to avoid hammering the API
        time.sleep(10)
        
        poll_response = poll_hook.run(f"/status/{task_id}")
        status = poll_response.json().get('status')
        
        if status == 'success':
            print(f"Task {task_id} successfully completed.")
            break
        elif status.startswith('failed'):
            raise AirflowException(f"Extraction failed in backend: {status}")
            
        print(f"Task {task_id} is {status}. Polling again...")

with DAG(
    "nba_analytics_pipeline",
    default_args={
        "depends_on_past": False,
        "email": [os.getenv('EMAIL')],
        "email_on_failure": True,
        "email_on_retry": False, 
        "retries": 3,
        "retry_delay": timedelta(minutes=10)
        },
    description="NBA Pipeline Orchestrator",
    schedule="@daily",
    start_date=datetime(2026, 3, 20),
    catchup=False,
    max_active_runs=1
    ) as dag:

        task_extract_stats = PythonOperator(
            task_id='extract_stats',
            python_callable=trigger_and_poll,
            op_kwargs={'endpoint': '/extract/game-logs'}
        )

        task_extract_player_info = PythonOperator(
            task_id='extract_player_info',
            python_callable=trigger_and_poll,
            op_kwargs={'endpoint': '/extract/player-info'}
        )

        task_extract_team_roster = PythonOperator(
            task_id='extract_team_roster',
            python_callable=trigger_and_poll,
            op_kwargs={'endpoint': '/extract/team-rosters'}
        )

        task_transform_data = DbtTaskGroup(
            group_id="transform_nba_data",
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            execution_config=ExecutionConfig(
                dbt_executable_path=DBT_EXECUTABLE_PATH
            ),
        )

        task_extract_stats >> task_extract_team_roster >> task_extract_player_info >> task_transform_data
