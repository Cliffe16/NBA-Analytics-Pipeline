from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from datetime import datetime, timedelta
import sys 
import os
from dotenv import load_dotenv
# Get working and main project directories
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
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dbt"
        )
)

# Configure dag
with DAG(
    "nba_analytics_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ['oluochcliffe13@gmail.com'],
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
            task_id="extract_stats",
            python_callable=extract_stats
            )
        task_extract_player_info = PythonOperator(
            task_id="extract_player_info",
            python_callable=extract_player_info
            )
        task_extract_team_roster = PythonOperator(
            task_id="extract_team_roster",
            python_callable=extract_team_rosters
            )
        task_transform_data = DbtTaskGroup(
            group_id="transform_nba_data",
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            execution_config=ExecutionConfig(
                dbt_executable_path=DBT_EXECUTABLE_PATH
                ),
            )

        # Compile dependenciees
        task_extract_stats >> task_extract_team_roster >> task_extract_player_info >> task_transform_data
