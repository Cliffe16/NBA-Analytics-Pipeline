from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys 
import os
from extraction.game_logs import extract_stats
from extraction.player_info import extract_player_info
from team_rosters import extract teeam_rosters

# Configure dag
with DAG(
    "nba_analytics_pipeline",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False  # COnvert to true to mimic production standards
        "retries": 3,
        "retry_delay": timedelta(minutes=10)
        },
    description="NBA Pipeline Orchestrator",
    schedule="@daily",
    start_date=datetime(2026, 3, 20),
    catchup=False
    ) as dag:
        extract_stats = PythonOperator(
            task_id="extract_stats",
            python_callable=extract_stats
            )
        extract_player_info = Python Operator(
            task_id="extract_player_info",
            python+callable=extract_player_info
            )
         extract_team_roster = PythonOperator(
            task_id="extract_team_roster",
            python_callable=extract_team_tosters
            )
            
    

