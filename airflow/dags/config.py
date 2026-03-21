import os
import sys

# Get working and main project directories
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAGS_DIR, '../../'))

# Makse sure airflow sees the extraction directory by adding it to python's path
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
