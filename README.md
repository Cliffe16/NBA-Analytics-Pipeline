# NBA Analytics Data Pipeline

![Airflow](https://img.shields.io/badge/Airflow-2.10.0-17A2B8?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?style=for-the-badge&logo=Snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Data_Build_Tool-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Local_Worker-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Tailscale](https://img.shields.io/badge/Tailscale-Mesh_VPN-4A4A4A?style=for-the-badge&logo=tailscale&logoColor=white)
![Azure](https://img.shields.io/badge/Microsoft_Azure-Cloud_Host-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)

## Project Overview
This project is an end-to-end, automated ELT (Extract, Load, Transform) pipeline designed to extract comprehensive player and game statistics from the official NBA API, load the raw data into a Snowflake data warehouse and transform it into a dimensional model for analytics.

## System Architecture

![Architecture Diagram](docs/pipeline_diagram.png) 

![Data Model Diagram](docs/data_model_diagram.png)

### The Tech Stack
* **Orchestration:** Apache Airflow (Dockerized on Azure VM)
* **Extraction Worker:** FastAPI, Uvicorn, Python `nba_api`(Running as a `systemd` background service)
* **Secure Networking:** Tailscale (Zero-Trust Mesh VPN)
* **Data Warehouse:** Snowflake
* **Transformation:** dbt (Data Build Tool) integrated via Astronomer Cosmos

## Repository Structure
```text
NBA-Analytics-Pipeline/
├── airflow/                             # Cloud Orchestration
│   └── dags/
│       ├── config.py
│       └── nba_analytics_pipeline.py    # Main Airflow DAG defining the ELT flow
├── docs/                                # Project documentation & images
│   ├── data_model_diagram.png
│   └── pipeline_diagram.png
├── extraction/                          # Local Python Extraction Scripts
│   ├── config.py
│   ├── game_logs.py
│   ├── player_info.py
│   ├── team_rosters.py
│   └── utils.py
├── nba_analytics/                       # dbt Project (Transformations)
│   ├── dbt_project.yml
│   ├── macros/
│   ├── models/
│   │   ├── staging/                     # Base views on top of RAW Snowflake tables
│   │   ├── intermediate/                # Joins and business logic
│   │   └── marts/                       # Materialized dimensional models
│   └── tests/                           # Custom data quality tests
├── sql/                                 # Snowflake setup & test scripts
│   ├── snowflake_database_schema.sql
│   ├── snowflake_raw_setup.sql
│   └── snowflake_warehouse.sql
├── test/                                # API Verification scripts
│   └── verify_api.py
├── api.py                               # FastAPI switchboard for remote triggering
├── Dockerfile                           # Custom Airflow image with Cosmos dependencies
├── requirements.txt
└── README.md
```
## Pipeline Execution Flow

1. **Scheduled Trigger:** Airflow, running on the Azure VM, triggers the daily `nba_analytics_pipeline` DAG.
2. **Secure Remote Control:** Airflow utilizes a `SimpleHttpOperator` to send an HTTP POST request across the Tailscale private network (`100.x.x.x:8000`) to the local FastAPI worker.
3. **WAF Bypass & Extraction:** The local Ubuntu worker receives the command and initiates the Python extraction scripts (`game_logs.py`, `player_info.py`, `team_rosters.py`). Because the traffic originates from a residential ISP, the Akamai WAF permits the connection. The scripts utilize rate-limiting safeguards (`time.sleep` and extended timeouts) to prevent API throttling.
4. **Direct Data Load:** The local worker formats the JSON responses into Pandas DataFrames and pushes the raw tables directly into the `RAW` database in Snowflake.
5. **Success Signal:** The FastAPI worker returns an `HTTP 200 OK` status to Airflow via the secure VPN tunnel.
6. **Data Transformation:** Upon receiving the success signal, Airflow initiates dbt's `transform_nba_data` task group. Astronomer Cosmos dynamically compiles and executes the dbt SQL models directly inside Snowflake, transforming the raw tables into clean, materialized dimensional models ready for BI consumption.

## Setup & Installation
**Prerequisites**
    Cloud: Azure Virtual Machine (Ubuntu) with Docker installed.

    Edge Node: Local machine (Ubuntu) for extraction.

    Acconts: Tailscale (free tier), Snowflake.

### Cloud Orchestrator (Azure VM)
The Airflow environment is containerized. To spin up the orchestrator:
```bash
git clone [https://github.com/Cliffe16/NBA-Analytics-Pipeline.git](https://github.com/Cliffe16/NBA-Analytics-Pipeline.git)
cd NBA-Analytics-Pipeline
docker compose up -d
```

### Airflow Connnecions
Configure these in the Airflow UI:
    `snowflake_dbt`: Snowflake credentials with the target schema, account and user specified for Astronomer-Cosmos
    `tailscale_api`: HTTP connection pointing to the local worker node's Tailscale IP

### Exraction worker node(Local Ubuntu pc)
Install Tailscale to join the mesh network, then set up the FastAPI worker as an always-on systemd service.
```text
# sudo nano /etc/systemd/system/nba-extraction.service
[Unit]
Description=NBA Local Extraction API (Tailscale Connection)
After=network.target

[Service]
User=<user>
WorkingDirectory=/path/to/project-dir
ExecStart=/path/to/.local/bin/uvicorn api:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```
Enable and start the service
```bash
sudo systemctl daemon-reload
sudo systemctl enable nba-extraction
sudo systemctl start nba-extraction
```

**The Engineering Challenge:** The official NBA API(`stats.nba.com`) utilizes a strict Akamai Web Application Firewall(WAF) that actively blocks and blacklists traffic originating fro>

**The Architectural Solution:** Rather than relying on unreliable public proxies or expensive commercial residential proxy networks, this pipeline implements a **Hybrid Extraction Arc>
* **Orchestration** is handled in the cloud via Apache Airflow hosted on an Azure Virtual Machine.
* **Extraction** is executed on a local edge node(my personal Ubuntu laptop) running a custom FastAPI worker.
* **Communication** between the Azure orchestrator and the local extraction node is secured via a **Tailscale WireGuard Mesh VPN**, completely bypassing the public internet, NAT route>

## Key Technical Learnings
**Distributed Systems:** Designed and debugged communication between cloud infrastructure and on-premise hardware using private mesh networking.

**API Rate Management:** Implemented robust error handling, dynamic timeouts and request throttling to maintain stable connections with heavily fortified enterprise APIs.

**Modern ELT Orchestration:** Utilized Astronomer Cosmos to treat dbt models as independent tasks rather than a single grouped tasks within Airflow DAGs, ensuring strict dependency management between extraction success and transformation execution.
