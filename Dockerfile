FROM apache/airflow:2.10.0-python3.12

USER root

# Install PostgreSQL build dependencies and curl
RUN apt-get update && \
    apt-get install -y gcc libpq-dev curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

# Bypass Airflow's pip wrapper by installing uv directly via curl, then install requirements as root
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh && \
    uv pip install --system --no-cache -r /requirements.txt

# Drop privileges back to the secure airflow user
USER airflow
