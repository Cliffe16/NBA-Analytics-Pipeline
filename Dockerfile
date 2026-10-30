FROM apache/airflow:2.10.0-python3.12

# Switch to root to install ALL necessary C-compilers
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user BEFORE installing Python packages
USER airflow
COPY requirements.txt /requirements.txt

# Use Airflow's built-in pip to install the requirements natively
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
