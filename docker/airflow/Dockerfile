FROM apache/airflow:2.8.3-python3.11

USER root

# Install basic OS utilities if needed (e.g., git or curl)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
