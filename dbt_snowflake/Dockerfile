# Python environment for the Snowflake dbt project.
FROM python:3.10-slim

# Install Git for dbt dependencies and repository operations.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install pinned dbt versions matching the Snowsight environment.
RUN pip install --no-cache-dir \
    dbt-core==1.9.4 \
    dbt-snowflake==1.9.2 \
    python-dotenv

# Set the working directory.
WORKDIR /usr/app/dbt_snowflake

# Keep the container running for Airflow orchestration.
CMD ["sleep", "infinity"]