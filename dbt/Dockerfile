# Same Python version as the rest of the project. Lightweight Python image used as the base for dbt.
FROM python:3.10-slim

# Installation of Git, used by dbt to manage certain dependencies and repository-related features.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Version of the dbt PostgreSQL adapter intentionally pinned to ensure environment reproducibility.
RUN pip install --no-cache-dir dbt-postgres==1.11.0

# dbt project working directory.
WORKDIR /usr/app/dbt

# By default, the container remains active.
# dbt commands will be run manually using "docker exec".
CMD ["sleep", "infinity"]