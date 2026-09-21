FROM apache/spark:3.5.1

# ---------------------------------------------------------
# Python Configuration
# ---------------------------------------------------------

# Allows Python to find our application modules located in /opt/spark.
ENV PYTHONPATH=/opt/spark

# ---------------------------------------------------------
# Installation of additional Python dependencies.
# ---------------------------------------------------------

USER root

COPY spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------
# Preparation of the Ivy cache used by Spark to download
# Maven dependencies, notably the Kafka connector.
# ---------------------------------------------------------

RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

# ---------------------------------------------------------
# PostgreSQL JDBC driver
# ---------------------------------------------------------
#
# Spark uses JDBC to communicate with PostgreSQL.
# The driver is a Java dependency, so it must be available
# on Spark's classpath.
# ---------------------------------------------------------

RUN curl -L \
    -o /opt/spark/jars/postgresql-42.7.8.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.8.jar

# ---------------------------------------------------------
# Copy of Spark application code.
# ---------------------------------------------------------

COPY spark/jobs /opt/spark/jobs
COPY spark/schemas /opt/spark/schemas
COPY spark/utils /opt/spark/utils
COPY config /opt/spark/config

# ---------------------------------------------------------
# The Spark job runs using the non-root user
# provided by the official Apache Spark image.
# ---------------------------------------------------------

USER spark

# ---------------------------------------------------------
# Project working directory.
# ---------------------------------------------------------

WORKDIR /opt/spark