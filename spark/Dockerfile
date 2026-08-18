FROM apache/spark:3.5.1

# ---------------------------------------------------------
# Configuration Python
# ---------------------------------------------------------

# Permet à Python de trouver nos modules applicatifs présents dans /opt/spark.
ENV PYTHONPATH=/opt/spark

# ---------------------------------------------------------
# Installation des dépendances Python supplémentaires.
# ---------------------------------------------------------

USER root

COPY spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------
# Préparation du cache Ivy utilisé par Spark pour télécharger
# les dépendances Maven, notamment le connecteur Kafka.
# ---------------------------------------------------------

RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

# ---------------------------------------------------------
# Copie du code applicatif Spark.
# ---------------------------------------------------------

COPY spark/jobs /opt/spark/jobs
COPY spark/schemas /opt/spark/schemas
COPY spark/utils /opt/spark/utils
COPY config /opt/spark/config

# ---------------------------------------------------------
# Le job Spark s'exécute avec l'utilisateur non-root
# fourni par l'image officielle Apache Spark.
# ---------------------------------------------------------

USER spark

# ---------------------------------------------------------
# Répertoire de travail du projet.
# ---------------------------------------------------------

WORKDIR /opt/spark