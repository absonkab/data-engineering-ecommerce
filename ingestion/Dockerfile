FROM python:3.10

WORKDIR /app

COPY ingestion/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/producer ./producer
COPY config ./config

CMD ["python", "-m", "producer.main"]
