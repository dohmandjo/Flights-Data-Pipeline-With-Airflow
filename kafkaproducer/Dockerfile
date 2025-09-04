FROM python:3.12-slim
WORKDIR /kafkaproducer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY kafka_prod.py .
ENV BOOTSTRAP_SERVERS=flight_kafka:9092 TOPIC=flights_live
CMD ["python", "kafka_prod.py"]