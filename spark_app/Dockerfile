FROM bitnami/spark:3.5
USER root
# If you need Python libs inside executors, install here; otherwise skip
# RUN pip install --no-cache-dir <libs>
USER 1001
WORKDIR /opt/bitnami/spark/app
RUN mkdir -p /opt/bitnami/spark/app/ivy
COPY stream_to_postgres.py .
