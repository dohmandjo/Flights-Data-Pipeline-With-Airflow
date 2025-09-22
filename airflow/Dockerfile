# Match Python explicitly to avoid the wrong constraints file
FROM apache/airflow:2.9.3-python3.11

# Install as the airflow user (no --user)
USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ENV CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Let the resolver pick versions compatible with Airflow
RUN python -V && pip install --no-cache-dir --constraint "${CONSTRAINTS_LOCATION}" \
    apache-airflow-providers-postgres==5.11.2 \
    gspread \
    gspread-dataframe \
    pandas \
    SQLAlchemy \
    psycopg2-binary
