FROM postgres:16

# Ensure init order: flight warehouse first, then Airflow DB.
# Rename on copy to guarantee lexicographic order at init time.
COPY init.sql /docker-entrypoint-initdb.d/00_init_flight.sql
COPY 01_init_airflow.sql /docker-entrypoint-initdb.d/01_init_airflow.sql
