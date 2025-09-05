\set ON_ERROR_STOP on

-- Create role 'airflow' if it doesn't exist
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END$$;

-- Conditionally create database 'airflow' owned by airflow.
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')
\gexec
