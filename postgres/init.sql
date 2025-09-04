
-- Staging table used by Spark for each batch
DROP TABLE IF EXISTS fact_flight_status_staging;
CREATE TABLE fact_flight_status_staging(
    flight_key TEXT,
    flight_date DATE,
    flight_status TEXT,
    airline_iata TEXT,
    airline_name TEXT,
    flight_number TEXT,
    flight_iata TEXT,
    flight_icao TEXT,
    dep_airport TEXT, -- iata or icao
    dep_terminal TEXT,
    dep_gate TEXT,
    dep_scheduled TIMESTAMPTZ,
    dep_actual TIMESTAMPTZ,
    dep_delay_min NUMERIC,
    arr_airport TEXT, -- iata or icao
    arr_terminal TEXT,
    arr_gate TEXT,
    arr_scheduled TIMESTAMPTZ,
    arr_estimated TIMESTAMPTZ,
    arr_actual TIMESTAMPTZ,
    arr_delay_min NUMERIC,
    ingest_time TIMESTAMPTZ
);

-- Schema for the warehouse

-- dimension tables
DROP TABLE IF EXISTS dim_airline;
CREATE TABLE dim_airline(
    airline_id SERIAL PRIMARY KEY,
    iata TEXT UNIQUE,
    airline_name TEXT
);

DROP TABLE IF EXISTS dim_airport;
CREATE TABLE dim_airport(
    airport_id SERIAL PRIMARY KEY,
    iata TEXT UNIQUE,
    airport_name TEXT
);

DROP TABLE IF EXISTS dim_route;
CREATE TABLE dim_route(
    route_id SERIAL PRIMARY KEY,
    dep_airport_id INT REFERENCES dim_airport,
    arr_airport_id INT REFERENCES dim_airport,
    UNIQUE (dep_airport_id, arr_airport_id)
);

-- flight fact table
DROP TABLE IF EXISTS fact_flight_status;
CREATE TABLE fact_flight_status(
    flight_key TEXT PRIMARY KEY,
    airline_id INT REFERENCES dim_airline(airline_id),
    route_id INT REFERENCES dim_route(route_id),
    dep_scheduled TIMESTAMPTZ,
    dep_actual TIMESTAMPTZ,
    dep_delay_min NUMERIC,
    arr_scheduled TIMESTAMPTZ,
    arr_estimated TIMESTAMPTZ,
    arr_actual TIMESTAMPTZ,
    arr_delay_min NUMERIC,
    flight_status TEXT,
    ingest_time TIMESTAMPTZ
);

-- indexes 
CREATE INDEX IF NOT EXISTS idx_staging_dep_ts ON fact_flight_status_staging (dep_scheduled);
CREATE INDEX IF NOT EXISTS idx_fact_dep_ts ON fact_flight_status(dep_scheduled);
