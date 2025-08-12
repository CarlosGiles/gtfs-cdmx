select t.VALUE:type::VARCHAR as type,
       t.VALUE:host::VARCHAR as host,
       t.VALUE:port as port
FROM TABLE(FLATTEN(input => PARSE_JSON(SYSTEM$ALLOWLIST()))) AS t;

-- 1) Database del proyecto
CREATE DATABASE IF NOT EXISTS GTFS_CDMX;

-- 2) Esquemas por capa
CREATE SCHEMA IF NOT EXISTS GTFS_CDMX.BRONZE;
CREATE SCHEMA IF NOT EXISTS GTFS_CDMX.SILVER;
CREATE SCHEMA IF NOT EXISTS GTFS_CDMX.GOLD;

CREATE STAGE IF NOT EXISTS GTFS_CDMX.BRONZE.STG_GTFS
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);

-- Crear tablas vacías para luego subir from file asegurando que la info sea cruda en bronze
USE SCHEMA GTFS_CDMX.BRONZE;

CREATE OR REPLACE TABLE AGENCY (
  agency_id STRING, agency_name STRING, agency_url STRING,
  agency_timezone STRING, agency_lang STRING
);

CREATE OR REPLACE TABLE CALENDAR (
  service_id STRING, monday NUMBER, tuesday NUMBER, wednesday NUMBER,
  thursday NUMBER, friday NUMBER, saturday NUMBER, sunday NUMBER,
  start_date STRING, end_date STRING
);

CREATE OR REPLACE TABLE ROUTES (
  route_id STRING, agency_id STRING, route_short_name STRING,
  route_long_name STRING, route_type NUMBER, route_color STRING, route_text_color STRING
);

CREATE OR REPLACE TABLE TRIPS (
  route_id STRING, service_id STRING, trip_id STRING,
  trip_headsign STRING, trip_short_name STRING, direction_id NUMBER, shape_id STRING
);

CREATE OR REPLACE TABLE STOP_TIMES (
  trip_id STRING, timepoint NUMBER, stop_id STRING, stop_sequence NUMBER,
  arrival_time STRING, departure_time STRING
);

CREATE OR REPLACE TABLE STOPS (
  stop_id STRING, stop_name STRING, stop_lat FLOAT, stop_lon FLOAT,
  zone_id STRING, wheelchair_boarding NUMBER
);

CREATE OR REPLACE TABLE FREQUENCIES (
  end_time STRING, exact_times NUMBER, headway_secs NUMBER,
  start_time STRING, trip_id STRING
);

CREATE OR REPLACE TABLE SHAPES (
  shape_id STRING, shape_pt_sequence NUMBER, shape_pt_lon FLOAT,
  shape_dist_traveled FLOAT, shape_pt_lat FLOAT
);

-- checks rápidos
SELECT COUNT(*) FROM GTFS_CDMX.BRONZE.AGENCY;
SELECT COUNT(*) FROM GTFS_CDMX.BRONZE.ROUTES;        -- debe tener Metro/MB (ver agency_id/route_type)
SELECT COUNT(*) FROM GTFS_CDMX.BRONZE.TRIPS;
SELECT COUNT(*) FROM GTFS_CDMX.BRONZE.STOP_TIMES;    -- suele ser el más grande

-- ¿routes con agency desconocida?
SELECT COUNT(*) FROM BRONZE.ROUTES r
LEFT JOIN BRONZE.AGENCY a USING (agency_id)
WHERE a.agency_id IS NULL;

-- ¿stop_times con trip_id inexistente?
SELECT COUNT(*) FROM BRONZE.STOP_TIMES st
LEFT JOIN BRONZE.TRIPS t USING (trip_id)
WHERE t.trip_id IS NULL;

-- ¿trips con route_id inexistente?
SELECT COUNT(*) FROM BRONZE.TRIPS t
LEFT JOIN BRONZE.ROUTES r USING (route_id)
WHERE r.route_id IS NULL;