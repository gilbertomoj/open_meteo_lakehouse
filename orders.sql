-- Data Contract: my-data-contract-id
-- SQL Dialect: databricks
CREATE OR REPLACE TABLE workspace.base.open_meteo_bronze (
  date TIMESTAMP COMMENT "",
  temperature_2m DOUBLE COMMENT "",
  relative_humidity_2m DOUBLE COMMENT "",
  rain DOUBLE COMMENT "",
  city STRING COMMENT "",
  latitude DOUBLE COMMENT "",
  longitude DOUBLE COMMENT "",
  elevation DOUBLE COMMENT ""
);