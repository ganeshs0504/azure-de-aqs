CREATE SCHEMA IF NOT EXISTS adls_warehouse_catalog.adls_warehouse
MANAGED LOCATION "abfss://aqs-warehouse@########.dfs.core.windows.net/";

CREATE SCHEMA IF NOT EXISTS adls_gold_layer_catalog.adls_data_marts
MANAGED LOCATION "abfss://gold@########.dfs.core.windows.net/";

-- CREATE DATABASE IF NOT EXISTS gv_aqs_managed_catalog.aqs_db
-- MANAGED LOCATION "abfss://aqs-warehouse@########.dfs.core.windows.net/";

use adls_warehouse_catalog.adls_warehouse;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.dim_unit(
    unit_surr_key STRING PRIMARY KEY,
    units_of_measure STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.dim_uncertainty(
    uncert_surr_key STRING PRIMARY KEY,
    uncertainty STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.dim_parameter(
    parameter_surr_key STRING PRIMARY KEY,
    parameter_code INT,
    parameter_name STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.dim_method(
    method_surr_key STRING PRIMARY KEY,
    method_code STRING,
    method_type STRING,
    method_name STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.dim_location(
    location_surr_key STRING PRIMARY KEY,
    state_code INT,
    county_code INT,
    site_num INT,
    latitude FLOAT,
    longitude FLOAT,
    state_name STRING,
    county_name STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.dim_datetime(
    date_surr_key STRING PRIMARY KEY,
    combined_datetime TIMESTAMP,
    extracted_date DATE,
    dayOfMonth INT,
    extracted_month INT,
    extracted_year INT,
    extracted_hour INT,
    extracted_minute INT,
    extracted_second INT
)USING DELTA;

DROP TABLE IF EXIStS adls_warehouse_catalog.adls_warehouse.fact_aqs;

CREATE TABLE IF NOT EXISTS adls_warehouse_catalog.adls_warehouse.fact_aqs(
    fact_surr_key STRING PRIMARY KEY,
    sample_measurement FLOAT,
    unit_surr_key STRING REFERENCES adls_warehouse_catalog.adls_warehouse.dim_unit(unit_surr_key),
    uncert_surr_key STRING REFERENCES adls_warehouse_catalog.adls_warehouse.dim_uncertainty(uncert_surr_key),
    parameter_surr_key STRING REFERENCES adls_warehouse_catalog.adls_warehouse.dim_parameter(parameter_surr_key),
    method_surr_key STRING REFERENCES adls_warehouse_catalog.adls_warehouse.dim_method(method_surr_key),
    location_surr_key STRING REFERENCES adls_warehouse_catalog.adls_warehouse.dim_location(location_surr_key),
    date_surr_key STRING REFERENCES adls_warehouse_catalog.adls_warehouse.dim_datetime(date_surr_key)
)USING DELTA
PARTITIONED BY (date_surr_key);

-- drop table adls_warehouse_catalog.adls_warehouse.dim_datetime;
-- drop table adls_warehouse_catalog.adls_warehouse.dim_location;
-- drop table adls_warehouse_catalog.adls_warehouse.dim_method;
-- drop table adls_warehouse_catalog.adls_warehouse.dim_parameter;
-- drop table adls_warehouse_catalog.adls_warehouse.dim_uncertainty;
-- drop table adls_warehouse_catalog.adls_warehouse.dim_unit;