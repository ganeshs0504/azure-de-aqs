CREATE DATABASE IF NOT EXISTS aqs_db;
use aqs_db;

CREATE TABLE IF NOT EXISTS aqs_db.dim_unit(
    unit_surr_key BIGINT PRIMARY KEY,
    units_of_measure STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS aqs_db.dim_uncertainty(
    uncert_surr_key BIGINT PRIMARY KEY,
    uncertainty STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS aqs_db.dim_parameter(
    parameter_surr_key BIGINT PRIMARY KEY,
    parameter_code INT,
    parameter_name STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS aqs_db.dim_method(
    method_surr_key BIGINT PRIMARY KEY,
    method_code STRING,
    method_type STRING,
    method_name STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS aqs_db.dim_location(
    location_surr_key BIGINT PRIMARY KEY,
    state_code INT,
    county_code INT,
    site_num INT,
    latitude FLOAT,
    longitude FLOAT,
    state_name STRING,
    county_name STRING
)USING DELTA;

CREATE TABLE IF NOT EXISTS aqs_db.dim_datetime(
    date_surr_key BIGINT PRIMARY KEY,
    combined_datetime TIMESTAMP,
    extracted_date DATE,
    dayOfMonth INT,
    extracted_month INT,
    extracted_year INT,
    extracted_hour INT,
    extracted_minute INT,
    extracted_second INT
)USING DELTA;

CREATE TABLE IF NOT EXISTS aqs_db.fact_aqs(
    fact_surr_key BIGINT PRIMARY KEY,
    sample_measurement FLOAT,
    unit_surr_key BIGINT REFERENCES aqs_db.dim_unit(unit_surr_key),
    uncert_surr_key BIGINT REFERENCES aqs_db.dim_uncertainty(uncert_surr_key),
    parameter_surr_key BIGINT REFERENCES aqs_db.dim_parameter(parameter_surr_key),
    method_surr_key BIGINT REFERENCES aqs_db.dim_method(method_surr_key),
    location_surr_key BIGINT REFERENCES aqs_db.dim_location(location_surr_key),
    date_surr_key BIGINT REFERENCES aqs_db.dim_datetime(date_surr_key)
)USING DELTA
PARTITIONED BY (date_surr_key);

-- drop table aqs_db.dim_datetime;
-- drop table aqs_db.dim_location;
-- drop table aqs_db.dim_method;
-- drop table aqs_db.dim_parameter;
-- drop table aqs_db.dim_uncertainty;
-- drop table aqs_db.dim_unit;
-- drop table aqs_db.fact_aqs;