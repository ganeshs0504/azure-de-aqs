CREATE OR REPLACE MATERIALIZED VIEW adls_gold_layer_catalog.adls_data_marts.reading_location_counts 
PARTITIONED BY (state_name, county_name)
AS(
    WITH fact_grouped_loc AS (
        SELECT location_surr_key, COUNT(1) AS total_readings
        FROM adls_warehouse_catalog.adls_warehouse.fact_aqs
        GROUP BY location_surr_key
    ),
    pre_aggregate AS (
        SELECT f_loc.location_surr_key, f_loc.total_readings, l.state_name, l.county_name, l.longitude, l.latitude
        FROM fact_grouped_loc f_loc
        JOIN adls_warehouse_catalog.adls_warehouse.dim_location l ON f_loc.location_surr_key = l.location_surr_key
        WHERE (l.longitude <> 0 AND l.latitude <> 0)
    )
    SELECT state_name, county_name, latitude, longitude, SUM(total_readings) AS total_readings
    FROM pre_aggregate
    GROUP BY state_name, county_name, latitude, longitude
);

CREATE OR REPLACE MATERIALIZED VIEW adls_gold_layer_catalog.adls_data_marts.combined_metrics_with_date
PARTITIONED BY (extracted_year, extracted_month, parameter_name)
AS(
    SELECT f.fact_surr_key, f.sample_measurement, dt.extracted_date, dt.extracted_year, dt.extracted_month, p.parameter_name, l.state_name, l.county_name
    FROM adls_warehouse_catalog.adls_warehouse.fact_aqs f
    JOIN adls_warehouse_catalog.adls_warehouse.dim_datetime dt ON f.date_surr_key = dt.date_surr_key
    JOIN adls_warehouse_catalog.adls_warehouse.dim_parameter p ON f.parameter_surr_key = p.parameter_surr_key
    JOIN adls_warehouse_catalog.adls_warehouse.dim_location l ON f.location_surr_key = l.location_surr_key
);


-- SELECT COUNT(*) 
-- FROM adls_gold_layer_catalog.adls_data_marts.reading_location_counts;