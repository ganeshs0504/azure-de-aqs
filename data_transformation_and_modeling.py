# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel
from delta.tables import DeltaTable
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

storage_account_name = "##########"
container_name = "silver"
storage_account_key = "##############################################"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

year = dbutils.widgets.get("year")
print(f"Processing Year: {year}")

gas_df = spark.read.format("delta").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gases/extracted_year={year}")
met_df = spark.read.format("delta").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/met/extracted_year={year}")

combined_df = gas_df.union(met_df)

combined_df = combined_df.withColumn("combined_datetime", F.expr("make_timestamp(year(date_gmt), month(date_gmt), day(date_gmt), hour(time_gmt), minute(time_gmt), second(time_gmt))"))

columns_to_be_dropped = ["poc", "datum", "date_local", "time_local", "date_gmt", "time_gmt", "mdl", "uncertainity", "qualifier", "date_of_last_change"]
combined_df = combined_df.drop(*columns_to_be_dropped)

combined_df = combined_df.dropna(subset=["state_code"])

combined_df = combined_df.withColumn("latitude", F.round("latitude", 5).alias("latitude"))\
    .withColumn("longitude", F.round("longitude", 5).alias("longitude"))

counts_df = combined_df.groupBy("state_code", "county_code", "site_num", "latitude", "longitude") \
    .agg(F.count("*").alias("count"))

window_spec = Window.partitionBy("state_code", "county_code", "site_num").orderBy(F.desc("count"))
lat_lon_mod_diff = counts_df.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("count", "rank")

combined_df = combined_df.drop("latitude", "longitude")\
.join(lat_lon_mod_diff, ["state_code", "county_code", "site_num"], "left")

dim_unit_table = DeltaTable.forName(spark, "adls_warehouse_catalog.adls_warehouse.dim_unit")
dim_unit = combined_df.select("units_of_measure").distinct() \
.withColumn("unit_surr_key", F.sha2(F.col("units_of_measure"), 256))
dim_unit_table.alias("existing").merge(
    dim_unit.alias("new"),
    "existing.units_of_measure = new.units_of_measure"
).whenNotMatchedInsertAll().execute()
# dim_unit.write.format("delta").mode("overwrite").saveAsTable("adls_warehouse_catalog.adls_warehouse.dim_unit")

dim_uncertainty_table = DeltaTable.forName(spark, "adls_warehouse_catalog.adls_warehouse.dim_uncertainty")
dim_uncertainty = combined_df.select("uncertainty").distinct() \
.withColumn("uncert_surr_key", F.sha2(F.coalesce(F.col("uncertainty"), F.lit("NULL")), 256))
dim_uncertainty_table.alias("existing").merge(
    dim_uncertainty.alias("new"),
    "existing.uncertainty = new.uncertainty"
).whenNotMatchedInsertAll().execute()
# dim_uncertainty.write.format("delta").mode("overwrite").saveAsTable("adls_warehouse_catalog.adls_warehouse.dim_uncertainty")

dim_parameter_table = DeltaTable.forName(spark, "adls_warehouse_catalog.adls_warehouse.dim_parameter")
dim_parameter = combined_df.select("parameter_code", "parameter_name").distinct()\
.withColumn("parameter_surr_key", F.sha2(F.concat_ws("_", F.col("parameter_code"), F.col("parameter_name")), 256))
dim_parameter_table.alias("existing").merge(
    dim_parameter.alias("new"),
    "existing.parameter_code = new.parameter_code"
).whenNotMatchedInsertAll().execute()
# dim_parameter.write.format("delta").mode("overwrite").saveAsTable("adls_warehouse_catalog.adls_warehouse.dim_parameter")

dim_method_table = DeltaTable.forName(spark, "adls_warehouse_catalog.adls_warehouse.dim_method")
dim_method = combined_df.select("method_type", "method_code", "method_name").distinct()\
    .withColumn("method_surr_key", F.sha2(F.concat_ws("_", F.col("method_type"), F.col("method_code"), F.col("method_name")), 256))
dim_method_table.alias("existing").merge(
    dim_method.alias("new"),
    """
    existing.method_type = new.method_type AND
    existing.method_code = new.method_code AND
    existing.method_name = new.method_name
    """
).whenNotMatchedInsertAll().execute()
# dim_method.write.format("delta").mode("overwrite").saveAsTable("adls_warehouse_catalog.adls_warehouse.dim_method")

dim_location_table = DeltaTable.forName(spark, "adls_warehouse_catalog.adls_warehouse.dim_location")
dim_location = combined_df.select("state_code", "county_code", "site_num", "latitude", "longitude", "state_name", "county_name").distinct()\
    .withColumn("location_surr_key", F.sha2(F.concat_ws("_", F.col("state_code"), F.col("county_code"), F.col("site_num")), 256))
dim_location_table.alias("existing").merge(
    dim_location.alias("new"),
    """
    existing.state_code = new.state_code AND
    existing.county_code = new.county_code AND
    existing.site_num = new.site_num
    """
).whenNotMatchedInsertAll().execute()
# dim_location.write.format("delta").mode("overwrite").saveAsTable("adls_warehouse_catalog.adls_warehouse.dim_location")

dim_date_table = DeltaTable.forName(spark, "adls_warehouse_catalog.adls_warehouse.dim_datetime")
dim_date = combined_df.select("combined_datetime").distinct()\
    .withColumn("date_surr_key", F.sha2(F.col("combined_datetime").cast("string"), 256))\
        .withColumn("extracted_date", F.to_date("combined_datetime"))\
            .withColumn("extracted_year", F.year("combined_datetime"))\
                .withColumn("extracted_month", F.month("combined_datetime"))\
                    .withColumn("dayOfMonth", F.dayofmonth("combined_datetime"))\
                        .withColumn("extracted_hour", F.hour("combined_datetime"))\
                            .withColumn("extracted_minute", F.minute("combined_datetime"))\
                                .withColumn("extracted_second", F.second("combined_datetime"))
dim_date_table.alias("existing").merge(
    dim_date.alias("new"),
    "existing.combined_datetime = new.combined_datetime"
).whenNotMatchedInsertAll().execute()
# dim_date.write.format("delta").mode("overwrite").saveAsTable("adls_warehouse_catalog.adls_warehouse.dim_datetime")

fact_aqs = combined_df.alias("a")\
.join(F.broadcast(dim_unit).alias("u"), ["units_of_measure"], "left")\
.join(F.broadcast(dim_location).alias("l"), ["state_code", "county_code", "site_num"], "left")\
.join(F.broadcast(dim_parameter).alias("p"), ["parameter_code"], "left")\
.join(F.broadcast(dim_uncertainty).alias("uncert"), ["uncertainty"], "left")\
.join(F.broadcast(dim_date).alias("d"), ["combined_datetime"], "left")\
.join(F.broadcast(dim_method).alias("m"), ["method_code"], "left")\
.withColumn("fact_surr_key", F.sha2(F.concat_ws("_", "state_code", "county_code", "site_num", 
                                            "units_of_measure", "parameter_code", 
                                            F.coalesce(F.col("uncertainty"), F.lit("NULL")), "combined_datetime", "method_code"), 256))\
.selectExpr(
    "fact_surr_key",
    "a.sample_measurement",
    "u.unit_surr_key",
    "l.location_surr_key",
    "p.parameter_surr_key",
    "uncert.uncert_surr_key",
    "d.date_surr_key",
    "m.method_surr_key"
)

fact_aqs.write.format("delta").mode("append").partitionBy("date_surr_key").saveAsTable("adls_warehouse_catalog.adls_warehouse.fact_aqs")
