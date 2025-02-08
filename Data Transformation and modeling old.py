# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

storage_account_name = "###########"
container_name = "silver"
storage_account_key = "#####################################"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

gas_df = spark.read.format("delta").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gases/extracted_year=2020")
met_df = spark.read.format("delta").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/met")

combined_df = gas_df.union(met_df)

# combined_df = gas_df.union(met_df)
combined_df.cache()
combined_df.count()

# COMMAND ----------

combined_df = combined_df.withColumn("combined_datetime", F.expr("make_timestamp(year(date_gmt), month(date_gmt), day(date_gmt), hour(time_gmt), minute(time_gmt), second(time_gmt))"))

columns_to_be_dropped = ["poc", "datum", "date_local", "time_local", "date_gmt", "time_gmt", "mdl", "uncertainity", "qualifier", "date_of_last_change"]
combined_df = combined_df.drop(*columns_to_be_dropped)

combined_df = combined_df.dropna(subset=["state_code"])

combined_df = combined_df.withColumn("latitude", F.round("latitude", 2).alias("latitude"))\
    .withColumn("longitude", F.round("longitude", 2).alias("longitude"))

combined_df.cache()
combined_df.count()

# COMMAND ----------

# repartition_keys = ["state_code", "county_code", "site_num"]
# combined_df = combined_df.repartition(*repartition_keys)

# COMMAND ----------

counts_df = combined_df.groupBy("state_code", "county_code", "site_num", "latitude", "longitude") \
    .agg(F.count("*").alias("count"))
# counts_df.cache()
# counts_df.count()

window_spec = Window.partitionBy("state_code", "county_code", "site_num").orderBy(F.desc("count"))
lat_lon_mod_diff = counts_df.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("count", "rank")
# lat_lon_mod_diff.cache()
# lat_lon_mod_diff.count()

# COMMAND ----------

combined_df = combined_df.drop("latitude", "longitude")\
    .join(lat_lon_mod_diff, ["state_code", "county_code", "site_num"], "left")

# combined_df = combined_df.orderBy("combined_datetime")

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing to table (Data Warehouse)

# COMMAND ----------

dim_unit = combined_df.select("units_of_measure").distinct() \
    .withColumn("unit_surr_key", F.monotonically_increasing_id())

dim_unit.write.format("delta").mode("overwrite").saveAsTable("aqs_db.dim_unit")

# COMMAND ----------

dim_uncertainty = combined_df.select("uncertainty").distinct() \
    .withColumn("uncert_surr_key", F.monotonically_increasing_id())

dim_uncertainty.write.format("delta").mode("overwrite").saveAsTable("aqs_db.dim_uncertainty")

# COMMAND ----------

dim_parameter = combined_df.select("parameter_name", "parameter_code").distinct()\
    .withColumn("parameter_surr_key", F.monotonically_increasing_id())

dim_parameter.write.format("delta").mode("overwrite").saveAsTable("aqs_db.dim_parameter")

# COMMAND ----------

dim_method = combined_df.select("method_type", "method_code", "method_name").distinct()\
    .withColumn("method_surr_key", F.monotonically_increasing_id())

dim_method.write.format("delta").mode("overwrite").saveAsTable("aqs_db.dim_method")

# COMMAND ----------

dim_location = combined_df.select("state_code", "county_code", "site_num", "latitude", "longitude", "state_name", "county_name").distinct()\
    .withColumn("location_surr_key", F.monotonically_increasing_id())

dim_location.write.format("delta").mode("overwrite").saveAsTable("aqs_db.dim_location")

# COMMAND ----------

dim_date = combined_df.select("combined_datetime").distinct()\
    .withColumn("date_surr_key", F.monotonically_increasing_id())\
        .withColumn("extracted_date", F.to_date("combined_datetime"))\
            .withColumn("extracted_year", F.year("combined_datetime"))\
                .withColumn("extracted_month", F.month("combined_datetime"))\
                    .withColumn("dayOfMonth", F.dayofmonth("combined_datetime"))\
                        .withColumn("extracted_hour", F.hour("combined_datetime"))\
                            .withColumn("extracted_minute", F.minute("combined_datetime"))\
                                .withColumn("extracted_second", F.second("combined_datetime"))
    
dim_date.write.format("delta").mode("overwrite").saveAsTable("aqs_db.dim_datetime")

# COMMAND ----------

fact_aqs = combined_df.alias("a")\
    .join(F.broadcast(dim_unit).alias("u"), ["units_of_measure"], "left")\
        .join(F.broadcast(dim_location).alias("l"), ["state_code", "county_code", "site_num"], "left")\
            .join(F.broadcast(dim_parameter).alias("p"), ["parameter_code"], "left")\
                .join(F.broadcast(dim_uncertainty).alias("uncert"), ["uncertainty"], "left")\
                    .join(F.broadcast(dim_date).alias("d"), ["combined_datetime"], "left")\
                        .join(F.broadcast(dim_method).alias("m"), ["method_code"], "left")\
                            .selectExpr(
                                "monotonically_increasing_id() AS fact_surr_key",
                                "a.sample_measurement",
                                "u.unit_surr_key",
                                "l.location_surr_key",
                                "p.parameter_surr_key",
                                "uncert.uncert_surr_key",
                                "d.date_surr_key",
                                "m.method_surr_key"
                            )

fact_aqs.write.format("delta").mode("overwrite").saveAsTable("aqs_db.fact_aqs")
