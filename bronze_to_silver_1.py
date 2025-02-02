# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DateType, FloatType
from pyspark.sql.functions import year, col

# COMMAND ----------

gas_schema = StructType([
    StructField("State Code", IntegerType(), False),
    StructField("County Code", IntegerType(), False),
    StructField("Site Num", IntegerType(), False),
    StructField("Parameter Code", IntegerType(), False),
    StructField("POC", StringType(), True),
    StructField("Latitude", FloatType(), False),
    StructField("Longitude", FloatType(), False),
    StructField("Datum", StringType(), True),
    StructField("Parameter Name", StringType(), False),
    StructField("Date Local", StringType(), True),
    StructField("Time Local", StringType(), True),
    StructField("Date GMT", DateType(), False),
    StructField("Time GMT", TimestampType(), False),
    StructField("Sample Measurement", FloatType(), True),
    StructField("Units of Measure", StringType(), True),
    StructField("MDL", StringType(), True),
    StructField("Uncertainty", StringType(), True),
    StructField("Qualifier", StringType(), True),
    StructField("Method Type", StringType(), True),
    StructField("Method Code", StringType(), True),
    StructField("Method Name", StringType(), True),
    StructField("State Name", StringType(), False),
    StructField("County Name", StringType(), False),
    StructField("Date of Last Change", StringType(), True)
])

# COMMAND ----------

storage_account_name = "#############"
container_name = "bronze"
storage_account_key = "###############################################"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

df = spark.read.format("csv").schema(gas_schema).option("header", "true").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/*/gases")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("extracted_year", year(col("Date GMT")))

# COMMAND ----------

for old_col in df.columns:
    df = df.withColumnRenamed(old_col, old_col.lower().replace(" ", "_"))

# COMMAND ----------

df.write.mode("overwrite").format("delta").partitionBy("extracted_year").save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/gases/")

# COMMAND ----------

met_df = spark.read.format("csv").option("header", "true").schema(gas_schema).load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/*/met")
met_df = met_df.withColumn("extracted_year", year(col("Date GMT")))
for old_col in met_df.columns:
    met_df = met_df.withColumnRenamed(old_col, old_col.lower().replace(" ", "_"))

# COMMAND ----------

met_df.write.mode("overwrite").format("delta").partitionBy("extracted_year").save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/met/")

# COMMAND ----------

