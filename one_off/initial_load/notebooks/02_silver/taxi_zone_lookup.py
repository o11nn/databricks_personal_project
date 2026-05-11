# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

df = spark.read.format("csv").options(header=True).load("/Volumes/nyctax/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctax.02_silver.taxi_zone_lookup")

# COMMAND ----------

spark.read.table("nyctax.02_silver.taxi_zone_lookup").display()