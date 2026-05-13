# Databricks notebook source
from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

# DBTITLE 1,Cell 2
spark.read.table("nyctax.01_bronze.yellow_trips_row") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()


# COMMAND ----------

spark.read.table("nyctax.`02_silver`.yellow_trips_cleansed") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

spark.read.table("nyctax.`02_silver`.yellow_trips_enriched") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

spark.read.table("nyctax.`03_gold`.daily_trip_summary") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(SUM("total_trips").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

spark.read.table("nyctax.`04_export`.yellow_trips_export") \
    .groupBy("year_month") \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

spark.read.table("nyctax.`02_silver`.taxi_zone_lookup").display()
