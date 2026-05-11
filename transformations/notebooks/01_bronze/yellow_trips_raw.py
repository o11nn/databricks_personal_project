# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timezone

# COMMAND ----------

#obtain the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formated_date = two_months_ago.strftime("%Y-%m")

df = spark.read.format("parquet").load(f"/Volumes/nyctax/00_landing/data_sources/nyctaxi_yellow/{formated_date}")

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())


# COMMAND ----------

df.write.mode("append").saveAsTable("nyctax.01_bronze.yellow_trips_row")