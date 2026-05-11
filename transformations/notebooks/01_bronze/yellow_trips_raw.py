import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)
    

# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timezone
from modules.transformations.metadata import add_processed_timestamp
from modules.utils.date_utils import get_target_yyyymm

# COMMAND ----------

#obtain the year-month for 2 months prior to the current month in yyyy-MM format
formated_date = get_target_yyyymm(months_ago=2)

df = spark.read.format("parquet").load(f"/Volumes/nyctax/00_landing/data_sources/nyctaxi_yellow/{formated_date}")

# COMMAND ----------

df = add_processed_timestamp(df)


# COMMAND ----------

df.write.mode("append").saveAsTable("nyctax.01_bronze.yellow_trips_row")