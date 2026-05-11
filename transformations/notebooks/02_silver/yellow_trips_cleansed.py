import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_month_start_n_months_ago
# COMMAND ----------

#get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

#get the first day of the month one month ago
one_month_ago_start = get_month_start_n_months_ago(1)

# COMMAND ----------

#read the yellow_trip_raw from the bronze table
#then filter rows where 'tpep_pickup_datetime' is >= two months ago start
#and < one month ago start 

df = spark.read.table("nyctax.01_bronze.yellow_trips_row").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}' and tpep_pickup_datetime < '{one_month_ago_start}'")

# COMMAND ----------

#Select and transform fields, decoding codes and computing duration
df = df.select(
    #map numeric VendorID to vendor names
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor")
    ,
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    timestamp_diff("MINUTE", df.tpep_pickup_datetime, df.tpep_dropoff_datetime).alias("trip_duration"),
    "passenger_count",
    "trip_distance",
    when(col("RatecodeID") == 1, "Standart Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated Fare")
        .when(col("RatecodeID") == 6, "Group ride")
        .otherwise("Unknown")
        .alias("rate_type")
    ,
    "store_and_fwd_flag",
    col("PuLocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided Trip")
        .otherwise("Unknown")
        .alias("payment_type"),
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee"). alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"
    
)
    
    

# COMMAND ----------

#write cleansed data to a Unity Catalog managed Delta table in the silver schema
df.write.mode("append").saveAsTable("nyctax.02_silver.yellow_trips_cleansed")