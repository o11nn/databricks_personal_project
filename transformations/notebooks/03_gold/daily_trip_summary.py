# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round
from dateutil.relativedelta import relativedelta
from datetime import date

# COMMAND ----------

#get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

df = spark.read.table("nyctax.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}'")

# COMMAND ----------

#aggregate trip data by pickup date with the key metricks 
df = df. \
    groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")). \
    agg(
        count("*").alias("total_trips"),                                #total number of trips per day
        round(avg("passenger_count"),1).alias("average_passengers"),    #average passengers per trip
        round(avg("trip_distance"),1).alias("average_distance"),        #average distance 
        round(avg("fare_amount"),1).alias("average_fare_per_trip"),     #average fare per trip
        max("fare_amount").alias("max_fare"),                           #highest single trip fare
        min("fare_amount").alias("min_fare"),                           #lowest single trip fare
        round(sum("total_amount"),2).alias("total_amount")              #total revenue for the day
    )

# COMMAND ----------

#write tha daily summary to a unity catalog managed Delta table in the gold schema 
df.write.mode("append").saveAsTable("nyctax.03_gold.daily_trip_summary")