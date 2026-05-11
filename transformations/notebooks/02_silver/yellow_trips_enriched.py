# Databricks notebook source
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

#load yellow_trips_cleansed from Silver layer(first part)
#and filter to only include trips with the pickup datetime

df_trips = spark.read.table("nyctax.02_silver.yellow_trips_cleansed").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}'")

# COMMAND ----------

#load taxi zone lookup from Silver layer
df_zones = spark.read.table("nyctax.02_silver.taxi_zone_lookup")

# COMMAND ----------

#join trips with pickup zone details (borough and zone name)
#join for pick up
df_join1 = df_trips.join(df_zones,
                        df_trips.pu_location_id == df_zones.location_id,
                        "left").select(
                            df_trips.vendor,
                            df_trips.tpep_pickup_datetime,
                            df_trips.tpep_dropoff_datetime,
                            df_trips.trip_duration,
                            df_trips.passenger_count,
                            df_trips.trip_distance,
                            df_trips.rate_type,
                            df_trips.store_and_fwd_flag,
                            df_zones.borough.alias("pu_borough"),
                            df_zones.zone.alias("pu_zone"),
                            df_trips.do_location_id,
                            df_trips.payment_type,
                            df_trips.fare_amount,
                            df_trips.extra,
                            df_trips.mta_tax,
                            df_trips.tip_amount,
                            df_trips.tolls_amount,
                            df_trips.improvement_surcharge,
                            df_trips.total_amount,
                            df_trips.congestion_surcharge,
                            df_trips.airport_fee,
                            df_trips.cbd_congestion_fee,
                            df_trips.processed_timestamp
                        
                        )

# COMMAND ----------

#join for drop off
df_join2 = df_join1.join(df_zones,
                        df_join1.do_location_id == df_zones.location_id,
                        "left").select(
                            df_join1.vendor,
                            df_join1.tpep_pickup_datetime,
                            df_join1.tpep_dropoff_datetime,
                            df_join1.trip_duration,
                            df_join1.passenger_count,
                            df_join1.trip_distance,
                            df_join1.rate_type,
                            df_join1.store_and_fwd_flag,
                            df_join1.pu_borough,
                            df_zones.borough.alias("do_borough"),
                            df_join1.pu_zone,
                            df_zones.zone.alias("do_zones"),
                            df_join1.payment_type,
                            df_join1.fare_amount,
                            df_join1.extra,
                            df_join1.mta_tax,
                            df_join1.tip_amount,
                            df_join1.tolls_amount,
                            df_join1.improvement_surcharge,
                            df_join1.total_amount,
                            df_join1.congestion_surcharge,
                            df_join1.airport_fee,
                            df_join1.cbd_congestion_fee,
                            df_join1.processed_timestamp
                        )

# COMMAND ----------

#write the enriched dataset to a Unity Catalog maaged table in the silver schema
df_join2.write.mode("append").saveAsTable("nyctax.02_silver.yellow_trips_enriched")