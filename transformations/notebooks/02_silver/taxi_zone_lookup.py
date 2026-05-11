# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import TimestampType, IntegerType, StringType
from datetime import datetime

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



# COMMAND ----------

#fixed in-point-time used to "close" any changed active records
#using Python timestamp ensures the exact same value is written and can be referenced if needed
end_timestamp = datetime.now()

#load the SCD2 delta table
dt = DeltaTable.forName(spark, "nyctax.02_silver.taxi_zone_lookup")

# COMMAND ----------

#merge 1
#merge only the *active* target rows (where end_date is null) with the same key
#if there any tracked column differs, set end_date to end_timestamp to retire that version

dt.alias("t").\
    merge(
        source = df.alias("s"),
        condition = "t.location_id = s.location_id AND t.end_date IS NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)"
    ). \
    whenMatchedUpdate(
        set = {"t.end_date": lit(end_timestamp).cast(TimestampType())}
    ). \
    execute()

# COMMAND ----------

#merge 2
#now insert a row for:
# - keys we just closed in merge1
# - brand-new keys not present in the target table

#get the lists of ids that have been closed 
insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id").collect()]

#if the list is empty, don't try to insert anything
if len(insert_id_list) == 0:
    print("no updated records to insert")
else:
    dt.alias("t").\
        merge(
            source = df.alias("s"),
            condition = f"s.location_id not in ({','.join(map(str,insert_id_list))})"
        ). \
        whenNotMatchedInsert(
            values = {
                "t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone": "s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType())
            }
        ). \
        execute()

# COMMAND ----------

#merge 3
dt.alias("t"). \
    merge(
        source = df.alias("s"),
        condition = "t.location_id = s.location_id"
    ). \
    whenNotMatchedInsert(
        values = {
            "t.location_id": "s.location_id",
            "t.borough": "s.borough",
            "t.zone": "s.zone",
            "t.service_zone": "s.service_zone",
            "t.effective_date": current_timestamp(),
            "t.end_date": lit(None).cast(TimestampType())
        }
    ). \
    execute()

# COMMAND ----------

#df.write.mode("overwrite").saveAsTable("nyctax.02_silver.taxi_zone_lookup")