# Databricks notebook source
# MAGIC %md
# MAGIC Which vendor makes revenue?
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.table("nyctax.02_silver.yellow_trips_enriched")

df.display()


# COMMAND ----------

result = spark.sql(
    """
        select vendor, sum(no2.total_amount) as max_amount
        from nyctax.02_silver.yellow_trips_enriched no2
        group by vendor
        order by max_amount desc
    """
)

# COMMAND ----------

result.display()

# COMMAND ----------

df.groupBy("vendor"). \
    agg(round(sum("total_amount"),2).alias("max_amount")). \
    orderBy("max_amount", ascending=False). \
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC What the most popular pickup borough?

# COMMAND ----------

df.groupBy("pu_borough"). \
    agg(count("*").alias("count_borough")). \
    orderBy("count_borough", ascending=False). \
    display()

# COMMAND ----------

spark.sql("""
          select pu_borough, count(*) as most_popular_pu_borough
          from nyctax.02_silver.yellow_trips_enriched
          group by pu_borough
          order by most_popular_pu_borough desc


        
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC what is the most common jorney

# COMMAND ----------

df.groupBy(concat("pu_borough", lit(" --> "), "do_borough").alias("the_road")). \
    agg(count("*").alias("count_borough")). \
    orderBy("count_borough", ascending=False). \
    display()

# COMMAND ----------

spark.sql(
    """
    select pu_borough, ("--> ") as the_road,  do_borough, count(*) as count_borough
    from nyctax.02_silver.yellow_trips_enriched
    group by pu_borough, do_borough
    order by count_borough desc
    
    
    """
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a time series chart showing the number of trips and total revenue per day

# COMMAND ----------

df2 = spark.read.table("nyctax.03_gold.daily_trip_summary")

df2.display()
