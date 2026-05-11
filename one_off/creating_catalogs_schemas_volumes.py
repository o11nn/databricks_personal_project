# Databricks notebook source
spark.sql("create catalog if not exists nyctax managed location 'abfss://unity-catalog-storage@dbstoragecrk3o4iqpmc4m.dfs.core.windows.net/7405615426711851'")

# COMMAND ----------

spark.sql("create schema if not exists nyctax.00_landing")
spark.sql("create schema if not exists nyctax.01_bronze")
spark.sql("create schema if not exists nyctax.02_silver")
spark.sql("create schema if not exists nyctax.03_gold")

# COMMAND ----------

spark.sql("create volume if not exists nyctax.00_landing.data_sources")