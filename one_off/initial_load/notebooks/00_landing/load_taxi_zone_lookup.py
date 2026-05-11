# Databricks notebook source
import os
import urllib.request
import shutil

# COMMAND ----------

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

response = urllib.request.urlopen(url)

dir_path = "/Volumes/nyctax/00_landing/data_sources/lookup"
os.makedirs(dir_path, exist_ok=True)

local_path = f"{dir_path}/taxi_zone_lookup.csv"

with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)