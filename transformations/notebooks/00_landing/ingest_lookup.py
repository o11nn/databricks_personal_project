# Databricks notebook source
import urllib.request
import os
import shutil

# COMMAND ----------

try:
    #construct the url for the Parquet file corresponding to this month
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    #open the connection and stream the remote file
    response = urllib.request.urlopen(url)

    #define and create the local directory for this dates data
    dir_path = f"/Volumes/nyctax/00_landing/data_sources/lookup"
    os.makedirs(dir_path, exist_ok=True)

    #define the full path for the downloaded file
    local_path = (f"{dir_path}/taxi_zone_lookup.csv")

    #save the streamed content to the local file in binary mode
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
        
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uplouded")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File downloaded failed: {str(e)}")
