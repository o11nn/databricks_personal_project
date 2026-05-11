# Databricks notebook source
import sys
import os
#go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../..")) #os.getcwd() returns the current working directory

if project_root not in sys.path: #sys.path is a list of directory paths that Python searches for modules
    sys.path.append(project_root) 

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
    local_path = f"{dir_path}/taxi_zone_lookup.csv"
    
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uplouded")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File downloaded failed: {str(e)}")
