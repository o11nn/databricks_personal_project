# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

# COMMAND ----------

#define the local directory for this date's data
dir_path = f"/Volumes/nyctax/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

# COMMAND ----------

#define the full path for the downloaded file
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# COMMAND ----------

try:
    #check if the file already exists 
    dbutils.fs.ls(local_path)

    #if the file already exists then set continue_downstream to yes
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already donloaded, aborting downstream tasks")
except:
    try:
        #Construct the url for the parquet file 
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"
        response = urllib.request.urlopen(url)
        
        #create the local directory if it doesn't exist
        os.makedirs(dir_path, exist_ok=True)

        #save the streamed content 
        with open(local_path, "wb") as f:
            shutil.copyfileobj(response, f)

        #set continue_downstream to yes if the file was loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File succesfully uploaded in current run")
    except Exception as e:
        #set continue_downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File dowloaded failed: {str(e)}")

# COMMAND ----------


