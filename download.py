# Databricks notebook source

import requests
from pathlib import Path
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

t = datetime.datetime.now().strftime('%Y%m%d')

# COMMAND ----------

url = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
volumepath = "/Volumes/mimi_ws_1/hhsoig/src"

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

download_file(url, f"leie_{t}.csv", "/Volumes/mimi_ws_1/hhsoig/src/")

# COMMAND ----------


