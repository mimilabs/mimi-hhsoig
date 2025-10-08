# Databricks notebook source
!pip install bs4

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
from dateutil.parser import parse
import pandas as pd
from datetime import datetime
url = "https://oig.hhs.gov"
page = "/reports-and-publications/workplan/archives/index.asp"
volumepath = "/Volumes/mimi_ws_1/hhsoig/src/workplans/"

# COMMAND ----------

response = requests.get(f"{url}{page}")
soup = BeautifulSoup(response.content, 'html.parser')

# COMMAND ----------

download_urls = []
for a in soup.find_all('a', href=True):
    if a['href'].endswith('.xlsx'):
        download_urls.append(f"{url}{a['href']}")

# COMMAND ----------

download_files(download_urls, volumepath)

# COMMAND ----------


