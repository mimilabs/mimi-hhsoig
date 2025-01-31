# Databricks notebook source
import pandas as pd
import re

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

path = "/Volumes/mimi_ws_1/hhsoig/src/Buprenorphine-Waivered Providers — County Data  Office of Inspector General  U.S. Department of Health and Human Services.csv"

pdf = pd.read_csv(path)

# COMMAND ----------

pdf.columns = change_header(pdf.columns)
pdf['total_number_of_waivered_providers'] = pd.to_numeric(pdf['total_number_of_waivered_providers'].str.replace(',',''))
pdf['patient_capacity'] = pd.to_numeric(pdf['patient_capacity'].str.replace(',',''))
pdf['patient_capacity_rate'] = pd.to_numeric(pdf['patient_capacity_rate'].str.replace(',',''))

# COMMAND ----------

df = spark.createDataFrame(pdf)
(df.write
    .format('delta')
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.hhsoig.buprenorphine_countydata"))

# COMMAND ----------


