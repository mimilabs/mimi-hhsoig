# Databricks notebook source
!pip install bs4

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
from dateutil.parser import parse
import pandas as pd
from datetime import datetime
url = "https://oig.hhs.gov"

# COMMAND ----------

item_urls = spark.read.table('mimi_ws_1.hhsoig.enforcement_summaries').toPandas()['url'].unique()

# COMMAND ----------

data = []
for page_num in range(1, 2):
    page = f"/fraud/enforcement/?page={page_num}"
    response = requests.get(f"{url}{page}")
    soup = BeautifulSoup(response.content, 'html.parser')
    for item in (soup.find('ul', class_='usa-card-group')
                .find_all('li', recursive=False)):
        link = item.find('a', href=True)
        item_url = url + link['href']
        if item_url in item_urls:
            continue
        title = link.text
        date = parse(item.find('span').text).date()
        tags = [x.text for x in item.find('ul').find_all('li', recursive=False)]
        row = [title, date, tags, item_url]
        data.append(row)

# COMMAND ----------

if len(data) > 0:
    pdf = pd.DataFrame(data, columns=['title', 'date', 'tags', 'url'])
    today = datetime.today().date()
    today_str = today.strftime('%Y-%m-%d')
    pdf['mimi_src_file_date'] = today
    pdf['mimi_src_file_name'] = url
    pdf['mimi_dlt_load_date'] = today
    (
        spark.createDataFrame(pdf).write.mode('append')
            .saveAsTable('mimi_ws_1.hhsoig.enforcement_summaries')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 2

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

from tqdm import tqdm
import time

# COMMAND ----------

df_summaries = spark.read.table('mimi_ws_1.hhsoig.enforcement_summaries')
df_details = (spark.read.table('mimi_ws_1.hhsoig.enforcement_details')
                .select('page_url'))
df_summaries = df_summaries.join(df_details, 
                                 on=(df_summaries.url == df_details.page_url), 
                                 how='left')
item_urls_crawl = df_summaries.where('page_url IS NULL').select('url').collect()

# COMMAND ----------

data = []
for item_url in tqdm(item_urls_crawl):
    try:
        response = requests.get(f"{item_url}")
    except Exception as e:
        # take a break - 10 second
        time.sleep(10)
        continue
    
    if response.status_code != 200:
        continue
    soup = BeautifulSoup(response.content, 'html.parser')
    d = {}
    article = soup.find('article')
    d["title"] = (article.find('h1').text)
    d["content"] = '\n'.join([p.text for p in 
                                article.select('p:not([style*="display:none"])')]).strip()
    d['page_url'] = item_url
    d['source_urls'] = [x['href'] for x in article.find_all('a', href=True)]
    for li in article.find('ul', class_='usa-list').find_all('li', recursive=False):
        span = li.find('span')
        if span is None:
            continue
        key = span.text.strip(':').lower().replace(' ', '_')  # Remove colon, convert to lowercase, and replace space with underscore
            # Get the text after the span
        if key not in {"date", "enforcement_types", "agency"}:
            continue
        value = span.next_sibling.strip()
        if key == 'enforcement_types':  # For enforcement types which has nested ul
            value = [x.text.strip() for x in li.find('ul').find_all('li')]
        elif key == 'date':
            try:
                value = parse(value).date()
            except Exception as e:
                value = None
        d[key] = value
    data.append(d)

if len(data) > 0:
    pdf = pd.DataFrame(data)
    today = datetime.today().date()
    today_str = today.strftime('%Y-%m-%d')
    pdf['mimi_src_file_date'] = today
    pdf['mimi_src_file_name'] = url
    pdf['mimi_dlt_load_date'] = today
    (
        spark.createDataFrame(pdf)
            .write.mode('append')
            .saveAsTable('mimi_ws_1.hhsoig.enforcement_details')
    )

# COMMAND ----------


