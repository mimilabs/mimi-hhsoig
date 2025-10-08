# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

!pip install bs4

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
from dateutil.parser import parse
import pandas as pd
import json
from datetime import datetime
import pyspark.sql.functions as F
import re

url = "https://oig.hhs.gov"
page0 = "/reports/all/"
volumepath = "/Volumes/mimi_ws_1/hhsoig/src/reports/titles/"

def extract_card_info(soup):
    
    # Extract title and URL
    heading = soup.find('h2', class_='usa-card__heading')
    link = heading.find('a') if heading else None
    
    title = link.text.strip() if link else None
    url = link['href'] if link else None
    
    # Extract metadata
    metadata = {}
    metadata_items = soup.find_all('div', class_='grid-col display-flex flex-column')
    
    for item in metadata_items:
        term_elem = item.find('dt', class_='pep-metadata__term')
        def_elem = item.find('dd', class_='pep-metadata__def')
        
        if term_elem and def_elem:
            term = term_elem.text.strip().lower()
            term = format_header_varname2(term)
            definition = def_elem.text.strip()
            if term in {'audit', 'evaluation', 'sar', 'tmc', 'hcfac', 
                        'top_unimp_recs', 'foia', 'medicaid_integrity', 'additional_reports', 'partnerships'}:
                metadata['report_type'] = term
                metadata['report_id'] = definition
            else:
                metadata[term] = definition
    
    result = {
        **metadata,
        'title': title,
        'url': url
    }

    return result

# COMMAND ----------

def extract_report_summaries(soup):
    
    result = {
        'title': None,
        'issued_date': None,
        'posted_date': None,
        'report_number': None,
        'report_materials': [],
        'recommendations': None,  # Store as JSON string
        'num_recommendations': 0,
        'report_type': None,  # Pipe-separated string
        'hhs_agencies': None,  # Pipe-separated string
        'issue_areas': None,  # Pipe-separated string
        'target_groups': None,  # Pipe-separated string
        'financial_groups': None,  # Pipe-separated string
        'notice': None
    }
    
    # Extract title
    title_elem = soup.find('h1')
    if title_elem:
        result['title'] = title_elem.get_text(strip=True)
    
    # Extract issued date, posted date, and report number
    # Look for the div in the breadcrumb section that contains date info
    date_section = soup.find('div', class_='grid-col')
    if date_section:
        # Find all text in this section
        date_div = date_section.find('div')
        if date_div:
            full_text = date_div.get_text(strip=True)
            
            # Extract report number from the text
            report_match = re.search(r'Report number:\s*([\w-]+)', full_text)
            if report_match:
                result['report_number'] = report_match.group(1)
        
        # Extract dates from <time> elements
        time_elements = date_section.find_all('time')
        for i, time_elem in enumerate(time_elements):
            text = time_elem.get_text(strip=True)
            
            # First time element is issued date, second is posted date
            if i == 0:
                result['issued_date'] = text
            elif i == 1:
                result['posted_date'] = text
    
    # Extract report materials (PDFs) - keep as array of dicts
    report_materials_list = soup.find('ul', class_='usa-icon-list')
    if report_materials_list:
        for item in report_materials_list.find_all('li', class_='usa-icon-list__item'):
            link = item.find('a')
            if link and link.get('href'):
                result['report_materials'].append({
                    'text': link.get_text(strip=True),
                    'url': link['href']
                })
    
    # Extract summary sections - use flexible pattern matching
    summary_sections = {}
    for section in soup.find_all('h3'):
        section_title = section.get_text(strip=True)
        
        content_parts = []
        for sibling in section.find_next_siblings():
            if sibling.name == 'h3':
                break
            if sibling.name == 'p':
                content_parts.append(sibling.get_text(strip=True))
        
        if content_parts:
            # Create a simplified key from the section title
            # Convert to lowercase, remove special chars, replace spaces with underscores
            key = 'summary_' + re.sub(r'[^\w\s]', '', section_title.lower()).replace(' ', '_')
            summary_sections[key] = ' '.join(content_parts)

    # Add all summary sections to result
    result.update(summary_sections)
    
    # Extract recommendations - store as JSON string for complex data
    recommendations = []
    for div in soup.find_all('div', class_='text-bold'):
        text = div.get_text(strip=True)
        
        if re.search(r'\d+-[A-Z]-\d+-\d+\.\d+', text):
            rec = {}
            
            parts = text.split(' - ')
            if len(parts) >= 3:
                rec['id'] = parts[0].strip()
                rec['agency'] = parts[1].strip()
                rec['status'] = parts[2].strip()
            
            date_div = div.find_next_sibling('div', class_='text-italic')
            if date_div:
                rec['closed_date'] = date_div.get_text(strip=True).replace('Closed on', '').strip()
            
            text_div = div.find_next_sibling('div', class_='grid-row padding-bottom-3 usa-prose')
            if text_div:
                rec['recommendation'] = text_div.get_text(strip=True)
            
            recommendations.append(rec)
    
    result['recommendations'] = json.dumps(recommendations) if recommendations else None
    result['num_recommendations'] = len(recommendations)
    
    # Extract metadata tags - flatten to pipe-separated strings
    term_mapping = {
        'Report Type': 'report_type',
        'HHS Agencies': 'hhs_agencies',
        'Issue Areas': 'issue_areas',
        'Target Groups': 'target_groups',
        'Financial Groups': 'financial_groups'
    }
    
    for section in soup.find_all('div', class_='grid-col display-flex flex-column'):
        term_elem = section.find('dt', class_='pep-metadata__term')
        if not term_elem:
            continue
        
        term = term_elem.get_text(strip=True)
        
        if term in term_mapping:
            tags = section.find_all('span', class_='usa-tag')
            tag_values = [tag.get_text(strip=True) for tag in tags]
            result[term_mapping[term]] = ' | '.join(tag_values) if tag_values else None
    
    # Extract notice
    notice_alert = soup.find('div', class_='usa-alert')
    if notice_alert:
        notice_text = notice_alert.find('p', class_='usa-alert-text')
        if notice_text:
            result['notice'] = notice_text.get_text(strip=True)
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest the titles

# COMMAND ----------

existing_reports = {x[0] for x in (spark.read.table('mimi_ws_1.hhsoig.all_reports_titles')
                                .select('report_id')
                                .collect())}

# COMMAND ----------

for pagenum in range(0, 395):
    page = f"/reports/all/?hhs-agency=all&issue-date=all&page={pagenum}"
    response = requests.get(f"{url}{page}")
    soup = BeautifulSoup(response.content, 'html.parser')
    files_exist = False
    for card_html in soup.find_all("li", class_="usa-card"):
        card_json = extract_card_info(card_html)

        if card_json['report_id'] in existing_reports:
            files_exist = True
            break

        fn = f"{volumepath}{card_json['report_id']}.json"
        with open(fn, 'w') as f:
            json.dump(card_json, f, indent=2)
    if files_exist:
        break

# COMMAND ----------

data = []
for filepath in Path(volumepath).glob('*.json'):
    if filepath.stem in existing_reports:
        continue
    with open(filepath, 'r') as f:
        card_json = json.load(f)
        card_json['issued'] = parse(card_json.get('issued')).date()
        data.append(card_json)

if len(data) > 0:
    pdf = pd.DataFrame(data)
    pdf['mimi_src_file_date'] = pdf['issued']
    pdf['mimi_src_file_name'] = 'https://oig.hhs.gov/reports/all/'
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('append')
            .saveAsTable('mimi_ws_1.hhsoig.all_reports_titles')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest the summaries

# COMMAND ----------

df = (spark.read.table('mimi_ws_1.hhsoig.all_reports_titles'))

# COMMAND ----------

report_id2info = {row['report_id']: {'issued': row['issued'],
                                    'report_type': row['report_type'],
                                    'url': row['url']}
                    for row in df.collect()}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/hhsoig/src/reports/summaries/"

# COMMAND ----------

existing_summaries = {x[0] for x in (spark.read.table('mimi_ws_1.hhsoig.all_reports_summaries')
                                .select('report_id')
                                .collect())}

# COMMAND ----------

for report_id, info in report_id2info.items():
    if report_id in existing_summaries:
        continue
    response = requests.get(url + info['url'])
    soup = BeautifulSoup(response.content, 'html.parser')
    result = extract_report_summaries(soup.find('main'))
    fn = f"{volumepath}{report_id}.json"
    with open(fn, 'w') as f:
        json.dump(result, f, indent=2)

# COMMAND ----------

data = []
for filepath in Path(volumepath).glob('*.json'):
    if filepath.stem in existing_summaries:
        continue
    with open(filepath, 'r') as f:
        summary_json = json.load(f)
        row_json = {'summaries': {}}
        for k, v in summary_json.items():
            if k.startswith('summary'):
                row_json['summaries'][k] = v
            else:
                row_json[k] = v
        info = report_id2info[filepath.stem]
        row_json['report_id'] = filepath.stem
        row_json['report_type'] = info['report_type']
        row_json['url'] = info['url']
        data.append(row_json)
if len(data) > 0:
    pdf = pd.DataFrame(data)
    pdf['issued_date'] = pd.to_datetime(pdf['issued_date'], errors='coerce', format='%m/%d/%Y').dt.date
    pdf['posted_date'] = pd.to_datetime(pdf['posted_date'], errors='coerce', format='%m/%d/%Y').dt.date
    pdf['mimi_src_file_date'] = pdf['issued_date']
    pdf['mimi_src_file_name'] = 'https://oig.hhs.gov/reports/all/'
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('append')
            .saveAsTable('mimi_ws_1.hhsoig.all_reports_summaries')
    )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC COMMENT ON TABLE mimi_ws_1.hhsoig.all_reports_titles IS '# [HHS-OIG All Reports and Publications - Titles](https://oig.hhs.gov/reports/all/) | interval: daily, resolution: report';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.hhsoig.all_reports_summaries IS '# [HHS-OIG All Reports and Publications - Summaries](https://oig.hhs.gov/reports/all/) | interval: daily, resolution: report';
