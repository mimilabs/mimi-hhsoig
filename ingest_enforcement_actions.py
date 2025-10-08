# Databricks notebook source
# MAGIC %pip install anthropic

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import anthropic
import json
import pandas as pd
from dateutil.parser import parse
from tqdm import tqdm
from datetime import datetime
import pickle
from pathlib import Path

anthropic_token = dbutils.secrets.get(scope="slackbot", key="anthropic")
output_path = "/Volumes/mimi_ws_1/hhsoig/src/claude_outputs"

# COMMAND ----------

class OIGArticleProcessor:
    def __init__(self, api_key):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.system_prompt = [{"type": "text",
            "cache_control": {"type": "ephemeral"},
            "text": """You are a healthcare compliance data extraction system. Your role is to analyze healthcare enforcement action articles and extract key information into structured JSON format.

Output Schema:
{
    "article_id": number,                    // Sequential ID of the article
    "case_status": string,              // Settlement, Criminal Charges, Guilty Plea, Sentencing, Indictment, Arrest
    "subject_name": string,                  // Names of individual/organization
    "subject_type": string,                  // "Individual" or "Organization"
    "subject_role": string,                  // e.g., "CEO", "Physician", "Nurse"
    "location_city": string,                 // City
    "location_state": string,                // State
    "settlement_amount": number | null,      // Amount in USD
    "restitution_amount": number | null,     // Amount in USD
    "violation_types": string[],             // Array of violations
    "violation_start_date": string | null,   // ISO date or year
    "violation_end_date": string | null,     // ISO date or year
    "affected_programs": string[],           // e.g., ["Medicare", "Medicaid"]
    "resolution_type": string,               // e.g., "settlement", "guilty plea"
    "resolution_date": string | null,        // Date of resolution/action
    "prison_term": string | null,            // Prison sentence if applicable
    "probation_term": string | null,         // Probation term if applicable
    "victims_affected": number | null,       // Number of victims/patients
    "victim_type": string | null             // e.g., "elderly", "disabled"
}

Extraction Rules:
1. Monetary values: Convert to numbers (remove $ and ,)
2. Dates: Use ISO format (YYYY-MM-DD) where possible, or YYYY if only year available
3. Missing data: Use null, never leave empty
4. Subject types: "Individual" or "Organization" only

Common violation categories include but are not limited to:
- fraud
- kickbacks
- false claims
- documentation fraud
- drug diversion
- patient abuse

Focus on accurate extraction without commentary. If information is ambiguous, use available context or default to null if unclear."""
        }]

    def create_extraction_prompt(self, article):
        user_prompt = f"""Given this healthcare enforcement action article, extract key information into a JSON object following the system-defined schema.

Articles to process:

{article}"""

        return user_prompt

    def process_articles(self, article):
        
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                temperature=0,
                system=self.system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": self.create_extraction_prompt(article)
                    }
                ]
            )
            # Extract JSON from response
            response_text = message.content[0].text
            # Find JSON block in the response
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                return json.loads(json_str)
            else:
                raise ValueError("No JSON found in response")
            
        except Exception as e:
            print(f"Error processing notice: {str(e)}")
            return []
        
    def post_process_article(self, article):
        """Simple post-processing of article data with date parsing"""
        
        # Fields that should be joined if they're arrays
        array_fields = {
            'case_status', 
            'subject_name', 
            'subject_role',
            'subject_type',
            'location_city', 
            'location_state', 
            'resolution_type',
            'prison_term',
            'victim_type'
        }
        
        # Date fields to parse
        date_fields = {
            'violation_start_date', 'violation_end_date', 'resolution_date'
        }
        
        # Process each field
        for field, value in article.items():
            if value is None:
                continue
                
            # Join arrays for specified fields
            if field in array_fields and isinstance(value, list):
                article[field] = '; '.join(str(v) for v in value if v)
                
            # Convert numeric fields
            elif field in {'settlement_amount', 
                           'restitution_amount', 
                           'victims_affected', 
                           'restitution_amount'}:
                if isinstance(value, list):
                    try:
                        article[field] = float(max(value))
                    except ValueError:
                        article[field] = None
                elif isinstance(value, str):
                    try:
                        article[field] = float(value.replace('$', '').replace(',', ''))
                    except ValueError:
                        article[field] = None
                        
            # Parse dates
            elif field in date_fields and value:
                try:
                    parsed_date = parse(str(value)).date()
                    article[field] = parsed_date
                except (ValueError, TypeError):
                    article[field] = None

        return article


# COMMAND ----------

# initialize our Claude connector
processor = OIGArticleProcessor(anthropic_token)

# COMMAND ----------

# Find the articles that are not enrichced yet
df_original = spark.read.table('mimi_ws_1.hhsoig.enforcement_details')
df_enriched = (spark.read.table('mimi_ws_1.hhsoig.enforcement_details_enriched')
                .withColumnRenamed('page_url', 'page_url_enriched')
                .select('page_url_enriched'))
df_original = df_original.join(df_enriched, 
                                on=(df_original.page_url == df_enriched.page_url_enriched), how='left')
pdf = df_original.where('page_url_enriched IS NULL').drop('page_url_enriched').toPandas()

# COMMAND ----------

data = []
for index, row in tqdm(pdf.iterrows()):
    page_url = row.page_url
    article = f"article_id: {index}\n\n{row.title}\n{row.content}\n"
    result = processor.process_articles(article)
    d_original = row.to_dict()
    d_clean = processor.post_process_article({**result, **d_original})
    article_id = d_clean.get('article_id')
    with open(f"{output_path}/{article_id}.pkl", 'wb') as fp:
        print(f'saving... {article_id}.pkl')
        pickle.dump(d_clean, fp)

# COMMAND ----------

data = []
for filepath in tqdm(Path(output_path).glob("*.pkl")):
    doc = pickle.load(open(filepath, "rb"))
    data.append(doc)

if len(data) > 0:
    pdf_output = pd.DataFrame(data).drop(columns=['article_id'])
    pdf_output['mimi_dlt_load_date'] = datetime.today().date()

    # delta table doesn't accept empty array, so we convert them to NULL
    dtypes = spark.read.table('mimi_ws_1.hhsoig.enforcement_details_enriched').dtypes
    for col, dtype in dtypes:
        if dtype == 'array<string>':
            pdf_output[col] = pdf_output[col].apply(lambda x: None if x is None or len(x) == 0 else x)
        elif dtype == 'string':
            pdf_output[col] = pdf_output[col].apply(lambda x: None if x is None or len(x) == 0 else str(x))
        elif col.endswith('_amount') or col == 'victims_affected':
            pdf_output[col] = pd.to_numeric(pdf_output[col], errors='coerce').astype('float')

    (
        spark.createDataFrame(pdf_output)
            .write
            .mode('append')
            .option('mergeSchema', 'true')
            .saveAsTable('mimi_ws_1.hhsoig.enforcement_details_enriched')
    )

# COMMAND ----------

# clean up the temporary files
for filepath in tqdm(Path(output_path).glob("*.pkl")):
    dbutils.fs.rm(str(filepath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/text_utils

# COMMAND ----------

if False:
    # no need to run it everyday...
    tablepath = 'mimi_ws_1.hhsoig.enforcement_details_enriched'
    cleanup_lst = [('case_status', 'typofix'),
                    ('subject_role', 'typofix'),
                    ('location_state', 'statename'),
                    ('violation_types', 'typofix'),
                    ('affected_programs', 'typofix'),
                    ('resolution_type', 'typofix'),
                    ('victim_type', 'typofix')]

    def remap_value(x, replace_map):
        if x is None:
            return x
        elif isinstance(x, str):
            return replace_map.get(x, x)
        else:
            return [replace_map.get(y, y) for y in x]
        
    pdf = spark.read.table(tablepath).toPandas()
    for colname, method in cleanup_lst:
        print(colname)
        replace_map = get_replace_mapping(tablepath, colname, method)
        pdf[colname] = pdf[colname].apply(lambda x: remap_value(x, replace_map))

    (
        spark.createDataFrame(pdf)
        .write
        .mode('overwrite')
        .saveAsTable('mimi_ws_1.hhsoig.enforcement_details_enriched_clean')
    )

# COMMAND ----------


