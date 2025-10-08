# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/hhsoig/src/workplans/"

# COMMAND ----------

def to_dt(x):
    try: 
        return parse(x).date()
    except:
        return None
    
pdf_lst = [] 
for filepath in Path(volumepath).glob("*"):

    tokens = filepath.stem.split('-')
    mimi_src_file_name = filepath.name
    mimi_src_file_date = parse(f"{tokens[-2]} 1, {tokens[-1]}").date()
    skiprows = 0
    for skiprows in range(5):
        pdf = pd.read_excel(filepath, dtype=str, skiprows=skiprows)
        if pdf.columns[0].startswith("Announced"):
            break
    pdf.columns = change_header(pdf.columns)
    pdf['announced_or_revised'] = pdf['announced_or_revised'].apply(lambda x: to_dt(x))
    if 'tags' not in pdf.columns:
        pdf['tags'] = ''
    if (filepath.name == 'Work-Plan-August-2021.xlsx'
        or filepath.name == 'Work-Plan-September-2021.xlsx'):
        pdf = pdf.rename(columns={'': 'report_numbers'})
    elif filepath.name == 'Work-Plan-September-2020.xlsx':
        pdf = pdf.rename(columns={'report': 'report_numbers', 
                                  'expected': 'expected_issue_date_fy'})
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    pdf = pdf.loc[pdf['title'].notnull()]
    pdf_lst.append(pdf)

pdf_full = pd.concat(pdf_lst)

# COMMAND ----------

def clean_tags(x):
    if not isinstance(x, str):
        return []
    x = x.encode('ascii', 'ignore').decode('ascii').lower()
    x = x.replace('food, drug, and device safety', 'food drug device safety')
    x = x.replace('\t', ',')
    x = x.replace(";", ',')
    x = x.replace('.', ',')
    x = x.replace('children and families', 'children & families')
    x = x.replace('children & familiesgrants', 'children & families, grants')
    x = x.replace('children & families grants', 'children & families, grants')
    x = x.replace('issuess', 'issues')
    x = x.replace('operationalissues', 'operational issues')
    x = x.replace('nuring facilties', 'nursing homes')
    x = x.replace('nursing facilities', 'nursing homes')
    x = x.replace('nursing facilities and assisted living facilities', 'nursing homes')
    x = x.replace('nursing facilities, and assisted living facilities', 'nursing homes')
    x = x.replace('nursing homes, and assisted living facilities', 'nursing homes')
    x = x.replace('nursing homes and assisted living facilities', 'nursing homes')
    x = x.replace('instituional', 'institutional')
    x = x.replace('elderly medicare', 'elderly, medicare')
    x = x.replace('elderlymedicare', 'elderly, medicare')
    x = x.replace('emergency preparedness and response', 'emergency preparedness')
    x = x.replace('americans', 'american').replace('american', 'americans')
    x = x.replace('oig statutory authority and regulatory matters', 'oig statutory authority')
    x = x.replace('oig statutory authority...', 'oig statutory authority')
    x = x.replace('other: health disparities', 'health disparities')
    x = x.replace('tgrants', 'grants')
    x = x.replace('physician and health care providers',
                  'phyisicians and healthcare practitioners')
    x = x.replace('physician and healthcare practiioners',
                  'phyisicians and healthcare practitioners')
    x = x.replace('physician and healthcare practitioners',
                  'phyisicians and healthcare practitioners')
    x = x.replace('physicians and healthcare providers', 
                  'phyisicians and healthcare practitioners')
    x = x.replace('medicare part b', 'medicare b')
    if x.startswith('ovid-19'):
        x = 'c' + x
    if 'and assisted living facilities' in [z.strip() for z in x.split(',')]:
        print(x)
    return list(set([z.strip() for z in x.split(',')
                     if z.strip() not in {'', 'etc', 'tags'}]))

# COMMAND ----------

pdf_full['tags'] = pdf_full['tags'].apply(clean_tags)
pdf_full['report_numbers'] = pdf_full['report_numbers'].apply(lambda x: [z.strip() 
                                                                         for z in x.split(';')])

# COMMAND ----------

pdf_latest = (pdf_full.groupby('title').agg({'mimi_src_file_date': 'max'})
                .reset_index())

# COMMAND ----------

pdf_final = pd.merge(pdf_full, pdf_latest, on=['title', 'mimi_src_file_date'], how='inner')

# COMMAND ----------

(
    spark.createDataFrame(pdf_final)
        .write
        .mode('overwrite')
        .saveAsTable('mimi_ws_1.hhsoig.workplans')
)

# COMMAND ----------


