# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest List of Excluded Individuals and Entities (LEIE)

# COMMAND ----------

import pandas as pd
from glob import glob
import re
from dateutil.parser import parse

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# https://oig.hhs.gov/exclusions/authorities.asp
rawtxt = """1128(a)(1)	1320a-7(a)(1)	Conviction of program-related crimes. Minimum Period: 5 years
1128(a)(2)	1320a-7(a)(2)	Conviction relating to patient abuse or neglect. Minimum Period: 5 years
1128(a)(3)	1320a-7(a)(3)	Felony conviction relating to health care fraud. Minimum Period: 5 years
1128(a)(4)	1320a-7(a)(4)	Felony conviction relating to controlled substance. Minimum Period: 5 years
1128(c)(3)(G)(i)	1320a-7(c)(3)(G)(i)	Conviction of second mandatory exclusion offense. Minimum Period: 10 years
1128(c)(3)(G)(ii)	1320a-7(c)(3)(G)(ii)	Conviction of third or more mandatory exclusion offenses. Permanent Exclusion
1128(b)(1)(A)	1320a-7(b)(1)(A)	Misdemeanor conviction relating to health care fraud. Baseline Period: 3 years
1128(b)(1)(B)	1320a-7(b)(1)(B)	Conviction relating to fraud in non-health care programs. Baseline Period: 3 years
1128(b)(2)	1320a-7(b)(2)	Conviction relating to obstruction of an investigation or audit. Baseline Period: 3 years
1128(b)(3)	1320a-7(b)(3)	Misdemeanor conviction relating to controlled substance. Baseline Period: 3 years
1128(b)(4)	1320a-7(b)(4)	License revocation, suspension, or surrender. Minimum Period: Period imposed by the state licensing authority.
1128(b)(5)	1320a-7(b)(5)	Exclusion or suspension under federal or state health care program. Minimum Period: No less than the period imposed by federal or state health care program.
1128(b)(6)	1320a-7(b)(6)	Claims for excessive charges, unnecessary services or services which fail to meet professionally recognized standards of health care, or failure of an HMO to furnish medically necessary services. Minimum Period: 1 year
1128(b)(7)	1320a-7(b)(7)	Fraud, kickbacks, and other prohibited activities. Minimum Period: None
1128(b)(8)	1320a-7(b)(8)	Entities controlled by a sanctioned individual. Minimum Period: Same as length of individual's exclusion.
1128(b)(8)(A)	1320a-7(b)(8)(A)	Entities controlled by a family or household member of an excluded individual and where there has been a transfer of ownership/control. Minimum Period: Same as length of individual's exclusion.
1128(b)(9)	1320a-7(b)(9)	Failure to disclose required information, supply requested information on subcontractors and suppliers; or supply payment information. Minimum Period: None
1128(b)(10)	1320a-7(b)(10)	Failure to disclose required information, supply requested information on subcontractors and suppliers; or supply payment information. Minimum Period: None
1128(b)(11)	1320a-7(b)(11)	Failure to disclose required information, supply requested information on subcontractors and suppliers; or supply payment information. Minimum Period: None
1128(b)(12)	1320a-7(b)(12)	Failure to grant immediate access. Minimum Period: None
1128(b)(13)	1320a-7(b)(13)	Failure to take corrective action. Minimum Period: None
1128(b)(14)	1320a-7(b)(14)	Default on health education loan or scholarship obligations. Minimum Period: Until default or obligation has been resolved.
1128(b)(15)	1320a-7(b)(15)	Individuals controlling a sanctioned entity. Minimum Period: Same as length of entity's exclusion.
1128(b)(16)	1320a-7(b)(16)	Making false statement or misrepresentations of material fact. Minimum period: None.
1156	1320c-5	Failure to meet statutory obligations of practitioners and providers to provide medically necessary services meeting professionally recognized standards of health care (Quality Improvement Organization (QIO) findings). Minimum Period: 1 year"""

# COMMAND ----------

data = [row.split("\t") for row in rawtxt.split("\n")]
pdf_code = pd.DataFrame(data, columns=["social_security_act", "42_usc", "amendment"])
pdf_code['excltype'] = pdf_code.social_security_act.str.replace('[^0-9a-zA-Z]', '', regex=True)

# COMMAND ----------

files = []
for f in glob('/Volumes/mimi_ws_1/hhsoig/src/leie*'):
    files.append((f, parse(f[-12:-4])))
entry = sorted(files, key=lambda x: x[1])[-1]
ifd = entry[1]
file = entry[0]

# COMMAND ----------

pdf_base = pd.read_csv(file, dtype=str, keep_default_na=False)
pdf_base.columns = change_header(pdf_base.columns)
for col in ["dob", "excldate", "reindate", "waiverdate", "wvrstate"]:
    #pdf_base[col] = pdf_base[col].str.replace("00000000", "")
    pdf_base[col] = pd.to_datetime(pdf_base[col], format="%Y%m%d", errors="coerce").dt.date

# COMMAND ----------

pdf = pd.merge(pdf_base, pdf_code, on="excltype", how="left")
pdf["_input_file_date"] = ifd

# COMMAND ----------

(spark.createDataFrame(pdf).write
        .format('delta')
        .mode("overwrite")
        .saveAsTable("mimi_ws_1.hhsoig.leie"))

# COMMAND ----------


