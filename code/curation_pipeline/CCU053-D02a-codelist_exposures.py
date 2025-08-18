# Databricks notebook source
# MAGIC %md # CCU053-D02a-codelist_exposures
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the codelist for the exposures: SGLT2i PMED codes.
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes, and work by Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_codelist_exposures`** : Codes for all exposures

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run "../SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU053-D01-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Data

# COMMAND ----------

bhf_phenotypes = spark.table(path_ref_bhf_phenotypes)

pmeds   = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

bnf = spark.table(f'dss_corporate.bnf_code_information')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Prepare

# COMMAND ----------

# ------------------------------------------------------------------------------
# bnf_pmeds
# ------------------------------------------------------------------------------
win = Window\
  .partitionBy('PrescribedBNFCode')\
  .orderBy('PrescribedBNFName')
pmeds_bnf = (
  pmeds
  .select('PrescribedBNFCode', 'PrescribedBNFName')
  .withColumn('rownum', f.row_number().over(win))
  .where(f.col('rownum') == 1)
  .drop('rownum')
)

# temp save
outName = f'{proj}_tmp_pmeds_bnf'.lower()
# pmeds_bnf = temp_save(pmeds_bnf, out_name=outName)
pmeds_bnf = spark.table(f'{dsa}.{outName}')

# check
count_var(pmeds_bnf, 'PrescribedBNFCode'); print()
print(pmeds_bnf.orderBy('PrescribedBNFCode').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 4. Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Exposure SGLT2i

# COMMAND ----------

# --------------------------------------------------------------------------
# STG2i - Sodium-glucose co-transporter-2 inhibitors
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/060102/
# https://github.com/BHFDSC/hds_phenotypes_diabetes/blob/main/codelists/medications_antidiabetic.csv


# Dapagliflozin (0601023AG)
# Dapagliflozin/metformin (0601023AL)
# Canagliflozin (0601023AM)
# Canagliflozin/metformin (0601023AP)
# Empagliflozin (0601023AN)
# Empagliflozin/linagliptin (0601023AY)
# Empagliflozin/metformin (0601023AR)
# Ertugliflozin (0601023AX)
# Saxagliptin/dapagliflozin (0601023AV)



codelist_sglt2i = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AG')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AL')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AM')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AP')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AN')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AY')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AR')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AX')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AV')
  )
  .withColumn('name', f.lit('sglt2i'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_sglt2i.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_sglt2i.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

display(codelist_sglt2i)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Save

# COMMAND ----------

save_table(codelist_sglt2i, save_previous=True, out_name=f'{proj}_out_codelist_exposures')

# COMMAND ----------

