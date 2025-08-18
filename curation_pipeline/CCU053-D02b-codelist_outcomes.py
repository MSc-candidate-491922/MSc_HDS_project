# Databricks notebook source
# MAGIC %md # CCU053-D02b-codelist_outcomes
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the codelist for the outcomes: DKA IC10 and SNOMED codes.
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes, and work by Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_codelist_outcomes`** : Codes for all outcomes

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

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

# MAGIC %run "../SHDS/common/functions"

# COMMAND ----------

# MAGIC %md #1. Parameters
# MAGIC

# COMMAND ----------

# MAGIC %run "./CCU053-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data
# MAGIC

# COMMAND ----------

# --------------------------------------------------------------------------
# DKA - Diabetic ketoacidosis  
# --------------------------------------------------------------------------
# https://github.com/BHFDSC/hds_phenotypes_diabetes/blob/main/codelists/diabetes_type2.csv

codelist_dka = spark.createDataFrame(
[
  ("dka","ICD10","E111","Type 2 diabetes mellitus With ketoacidosis","",""  ),
  ("dka","SNOMED","421750000","Ketoacidosis due to type 2 diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","421847006","Ketoacidotic coma due to type 2 diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","721284006","Acidosis due to type 2 diabetes mellitus (disorder)","",""  ),
  ("dka","ICD10","E101","Type 1 diabetes mellitus With ketoacidosis","",""  ),
  ("dka","SNOMED","420270002","Ketoacidosis due to type 1 diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","421075007","Ketoacidotic coma due to type 1 diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","721283000","Acidosis due to type 1 diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","26298008","Ketoacidotic coma due to diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","111556005","Ketoacidosis without coma due to diabetes mellitus (disorder)","",""  ),
  ("dka","SNOMED","267467004","Diabetes mellitus & ketoacidosis (disorder)","",""  ),
  ("dka","SNOMED","420422005","Ketoacidosis due to diabetes mellitus (disorder)","",""  ),
  ("dka","ICD10","E131","Other specified diabetes mellitus with ketoacidosis","",""  ),
  ("dka","ICD10","E141","Unspecified diabetes mellitus with ketoacidosis","",""  ),
  ("dka","ICD10","E121","Malnutrition-related diabetes mellitus With ketoacidosis","",""  ),
  ("dka","SNOMED","190406000","Ketoacidosis due to malnutrition related diabetes mellitus (disorder)","",""  )
],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
).select("name","terminology","code","term")


codelist_outcomes = (
    codelist_dka.withColumn("codelist", f.lit("Diabetic ketoacidosis"))
)

display(display(codelist_outcomes.select("codelist","name","terminology","code","term")))

# COMMAND ----------

# MAGIC %md # 3. Save
# MAGIC

# COMMAND ----------

save_table(codelist_outcomes, out_name=f'{proj}_out_codelist_outcomes')
