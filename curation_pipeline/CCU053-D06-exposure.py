# Databricks notebook source
# MAGIC %md # CCU053-D06-exposure
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the exposures - cases and controls prescribed with SGLT2 inhibitors
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes. Based on work by  Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_exposure`** : project exposure

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

# # Diagnostic exclusion criteria
# exclusions_from_diabetes_diagnosis = True

# # Cases included as potential controls
# cases_included = True

# COMMAND ----------

# MAGIC %md # 2. Data
# MAGIC

# COMMAND ----------


codelist = spark.table(f'{dsa}.{proj}_out_codelist_exposures')

pmeds   = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

# # To be added back later on :
# cohort = spark.table(f'{dsa}.{proj}_out_outcomes')
# cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls')


outcomes = spark.table(f'{dsa}.{proj}_out_outcome')
cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean')
cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls')

# if exclusions_from_diabetes_diagnosis:
#     outcomes = spark.table(f'{dsa}.{proj}_out_outcome_b')
#     cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean_b')
#     if cases_included:
#         cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls_exclusions_full')
#     else:
#         cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls_exclusions')
# else:
#     outcomes = spark.table(f'{dsa}.{proj}_out_outcome')
#     cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean')
#     if cases_included:
#         cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls_full')
#     else:
#         cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls')


# COMMAND ----------

display(cases_controls)

# COMMAND ----------

# display(pmeds)

# COMMAND ----------

# display(codelist)

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare
# MAGIC

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')

cohort_prepared = (
    cases_controls
    .select(['set','case','PERSON_ID', 'index_date'])
)

cohort_prepared = (
    cohort_prepared
    .withColumn('CENSOR_DATE_START', f.add_months(f.col('index_date'), -12))
    .withColumnRenamed('index_date','CENSOR_DATE_END')
)


# COMMAND ----------

# display(cohort_prepared)

# COMMAND ----------

min_date = cohort_prepared.select(f.min(f.col('CENSOR_DATE_START')))
display(min_date)

# COMMAND ----------

# Join medications table to cohort of cases and controls to filter only relevant medication records
# Filter medication records to only those that fall within the cohorts individual censor dates 

print('--------------------------------------------------------------------------------------')
print('pmeds')
print('--------------------------------------------------------------------------------------')

# reduce and rename columns
pmed_prepared = pmeds\
  .select(['Person_ID_DEID', 'ProcessingPeriodDate', 'PrescribedBNFCode'])\
  .withColumnRenamed('Person_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('ProcessingPeriodDate', 'DATE')\
  .withColumnRenamed('PrescribedBNFCode', 'CODE')

# Join medications table to cohort of cases and controls 
pmeds_cohort = pmed_prepared\
  .join(cohort_prepared, on='PERSON_ID', how='inner')


# filter combined table to only records with a date within the CENSOR_DATE_START and CENSOR_DATE_END for each person
pmeds_cohort_prepared = pmeds_cohort\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# # check
# count_var(pmeds_cohort_prepared, 'PERSON_ID'); print()
# display(pmeds_cohort_prepared)


# COMMAND ----------

# Define dictionary of codelists for the exposure, SGLT2i
# Match the combined medication records dataset to the exposures codelist dictory to find exposures of SGLT2is

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_sglt_out = {
    'pmeds':  ['pmeds_cohort_prepared',  'codelist',  1]
}

# run codelist match and codelist match summary functions
_out_exposures, _out_exposures_1st, _out_exposures_1st_wide = codelist_match(dict_sglt_out, _name_prefix=f'out_', _last_event=1, broadcast=1)
_out_exposures_summ_name, _out_exposures_summ_name_code = codelist_match_summ(dict_sglt_out, _out_exposures)

# COMMAND ----------

# # check result
# display(_out_exposures_1st_wide)
# # count_var(_out_exposures_1st_wide,'PERSON_ID'); print()

# COMMAND ----------

# display(_out_exposures['all'])

# COMMAND ----------

all_exposures = _out_exposures['all']

all_exposures = (
    all_exposures.withColumn("days_diff", f.datediff(f.col("CENSOR_DATE_END"),f.col("DATE")))
)
    


# COMMAND ----------

# display(all_exposures)

# COMMAND ----------


all_exposures = ( all_exposures
                 .withColumn("sglt_12_mos",f.when(f.col("days_diff") <= 365,1).otherwise(0))
                 .withColumn("sglt_3_mos_distinct",f.when(f.col("days_diff") <= 90,1).otherwise(0))
                 .withColumn("sglt_6_mos_distinct",f.when((f.col("days_diff") > 91) & (f.col("days_diff") <= 180) ,1).otherwise(0))
                 .withColumn("sglt_12_mos_distinct",f.when((f.col("days_diff") > 181) & (f.col("days_diff") <= 365),1).otherwise(0))
)

# COMMAND ----------

# display(all_exposures)

# COMMAND ----------

aggregated_exposures = all_exposures.groupBy("PERSON_ID", "set") \
    .agg(
        f.max("sglt_12_mos").alias("sglt_12_mos"),
        f.max("sglt_3_mos_distinct").alias("sglt_3_mos_distinct"),
        f.max("sglt_6_mos_distinct").alias("sglt_6_mos_distinct"),
        f.max("sglt_12_mos_distinct").alias("sglt_12_mos_distinct")
    )


# COMMAND ----------

display(aggregated_exposures)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

# To be added back later
# save_table(cases_controls_exposures, out_name=f'{proj}_out_exposures')


# # Cohort excludes those who got diabetes after the follow up start AND Cases are included in potential controls
# if not exclusions_from_diabetes_diagnosis and not cases_included :
#     save_table(aggregated_exposures, out_name=f'{proj}_out_exposures')
# elif exclusions_from_diabetes_diagnosis and not cases_included :
#     save_table(aggregated_exposures, out_name=f'{proj}_out_exposures_exclusions')
# elif not exclusions_from_diabetes_diagnosis and cases_included :
#     save_table(aggregated_exposures, out_name=f'{proj}_out_exposures_full')
# elif exclusions_from_diabetes_diagnosis and cases_included :
#     save_table(aggregated_exposures, out_name=f'{proj}_out_exposures_exclusions_full')

save_table(aggregated_exposures, out_name=f'{proj}_out_exposures')

 
