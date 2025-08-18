# Databricks notebook source
# MAGIC %md # CCU053-D05a-outcome
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the outcome - people hospitalised with DKA or death with DKA
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes. Based on work by Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_outcome_all_cohort`** : project outcome

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

codelist = spark.table(f'{dsa}.{proj}_out_codelist_outcomes')

cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean')
# cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean_b')


hes_apc_long = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long') 
deaths_long = spark.table(f'{dsa}.{proj}_cur_deaths_archive_long') 

# COMMAND ----------

cohort.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Prepare
# MAGIC

# COMMAND ----------

# Select and rename specific columns
_hes_apc = hes_apc_long\
  .select(['PERSON_ID', 'EPISTART', 'CODE', 'DIAG_POSITION'])\
  .withColumnRenamed('EPISTART', 'DATE')

_deaths_long = deaths_long\
  .select(['PERSON_ID', 'DATE', 'CODE', 'DIAG_POSITION'])


# COMMAND ----------

print(study_start_date)
print(study_end_date)

# COMMAND ----------

# Adding individual censor dates for code matching for outcome (hospitalisation with DKA):
# CENSOR_DATE_START: date of study entry - date of first diabetes diagnosis (date_of_diagnosis)
# CENSOR_DATE_END: date or death (date_of_death), or end of study date (study_end_date)

_cohort_prepared = (
    cohort
    .withColumnRenamed("person_id","PERSON_ID")
    .withColumn("CENSOR_DATE_START",\
        f.when(\
            (f.col('date_of_diagnosis') > f.to_date(f.lit(f'{study_start_date}'))), f.col('date_of_diagnosis')).otherwise(f.to_date(f.lit(f'{study_start_date}'))))
    .withColumn("CENSOR_DATE_END",\
        f.when(\
            (f.col('date_of_death').isNotNull()), f.col('date_of_death')).otherwise(f.to_date(f.lit(f'{study_end_date}'))))
)



# COMMAND ----------

# Checks
# Filter where CENSOR_DATE_END is after CENSOR_DATE_START
print("Number of (incorrect) records with Censor end before censor start date")
print(_cohort_prepared.filter(f.col("CENSOR_DATE_END") < f.col("CENSOR_DATE_START")).count())



# COMMAND ----------

# Join cohort with hospital and deaths records

# Join HES table to prepared cohort table with inner join 
_hes_apc_cohort = _hes_apc\
    .join(_cohort_prepared, on='PERSON_ID', how='inner' )

# Join Deaths table to prepared cohort table with inner join 
_deaths_cohort = _deaths_long\
    .join(_cohort_prepared, on='PERSON_ID', how='inner' )


# COMMAND ----------

#display(_hes_apc_cohort)

# COMMAND ----------

#count_var(_hes_apc_cohort,"PERSON_ID");print();

# COMMAND ----------

# Prepare outcome codelist:
# filter codelist on hospitalisation with DKA, only ICD10 codes
_codelist_icd = (codelist.filter(f.col("terminology")=="ICD10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Codelist match

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_dka_out = {
    'hes_apc':  ['_hes_apc_cohort',  '_codelist_icd',  1],
    'deaths':  ['_deaths_cohort',  '_codelist_icd',  2]
}

# run codelist match and codelist match summary functions
_out_outcomes, _out_outcomes_1st, _out_outcomes_1st_wide = codelist_match(dict_dka_out, _name_prefix=f'out_', broadcast=1)
_out_outcomes_summ_name, _out_outcomes_summ_name_code = codelist_match_summ(dict_dka_out, _out_outcomes)


# COMMAND ----------

display(_out_outcomes_1st)

# COMMAND ----------

sampling_window = Window.partitionBy('PERSON_ID').orderBy('DATE')

_out_outcomes_1st_new = _out_outcomes['all'].withColumn(
    'rank', f.row_number().over(sampling_window)
).filter(f.col('rank') == 1)

# COMMAND ----------

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_out_outcomes_filtered_1st = _out_outcomes_1st_new\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

print(_out_outcomes_1st_new.count())
print(_out_outcomes_filtered_1st.count())


# COMMAND ----------

display(_out_outcomes_filtered_1st)

# COMMAND ----------

min_date = _out_outcomes_filtered_1st.agg(f.min("DATE")).collect()[0][0]
max_censor_date = _out_outcomes_filtered_1st.agg(f.max("CENSOR_DATE_START")).collect()[0][0]


print(min_date)
print(max_censor_date)

# COMMAND ----------

count_var(_out_outcomes_filtered_1st, "PERSON_ID")

# COMMAND ----------

tab(_out_outcomes_filtered_1st, "diabetes_type")

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(_out_outcomes_filtered_1st, out_name=f'{proj}_out_outcome')
# save_table(_out_outcomes_filtered_1st, out_name=f'{proj}_out_outcome_b')

# COMMAND ----------

# MAGIC %md # Unused code
# MAGIC

# COMMAND ----------

# Unused code

# # Filter out outcomes that are not in cohort (filtering out people who have DKA but who aren't in the diabetes cohort)

# cohort_ids = cohort\
#     .select(['PERSON_ID'])

# # Check number of outcomes which are not in cohort 
# cases_not_in_cohort = _out_outcomes_1st\
#     .join(cohort_ids, on='PERSON_ID', how='leftanti' )

# count_var(cases_not_in_cohort, "PERSON_ID"); print();

# # Filter out outcomes that are not in cohort 
# cases = _out_outcomes_1st\
#     .join(cohort_ids, on='PERSON_ID', how='inner')

# # Check number of cases 
# count_var(cases, "PERSON_ID"); print();

# # Filter out the controls (people in the cophort who are not cases)
# controls_all = cohort_ids\
#     .join(cases, on='PERSON_ID', how='leftanti')
# count_var(controls_all,"PERSON_ID"); print()


# save_table(_out_outcomes_1st, out_name=f'{proj}_out_outcomes')
# save_table(cases, out_name=f'{proj}_out_outcome_cases')
# save_table(controls_all, out_name=f'{proj}_out_outcomes_controls_all')

#spark.sql(f'REFRESH TABLE dss_corporate.gdppr_cluster_refset')
#gdppr_refset = spark.table(path_ref_gdppr_refset)

#pmeds = spark.table(f'{dbc}.primary_care_meds_{db}_archive').where(f.col('archived_on') == '2023-01-31' )

# gdppr = spark.table(f'{dbc}.gdppr_{db}_archive').where(f.col('archived_on') == '2023-02-28' )
# hes_apc_all = spark.table(f'{dbc}.hes_apc_all_years_archive') 

# gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
# pmeds   = extract_batch_from_archive(parameters_df_datasets, 'pmeds')


#bhf_phenotypes = spark.table(path_ref_bhf_phenotypes)
#bnf = spark.table(f'dss_corporate.bnf_code_information')


# _hes_apc = hes_apc_long\
#   .where(f.col('DIAG_POSITION') == 1)\
#   .select(['PERSON_ID', 'EPISTART', 'CODE', 'DIAG_POSITION'])\
#   .withColumnRenamed('EPISTART', 'DATE')

# # check
# count_var(_hes_apc, 'PERSON_ID'); print()
# tmpt = tab(_hes_apc, 'DIAG_POSITION'); print()

# # check
# print(f'study_end_date = {study_end_date}')
# print(f'study_start_date = {study_start_date}')


# _hes_apc = (
#   _hes_apc
#   .withColumn('CENSOR_DATE_START', f.to_date(f.lit(f'{study_start_date}')))
#   .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{study_end_date}')))
#   )


# MPR Check if this is needed
# # filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
# _hes_apc = _hes_apc\
#   .where(\
#     (f.col('DATE') > f.col('CENSOR_DATE_START'))\
#     & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
#   )

# display(_hes_apc)
# # check
# count_var(_hes_apc, 'PERSON_ID'); print()






# hx_out, hx_out_1st, hx_out_1st_wide = codelist_match(dict_hx_out, _name_prefix=f'cov_'); print() #default _last_event=0: filter to 1st event
# hx_out_summ_name, hx_out_summ_name_code = codelist_match_summ(dict_hx_out, hx_out); print()


# # dictionary - dataset, codelist, and ordering in the event of tied records
# _out_in = {
#   'hes_apc': ['_hes_apc', 'nonfatal_codelist',  1]
#   , 'deaths':  ['_deaths',  'fatal_codelist',  2]
# }

# # run codelist match and codelist match summary functions
# _out_outcomes, _out_outcomes_1st, _out_outcomes_1st_wide = codelist_match(_out_in, _name_prefix=f'out_')
# _out_outcomes_summ_name, _out_outcomes_summ_name_code = codelist_match_summ(_out_in, _out_outcomes)



# MPR potentially add functionality to check for death
# _cohort_prepared = (
#     cohort
#     .withColumnRenamed("person_id","PERSON_ID")
#     .withColumn('CENSOR_DATE_START', f.col('date_of_diagnosis'))
#     .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{study_end_date}')))
# )




# .withColumn("included_diagnosis_cutoff",\
#         f.when(\
#             (f.col('date_of_diagnosis').isNotNull())\
#             & (f.col("date_of_diagnosis") <= f.to_date(f.lit(f'{diagnosis_cut_off}')))\
#             , 1).otherwise(0))\

# _cohort_prepared = (
#     cohort
#     .withColumnRenamed("person_id","PERSON_ID")
#     .withColumn('CENSOR_DATE_START', f.col('date_of_diagnosis'))
#     .withColumn("CENSOR_DATE_END",\
#         f.when(\
#             (f.col('date_of_death').isNotNull()), f.col('date_of_death')).otherwise(f.to_date(f.lit(f'{study_end_date}'))))
# )
