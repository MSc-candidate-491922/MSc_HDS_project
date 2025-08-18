# Databricks notebook source
# MAGIC %md # CCU053-D03b-cohort_inclusion
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook links the cohort table to the key patient demographics table, and removes particpants who do not meet the inclusion criteria for the study
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Using the key patients demographics table developed by the BHF Data Science Centre team
# MAGIC https://bhfdsc.github.io/documentation/curated_assets/kpcs 
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Intput**
# MAGIC - **`ccu053_out_cohort`** : project cohort table
# MAGIC - **`hds_curated_assets__demographics_[date]`** : key demographics table
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_cohort_clean`** : project cleaned cohort table

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

cohort = spark.table(f'{dsa}.{proj}_out_cohort')
# demographics_table = spark.table(f'{dsa}.hds_curated_assets__demographics_2024_06_04')
demographics_table = spark.table(f'{dsa}.hds_curated_assets__demographics_2024_12_02') 

hes_apc_long = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long') 

# Codelist for DKA 
codelist = spark.table(f'{dsa}.{proj}_out_codelist_outcomes')

# COMMAND ----------

#count_var(demographics_table, 'person_id'); print()
count_var(cohort, 'PERSON_ID');print();

# COMMAND ----------

# Slim cohort

cohort = cohort.withColumn("diabetes_type", f.when(f.col('out_diabetes') == "Type 1","T1DM") \
    .when(f.col('out_diabetes') == "Type 2","T2DM") \
    .when(f.col('out_diabetes') == "NOS","T2DM") \
    .otherwise(f.col('out_diabetes')))

cohort_slim = cohort\
    .select(['PERSON_ID',"diabetes_type","date_of_diagnosis","age_at_diagnosis"])\
    .withColumnRenamed('PERSON_ID', 'person_id')

cohort_demographics = cohort_slim\
   .join(demographics_table, on='person_id', how='inner')

# COMMAND ----------

# check
count_var(cohort_demographics, 'person_id'); print()

# COMMAND ----------

tab(cohort_demographics,"sex");print()
tab(cohort_demographics,"ethnicity_18_code");print()
tab(cohort_demographics,"region");print()
tab(cohort_demographics,"death_flag");print()
tab(cohort_demographics,"diabetes_type");print()
tab(cohort_demographics,"in_gdppr");print()


# COMMAND ----------

# MAGIC %md
# MAGIC ## DKA before cohort entry

# COMMAND ----------

# Select and rename specific columns
_hes_apc = hes_apc_long\
  .select(['PERSON_ID', 'EPISTART', 'CODE', 'DIAG_POSITION'])\
  .withColumnRenamed('EPISTART', 'DATE')


# COMMAND ----------

print(study_start_date)
print(study_end_date)

# COMMAND ----------

diagnosis_null_count = cohort_demographics.filter(f.col("date_of_diagnosis").isNull()).count()
dob_null_count = cohort_demographics.filter(f.col("date_of_birth").isNull()).count()

print(diagnosis_null_count)
print(dob_null_count)

# COMMAND ----------

# Adding individual censor dates for code matching for hospitalisation with DKA before diabetes diagnosis date 
# CENSOR_DATE_START: date of birth
# CENSOR_DATE_END: date of first diabetes diagnosis (date_of_diagnosis)

_cohort_prepared = (
    cohort_demographics
    .withColumnRenamed("person_id","PERSON_ID")
    .withColumn("CENSOR_DATE_START",
        f.col('date_of_birth'))
    .withColumn("CENSOR_DATE_END",
        f.col('date_of_diagnosis'))
    .filter(f.col("date_of_diagnosis").isNotNull())
)



# COMMAND ----------

diagnosis_null_count_1 = _cohort_prepared.filter(f.col("date_of_diagnosis").isNull()).count()
dob_null_count_1 = _cohort_prepared.filter(f.col("date_of_birth").isNull()).count()

print(diagnosis_null_count_1)
print(dob_null_count_1)
print(_cohort_prepared.count())

# COMMAND ----------

display(_cohort_prepared)

# COMMAND ----------

# Join HES table to prepared cohort table with inner join 
_hes_apc_cohort = _hes_apc\
    .join(_cohort_prepared, on='PERSON_ID', how='inner' )

# COMMAND ----------

codelist = spark.table(f'{dsa}.{proj}_out_codelist_outcomes')
_codelist_icd = (codelist.filter(f.col("terminology")=="ICD10"))

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_dka_out = {
    'hes_apc':  ['_hes_apc_cohort',  '_codelist_icd',  1]
}

# run codelist match and codelist match summary functions
_out_outcomes, _out_outcomes_1st, _out_outcomes_1st_wide = codelist_match(dict_dka_out, _name_prefix=f'out_', broadcast=1)
_out_outcomes_summ_name, _out_outcomes_summ_name_code = codelist_match_summ(dict_dka_out, _out_outcomes)

# COMMAND ----------

_out_outcomes_all = _out_outcomes['all']
#display(_out_outcomes_all)

# COMMAND ----------

# filter out indivduals that had a DKA event before diabetes diagnosis
_out_outcomes_filtered_all_filtered = ( _out_outcomes_all
  .where(
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))
    & (f.col('DATE') < f.col('CENSOR_DATE_END'))
    )
)


print(_out_outcomes_all.count())
print(_out_outcomes_filtered_all_filtered.count())

# COMMAND ----------

display(_out_outcomes_filtered_all_filtered)

# COMMAND ----------

dka_before_diagnosis = (
    _out_outcomes_filtered_all_filtered
    .select('PERSON_ID').distinct()
    .withColumn("dka_before_diagnosis", f.lit(1))
)

cohort_demographics = (
    cohort_demographics
    .join(dka_before_diagnosis, on='PERSON_ID', how='left')
    .withColumn("is_dka_before_diagnosis",  
        f.when(f.col('dka_before_diagnosis').isNotNull(), 1)
        .otherwise(0))
)


# COMMAND ----------

display(cohort_demographics)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cohort inclusion rules

# COMMAND ----------

# Aged 18 years or older before 1st April 2019
# To be 18 or older before 1st April 2019, a person needs to be born before 1st April 2001

dob_cut_off = "2001-04-01"
diagnosis_cut_off = "2019-04-01"
death_cut_off = "2019-04-01"

cohort_demographics = ( cohort_demographics\
    .withColumn("na_diagnosis_date",
        f.when(
            f.col("date_of_diagnosis").isNull(), 1).otherwise(0)
    )
    .withColumn("included_age_cutoff",\
        f.when(\
            (f.col('date_of_birth').isNotNull())\
            & (f.col("date_of_birth") <= f.to_date(f.lit(f'{dob_cut_off}')))\
            , 1).otherwise(0))\
    .withColumn("included_diagnosis_cutoff",\
        f.when(\
            (f.col('date_of_diagnosis').isNotNull())\
            & (f.col("date_of_diagnosis") <= f.to_date(f.lit(f'{diagnosis_cut_off}')))\
            , 1).otherwise(0))\
    .withColumn("included_death_cutoff",\
        f.when(\
            (f.col('death_flag') == 1)\
            & (f.col("date_of_death") < f.to_date(f.lit(f'{death_cut_off}')))\
            , 0).otherwise(1))\
)

tab(cohort_demographics,"included_age_cutoff");print()
tab(cohort_demographics,"included_diagnosis_cutoff");print()
tab(cohort_demographics,"included_death_cutoff");print()
tab(cohort_demographics,"na_diagnosis_date");print()
tab(cohort_demographics,"is_dka_before_diagnosis");print()


# COMMAND ----------



cohort_clean = cohort_demographics\
    .where(\
        (f.col("included_age_cutoff") == 1)\
        & (f.col("in_gdppr") == 1)\
        & (f.col("included_death_cutoff") == 1)\
        & ((f.col("sex") == "M") | (f.col("sex") == "F"))\
        & ((f.col('diabetes_type') == "T1DM") | (f.col('diabetes_type') == "T2DM") )\
        & (f.col("na_diagnosis_date") == 0)\
        & (f.col("is_dka_before_diagnosis") == 0)\
    )

# # cohort b excludes those who were not diagnosed by diabetes before the start of the follow up window (1 April 2019)
# cohort_clean_b = cohort_demographics\
#     .where(\
#         (f.col("included_age_cutoff") == 1)\
#         & (f.col("in_gdppr") == 1)\
#         & (f.col("included_death_cutoff") == 1)\
#         & ((f.col("sex") == "M") | (f.col("sex") == "F"))\
#         & ((f.col('diabetes_type') == "T1DM") | (f.col('diabetes_type') == "T2DM") )\
#         & (f.col("included_diagnosis_cutoff") == 1)\
#     )

tab(cohort_clean,"included_age_cutoff");print()
tab(cohort_clean,"included_death_cutoff");print()
tab(cohort_clean,"sex");print()
tab(cohort_clean,"diabetes_type");print()
tab(cohort_clean,"na_diagnosis_date");print()
tab(cohort_clean,"is_dka_before_diagnosis");print()


# COMMAND ----------

# MAGIC %md # 3. Save
# MAGIC

# COMMAND ----------

save_table(cohort_clean, out_name=f'{proj}_out_cohort_clean')
# save_table(cohort_clean_b, out_name=f'{proj}_out_cohort_clean_b')

