# Databricks notebook source
# MAGIC %md # CCU053-D05-case_control_matching
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook conducts density set sampling to produce matched case controls. Up to 10 controls per case, matched on year of birth (plus, minus year), sex, diabetes type, year of diabetes diagnosis
# MAGIC
# MAGIC **Authors**  Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** 
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Input**
# MAGIC - **`ccu053_out_outcome_all_cohort`** : Diabetes cohort with details of outcome of DKA marked with "case" variable
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_matched_cases_controls`** : Cases and upto 10 matched controls

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
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# # Cases included as potential controls
# cases_included = True

# # Diagnostic exclusion criteria
# exclusions_from_diabetes_diagnosis = False

# COMMAND ----------

# MAGIC %md # 2. Data
# MAGIC

# COMMAND ----------

# if exclusions_from_diabetes_diagnosis:
#     outcomes = spark.table(f'{dsa}.{proj}_out_outcome_b')
#     cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean_b')
# else:
#     outcomes = spark.table(f'{dsa}.{proj}_out_outcome')
#     cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean')

outcomes = spark.table(f'{dsa}.{proj}_out_outcome')
cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean')

#cases_controls = spark.table(f'{dsa}.{proj}_out_outcome_all_cohort')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Prepare data

# COMMAND ----------

print(outcomes.count())

# COMMAND ----------

_outcomes_cleaned = outcomes\
    .select(f.col("PERSON_ID"),\
        f.col("DATE"), \
        f.col("source"),\
        f.col("code")
    )

_outcomes_cohort = cohort\
    .join(_outcomes_cleaned, on='PERSON_ID', how='left')

print(outcomes.count())
print(_outcomes_cohort.count())

# COMMAND ----------

print(study_start_date)
print(study_end_date)

# COMMAND ----------

# Adding individual censor dates:
# CENSOR_DATE_START = Date of diagnosis of diabetes
# CENSOR_DATE_END = Date of outcome (DKA), or date of death, or end of study date

_outcomes_cohort = (
    _outcomes_cohort
    .withColumnRenamed("person_id","PERSON_ID")
    .withColumn('CENSOR_DATE_START', 
            f.when(
               f.col('date_of_diagnosis') <= f.to_date(f.lit(f'{study_start_date}')),f.to_date(f.lit(f'{study_start_date}')))
            .otherwise(f.col('date_of_diagnosis')))
    .withColumn("CENSOR_DATE_END",\
        f.when(\
            f.col('DATE').isNotNull(), f.col('DATE')).when(\
            f.col('date_of_death').isNotNull(), f.col('date_of_death')).otherwise(f.to_date(f.lit(f'{study_end_date}'))))
    .withColumn("case",\
        f.when(\
            (f.col('DATE').isNotNull()),1).otherwise(0))\
    .withColumn("FOLLOW_UP_DAYS",\
        f.datediff(f.col("CENSOR_DATE_END"), f.col("CENSOR_DATE_START"))+1)\
    .withColumn('diag_month_year', f.date_format(f.col('date_of_diagnosis'), 'yyyy-MM'))
    .withColumn('diag_year', f.date_format(f.col('date_of_diagnosis'), 'yyyy'))
)

# COMMAND ----------

display(_outcomes_cohort)

# COMMAND ----------

tab(_outcomes_cohort, "case");print();

# COMMAND ----------

min_date = _outcomes_cohort.agg(f.min("DATE")).collect()[0][0]

print(min_date)

# COMMAND ----------

# Checks
# Filter where CENSOR_DATE_END is after CENSOR_DATE_START
_invalid_censor_dates = _outcomes_cohort.filter(f.col("CENSOR_DATE_END") < f.col("CENSOR_DATE_START"))

# # Count the number of rows which are invalid
print(_invalid_censor_dates.count())

# display(_invalid_censor_dates)

tab(_invalid_censor_dates, "diabetes_type")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 Matching algorithm
# MAGIC ## 3.1 Add matching column

# COMMAND ----------


# # create matching string and reduce columns
# _cases_controls = (
#     _outcomes_cohort
#         .withColumn('matching_ID', f.concat(f.year(f.col('date_of_birth')), f.col('sex'), f.col('diabetes_type'), f.col('diag_year')))
#         .select("PERSON_ID", "CENSOR_DATE_START", "CENSOR_DATE_END", "DATE", "source", "code", "case","matching_ID", "FOLLOW_UP_DAYS", "diag_month_year", "diag_year")
# )


# create matching string and reduce columns
_cases_controls = (
    _outcomes_cohort
        .withColumn('year_of_birth', f.year(f.col('date_of_birth')))
        .select("PERSON_ID", "CENSOR_DATE_START", "CENSOR_DATE_END", "DATE", "source", "code", "case", "FOLLOW_UP_DAYS", "diag_month_year", "diag_year", 'sex', 'year_of_birth', 'diabetes_type')
)


# COMMAND ----------

_cases_matching_id = (
    _cases_controls
    .where(f.col("case") ==1)
)

_non_cases_matching_id = (
    _cases_controls
    .where(f.col("case") ==0)
)

_case_ids =  (
    _cases_controls
    .where(f.col("case") ==1)
    .select(f.col("matching_ID"))
    .distinct()
) 

_non_case_ids =  (
    _cases_controls
    .where(f.col("case") ==0)
    .select(f.col("matching_ID"))
    .distinct()
) 


#Checks:
# count_var(_cases_matching_id, "matching_ID");print();
# count_var(_non_cases_matching_id, "matching_ID");print();


# COMMAND ----------

def label_rows(entries, match_column):
    window = Window.partitionBy(match_column).orderBy(match_column)
    grouped_entries = entries.withColumn('row_number', f.row_number().over(window))
    max_entries = (
        grouped_entries
            .groupBy(f.col(match_column[0]), f.col(match_column[1]))
            .agg(f.max('row_number'))
            .withColumnRenamed('max(row_number)', f'num_matches')
    )
    return grouped_entries, max_entries

grouped_cases, total_cases_per_group = label_rows(_cases_controls, ['matching_ID','case'])

# Collect the matching_IDs that have at least 1 case
included_ids = [row['matching_ID'] for row in _case_ids.select('matching_ID').collect()]

# Add an "inclusion" column to df based on whether matching_ID is in included_ids
total_cases_per_group = total_cases_per_group.withColumn('inclusion', f.when(f.col('matching_ID').isin(included_ids), 1).otherwise(0))

# COMMAND ----------

display(total_cases_per_group)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Join cases to all potential controls 

# COMMAND ----------

# Create table for cases (people with the outcome) and table for potential controls which can contain everyone in cohort or only those who have no record of the outcome
# Parameter cases_included is set at the top of notebook 

cases_df = _cases_controls.filter(f.col("case") == 1)  # Cases
    
# if cases_included:
#     all_controls_df = _cases_controls  # Controls, which include cases as potenital controls
# else:
#     all_controls_df = _cases_controls.filter(f.col("case") == 0)  # Controls, which don't include cases as potenital controls

all_controls_df = _cases_controls  # Controls, which include cases as potenital controls



# Join cases with all participants (controls and cases), with additional criteria to have over lapping periods
merged_df = all_controls_df.alias('controls').join(
    cases_df.alias('cases'),
    (f.abs(f.col("controls.year_of_birth") - f.col("cases.year_of_birth")) <= 1) & # Have year of birth within +- a year 
    (f.col("controls.sex") == f.col("cases.sex")) & 
    (f.col("controls.diabetes_type") == f.col("cases.diabetes_type")) & # Have same diabetes type
    (f.col("controls.diag_year") == f.col("cases.diag_year")) & # Have same diabetes type
    (f.col('controls.PERSON_ID') != f.col('cases.PERSON_ID')) &  # Prevent a case from being its own control
    (f.col('controls.FOLLOW_UP_DAYS') >= f.col('cases.FOLLOW_UP_DAYS')), # controls must have  follow-up time (in days) equal to or longer than the case
    how='right'
)




# # Join cases with all participants (controls and cases), with additional criteria to have over lapping periods
# merged_df = all_controls_df.alias('controls').join(
#     cases_df.alias('cases'),
#     (f.col('controls.matching_ID') == f.col('cases.matching_ID')) &  # Same matching_id
#     (f.col('controls.CENSOR_DATE_START') <= f.col('cases.CENSOR_DATE_END')) &  # A control's follow up start date must be on or before the case's follow up end period
#     (f.col('controls.CENSOR_DATE_END') >= f.col('cases.CENSOR_DATE_START')) &  # A control's follow up end date must be on or after the case's follow up start period
#     (f.col('controls.PERSON_ID') != f.col('cases.PERSON_ID')) &  # Prevent a case from being its own control
#     (f.col('controls.FOLLOW_UP_DAYS') >= f.col('cases.FOLLOW_UP_DAYS')), # controls must have  follow-up time (in days) equal to or longer than the case
#     how='right'
# )

# COMMAND ----------

# Number of cases with no matching controls 
merged_df.filter(f.col("controls.PERSON_ID").isNull()).count()

# COMMAND ----------

# Remove cases with no matching controls
merged_df = merged_df.filter(f.col("controls.PERSON_ID").isNotNull())


# COMMAND ----------

# Renaming variables with cases_ and controls_ prefixes for clarity
merged_df = merged_df.select(
    f.col('cases.PERSON_ID').alias('cases_PERSON_ID'), 
    f.col('cases.CENSOR_DATE_START').alias('cases_CENSOR_DATE_START'),
    f.col('cases.CENSOR_DATE_END').alias('cases_CENSOR_DATE_END'), 
    f.col('cases.DATE').alias('cases_DATE'),
    f.col('cases.source').alias('cases_source'), 
    f.col('cases.code').alias('cases_code'),
    f.col('cases.case').alias('cases_case'), 
    f.col('cases.sex').alias('cases_sex'),
    f.col('cases.diabetes_type').alias('cases_diabetes_type'),
    f.col('cases.year_of_birth').alias('cases_year_of_birth'),
    f.col('cases.FOLLOW_UP_DAYS').alias('cases_follow_up_days'),
    f.col('controls.PERSON_ID').alias('controls_PERSON_ID'), 
    f.col('controls.CENSOR_DATE_START').alias('controls_CENSOR_DATE_START'),
    f.col('controls.CENSOR_DATE_END').alias('controls_CENSOR_DATE_END'), 
    f.col('controls.DATE').alias('controls_DATE'),
    f.col('controls.source').alias('controls_source'), 
    f.col('controls.code').alias('controls_code'),
    f.col('controls.case').alias('controls_case'),
    f.col('controls.sex').alias('controls_sex'),
    f.col('controls.diabetes_type').alias('controls_diabetes_type'),
    f.col('controls.year_of_birth').alias('controls_year_of_birth'),
    f.col('controls.FOLLOW_UP_DAYS').alias('controls_follow_up_days')
)

# COMMAND ----------

min_date = merged_df.agg(f.min("cases_DATE")).collect()[0][0]

print(min_date)

# COMMAND ----------

count_var(merged_df, "cases_PERSON_ID");print();

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Randomly sampling up to 10 controls for each case

# COMMAND ----------

# Set a seed value for reproducibility
seed_value = 42  

# Create a random number column to allow for sampling with replacement
merged_df = merged_df.withColumn('random_num', f.rand(seed_value))


# COMMAND ----------

# Window partition for sampling (up to 10 controls for each case)
sampling_window = Window.partitionBy('cases_PERSON_ID').orderBy('random_num')

# Select up to 10 controls for each case
sampled_controls_df = merged_df.withColumn(
    'control_rank', f.row_number().over(sampling_window)
).filter(f.col('control_rank') <= 10)
  
# Add a set column which provides an index (starting at 1) for each case and the 10 controls
sampled_controls_df = sampled_controls_df.withColumn("set", f.dense_rank().over(Window.orderBy("cases_PERSON_ID")))


# COMMAND ----------

tab(sampled_controls_df, "controls_case");print();



# COMMAND ----------

count_var(sampled_controls_df,"controls_PERSON_ID" );print();



# COMMAND ----------

display(sampled_controls_df)

# COMMAND ----------

# Reshaping the dataframe so that each case appears directly above it's corresponding control
_cases_df = (sampled_controls_df.withColumn("random_num", f.lit(None))
            .withColumn("control_rank",f.lit(None))
            .select(
                f.col('set'),
                f.col('cases_PERSON_ID').alias('PERSON_ID'),
                f.col('cases_CASE').alias('case'),
                f.col('cases_DATE').alias('index_date'),
                f.col('cases_source').alias('source'),
                f.col('cases_code').alias('code'),
                f.col('random_num'),
                f.col('control_rank')
            ).distinct()
)

# Controls will all be set with 0 as their "case" value to represent a control
# Controls will take their corresponding case's index date
_controls_df = (sampled_controls_df.withColumn("case", f.lit(0))
             .withColumn("source",f.lit(None))
             .withColumn("code",f.lit(None))
            .select(
            f.col('set'),
            f.col('controls_PERSON_ID').alias('PERSON_ID'),
            f.col('case'),
            f.col('cases_DATE').alias('index_date'),
            f.col('source'),
            f.col('code'),
            f.col('random_num'),
            f.col('control_rank')
            )
)

final_df = _cases_df.union(_controls_df).orderBy("set")


# COMMAND ----------

display(final_df)

# COMMAND ----------

# Step 1: Count the number of controls (case == 0) in each set
controls_per_set = final_df.groupBy("set").agg(f.count(f.when(f.col("case") == 0, 1)).alias("num_controls"))

# Step 2: Count the number of cases per control count
cases_per_control_count = final_df.filter(f.col("case") == 1).join(
    controls_per_set, "set", "left"
).groupBy("num_controls").agg(f.count("*").alias("num_cases"))

# Step 3: Calculate the percentage
total_cases = cases_per_control_count.agg(f.sum("num_cases")).collect()[0][0]
summary_df = cases_per_control_count.withColumn(
    "percentage", (f.col("num_cases") / total_cases * 100)
)

# Show the result
summary_df.orderBy("num_controls").show()

# COMMAND ----------

# MAGIC %md # 4. Save
# MAGIC

# COMMAND ----------

# # Cohort excludes those who got diabetes after the follow up start AND Cases are included in potential controls
# if not exclusions_from_diabetes_diagnosis and not cases_included :
#     save_table(final_df, out_name=f'{proj}_out_matched_cases_controls')
# elif exclusions_from_diabetes_diagnosis and not cases_included :
#     save_table(final_df, out_name=f'{proj}_out_matched_cases_controls_exclusions')
# elif not exclusions_from_diabetes_diagnosis and cases_included :
#     save_table(final_df, out_name=f'{proj}_out_matched_cases_controls_full')
# elif exclusions_from_diabetes_diagnosis and cases_included :
#     save_table(final_df, out_name=f'{proj}_out_matched_cases_controls_exclusions_full')


save_table(final_df, out_name=f'{proj}_out_matched_cases_controls')
