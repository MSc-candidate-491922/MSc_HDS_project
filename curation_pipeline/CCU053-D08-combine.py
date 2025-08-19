# Databricks notebook source
# MAGIC %md # CCU053-D08-combine
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook combines all cohort, case_control, exposure and covariate notebooks into a single research-ready dataset
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes. Based on work by Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_combined`** : project's combined research-ready dataset

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

checking = False

# COMMAND ----------

# MAGIC %md # 2. Data
# MAGIC

# COMMAND ----------


cohort = spark.table(f'{dsa}.{proj}_out_cohort_clean')
cases_controls = spark.table(f'{dsa}.{proj}_out_matched_cases_controls')
exposure = spark.table(f'{dsa}.{proj}_out_exposures')
covariates = spark.table(f'{dsa}.{proj}_out_covariates')


# COMMAND ----------

if checking:
    print(cohort.count())
    print(cases_controls.count())
    print(exposure.count())
    tab(cases_controls, "case");print();
    tab(exposure, "sglt_12_mos")

# COMMAND ----------

if checking:
    display(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining cases and controls with cohort for demographic variables

# COMMAND ----------

cohort_prepared = (
    cohort
    .withColumnRenamed("person_id", "PERSON_ID")
    .select([
        "PERSON_ID",
        "diabetes_type",
        "date_of_diagnosis",
        "age_at_diagnosis",
        "date_of_birth",
        "sex",
        "ethnicity_18_code",
        "ethnicity_18_group",
        "ethnicity_5_group",
        "region",
        "imd_decile",
        "imd_quintile",
        "death_flag",
        "date_of_death"
    ])
)

cases_controls_cohort =(
    cases_controls
    .join(cohort_prepared, "PERSON_ID", how="left")
)

print(cases_controls_cohort.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining exposure data

# COMMAND ----------

cases_controls_cohort_exposures = (
   cases_controls_cohort
   .join(exposure, on=["PERSON_ID", "set"], how="left")
)


# COMMAND ----------

if checking:
    display(cases_controls_cohort_exposures)

# COMMAND ----------


if checking:
    tab(cases_controls_cohort_exposures, "case", "sglt_12_mos")

# COMMAND ----------

if checking:
    null_exposures = cases_controls_cohort_exposures.filter(f.col("PERSON_ID").isNull())
    display(null_exposures)

# COMMAND ----------

cases_controls_cohort_exposures = (
    cases_controls_cohort_exposures
    .withColumn("sglt_12_mos",f.when(f.col("sglt_12_mos") == 1,1).otherwise(0))
    .withColumn("sglt_3_mos_distinct",f.when(f.col("sglt_3_mos_distinct") == 1,1).otherwise(0))
    .withColumn("sglt_6_mos_distinct",f.when(f.col("sglt_6_mos_distinct") == 1,1).otherwise(0))
    .withColumn("sglt_12_mos_distinct",f.when(f.col("sglt_12_mos_distinct") == 1,1).otherwise(0))
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining covariate data

# COMMAND ----------

columns_to_select = ["PERSON_ID", "set", "covid", "covid_offset", "bmi", "bmi_group", "bmi_group_1", "bmi_group_2", "hba1c", "hba1c_group", "egfr", "scr_egfr", "scr_egfr_hx_ckd", "total_chol","hdl_chol", "hx_hf", "raas_inhibitor", "antipsychotic",  "beta_blocker", "calcium_channel_blocker", "corticosteroid", "diuretic", "glucose_lowering", "statin"]
covariates_reduced = covariates.select(columns_to_select)

cases_controls_cohort_exposures_covariates = (
   cases_controls_cohort_exposures
   .join(covariates_reduced, on=["PERSON_ID", "set"], how="left")
)

# print(cases_controls_cohort_exposures.count())
# tab(cases_controls_cohort_exposures,"sglt_12_mos")

# exposure_cohort = exposure\
#    .join(cohort, on='PERSON_ID', how='left')

# COMMAND ----------

cases_controls_cohort_exposures_covariates = (
    cases_controls_cohort_exposures_covariates
    .withColumn("covid",f.when(f.col("covid") == 1,1).otherwise(0))
    .withColumn("covid_offset",f.when(f.col("covid_offset") == 1,1).otherwise(0))
    .withColumn("scr_egfr_hx_ckd",f.when(f.col("scr_egfr_hx_ckd") == 1,1).otherwise(0))
    .withColumn("hx_hf",f.when(f.col("hx_hf") == 1,1).otherwise(0))
    .withColumn("raas_inhibitor",f.when(f.col("raas_inhibitor") == 1,1).otherwise(0))
    .withColumn("antipsychotic",f.when(f.col("antipsychotic") == 1,1).otherwise(0))
    .withColumn("beta_blocker",f.when(f.col("beta_blocker") == 1,1).otherwise(0))
    .withColumn("calcium_channel_blocker",f.when(f.col("calcium_channel_blocker") == 1,1).otherwise(0))
    .withColumn("corticosteroid",f.when(f.col("corticosteroid") == 1,1).otherwise(0))
    .withColumn("diuretic",f.when(f.col("diuretic") == 1,1).otherwise(0))
    .withColumn("glucose_lowering",f.when(f.col("glucose_lowering") == 1,1).otherwise(0))
    .withColumn("statin",f.when(f.col("statin") == 1,1).otherwise(0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(cases_controls_cohort_exposures_covariates, out_name=f'{proj}_out_combined')

