# Databricks notebook source
# MAGIC %md # CCU053-D07-covariates
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the covariates including: 
# MAGIC - COVID-19 Infection
# MAGIC - Influenza Infection
# MAGIC - Calendar year (of the index date)
# MAGIC - Ethnicity
# MAGIC - Socioeconomic deprivation 
# MAGIC - Most recent HbA1c
# MAGIC - Most recent BMI 
# MAGIC   - BMI measurement
# MAGIC   - BMI group pre
# MAGIC   - BMI group calculated
# MAGIC - Most recent total cholesterol 
# MAGIC - Most recent HDL cholesterol 
# MAGIC - Most recent eGFR 
# MAGIC   - eGFR measurement
# MAGIC   - eGFR calculated by SCr
# MAGIC - History of chronic kidney disease calculated by SCr/eGFR
# MAGIC - History of heart failure
# MAGIC - History of chronic kidney disease
# MAGIC - Currently used medications 
# MAGIC   - glucose lowering drugs other than SGLT2i 
# MAGIC   - ACE 
# MAGIC   - inhibitors
# MAGIC   - ARBs
# MAGIC   - calcium channel blockers
# MAGIC   - beta blockers
# MAGIC   - diuretics
# MAGIC   - statins
# MAGIC   - corticosteroids
# MAGIC   - antipsychotics
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_out_covariates`** : project covariates
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

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
pmeds   = extract_batch_from_archive(parameters_df_datasets, 'pmeds')
hes_apc_long = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long') 

codelist_covariates = spark.table(f'{dsa}.{proj}_out_codelist_covariates')

# Curated tables for BMI, haB1c, ects 
# bmi = spark.table('dsa_391419_j3w9t_collab.ddsc_curated_assets_bmi_2024_07')
# hba1c = spark.table('dsa_391419_j3w9t_collab.ddsc_curated_assets_hba1c_2024_07')
# covid = spark.table('dsa_391419_j3w9t_collab.hds_curated_assets__covid_positive_2024_10_01')

bmi = spark.table('dsa_391419_j3w9t_collab.hds_curated_assets_bmi_2024_12_02')
hba1c = spark.table('dsa_391419_j3w9t_collab.hds_curated_assets_hba1c_2024_12_02')
covid = spark.table('dsa_391419_j3w9t_collab.hds_curated_assets__covid_positive_2024_12_02')




# COMMAND ----------

count_var(gdppr, 'NHS_NUMBER_DEID')


# COMMAND ----------

tab(cohort, 'diabetes_type')

# COMMAND ----------

print(parameters_df_datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare

# COMMAND ----------

# Select and rename specific columns
_gdppr = ( gdppr
  .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')
)

# COMMAND ----------

# Join cohort to cases and controls

cohort_dob = (
    cohort
    .select(['PERSON_ID', 'date_of_birth'])
)

cases_controls = (
    cases_controls
    .join(cohort_dob, on="PERSON_ID", how="inner")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Custom functions

# COMMAND ----------

def add_censor_dates (cases_controls_df, has_selection_period = True, selection_period_months= 12, censor_start_offset= 0, censor_end_offset = 0):
    if(has_selection_period ==False):
        cases_controls_df_censor_dates = (
            cases_controls_df
            .withColumn('CENSOR_DATE_START',f.col('date_of_birth'))
            .withColumn('CENSOR_DATE_END', f.date_add(f.col('index_date'), censor_end_offset))
        )
    else:
        cases_controls_df_censor_dates = (
            cases_controls_df
            .withColumn('CENSOR_DATE_START', f.date_add((f.add_months(f.col('index_date'), - selection_period_months)), censor_start_offset))
            .withColumn('CENSOR_DATE_END', f.date_add(f.col('index_date'), censor_end_offset))
        )
    return cases_controls_df_censor_dates

def join_participants_covariates (covariate_df, cases_controls_df):
    cases_controls_covariate = (
        cases_controls_df
        .join(covariate_df, on="PERSON_ID", how="inner")
    )
    return cases_controls_covariate

def find_lastest_covariate_record (participants_covariates_df):
    # Filter dataset based on censor start and end dates 
    participants_covariates_df_filtered = (
        participants_covariates_df
        .where(
            (f.col("CENSOR_DATE_START") <= f.col("DATE")) &
            (f.col("CENSOR_DATE_END") >= f.col("DATE"))
        )
    )
    
    # Define window specification: partition by PERSON_ID and order by date descending
    window_spec = Window.partitionBy("Set","PERSON_ID").orderBy(f.col("DATE").desc())

    # Add a row number to each row, starting from 1 for each partition
    participants_covariates_df_with_row_num = participants_covariates_df_filtered.withColumn("row_number", f.row_number().over(window_spec))

    # Filter the rows where row_number is 1 (latest date per PERSON_ID and set)
    latest_covariate_record = participants_covariates_df_with_row_num.filter(f.col("row_number") == 1).drop("row_number")
    return latest_covariate_record

def filter_on_censor_dates (participants_covariates_df):
    # Filter dataset based on censor start and end dates 
    participants_covariates_df_filtered = (
        participants_covariates_df
        .where(
            (f.col("CENSOR_DATE_START") <= f.col("DATE")) &
            (f.col("CENSOR_DATE_END") >= f.col("DATE"))
        )
    )
    return participants_covariates_df_filtered

def groupby_describe(dataframe, groupby_col, stat_col):
    """From a grouby dataframe object provide the stats
    of describe for each key in the groupby object.

    Parameters
    ----------
    dataframe : spark dataframe groupby object
    groupby_col : column to groupby
    stat_col : column to compute statistics on
    
    """
    output = dataframe.groupby(groupby_col).agg(
        f.count(stat_col).alias("count"),
        f.mean(stat_col).alias("mean"),
        f.stddev(stat_col).alias("std"),
        f.min(stat_col).alias("min"),
        f.expr(f"percentile({stat_col}, array(0.25))")[0].alias("%25"),
        f.expr(f"percentile({stat_col}, array(0.5))")[0].alias("%50"),
        f.expr(f"percentile({stat_col}, array(0.75))")[0].alias("%75"),
        f.max(stat_col).alias("max"),
    )
    print(output.orderBy(groupby_col).show())
    return output

def print_covariate_null_counts(covariate_df, covariate_col):
    """Prints the number of null values in a column of a spark dataframe.
    """
    # null_counts = df.select(

    cases_controls_df = cases_controls.join(covariate_df, on=['PERSON_ID', 'set'], how='left')
    
    total_rows = cases_controls_df.count()

    # Calculate null counts and percentages for 'covaiate column'
    null_counts = cases_controls_df.select(
        f.count(f.when(f.col(covariate_col).isNull(), 1)).alias("cov_null_count"),
    ).collect()[0]

    covariate_null_count = null_counts['cov_null_count']

    # Calculate percentages
    covariate_null_percentage = (covariate_null_count / total_rows) * 100 if total_rows > 0 else 0

    # Print results
    print(f"Covariate {covariate_col}: Null Count = {covariate_null_count}, Null Percentage = {covariate_null_percentage:.2f}%, Total Rows = {total_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID-19
# MAGIC
# MAGIC Using curated tables which define COVID-19 infection across chess, gdppr, hes, SGSS, and Pillar 2
# MAGIC
# MAGIC https://bhfdsc.github.io/documentation/docs/curated_phenotypes/covid_trajectories 
# MAGIC
# MAGIC Confirmed COVID-19 infection record from 6 months prior to index date

# COMMAND ----------

tab(covid, 'covid_status', 'data_source');print();

# COMMAND ----------

# Filter on only confirmed COVID-19 cases
covid_confirmed = covid.filter(f.col("covid_status") == "confirmed")

# COMMAND ----------

# Look back 6 months, with 0 offset days 
cases_control_censored_for_covid = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=6, censor_end_offset=0)
cases_controls_covid = join_participants_covariates(covid_confirmed, cases_control_censored_for_covid)
latest_covid = find_lastest_covariate_record(cases_controls_covid)

# Sensitivity analysis 
# Look back 6 months, with -1 day offset   
cases_control_censored_for_covid_offset = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=6, censor_end_offset=-1)
cases_controls_covid_offset = join_participants_covariates(covid_confirmed, cases_control_censored_for_covid_offset)
latest_covid_offset = find_lastest_covariate_record(cases_controls_covid_offset)

# COMMAND ----------

if checking:
    display(cases_control_censored_for_covid)

# COMMAND ----------

cov_covid = (
    latest_covid
    .withColumn("covid", f.lit(1))
    .select("PERSON_ID", "set", "covid")
)
cov_covid_offset = (
    latest_covid_offset
    .withColumn("covid_offset", f.lit(1))
    .select("PERSON_ID", "set", "covid_offset")
)

# COMMAND ----------

# # Review see how many records are missing covid dates 
# _cases_controls_covid = ( 
#     cases_controls
#     .join(cov_covid, on=['PERSON_ID', 'set'], how='left')
# )

# print_covariate_null_counts(_cases_controls_covid, "covid")


# COMMAND ----------

if checking:
    # Check for duplicate cases/control records within each set
    # Group by PERSON_ID and set, and count the occurrences
    covid_check = (
        latest_covid.groupBy("PERSON_ID", "set")
        .count()
        .filter(f.col("count") > 1)
    )

    # Show the result
    covid_check.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Influenza

# COMMAND ----------

codelist_influenza_snomed = (
    codelist_covariates
    .filter((f.col("name") == "influenza") & (f.col("terminology") =="SNOMED"))
)

codelist_influenza_icd = (
    codelist_covariates
    .filter((f.col("name") == "influenza") & (f.col("terminology") =="ICD10"))
)
codelist_influenza_snomed.show()
codelist_influenza_icd.show()

# COMMAND ----------

# Prepare of infection with influenza data from gdppr
influenza_codes_snomed = [row['code'] for row in codelist_influenza_snomed.select("code").distinct().collect()]
gdppr_influenza = (
    gdppr
    .filter(f.col("CODE").isin(influenza_codes_snomed))
    .withColumnRenamed("NHS_NUMBER_DEID","PERSON_ID")
    .select("PERSON_ID", "DATE", "CODE", )
)

# Prepare of infection with influenza data from hes
influenza_codes_icd = [row['code'] for row in codelist_influenza_icd.select("code").distinct().collect()]
hes_influenza = (
    hes_apc_long
    .filter(f.col("CODE").isin(influenza_codes_icd))
    .withColumnRenamed("EPISTART","DATE")
    .select("PERSON_ID", "DATE", "CODE")
)

# COMMAND ----------

gdppr_influenza.count()

# COMMAND ----------

# Look back for 6 months, with 0 additional days
cases_control_censored_for_influenza = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=6, censor_end_offset=-1)

cases_control_censored_for_influenza = cases_control_censored_for_influenza.select("PERSON_ID", "SET", "case", "index_date", "date_of_birth", "CENSOR_DATE_START", "CENSOR_DATE_END")

cases_controls_influenza_gdppr = join_participants_covariates(gdppr_influenza, cases_control_censored_for_influenza)
cases_controls_influenza_hes = join_participants_covariates(hes_influenza, cases_control_censored_for_influenza)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_influenza_out = {
    'hes_apc':  ['cases_controls_influenza_hes',  'codelist_influenza_icd',  1],
    'gdppr':  ['cases_controls_influenza_gdppr',  'codelist_influenza_snomed',  2]
}

# run codelist match and codelist match summary functions
_out_influenza, _out_influenza_1st, _out_influenza_1st_wide = codelist_match(dict_influenza_out, _name_prefix=f'out_', broadcast=1)
_out_influenza_summ_name, _out_influenza_summ_name_code = codelist_match_summ(dict_influenza_out, _out_influenza)

# COMMAND ----------

_out_influenza['all'].show()

# COMMAND ----------

tab(_out_influenza['all'], "source")

# COMMAND ----------

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_out_influenza_filtered = _out_influenza['all']\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# COMMAND ----------

display(_out_influenza_filtered)

# COMMAND ----------

cov_influenza = (
    _out_influenza_filtered
    .select("PERSON_ID", "set").distinct()
    .withColumn("influenza", f.lit("1"))
)


# COMMAND ----------

print(cov_influenza.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## BMI - Body Max Index
# MAGIC Using curated tables which define BMI across primary and secondary care, from the diabetes algorthim
# MAGIC https://bhfdsc.github.io/documentation/docs/curated_phenotypes/diabetes
# MAGIC
# MAGIC Latest BMI record from 36 months prior to index date
# MAGIC
# MAGIC Custom BMI groups for ethnicities based on NICE guidelines https://cks.nice.org.uk/topics/obesity/diagnosis/identification-classification/ 

# COMMAND ----------

# Restrict BMI values to between 10 -150 inclusive
print(bmi.count())

bmi_restricted = (
    bmi
    .where((f.col("bmi") >= 10) & (f.col("bmi") <= 150))
)

print(bmi_restricted.count())

# COMMAND ----------

# Look back 1 year, with 30 additional days 
cases_control_censored_for_bmi = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=36)
cases_controls_bmi = join_participants_covariates(bmi_restricted, cases_control_censored_for_bmi)
latest_bmi = find_lastest_covariate_record(cases_controls_bmi)

# COMMAND ----------

# Print number of missing BMI values
print_covariate_null_counts(latest_bmi, "bmi")

# Print number of missing BMI Group values
print_covariate_null_counts(latest_bmi, "bmi_group")

# COMMAND ----------

cohort_ethinicity = (
    cohort.select("person_id", "ethnicity_raw_code", "ethnicity_raw_description", "ethnicity_18_code", "ethnicity_18_group", "ethnicity_5_group")
    .withColumnRenamed("person_id", "PERSON_ID")
)

latest_bmi_ethinicity = (
    latest_bmi
    .join(cohort_ethinicity, on="PERSON_ID", how="left")
)
# latest_bmi_ethinicity.show()

latest_bmi_ethinicity =(
    latest_bmi_ethinicity
    .withColumn("mapping_1_calculation_type", 
                f.when(f.col("ethnicity_5_group") == "White", 1)
               .when(f.col("ethnicity_5_group") == "Asian or Asian British", 2)
               .when(f.col("ethnicity_5_group") == "Black, Black British, Caribbean or African", 2)
               .when(f.col("ethnicity_5_group") == "Mixed or multiple", 1)
               .when(f.col("ethnicity_5_group") == "Other ethnic group", 1)
                .otherwise(None)
               )
    .withColumn("mapping_2_calculation_type", 
                f.when(f.col("ethnicity_5_group") == "White", 1)
               .when(f.col("ethnicity_5_group") == "Asian or Asian British", 2)
               .when(f.col("ethnicity_5_group") == "Black, Black British, Caribbean or African", 2)
               .when(f.col("ethnicity_5_group") == "Mixed or multiple", 2)
               .when(f.col("ethnicity_5_group") == "Other ethnic group", 2)
               .otherwise(None)
               )
    .withColumn("bmi_group_1",
                f.when(f.isnull(f.col("bmi")) | f.isnull(f.col("ethnicity_5_group")), None) 
                .when((f.col("mapping_1_calculation_type") == 1) & (f.col("bmi") <= 18.4), "Underweight")
                .when((f.col("mapping_1_calculation_type") == 1) & (f.col("bmi") >= 18.5) & (f.col("bmi") <= 24.9), "Healthy weight")
                .when((f.col("mapping_1_calculation_type") == 1) & (f.col("bmi") >= 25) & (f.col("bmi") <= 29.9), "Overweight")
                .when((f.col("mapping_1_calculation_type") == 1) & (f.col("bmi") >= 30), "Obese")
               .when((f.col("mapping_1_calculation_type") == 2) & (f.col("bmi") <= 18.4), "Underweight")
               .when((f.col("mapping_1_calculation_type") == 2) & (f.col("bmi") >= 18.5) & (f.col("bmi") <= 22.9), "Healthy weight")
               .when((f.col("mapping_1_calculation_type") == 2) & (f.col("bmi") >= 23) & (f.col("bmi") <= 27.4), "Overweight")
               .when((f.col("mapping_1_calculation_type") == 2) & (f.col("bmi") >= 27.5), "Obese")
               .otherwise(None)
                )
    .withColumn("bmi_group_2",
                f.when(f.isnull(f.col("bmi")) | f.isnull(f.col("ethnicity_5_group")), None)  
               .when((f.col("mapping_2_calculation_type") == 1) & (f.col("bmi") <= 18.4), "Underweight")
               .when((f.col("mapping_2_calculation_type") == 1) & (f.col("bmi") >= 18.5) & (f.col("bmi") <= 24.9), "Healthy weight")
               .when((f.col("mapping_2_calculation_type") == 1) & (f.col("bmi") >= 25) & (f.col("bmi") <= 29.9), "Overweight")
               .when((f.col("mapping_2_calculation_type") == 1) & (f.col("bmi") >= 30), "Obese")
               .when((f.col("mapping_2_calculation_type") == 2) & (f.col("bmi") <= 18.4), "Underweight")
                .when((f.col("mapping_2_calculation_type") == 2) & (f.col("bmi") >= 18.5) & (f.col("bmi") <= 22.9), "Healthy weight")
               .when((f.col("mapping_2_calculation_type") == 2) & (f.col("bmi") >= 23) & (f.col("bmi") <= 27.4), "Overweight")
               .when((f.col("mapping_2_calculation_type") == 2) & (f.col("bmi") >= 27.5), "Obese")
               .otherwise(None)
               )
)

display(latest_bmi_ethinicity)


# COMMAND ----------

if checking:
    display(latest_bmi)

# COMMAND ----------

# covariate table for covid
cov_bmi = (
    latest_bmi_ethinicity
    .select("PERSON_ID", "set", "bmi", "bmi_group", "bmi_group_1", "bmi_group_2")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HbA1c - Hemoglobin A1c
# MAGIC
# MAGIC Using curated tables which define HbA1c across primary and secondary care, from the diabetes algorthim
# MAGIC https://bhfdsc.github.io/documentation/docs/curated_phenotypes/diabetes
# MAGIC
# MAGIC Latest HbA1c record from 24 months prior to index date

# COMMAND ----------

# Restricting values not in desired range 30 - 130 inclusive

print(hba1c.count())

hba1c_restricted = (
    hba1c.where((f.col("hba1c") >= 30) & (f.col("hba1c") <= 130))
)

print(hba1c_restricted.count())

# COMMAND ----------

# Look back 2 years, with 0 additional days 
cases_control_censored_for_hba1c = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=24)
cases_controls_hba1c = join_participants_covariates(hba1c_restricted, cases_control_censored_for_hba1c)
latest_hba1c = find_lastest_covariate_record(cases_controls_hba1c)

# COMMAND ----------

if checking:
    display(latest_hba1c)

# COMMAND ----------

# Print number of missing HbA1c values
print_covariate_null_counts(latest_hba1c, "hba1c")

# Print number of missing HbA1c Group values
print_covariate_null_counts(latest_hba1c, "hba1c_group")

# COMMAND ----------

if checking:
    latest_hba1c_filtered = (
    latest_hba1c.select(f.col("PERSON_ID"), f.col("set"), f.col("hba1c"), f.col("hba1c_group"))
    )

    latest_hba1c_filtered.summary().show()

# COMMAND ----------

if checking:
    # Check values with hba1c groups
    groupby_describe(latest_hba1c, "hba1c_group", "hba1c")

# COMMAND ----------

# covariate table for hba1c
cov_hba1c = (
    latest_hba1c
    .select("PERSON_ID", "set", "hba1c", "hba1c_group")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## eGFR - estimated glomerular filtration rate
# MAGIC
# MAGIC Defined by eGFR codelist
# MAGIC
# MAGIC Latest eGFR record 24 months prior to index date
# MAGIC

# COMMAND ----------

# Codelist for eGFR
codelist_egfr = (
    codelist_covariates
    .filter(f.col("name") == "egfr")
)
codelist_egfr.show()

# COMMAND ----------


# Prepare eGFR data from gdppr
egfr_codes = [row['code'] for row in codelist_egfr.select("code").distinct().collect()]
gdppr_egfr = gdppr.filter(f.col("CODE").isin(egfr_codes))


egfr = (
  gdppr_egfr.drop("VALUE2_CONDITION")
  .withColumnRenamed("VALUE1_CONDITION","val")
  .withColumn('val', f.col('val'))
  .filter(f.col('val').isNotNull())
  .withColumnRenamed("val","eGFR")
  .select(f.col("NHS_NUMBER_DEID").alias("PERSON_ID"),"DATE",f.col("code").alias("egfr_code"),"eGFR")
)


# COMMAND ----------

# Look back 2 years, with 0 additional days
cases_control_censored_for_egfr = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=24)
cases_controls_egfr = join_participants_covariates(egfr, cases_control_censored_for_egfr)
latest_egfr = find_lastest_covariate_record(cases_controls_egfr)

# COMMAND ----------

# Print number of missing eGFR values
print_covariate_null_counts(latest_egfr, "eGFR")

# COMMAND ----------

if checking:
    #Check 
    groupby_describe(latest_egfr, "egfr_code", "eGFR")

# COMMAND ----------

cov_egfr = (
    latest_egfr
    .select("PERSON_ID", "set", "egfr")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # eGFR through Serum creatinine (SCr) calculation
# MAGIC
# MAGIC
# MAGIC
# MAGIC Defined by SCr codelist
# MAGIC
# MAGIC Latest SCr record 24 months prior to index date, converted to eGFR using CKD-EPI 2021 calculation - https://www.kidney.org/ckd-epi-creatinine-equation-2021-0

# COMMAND ----------


# Codelist for eGFR
codelist_scr = (
    codelist_covariates
    .filter(f.col("name") == "SCR")
)
codelist_scr.show()


# COMMAND ----------

gdppr_subset_scr = (
  gdppr
  .join(codelist_scr, on = 'code', how = 'inner')
  .withColumn('DATE', f.to_date(f.col('DATE'), 'yyyyMMdd'))
  .withColumn('scr', f.col('VALUE1_CONDITION').cast("int"))
  .filter(f.col('scr').isNotNull())
  .filter((f.col('scr') > '0') & (f.col('scr') < '1000'))
  .select('NHS_NUMBER_DEID', 'DATE', 'CODE', 'scr')
)


# COMMAND ----------

if checking:
    # Check earliest and latest dates
    gdppr_subset_scr.select(f.min('DATE'), f.max('DATE')).show()

# COMMAND ----------

cohort_dob_sex = ( 
    cohort
    .select(['person_id', 'date_of_birth', 'sex'])
)
scr_dob_sex = (
    gdppr_subset_scr
    .withColumnRenamed('NHS_NUMBER_DEID', 'person_id')
    .join(cohort_dob_sex, on='person_id', how='inner')
    .withColumn('age', f.floor(f.datediff(f.col('DATE'), f.col('date_of_birth'))/365.25))
    )


# COMMAND ----------

display(scr_dob_sex)

# COMMAND ----------

# Restrict scr values to between 20 - 2000 inclusive
print(scr_dob_sex.count())

scr_dob_sex_restricted = (
    scr_dob_sex
    .where((f.col("scr") >= 20) & (f.col("scr") <= 2000))
)

print(scr_dob_sex_restricted.count())

# COMMAND ----------

if checking:
    #Check
    scr_dob_sex.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sCr to eGFR Formaula - using CDK-EPI 2021 
# MAGIC eGFRcr = 142 * min(Scr/κ, 1)^α * max(Scr/κ, 1)-1.200 * 0.9938^Age * 1.012 [if female]
# MAGIC
# MAGIC where:
# MAGIC - Scr = standardized serum creatinine in mg/dL
# MAGIC - κ = 0.7 (females) or 0.9 (males)
# MAGIC - α = -0.241 (female) or -0.302 (male)
# MAGIC - min(Scr/κ, 1) is the minimum of Scr/κ or 1.0
# MAGIC - max(Scr/κ, 1) is the maximum of Scr/κ or 1.0
# MAGIC - Age (years)

# COMMAND ----------

# SCr to eGFR algorithm:

# Define constants based on sex
k = f.when(f.col("sex") == "F", 0.7).otherwise(0.9)  # k = 0.7 for females, 0.9 for males
a = f.when(f.col("sex") == "F", -0.241).otherwise(-0.302)  # a = -0.241 for females, -0.302 for males
female_factor = f.when(f.col("sex") == "F", 1.012).otherwise(1.0)  # 1.012 for females, 1.0 for males

# Step 1: Convert sCr from µmol/L to mg/dL
scr_mg_dl = f.col("scr") / 88.4

# Step 2: Calculate min(SCr/k, 1)^a
min_scr_k = f.pow(f.least(scr_mg_dl / k, f.lit(1)), a)

# Step 3: Calculate max(SCr/k, 1)^-1.2
max_scr_k = f.pow(f.greatest(scr_mg_dl / k, f.lit(1)), -1.2)

# Step 4: Apply age adjustment
age_factor = f.pow(0.9938, f.col("age"))

# Step 5: Combine all components of the CKD-EPI equation 
eGFR = 142 * min_scr_k * max_scr_k * age_factor * female_factor

# Add the eGFR column to the DataFrame rounded to nearest whole number
scr_egfr = scr_dob_sex_restricted.withColumn("eGFR", f.round(eGFR,0))

# Show the results
scr_egfr.show()


# COMMAND ----------

scr_egfr = scr_egfr.select("PERSON_ID", "DATE", "eGFR")

# COMMAND ----------

# Look back 2 year, with 0 additional days
cases_control_censored_for_scr_egfr = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=24)
cases_controls_scr_egfr = join_participants_covariates(scr_egfr, cases_control_censored_for_scr_egfr)
latest_scr_egfr = find_lastest_covariate_record(cases_controls_scr_egfr)

# COMMAND ----------

print_covariate_null_counts(latest_scr_egfr, "eGFR")

# COMMAND ----------

cov_scr_egfr = (
    latest_scr_egfr
    .withColumnRenamed("eGFR","scr_egfr")
    .select("PERSON_ID", "set", "scr_egfr")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # History of Chronic Kidney Disease (CKD), calculated by eGFR

# COMMAND ----------

scr_egfr_low = (
  scr_egfr
  .filter(f.col("eGFR") < 60)
)

# COMMAND ----------

# Look back for all history, with 0 additional days
cases_control_censored_for_egfr_ckd = add_censor_dates(cases_controls, has_selection_period=False)

cases_controls_egfr_ckd = join_participants_covariates(scr_egfr_low, cases_control_censored_for_egfr_ckd)


# cases_control_censored_for_hf = cases_control_censored_for_hf.select("PERSON_ID", "SET", "matching_ID", "case", "index_date", "date_of_birth", "CENSOR_DATE_START", "CENSOR_DATE_END")

# cases_controls_hf_gdppr = join_participants_covariates(gdppr_hf, cases_control_censored_for_hf)
# cases_controls_hf_hes = join_participants_covariates(hes_hf, cases_control_censored_for_hf)



# COMMAND ----------

display(cases_controls_egfr_ckd)

# COMMAND ----------

# if checking:
#     display(scr_egfr_dkf)
display(scr_egfr_low)

# COMMAND ----------

if checking:
    count_var(scr_egfr_low, "PERSON_ID")

# COMMAND ----------

print(cases_controls_egfr_ckd.count())

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
cases_controls_egfr_ckd_filtered = ( cases_controls_egfr_ckd
  .where(
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

print(cases_controls_egfr_ckd_filtered.count())

# COMMAND ----------

# Find the earliest and latest eGFR reading date for each person
scr_egfr_ckd = (
    cases_controls_egfr_ckd_filtered
    .groupBy("person_id", "set")
    .agg(
        f.min("DATE").alias("earliest_date"),
        f.max("DATE").alias("latest_date")
    )
    .filter(f.datediff(f.col("latest_date"), f.col("earliest_date")) >= 90)  # CKD condition
    .select("person_id","set","earliest_date","latest_date" )
)


# COMMAND ----------

if checking:
    display(scr_egfr_ckd)

# COMMAND ----------

count_var(scr_egfr_ckd, "person_id")

# COMMAND ----------


cov_scr_egfr_hx_ckd = (
    scr_egfr_ckd
    .withColumn('scr_egfr_hx_ckd',f.lit(1))
    .select ("PERSON_ID", "set", "scr_egfr_hx_ckd")
)



# COMMAND ----------

if checking:
    count_var(cov_scr_egfr_hx_ckd, "PERSON_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC # Total Cholesterol

# COMMAND ----------

codelist_total_chol = (
    codelist_covariates
    .filter(f.col("name") == "total_cholesterol")
)

codelist_total_chol.show()

# COMMAND ----------


# Prepare Total Cholesterol data from gdppr
total_chol_codes = [row['code'] for row in codelist_total_chol.select("code").distinct().collect()]
gdppr_total_chol = gdppr.filter(f.col("CODE").isin(total_chol_codes))


total_chol = (
  gdppr_total_chol.drop("VALUE2_CONDITION")
  .withColumnRenamed("VALUE1_CONDITION","val")
  .withColumn('val', f.col('val'))
  .filter(f.col('val').isNotNull())
  .withColumnRenamed("val","total_chol")
  .select(f.col("NHS_NUMBER_DEID").alias("PERSON_ID"),"DATE", f.col("code").alias("chol_code"),"total_chol")
)



# COMMAND ----------

display(total_chol)


# COMMAND ----------

# Restrict Total Cholesterol values to between 1-30 inclusive
print(total_chol.count())

total_chol_restricted = (
    total_chol
    .where((f.col("total_chol") >= 1) & (f.col("total_chol") <= 30))
)

print(total_chol_restricted.count())

# COMMAND ----------

# Total Cholesterol 
# Look back 3 years, with 0 additional days
cases_control_censored_for_total_chol = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=36)
cases_controls_total_chol = join_participants_covariates(total_chol_restricted, cases_control_censored_for_total_chol)
latest_total_chol = find_lastest_covariate_record(cases_controls_total_chol)

# COMMAND ----------

# Check number of missing values for Total Cholesterol
print_covariate_null_counts(latest_total_chol, "total_chol")

# COMMAND ----------

if checking:
    groupby_describe(latest_total_chol, "chol_code" , "total_chol")

# COMMAND ----------

cov_total_chol = (
    latest_total_chol
    .select("PERSON_ID", "set", "total_chol")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # High-Density Lipoprotein (HDL) Cholesterol
# MAGIC
# MAGIC As defined by HDL Cholesterol codelist
# MAGIC
# MAGIC Latest HDL Cholesteral record 36 months prior to the index date

# COMMAND ----------

codelist_hdl_chol = (
    codelist_covariates
    .filter(f.col("name") == "hdl_cholesterol")
)
codelist_hdl_chol.show()


# COMMAND ----------

# Prepare HDL Cholesterol data from gdppr
hdl_chol_codes = [row['code'] for row in codelist_hdl_chol.select("code").distinct().collect()]
gdppr_hdl_chol = gdppr.filter(f.col("CODE").isin(hdl_chol_codes))


hdl_chol = (
  gdppr_hdl_chol.drop("VALUE2_CONDITION")
  .withColumnRenamed("VALUE1_CONDITION","val")
  .withColumn('val', f.col('val'))
  .filter(f.col('val').isNotNull())
  .withColumnRenamed("val","hdl_chol")
  .select(f.col("NHS_NUMBER_DEID").alias("PERSON_ID"),"DATE", f.col("code").alias("chol_code"),"hdl_chol")
)

# COMMAND ----------

display(hdl_chol)

# COMMAND ----------

# Restrict HDL Cholesterol values to between 0.1-10 inclusive
print(hdl_chol.count())

hdl_chol_restricted = (
    hdl_chol
    .where((f.col("hdl_chol") >= 0.1) & (f.col("hdl_chol") <= 10))
)

print(hdl_chol_restricted.count())

# COMMAND ----------

# Look back 3 years, with 0 additional days
cases_control_censored_for_hdl_chol = add_censor_dates(cases_controls, has_selection_period=True, selection_period_months=36)
cases_controls_hdl_chol = join_participants_covariates(hdl_chol_restricted, cases_control_censored_for_hdl_chol)
latest_hdl_chol = find_lastest_covariate_record(cases_controls_hdl_chol)

# COMMAND ----------

# Check number of missing values for HDL Cholesterol
print_covariate_null_counts(latest_hdl_chol, "hdl_chol")

# COMMAND ----------

if checking:
    groupby_describe(latest_hdl_chol, "chol_code" , "hdl_chol")

# COMMAND ----------

cov_hdl_chol = (
    latest_hdl_chol
    .select("PERSON_ID", "set", "hdl_chol")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # History of Heart Failure

# COMMAND ----------

codelist_hf_snomed = (
    codelist_covariates
    .filter((f.col("name") == "HF") & (f.col("terminology") =="SNOMED"))
)

codelist_hf_icd = (
    codelist_covariates
    .filter((f.col("name") == "HF") & (f.col("terminology") =="ICD10"))
)
codelist_hf_snomed.show()
codelist_hf_icd.show()

# COMMAND ----------

# Prepare history of heart failure data from gdppr
hf_codes_snomed = [row['code'] for row in codelist_hf_snomed.select("code").distinct().collect()]
gdppr_hf = (
    gdppr
    .filter(f.col("CODE").isin(hf_codes_snomed))
    .withColumnRenamed("NHS_NUMBER_DEID","PERSON_ID")
    .select("PERSON_ID", "DATE", "CODE", )
)

# Prepare history of heart failure data from hes
hf_codes_icd = [row['code'] for row in codelist_hf_icd.select("code").distinct().collect()]
hes_hf = (
    hes_apc_long
    .filter(f.col("CODE").isin(hf_codes_icd))
    .withColumnRenamed("EPISTART","DATE")
    .select("PERSON_ID", "DATE", "CODE")
)

# COMMAND ----------

if checking:
    gdppr_hf.show()

# COMMAND ----------

# Look back for all history, with 0 additional days
cases_control_censored_for_hf = add_censor_dates(cases_controls, has_selection_period=False)

cases_control_censored_for_hf = cases_control_censored_for_hf.select("PERSON_ID", "SET", "case", "index_date", "date_of_birth", "CENSOR_DATE_START", "CENSOR_DATE_END")

cases_controls_hf_gdppr = join_participants_covariates(gdppr_hf, cases_control_censored_for_hf)
cases_controls_hf_hes = join_participants_covariates(hes_hf, cases_control_censored_for_hf)


# COMMAND ----------

if checking:
    cases_controls_hf_gdppr.show()

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hf_out = {
    'hes_apc':  ['cases_controls_hf_hes',  'codelist_hf_icd',  1],
    'gdppr':  ['cases_controls_hf_gdppr',  'codelist_hf_snomed',  2]
}

# run codelist match and codelist match summary functions
_out_hf, _out_hf_1st, _out_hf_1st_wide = codelist_match(dict_hf_out, _name_prefix=f'out_', broadcast=1)
_out_hf_summ_name, _out_hf_summ_name_code = codelist_match_summ(dict_hf_out, _out_hf)


# COMMAND ----------

if checking:
    display(_out_hf['all'])


# COMMAND ----------

if checking:
    tab(_out_hf['all'],'source')

# COMMAND ----------

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_out_hf_filtered = _out_hf['all']\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# COMMAND ----------

sampling_window = Window.partitionBy(['PERSON_ID','set']).orderBy('DATE')

_out_hf_filtered_1st = _out_hf_filtered.withColumn(
    'rank', f.row_number().over(sampling_window)
).filter(f.col('rank') == 1)

# COMMAND ----------

if checking:
    tab(_out_hf_filtered_1st,'source')

# COMMAND ----------

cov_hx_hf = (
    _out_hf_filtered_1st
    .withColumn('hx_hf',f.lit(1))
    .select ("PERSON_ID", "set", "hx_hf")
)

# cov_hx_hf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # History of Chronic Kidney Disease

# COMMAND ----------

codelist_ckd_snomed = (
    codelist_covariates
    .filter((f.col("name") == "CKD") & (f.col("terminology") =="SNOMED"))
)

codelist_ckd_icd = (
    codelist_covariates
    .filter((f.col("name") == "CKD") & (f.col("terminology") =="ICD10"))
)
codelist_ckd_snomed.show()
codelist_ckd_icd.show()

# COMMAND ----------

# Prepare history of chronic kidney disease data from gdppr
ckd_codes_snomed = [row['code'] for row in codelist_ckd_snomed.select("code").distinct().collect()]
gdppr_ckd = (
    gdppr
    .filter(f.col("CODE").isin(ckd_codes_snomed))
    .withColumnRenamed("NHS_NUMBER_DEID","PERSON_ID")
    .select("PERSON_ID", "DATE", "CODE", )
)

# Prepare history ofchronic kidney disease data from hes
ckd_codes_icd = [row['code'] for row in codelist_ckd_icd.select("code").distinct().collect()]
hes_ckd = (
    hes_apc_long
    .filter(f.col("CODE").isin(ckd_codes_icd))
    .withColumnRenamed("EPISTART","DATE")
    .select("PERSON_ID", "DATE", "CODE")
)

# COMMAND ----------

# Look back for all history, with 0 additional days
cases_control_censored_for_ckd = add_censor_dates(cases_controls, has_selection_period=False)

cases_control_censored_for_ckd = cases_control_censored_for_ckd.select("PERSON_ID", "SET", "case", "index_date", "date_of_birth", "CENSOR_DATE_START", "CENSOR_DATE_END")

cases_controls_ckd_gdppr = join_participants_covariates(gdppr_ckd, cases_control_censored_for_ckd)
cases_controls_ckd_hes = join_participants_covariates(hes_ckd, cases_control_censored_for_ckd)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_ckd_out = {
    'hes_apc':  ['cases_controls_ckd_hes',  'codelist_ckd_icd',  1],
    'gdppr':  ['cases_controls_ckd_gdppr',  'codelist_ckd_snomed',  2]
}

# run codelist match and codelist match summary functions
_out_ckd, _out_ckd_1st, _out_ckd_1st_wide = codelist_match(dict_ckd_out, _name_prefix=f'out_', broadcast=1)
_out_ckd_summ_name, _out_ckd_summ_name_code = codelist_match_summ(dict_ckd_out, _out_ckd)

# COMMAND ----------

if checking:
    tab(_out_ckd['all'],'source')

# COMMAND ----------

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_out_ckd_filtered = _out_ckd['all']\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# COMMAND ----------

sampling_window = Window.partitionBy(['PERSON_ID','set']).orderBy('DATE')

_out_ckd_filtered_1st = _out_ckd_filtered.withColumn(
    'rank', f.row_number().over(sampling_window)
).filter(f.col('rank') == 1)

# COMMAND ----------

if checking:
    tab(_out_hf_filtered_1st,'source')

# COMMAND ----------

cov_hx_ckd = (
    _out_ckd_filtered_1st
    .withColumn('hx_ckd',f.lit(1))
    .select ("PERSON_ID", "set", "hx_ckd")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Medications

# COMMAND ----------

if checking:
    tab(codelist_covariates, "name");print();

# COMMAND ----------

# Prepare dataset for medications

# reduce and rename columns medications dataset
pmed_prepared = pmeds\
  .select(['Person_ID_DEID', 'ProcessingPeriodDate', 'PrescribedBNFCode'])\
  .withColumnRenamed('Person_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('ProcessingPeriodDate', 'DATE')\
  .withColumnRenamed('PrescribedBNFCode', 'CODE')

cases_controls_prepared = (
    cases_controls
    .select(['PERSON_ID','set','case','index_date'])
)

# Look back 1 year, with no additional days
cases_control_censored_for_meds = add_censor_dates(cases_controls_prepared, has_selection_period=True, selection_period_months=12)
cases_controls_meds = join_participants_covariates(pmed_prepared, cases_control_censored_for_meds)
cases_controls_meds_censored = filter_on_censor_dates(cases_controls_meds)


# COMMAND ----------

display(codelist_covariates)

# COMMAND ----------

medications_code_names = ["glucose_lowering", "raas_inhibitor","calcium_channel_blocker", "antipsychotic","beta_blocker", "corticosteroid", "diuretic", "statin"]

codelist_medications = (
    codelist_covariates
    .filter(f.col('name').isin(medications_code_names))
)


# dictionary - dataset, codelist, and ordering in the event of tied records
dict_meds_out = {
    'pmed':  ['cases_controls_meds_censored',  'codelist_medications',  1]
}

# run codelist match and codelist match summary functions
_out_covariate_meds, _out_covariate_meds_1st, _out_covariate_meds_1st_wide = codelist_match(dict_meds_out, _name_prefix=f'out_', _last_event=1, broadcast=1) # filtered to last event = true

# COMMAND ----------

_out_covariates_meds_all = _out_covariate_meds["all"]


_out_covariates_meds_all = (
    _out_covariates_meds_all.withColumn("days_diff", f.datediff(f.col("CENSOR_DATE_END"),f.col("DATE")))
)

# COMMAND ----------

# Filter on medication within 12 months and create a binary column indicating the presence of the medication
_out_covariates_meds_all_12mos = ( _out_covariates_meds_all
    .filter(f.col("days_diff") <= 365)
    .withColumn("value", f.lit(1))
)

# # filter on only records with value = 1
# _out_covariates_meds_all_12mos = 

# # Create a binary column indicating the presence of the medication
# _out_covariates_meds_all = _out_covariates_meds_all.withColumn("value", f.lit(1))

# Pivot the DataFrame
cov_meds = (
    _out_covariates_meds_all_12mos.groupBy("PERSON_ID", "set")
    .pivot("name")
    .agg(f.first("value"))
)

# Fill nulls with 0 (indicating no record of the medication)
cov_meds = cov_meds.fillna(0)


# display(cov_meds)

# COMMAND ----------

# # Group by PERSON_ID and count the occurrences
# check_df = (
#     pivot_df.groupBy("PERSON_ID")
#     .count()
#     .filter(f.col("count") > 1)
# )


# # Get the list of PERSON_IDs that appear more than once
# duplicate_ids = [row["PERSON_ID"] for row in check_df.collect()]

# # Filter the original DataFrame to show records for these PERSON_IDs
# check_df_values = pivot_df.filter(f.col("PERSON_ID").isin(duplicate_ids)).show()

# display(check_df_values)

# COMMAND ----------

#display(_out_covariate_meds_1st)
count_var(_out_covariate_meds_1st, "PERSON_ID");print();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine

# COMMAND ----------


covariates_combined = (
    cases_controls
    .join(cov_covid, on=["PERSON_ID", "set"], how="left")
    .join(cov_covid_offset, on=["PERSON_ID", "set"], how="left")
    .join(cov_influenza, on=["PERSON_ID", "set"], how="left")
    .join(cov_bmi, on=["PERSON_ID", "set"], how="left")
    .join(cov_hba1c, on=["PERSON_ID", "set"], how="left")
    .join(cov_egfr, on=["PERSON_ID", "set"], how="left")
    .join(cov_scr_egfr, on=["PERSON_ID", "set"], how="left")
    .join(cov_scr_egfr_hx_ckd, on=["PERSON_ID", "set"], how="left")
    .join(cov_total_chol, on=["PERSON_ID", "set"], how="left")
    .join(cov_hdl_chol, on=["PERSON_ID", "set"], how="left")
    .join(cov_hx_hf, on=["PERSON_ID", "set"], how="left")
    .join(cov_meds, on=["PERSON_ID", "set"], how="left")
)

# COMMAND ----------

# MAGIC %md # 3. Save
# MAGIC

# COMMAND ----------

save_table(covariates_combined, out_name=f'{proj}_out_covariates')

