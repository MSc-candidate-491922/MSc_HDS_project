# Databricks notebook source
# MAGIC %md # CCU053-D04-curated_data
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the curated tables.
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on CCU004_01, CCU002_07 and subsequently CCU003_05-D03a-curated_data. Based on work by Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu053_cur_hes_apc_all_years_archive_long`** : HES APC codes in long format
# MAGIC - **`ccu053_cur_hes_apc_all_years_archive_op_long`** : HES APC operation codes in long format
# MAGIC - **`ccu053_archive_sing`** : Death codes in wide format (one row per person)
# MAGIC - **`ccu053_archive_long`** : Death codes in long format

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

# MAGIC %md # 2. Data

# COMMAND ----------

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
#hes_apc_otr = extract_batch_from_archive(parameters_df_datasets, 'hes_apc_otr')
deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')

# COMMAND ----------

# MAGIC %md ## Adhoc investigation
# MAGIC
# MAGIC The pipeline was last ran as at 2023-08-15 using HES APC archived_on date 2023-06-21. 

# COMMAND ----------

hes_apc_all = spark.table(f'dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive')
# display(hes_apc_all.select("archived_on").groupBy("archived_on").count())

# COMMAND ----------

hes_apc_otr_all = spark.table(f'dars_nic_391419_j3w9t_collab.hes_apc_otr_all_years_archive')
# display(hes_apc_otr_all.select("archived_on").groupBy("archived_on").count())

# COMMAND ----------

# MAGIC %md ### Duplicated records in HES APC

# COMMAND ----------

# hes_apc = spark.table(f'dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive').where(f.col('archived_on') == '2023-06-21')

# COMMAND ----------

# MAGIC %md Ran the HES APC section

# COMMAND ----------

# hes_apc_long_new = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long')
# hes_apc_long_old = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long_pre20230822_143831')

# COMMAND ----------

# print(hes_apc_long_new.count())
# print(hes_apc_long_old.count())

# COMMAND ----------

# key = ['PERSON_ID', 'EPISTART']

# old = hes_apc_long_old
# new = hes_apc_long_new
# count_varlist(old, key)
# count_varlist(new, key)

# old1 = (
#   old
#   .dropDuplicates(key)
#   .where(f.col('PERSON_ID').isNotNull())
#   .where(f.col('EPISTART').isNotNull())
# )
# new1 = (
#   new
#   .dropDuplicates(key)
#   .where(f.col('PERSON_ID').isNotNull())
#   .where(f.col('EPISTART').isNotNull())
# )


# count_varlist(old1, key)
# count_varlist(new1, key)

# COMMAND ----------

# MAGIC %md # 3. HES_APC

# COMMAND ----------

display(hes_apc.limit(100))

# COMMAND ----------

# select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic columns)
# rename PERSON_ID
_hes_apc = (
  hes_apc  
  .select(['PERSON_ID_DEID', 'EPIKEY', 'EPISTART', 'ADMIDATE'] 
          + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .orderBy('PERSON_ID', 'EPIKEY')
)

# check
#count_var(hes_apc, 'PERSON_ID_DEID'); print()
#count_var(hes_apc, 'EPIKEY'); print()

# check for null EPISTART and potential use ADMIDATE to supplement
tmpp = (
  hes_apc
  .select('EPISTART', 'ADMIDATE')
  .withColumn('_EPISTART', f.when(f.col('EPISTART').isNotNull(), 1).otherwise(0))
  .withColumn('_ADMIDATE', f.when(f.col('ADMIDATE').isNotNull(), 1).otherwise(0))
)
tmpt = tab(tmpp, '_EPISTART', '_ADMIDATE', var2_unstyled=1); print()
# => ADMIDATE is always null when EPISTART is null

# COMMAND ----------

# check
# display(_hes_apc)

# COMMAND ----------

# MAGIC %md ## 3.1 Diagnosis

# COMMAND ----------

# MAGIC %md ### 3.1.1 Create

# COMMAND ----------

# reshape twice, tidy, and remove records with missing code

hes_apc_long = (
  reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIDATE'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
  .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
  .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
  .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
  .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
)

# COMMAND ----------

# # check
# # tmpt = tab(hes_apc_long, '_chk'); print()
# assert hes_apc_long.where(f.col('_chk') == 0).count() == 0
# tmpt = tab(hes_apc_long, '_DIAG_4_len'); print()
# tmpt = tab(hes_apc_long, '_chk2'); print()
# assert hes_apc_long.where(f.col('_chk2') == 0).count() == 0

# tidy
hes_apc_long = (
  hes_apc_long
  .drop('_tmp', '_chk')
)

hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIDATE', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

display(hes_apc_long.limit(1))

# COMMAND ----------

# MAGIC %md ### 3.1.2 Save

# COMMAND ----------

save_table(df=hes_apc_long, out_name=f'{proj}_cur_hes_apc_all_years_archive_long', save_previous=True, data_base=dsa)

# COMMAND ----------

# MAGIC %md ### 3.1.3 Check

# COMMAND ----------

# check
#count_var(hes_apc_long, 'PERSON_ID'); print()
#count_var(hes_apc_long, 'EPIKEY'); print()

# # check removal of trailing X
# tmpp = hes_apc_long\
#   .where(f.col('CODE').rlike('X'))\
#   .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
# tmpt = tab(tmpp, 'flag'); print()
# tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 4. HES_APC Operations

# COMMAND ----------

# MAGIC %md Operations derived from HES APC

# COMMAND ----------

# # select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic columns)
# # rename PERSON_ID
# _hes_apc_op = (
#   hes_apc  
#   .select(['PERSON_ID_DEID', 'EPIKEY', 'EPISTART', 'ADMIDATE'] 
#           + [col for col in list(hes_apc.columns) if re.match(r'^OPERTN_(3|4)_\d\d$', col)])
#   .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
#   .orderBy('PERSON_ID', 'EPIKEY')
# )

# # check
# #count_var(hes_apc, 'PERSON_ID_DEID'); print()
# #count_var(hes_apc, 'EPIKEY'); print()

# # check for null EPISTART and potential use ADMIDATE to supplement
# tmpp = (
#   hes_apc
#   .select('EPISTART', 'ADMIDATE')
#   .withColumn('_EPISTART', f.when(f.col('EPISTART').isNotNull(), 1).otherwise(0))
#   .withColumn('_ADMIDATE', f.when(f.col('ADMIDATE').isNotNull(), 1).otherwise(0))
# )
# tmpt = tab(tmpp, '_EPISTART', '_ADMIDATE', var2_unstyled=1); print()
# # => ADMIDATE is always null when EPISTART is null


# COMMAND ----------

# display(_hes_apc_op)

# COMMAND ----------

# MAGIC %md ## 4.1 Diagnosis

# COMMAND ----------

# MAGIC %md ### 4.1.1 Create

# COMMAND ----------

# # reshape twice, tidy, and remove records with missing code

# hes_apc_op_long = (
#   reshape_wide_to_long_multi(_hes_apc_op, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIDATE'], j='POSITION', stubnames=['OPERTN_4', 'OPERTN_3'])
#   .withColumn('_tmp', f.substring(f.col('OPERTN_4'), 1, 3))
#   .withColumn('_chk', udf_null_safe_equality('OPERTN_3', '_tmp').cast(t.IntegerType()))
#   .withColumn('_OPERTN_4_len', f.length(f.col('OPERTN_4')))
#   .withColumn('_chk2', f.when((f.col('_OPERTN_4_len').isNull()) | (f.col('_OPERTN_4_len') <= 4), 1).otherwise(0))
# )

# COMMAND ----------

# display(hes_apc_op_long)

# COMMAND ----------

# # check
# tmpt = tab(hes_apc_op_long, '_chk'); print()

# # getting rid of this check for now as Operation codes do have -
# #assert hes_apc_op_long.where(f.col('_chk') == 0).count() == 0
# tmpt = tab(hes_apc_op_long, '_OPERTN_4_len'); print()
# tmpt = tab(hes_apc_op_long, '_chk2'); print()
# assert hes_apc_op_long.where(f.col('_chk2') == 0).count() == 0

# COMMAND ----------

# # tidy
# hes_apc_op_long = (
#   hes_apc_op_long
#   .drop('_tmp', '_chk')
# )

# hes_apc_op_long = reshape_wide_to_long_multi(hes_apc_op_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIDATE', 'POSITION'], j='OPERTN_DIGITS', stubnames=['OPERTN_'])\
#   .withColumnRenamed('POSITION', 'OPERTN_POSITION')\
#   .withColumn('OPERTN_POSITION', f.regexp_replace('OPERTN_POSITION', r'^[0]', ''))\
#   .withColumn('OPERTN_DIGITS', f.regexp_replace('OPERTN_DIGITS', r'[_]', ''))\
#   .withColumn('OPERTN_', f.regexp_replace('OPERTN_', r'X$', ''))\
#   .withColumn('OPERTN_', f.regexp_replace('OPERTN_', r'[.,\-\s]', ''))\
#   .withColumnRenamed('OPERTN_', 'CODE')\
#   .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
#   .orderBy(['PERSON_ID', 'EPIKEY', 'OPERTN_DIGITS', 'OPERTN_POSITION'])

# COMMAND ----------

# display(hes_apc_op_long)

# COMMAND ----------

# MAGIC %md #### Check
# MAGIC
# MAGIC Checking that there are no instances in which an operation has a 3 digit code but their 4 digit code is missing.
# MAGIC
# MAGIC If this were the case, then when we filter for the operation codes in the codelist (which are in 4 digit format) then we would miss some parent 3 digit codes that could be used - as for this project cases are defined using 3 digit codes anyway.

# COMMAND ----------

# hes_op_wide = (hes_apc_op_long.groupBy("PERSON_ID","EPIKEY","EPISTART","ADMIDATE","OPERTN_POSITION").pivot("OPERTN_DIGITS").agg(f.first("CODE")))

# COMMAND ----------

# tmp1 = (
#     hes_op_wide
#     .withColumnRenamed('3','OPERTRN_3').withColumnRenamed('4','OPERTRN_4')
#     .withColumn("OPERTRN_4_3", f.col("OPERTRN_4").substr(1, 3))
#     )

# COMMAND ----------

# display(tmp1.filter(f.col("OPERTRN_4_3")!=f.col("OPERTRN_3")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As seen above, there are no cases in which we have a 3 digit code but a missing 4 digit code.
# MAGIC

# COMMAND ----------

# MAGIC %md ### 4.1.2 Save

# COMMAND ----------

# save_table(df=hes_apc_op_long, out_name=f'{proj}_cur_hes_apc_all_years_archive_op_long', save_previous=True, data_base=dsa)

# COMMAND ----------

# MAGIC %md ### 4.1.3 Check

# COMMAND ----------

# # check
# #count_var(hes_apc_long, 'PERSON_ID'); print()
# #count_var(hes_apc_long, 'EPIKEY'); print()

# # check removal of trailing X
# tmpp = hes_apc_op_long\
#   .where(f.col('CODE').rlike('X'))\
#   .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
# tmpt = tab(tmpp, 'flag'); print()
# tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 4.2 HES_APC_OTR - Operation Dates

# COMMAND ----------

# MAGIC %md Operation dates derived from HES APC OTR
# MAGIC
# MAGIC These dates will be joined onto the operations, derived above, as part of the R pipeline.
# MAGIC
# MAGIC (In future probably nicer to join here but was having issues with Databricks as at August so continued this work in RStudio).

# COMMAND ----------

# display(hes_apc_otr)

# COMMAND ----------

# _hes_apc_op_otr = (
#   hes_apc_otr
#   .select(['PERSON_ID_DEID', 'EPIKEY'] 
#           + [col for col in list(hes_apc_otr.columns) if re.match(r'^OP(DATE|ERTN)_\d\d$', col)])
#   .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
#   .orderBy('PERSON_ID', 'EPIKEY')
# )

# COMMAND ----------

# hes_apc_op_otr_long = (
#   reshape_wide_to_long_multi(_hes_apc_op_otr, i=['PERSON_ID', 'EPIKEY'], j='POSITION', stubnames=['OPDATE', 'OPERTN'])
#   #.drop(f.col("POSITION"))
#   .filter(f.col("OPERTN").isNotNull())
#   .filter(f.col("OPERTN")!="-")
#   .distinct()
# )

# COMMAND ----------

# display(hes_apc_op_otr_long)

# COMMAND ----------

# save_table(df=hes_apc_op_otr_long, out_name=f'{proj}_cur_hes_apc_all_years_archive_op_otr_long', save_previous=True, data_base=dsa)

# COMMAND ----------

# MAGIC %md # 5. Deaths

# COMMAND ----------

# MAGIC %md ## 5.1 Create

# COMMAND ----------

# check
#count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID'); print()
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# define window for the purpose of creating a row number below as per the skinny patient table
_win = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# rename ID
# remove records with missing IDs
# reformat dates
# reduce to a single row per individual as per the skinny patient table
# select columns required
# rename column ahead of reshape below
# sort by ID
deaths_out = (
    deaths
    .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')
    .where(f.col('PERSON_ID').isNotNull())
    .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))
    
    .withColumn("REG_DATE_OF_DEATH",
                       f.when(f.length("REG_DATE_OF_DEATH")==7,
                                   f.to_date(
                                          f.concat(f.substring(f.col('REG_DATE_OF_DEATH'),1,6), f.lit("0"), f.substring(f.col('REG_DATE_OF_DEATH'),7,1)),
                                          'yyyyMMdd')
                                    )
                      .otherwise(f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))
                      )
    .withColumn('_rownum', f.row_number().over(_win))
    .where(f.col('_rownum') == 1)
    .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])
    .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')
    .orderBy('PERSON_ID')
)

# check
#count_var(deaths_out, 'PERSON_ID'); print()
#count_var(deaths_out, 'REG_DATE_OF_DEATH'); print()
#count_var(deaths_out, 'S_COD_CODE_UNDERLYING'); print()

# single row deaths 
deaths_out_sing = deaths_out

# remove records with missing DOD
deaths_out = deaths_out\
  .where(f.col('REG_DATE_OF_DEATH').isNotNull())\
  .drop('REG_DATE')

# check
#count_var(deaths_out, 'PERSON_ID'); print()

# reshape
# add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# rename 
# remove records with missing cause of death
deaths_out_long = reshape_wide_to_long(deaths_out, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
  .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
  .withColumnRenamed('S_COD_CODE_', 'CODE4')\
  .where(f.col('CODE4').isNotNull())\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
  .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
deaths_out_long = reshape_wide_to_long(deaths_out_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
  .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
  
# check
#count_var(deaths_out_long, 'PERSON_ID'); print()  
#tmpt = tab(deaths_out_long, 'DIAG_POSITION', 'DIAG_DIGITS', var2_unstyled=1); print() 
#tmpt = tab(deaths_out_long, 'CODE', 'DIAG_DIGITS', var2_unstyled=1); print()   
# TODO - add valid ICD-10 code checker...

# COMMAND ----------

# MAGIC %md ## 5.2 Check

# COMMAND ----------

# display(deaths_out_sing)

# COMMAND ----------

# display(deaths_out_long)

# COMMAND ----------

# MAGIC %md ## 5.3 Save

# COMMAND ----------

save_table(df=deaths_out_sing, out_name=f'{proj}_cur_deaths_archive_sing', save_previous=True)

# COMMAND ----------

save_table(df=deaths_out_long, out_name=f'{proj}_cur_deaths_archive_long', save_previous=True)
