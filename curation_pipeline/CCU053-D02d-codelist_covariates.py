# Databricks notebook source
# MAGIC %md # CCU0053-D02d_codelist_covariates
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook creates the covariate codelist needed for CCU053:
# MAGIC
# MAGIC - History of Chronic Kidney Disease
# MAGIC - History of Heart Failure
# MAGIC - BMI
# MAGIC - Hb1ac
# MAGIC - eGFR
# MAGIC - Total Cholesterol
# MAGIC - HDL Cholesterol
# MAGIC - Influenza
# MAGIC
# MAGIC - Medications:
# MAGIC   - glucose lowering drugs other than SGLT2i
# MAGIC   - RAAS innhibitors 
# MAGIC     - ACE inhibitors
# MAGIC     - ARBs
# MAGIC   - calcium channel blockers
# MAGIC   - beta blockers
# MAGIC   - diuretics
# MAGIC   - statins
# MAGIC   - corticosteroids
# MAGIC   - antipsychotics
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Author(s)** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, John Nolan, and earlier projects including CCU051_D02a_codelist_covid and CCU002_07. Carmen Petitjean
# MAGIC
# MAGIC **Data output**
# MAGIC - **`ccu053_out_codelist_covariates`** : Codes for all covariates

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

bhf_phenotypes = spark.table(path_ref_bhf_phenotypes)

spark.sql(f'REFRESH TABLE dss_corporate.gdppr_cluster_refset')
gdppr_refset = spark.table(path_ref_gdppr_refset)

pmeds   = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
# gdppr = spark.table(f'{dbc}.gdppr_{db}_archive').where(f.col('archived_on') == '2023-02-28' )

bnf = spark.table(f'dss_corporate.bnf_code_information')

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# ------------------------------------------------------------------------------
# bhf_phenotypes
# ------------------------------------------------------------------------------
# check
tmpt = tab(bhf_phenotypes, 'name', 'terminology'); print()

# reduce (2 duplicates within - PE and liver_disease)
bhf_phenotypes = (
  bhf_phenotypes
  .select('name', 'terminology', 'code', 'term', 'code_type', 'RecordDate')
  .dropDuplicates()
)

# cache
bhf_phenotypes.cache()
print(f'{bhf_phenotypes.count():,}'); print()

# check
tmpt = tab(bhf_phenotypes, 'name', 'terminology'); print()

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
# pmeds_bnf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
pmeds_bnf = spark.table(f'{dsa}.{outName}')

# check
count_var(pmeds_bnf, 'PrescribedBNFCode'); print()
print(pmeds_bnf.orderBy('PrescribedBNFCode').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# check
with pd.option_context('display.max_colwidth', None): 
  tmpt = tab(gdppr_refset, 'Cluster_ID', 'Cluster_Desc', var2_wide=0); print()
display(tmpt)

# COMMAND ----------

# MAGIC %md # 4. Codelists

# COMMAND ----------

# MAGIC %md
# MAGIC ## History of Chronic Kidney Disease

# COMMAND ----------

codelist_ckd = (
  bhf_phenotypes
  .select(bhf_phenotypes['*'])
  .where(f.col('name') == 'CKD')
)
display(codelist_ckd)


# COMMAND ----------

# MAGIC %md
# MAGIC ## History of Heart Failure

# COMMAND ----------

codelist_hf = (
  bhf_phenotypes
  .select(bhf_phenotypes['*'])
  .where(f.col('name') == 'HF')
)
display(codelist_hf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## BMI
# MAGIC Not required - taken from diabetes algorithm
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## HbA1c
# MAGIC Not required - taken from diabetes algorithm

# COMMAND ----------

# MAGIC %md
# MAGIC ## eGFR

# COMMAND ----------

# --------------------------------------------------------------------------
# eGFR
# --------------------------------------------------------------------------

codelist_eGFR = spark.createDataFrame(
  [
    ('egfr','SNOMED','1011481000000105','Estimated glomerular filtration rate using creatinine Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres'),
    ('egfr','SNOMED','1011491000000107','Estimated glomerular filtration rate using cystatin C Chronic Kidney Disease Epidemiology Collaboration equation per 1.73 square metres'),
    ('egfr','SNOMED','1020291000000106','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation'),
    ('egfr','SNOMED','1107411000000104','Estimated glomerular filtration rate by laboratory calculation'),
    ('egfr','SNOMED','166180000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation'),
    ('egfr','SNOMED','168360000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation'),
    ('egfr','SNOMED','172940000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation'),
    ('egfr','SNOMED','177470000000000','Glomerular filtration rate'),
    ('egfr','SNOMED','187400000000000','Glomerular filtration rate'),
    ('egfr','SNOMED','190840000000000','Glomerular filtration rate'),
    ('egfr','SNOMED','222520000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin'),
    ('egfr','SNOMED','231130000000000','Glomerular filtration rate testing'),
    ('egfr','SNOMED','250020000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin'),
    ('egfr','SNOMED','252650000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin'),
    ('egfr','SNOMED','271920000000000','Glomerular filtration rate testing'),
    ('egfr','SNOMED','443759003','Calculation of quantitative volume rate of glomerular filtration based on concentration of analyte in serum or plasma specimen'),
    ('egfr','SNOMED','444160009','Calculation of quantitative volume rate of glomerular filtration based on cystatin C concentration in serum or plasma specimen'),
    ('egfr','SNOMED','444275009','Measurement of creatinine concentration in serum or plasma specimen with calculation of glomerular filtration rate'),
    ('egfr','SNOMED','444336003','Calculation of quantitative volume rate of glomerular filtration based on creatinine concentration in serum or plasma specimen'),
    ('egfr','SNOMED','791800000000000','Estimation of glomerular filtration rate'),
    ('egfr','SNOMED','80274001','Glomerular filtration rate'),
    ('egfr','SNOMED','857970000000000','Estimated glomerular filtration rate using Chronic Kidney Disease Epidemiology Collaboration formula'),
    ('egfr','SNOMED','857980000000000','Estimated glomerular filtration rate using Chronic Kidney Disease Epidemiology Collaboration formula per 1.73 square metres'),
    ('egfr','SNOMED','88024006','Glomerular filtration, function'),
    ('egfr','SNOMED','963600000000000','Estimated glomerular filtration rate using cystatin C Chronic Kidney Disease Epidemiology Collaboration equation'),
    ('egfr','SNOMED','963610000000000','Estimated glomerular filtration rate using cystatin C per 1.73 square metres'),
    ('egfr','SNOMED','963620000000000','Estimated glomerular filtration rate using creatinine Chronic Kidney Disease Epidemiology Collaboration equation'),
    ('egfr','SNOMED','963630000000000','Estimated glomerular filtration rate using serum creatinine per 1.73 square metres'),
    ('egfr','SNOMED','996230000000000','Glomerular filtration rate calculated by abbreviated Modification of Diet in Renal Disease Study Group calculation adjusted for African American origin')
  ],
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serum creatinine (sCr)

# COMMAND ----------

codelist_scr = spark.createDataFrame(
  [
    ('SCR','SNOMED','1000731000000107','Serum creatinine level'),
    ('SCR','SNOMED','1000991000000106','Corrected serum creatinine level')
],
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Cholesterol

# COMMAND ----------

# --------------------------------------------------------------------------
# Total Cholesterol
# --------------------------------------------------------------------------

codelist_total_cholesterol = spark.createDataFrame(
  [
('total_cholesterol','SNOMED','1005671000000100','Serum cholesterol level'),
('total_cholesterol','SNOMED','1017161000000100','Plasma total cholesterol level'),
('total_cholesterol','SNOMED','114711000000101','Serum total cholesterol level'),
('total_cholesterol','SNOMED','119291000000100','Serum total cholesterol level'),
('total_cholesterol','SNOMED','270996006','Serum cholesterol measurement'),
('total_cholesterol','SNOMED','365793008','Finding of cholesterol level'),
('total_cholesterol','SNOMED','365794002','Finding of serum cholesterol level'),
('total_cholesterol','SNOMED','389608004','Plasma total cholesterol level'),
('total_cholesterol','SNOMED','390340002','Plasma total cholesterol level'),
('total_cholesterol','SNOMED','390956002','Plasma total cholesterol level'),
('total_cholesterol','SNOMED','393025009','Pre-treatment serum cholesterol level'),
('total_cholesterol','SNOMED','393981000','Pre-treatment serum cholesterol level'),
('total_cholesterol','SNOMED','395153009','Pre-treatment serum cholesterol level'),
('total_cholesterol','SNOMED','412808005','Serum total cholesterol measurement'),
('total_cholesterol','SNOMED','850981000000101','Cholesterol level'),
('total_cholesterol','SNOMED','853681000000104','Total cholesterol level'),
('total_cholesterol','SNOMED','667191000006107','Fasting serum cholesterol (EMIS code)'),
('total_cholesterol','SNOMED','994351000000103','Serum total cholesterol level'),
('total_cholesterol','SNOMED','1005671000000105','Serum cholesterol level'),
('total_cholesterol','SNOMED','1017161000000104','Plasma total cholesterol level')
  ],
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HDL Cholesterol

# COMMAND ----------

# --------------------------------------------------------------------------
# HDL Cholesterol
# --------------------------------------------------------------------------

codelist_hdl_cholesterol = spark.createDataFrame(
  [
    ('hdl_cholesterol','SNOMED','1005681000000107','Serum high density lipoprotein cholesterol level'),
    ('hdl_cholesterol','SNOMED','1010581000000101','Plasma high density lipoprotein cholesterol level'),
    ('hdl_cholesterol','SNOMED','1026451000000102','Serum fasting high density lipoprotein cholesterol level'),
    ('hdl_cholesterol','SNOMED','1026461000000104','Serum random high density lipoprotein cholesterol level'),
    ('hdl_cholesterol','SNOMED','1028831000000106','Plasma random high density lipoprotein cholesterol level'),
    ('hdl_cholesterol','SNOMED','1028841000000102','Plasma fasting high density lipoprotein cholesterol level'),
    ('hdl_cholesterol','SNOMED','1107661000000104','Substance concentration of high density lipoprotein cholesterol in plasma'),
    ('hdl_cholesterol','SNOMED','1107681000000108','Substance concentration of high density lipoprotein cholesterol in serum'),
    ('hdl_cholesterol','SNOMED','855770000000000','HDL cholesterol level (EMIS code)'),
    ('hdl_cholesterol','SNOMED','855771000006102','HDL cholesterol level (EMIS code)')
  ],
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Influenza

# COMMAND ----------

# --------------------------------------------------------------------------
# Influenza
# --------------------------------------------------------------------------

codelist_influenza = spark.createDataFrame(
  [
    ('influenza','ICD10','J09','Influenza due to certain identified influenza viruses'),
    ('influenza','ICD10','J10','Influenza due to other identified influenza virus'),
    ('influenza','ICD10','J10.0','Influenza with pneumonia, seasonal influenza virus identified'),
    ('influenza','ICD10','J10.1','Influenza with other respiratory manifestations, seasonal influenza virus identified'),
    ('influenza','ICD10','J10.8','Influenza with other manifestations, seasonal influenza virus identified'),
    ('influenza','ICD10','J11','Influenza due to unidentified influenza virus'),
    ('influenza','ICD10','J11.0','Influenza with pneumonia, virus not identified'),
    ('influenza','ICD10','J11.1','Influenza with other respiratory manifestations, virus not identified'),
    ('influenza','ICD10','J11.8','Influenza with other manifestations, virus not identified'),
    ('influenza','SNOMED','1033051000000101','Influenza due to zoonotic influenza virus'),
    ('influenza','SNOMED','1033061000000103','Influenza due to zoonotic influenza virus'),
    ('influenza','SNOMED','1033071000000105','Influenza due to pandemic influenza virus'),
    ('influenza','SNOMED','1033081000000107','Influenza due to pandemic influenza virus'),
    ('influenza','SNOMED','1033091000000109','Influenza due to seasonal influenza virus'),
    ('influenza','SNOMED','1033101000000101','Influenza due to seasonal influenza virus'),
    ('influenza','SNOMED','1033111000000104','Influenza with pneumonia due to seasonal influenza virus'),
    ('influenza','SNOMED','1033121000000105','Influenzal bronchopneumonia due to seasonal influenza virus'),
    ('influenza','SNOMED','103516006','Influenzavirus A Bangkok'),
    ('influenza','SNOMED','103517002','Influenzavirus A England'),
    ('influenza','SNOMED','103518007','Influenzavirus A Hong Kong'),
    ('influenza','SNOMED','103519004','Influenzavirus A Leningrad'),
    ('influenza','SNOMED','103520005','Influenzavirus A Mississippi'),
    ('influenza','SNOMED','103521009','Influenzavirus A Phillipines'),
    ('influenza','SNOMED','103522002','Influenzavirus A Port Chalmers'),
    ('influenza','SNOMED','103523007','Influenzavirus A Texas'),
    ('influenza','SNOMED','103524001','Influenzavirus A Victoria'),
    ('influenza','SNOMED','1050601000000101','Influenza due to seasonal influenza virus'),
    ('influenza','SNOMED','1050981000000100','Influenza due to seasonal influenza virus'),
    ('influenza','SNOMED','10624911000119107','Otitis media caused by H1N1 influenza'),
    ('influenza','SNOMED','10624951000119108','Otitis media caused by influenza'),
    ('influenza','SNOMED','10628871000119101','Gastroenteritis caused by influenza'),
    ('influenza','SNOMED','10628911000119103','Gastroenteritis caused by Influenza A virus'),
    ('influenza','SNOMED','10629191000119100','Bronchiolitis caused by influenza virus'),
    ('influenza','SNOMED','10629351000119108','Myocarditis caused by Influenza A virus'),
    ('influenza','SNOMED','10674911000119108','Otitis media caused by Influenza A virus'),
    ('influenza','SNOMED','10677711000119101','Encephalopathy caused by Influenza A virus'),
    ('influenza','SNOMED','10685111000119102','Upper respiratory tract infection caused by Influenza'),
    ('influenza','SNOMED','111876006','Influenza due to other Influenza virus'),
    ('influenza','SNOMED','1119277000','Antigen of Influenza A virus A/Indonesia/05/2005 (H5N1)-like virus strain split virion hemagglutinin'),
    ('influenza','SNOMED','1149091008','Influenza caused by Influenza A virus subtype H2'),
    ('influenza','SNOMED','1157322000','Antigen of Influenza A virus A/California/7/2009 (H1N1)-like virus strain split virion hemagglutinin'),
    ('influenza','SNOMED','121006005','Antigen of Influenza A virus'),
    ('influenza','SNOMED','121008006','Antigen of Influenza B virus'),
    ('influenza','SNOMED','121026009','Antigen of Influenza C virus'),
    ('influenza','SNOMED','134251009','Influenza B antigen level'),
    ('influenza','SNOMED','142920000000000','Upper respiratory tract infection caused by avian influenza'),
    ('influenza','SNOMED','142930000000000','Pneumonia caused by H1N1 influenza'),
    ('influenza','SNOMED','142940000000000','Upper respiratory tract infection caused by H1N1 influenza'),
    ('influenza','SNOMED','142950000000000','Myocarditis caused by Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','142960000000000','Gastroenteritis caused by H1N1 influenza'),
    ('influenza','SNOMED','142970000000000','Encephalopathy caused by H1N1 influenza'),
    ('influenza','SNOMED','142980000000000','Myocarditis caused by avian influenza'),
    ('influenza','SNOMED','142990000000000','Gastroenteritis caused by avian influenza'),
    ('influenza','SNOMED','143000000000000','Encephalopathy caused by avian influenza'),
    ('influenza','SNOMED','143110000000000','Pneumonia caused by avian influenza'),
    ('influenza','SNOMED','1553003','Influenzavirus B'),
    ('influenza','SNOMED','155559006','Influenza'),
    ('influenza','SNOMED','155560001','Influenzal pneumonia'),
    ('influenza','SNOMED','155561002','Influenza with encephalopathy'),
    ('influenza','SNOMED','16311000000000','Pneumonia caused by influenza'),
    ('influenza','SNOMED','181000000000','Influenza A virus present'),
    ('influenza','SNOMED','192691003','Encephalitis due to influenza'),
    ('influenza','SNOMED','192693000','Encephalitis due to influenza-virus identified'),
    ('influenza','SNOMED','194946005','Acute myocarditis - influenzal'),
    ('influenza','SNOMED','195878008','Pneumonia and influenza'),
    ('influenza','SNOMED','195920000','Influenza with pneumonia, influenza virus identified'),
    ('influenza','SNOMED','195921001','Influenza with pneumonia NOS'),
    ('influenza','SNOMED','195922008','Influenza with other respiratory manifestation'),
    ('influenza','SNOMED','195923003','Influenza with laryngitis'),
    ('influenza','SNOMED','195924009','Influenza with pharyngitis'),
    ('influenza','SNOMED','195925005','Influenza with respiratory manifestations NOS'),
    ('influenza','SNOMED','195927002','Influenza with other manifestations'),
    ('influenza','SNOMED','195928007','Influenza with encephalopathy'),
    ('influenza','SNOMED','195929004','Influenza with gastrointestinal tract involvement'),
    ('influenza','SNOMED','195930009','Influenza with other manifestations NOS'),
    ('influenza','SNOMED','196200002','[X]Influenza with other respiratory manifestations, influenza virus identified'),
    ('influenza','SNOMED','196201003','[X]Influenza with other manifestations, influenza virus identified'),
    ('influenza','SNOMED','196202005','[X]Influenza with other respiratory manifestations, virus not identified'),
    ('influenza','SNOMED','196203000','[X]Influenza with other manifestations, virus not identified'),
    ('influenza','SNOMED','243612002','Influenza virus A and B'),
    ('influenza','SNOMED','243613007','Influenza virus A'),
    ('influenza','SNOMED','243614001','Influenza virus C'),
    ('influenza','SNOMED','24601000000000','Antigen of live attenuated Influenza virus'),
    ('influenza','SNOMED','24662006','Influenza caused by Influenza B virus'),
    ('influenza','SNOMED','25031000000000','Antigen of Influenza A virus A/Kansas/14/2017 (H3N2)-like virus strain split virion hemagglutinin'),
    ('influenza','SNOMED','25041000000000','Antigen of Influenza A virus A/Brisbane/02/2018 (H1N1)pdm09-like virus strain split virion hemagglutinin'),
    ('influenza','SNOMED','25061000000000','Antigen of Influenza B virus B/Colorado/06/2017 (B/Victoria/2/87 lineage)-like virus strain split virion hemagglutinin'),
    ('influenza','SNOMED','25081000000000','Antigen of Influenza B virus B/Phuket/3073/2013 (B/Yamagata/16/88 lineage)-like virus strain split virion hemagglutinin'),
    ('influenza','SNOMED','25161000000000','Antigen of Influenza A virus A/Brisbane/02/2018 (H1N1)pdm09-like virus strain surface subunit hemagglutinin'),
    ('influenza','SNOMED','25181000000000','Antigen of Influenza A virus A/Kansas/14/2017 (H3N2)-like virus strain surface subunit hemagglutinin'),
    ('influenza','SNOMED','25201000000000','Antigen of Influenza B virus B/Colorado/06/2017 (B/Victoria/2/87 lineage)-like virus strain surface subunit hemagglutinin'),
    ('influenza','SNOMED','25221000000000','Antigen of Influenza B virus B/Phuket/3073/2013 (B/Yamagata/16/88 lineage)-like virus strain surface subunit hemagglutinin'),
    ('influenza','SNOMED','25231000000000','Antigen of live attenuated Influenza A virus A/Brisbane/02/2018 (H1N1)pdm09-like strain'),
    ('influenza','SNOMED','25241000000000','Antigen of live attenuated Influenza A virus A/Kansas/14/2017 (H3N2)-like strain'),
    ('influenza','SNOMED','25251000000000','Antigen of live attenuated Influenza B virus B/Colorado/06/2017 (B/Victoria/2/87 lineage)-like strain'),
    ('influenza','SNOMED','25261000000000','Antigen of live attenuated Influenza B virus B/Phuket/3073/2013 (B/Yamagata/16/88 lineage)-like strain'),
    ('influenza','SNOMED','25371000000000','Antigen of Influenza A virus subtype H3N2'),
    ('influenza','SNOMED','25481000000000','Antigen of Influenza B virus Yamagata lineage'),
    ('influenza','SNOMED','25491000000000','Antigen of Influenza B virus Victoria lineage'),
    ('influenza','SNOMED','25501000000000','Antigen of live attenuated Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','25511000000000','Antigen of live attenuated Influenza A virus subtype H3N2'),
    ('influenza','SNOMED','25521000000000','Antigen of live attenuated Influenza B virus Yamagata lineage'),
    ('influenza','SNOMED','25531000000000','Antigen of live attenuated Influenza B virus Victoria lineage'),
    ('influenza','SNOMED','260210008','Antigen of Influenza virus'),
    ('influenza','SNOMED','266353003','Influenza NOS'),
    ('influenza','SNOMED','280330000000000','Avian influenza'),
    ('influenza','SNOMED','28105000','Influenzal laryngitis'),
    ('influenza','SNOMED','292630000000000','Avian influenza'),
    ('influenza','SNOMED','309789002','Encephalitis caused by influenza'),
    ('influenza','SNOMED','309806000','Encephalitis caused by influenza-virus identified'),
    ('influenza','SNOMED','313251006','Encephalitis caused by influenza-specific virus not identified'),
    ('influenza','SNOMED','328530000000000','Upper respiratory tract infection caused by Influenza A'),
    ('influenza','SNOMED','359370000000000','Influenza H1 virus detected'),
    ('influenza','SNOMED','359380000000000','Influenza H1 detected'),
    ('influenza','SNOMED','359390000000000','Influenza H1 detected'),
    ('influenza','SNOMED','359400000000000','Influenza H2 virus detected'),
    ('influenza','SNOMED','359410000000000','Influenza H2 detected'),
    ('influenza','SNOMED','359420000000000','Influenza H2 detected'),
    ('influenza','SNOMED','359430000000000','Influenza H3 virus detected'),
    ('influenza','SNOMED','359440000000000','Influenza H3 detected'),
    ('influenza','SNOMED','359450000000000','Influenza H3 detected'),
    ('influenza','SNOMED','359460000000000','Influenza H5 virus detected'),
    ('influenza','SNOMED','359470000000000','Influenza H5 detected'),
    ('influenza','SNOMED','359480000000000','Influenza H5 detected'),
    ('influenza','SNOMED','359490000000000','Influenza A virus, other or untyped strain detected'),
    ('influenza','SNOMED','359500000000000','Influenza A, other or untyped strain detected'),
    ('influenza','SNOMED','359510000000000','Influenza A, other or untyped strain detected'),
    ('influenza','SNOMED','359520000000000','Influenza B virus detected'),
    ('influenza','SNOMED','359530000000000','Influenza B detected'),
    ('influenza','SNOMED','359540000000000','Influenza B detected'),
    ('influenza','SNOMED','359829002','Influenzavirus, type A, avian'),
    ('influenza','SNOMED','359831006','Influenzavirus, type A, equine'),
    ('influenza','SNOMED','359833009','Influenzavirus, type A, porcine'),
    ('influenza','SNOMED','389069008','Avian influenza virus, low pathogenic'),
    ('influenza','SNOMED','389070009','Avian influenza virus, highly pathogenic'),
    ('influenza','SNOMED','407477006','Genus Alphainfluenzavirus'),
    ('influenza','SNOMED','407478001','Genus Betainfluenzavirus'),
    ('influenza','SNOMED','407479009','Influenza A virus'),
    ('influenza','SNOMED','407480007','Influenza B virus'),
    ('influenza','SNOMED','407481006','Genus Gammainfluenzavirus'),
    ('influenza','SNOMED','407482004','Influenza C virus'),
    ('influenza','SNOMED','408687004','Healthcare associated influenza disease'),
    ('influenza','SNOMED','41269000','Influenzal bronchopneumonia'),
    ('influenza','SNOMED','418180000000000','[X]Influenza with other respiratory manifestations, influenza virus identified'),
    ('influenza','SNOMED','418190000000000','[X]Influenza with other manifestations, influenza virus identified'),
    ('influenza','SNOMED','420362005','Influenzavirus type A, avian, H1N1 strain'),
    ('influenza','SNOMED','420508007','Influenzavirus type A, avian, H3N2 strain'),
    ('influenza','SNOMED','421264001','Influenzavirus type A, avian, H5N1 strain'),
    ('influenza','SNOMED','421539000','Influenzavirus type A, avian, H1N2 strain'),
    ('influenza','SNOMED','42501000000000','Number concentration of Influenza virus by culture'),
    ('influenza','SNOMED','426959005','Influenza B virus Yamagata lineage'),
    ('influenza','SNOMED','427672003','Influenza B virus Victoria lineage'),
    ('influenza','SNOMED','427873006','Influenza caused by influenza virus type A, avian, H5N1 strain'),
    ('influenza','SNOMED','42964004','Influenza with pneumonia'),
    ('influenza','SNOMED','430890000000000','[X]Influenza with other respiratory manifestations, virus not identified'),
    ('influenza','SNOMED','43692000','Influenzal acute upper respiratory infection'),
    ('influenza','SNOMED','440927002','Influenza A virus subtype H2 present'),
    ('influenza','SNOMED','441043003','Influenza A virus subtype H1 present'),
    ('influenza','SNOMED','441049004','Influenza A virus subtype H3 present'),
    ('influenza','SNOMED','441130000000000','[X]Influenza with other manifestations, virus not identified'),
    ('influenza','SNOMED','441343005','Influenza A virus subtype H5 present'),
    ('influenza','SNOMED','441345003','Influenza B virus present'),
    ('influenza','SNOMED','442269004','Antigen of Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','442352004','Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','442438000','Influenza caused by Influenza A virus'),
    ('influenza','SNOMED','442696006','Influenza caused by Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','445346006','Detection of avian influenza A using polymerase chain reaction technique'),
    ('influenza','SNOMED','445403009','Detection of Influenza A virus subtype H1N1 using polymerase chain reaction technique'),
    ('influenza','SNOMED','446396002','Influenza A virus subtype H1'),
    ('influenza','SNOMED','446397006','Influenza A virus subtype H2'),
    ('influenza','SNOMED','446524007','Influenza A virus A/Leningrad/621/86 (H1N1)'),
    ('influenza','SNOMED','446525008','Influenza A virus A/Leningrad/624/86 (H1N1)'),
    ('influenza','SNOMED','446601007','Influenzavirus A subtype H3N2'),
    ('influenza','SNOMED','446645007','Influenza A virus subtype H3'),
    ('influenza','SNOMED','446646008','Influenza A virus subtype H3N2'),
    ('influenza','SNOMED','446647004','Influenza A virus subtype H5'),
    ('influenza','SNOMED','446648009','Influenza A virus subtype H7'),
    ('influenza','SNOMED','446649001','Influenza A virus subtype H9'),
    ('influenza','SNOMED','447493004','Influenza A virus A/Bangkok/1/79 (H3N2)'),
    ('influenza','SNOMED','447499000','Influenza A virus A/Bangkok/2/79 (H3N2)'),
    ('influenza','SNOMED','447503006','Influenza A virus A/Port Chalmers/1/73 (H3N2)'),
    ('influenza','SNOMED','447504000','Influenza A virus A/Texas/1/77 (H3N2)'),
    ('influenza','SNOMED','447508002','Influenza A virus A/Leningrad/385/80 (H3N2)'),
    ('influenza','SNOMED','447522002','Influenza A virus A/Mississippi/1/85 (H3N2)'),
    ('influenza','SNOMED','447530001','Influenza A virus A/Philippines/2/82 (H3N2)'),
    ('influenza','SNOMED','447573005','Influenza A virus A/England/42/72 (H3N2)'),
    ('influenza','SNOMED','447578001','Influenza A virus A/Hong Kong/1/68 (H3N2)'),
    ('influenza','SNOMED','447587005','Influenza A virus A/Victoria/3/75 (H3N2)'),
    ('influenza','SNOMED','448325003','Influenza A virus subtype H9N2'),
    ('influenza','SNOMED','448803002','Influenza A virus subtype N1'),
    ('influenza','SNOMED','448835002','Influenza A virus subtype N2'),
    ('influenza','SNOMED','449024000','Pandemic Influenza A (H1N1) 2009'),
    ('influenza','SNOMED','450480003','Influenza A virus subtype H3N2v'),
    ('influenza','SNOMED','450715004','Influenza caused by Influenza A virus subtype H7'),
    ('influenza','SNOMED','450716003','Influenza caused by Influenza A virus subtype H9'),
    ('influenza','SNOMED','46171006','Influenza due to Influenza virus, type A, porcine'),
    ('influenza','SNOMED','505130000000000','Influenza due to Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','506780000000000','Influenza A virus H1N1 subtype detected'),
    ('influenza','SNOMED','506790000000000','Influenza H1N1 virus detected'),
    ('influenza','SNOMED','506800000000000','Influenza H1N1 virus detected'),
    ('influenza','SNOMED','510670000000000','Influenza due to Influenza A virus subtype H1N1'),
    ('influenza','SNOMED','540120000000000','Influenza with other manifestations'),
    ('influenza','SNOMED','540130000000000','Influenza with other manifestations NOS'),
    ('influenza','SNOMED','55207002','Influenzavirus A (living organism)'),
    ('influenza','SNOMED','55604004','Avian influenza'),
    ('influenza','SNOMED','609444009','Influenza A virus subtype H7N9'),
    ('influenza','SNOMED','6142004','Influenza'),
    ('influenza','SNOMED','616160000000000','Influenza with pneumonia NOS'),
    ('influenza','SNOMED','616170000000000','Influenza with other respiratory manifestation'),
    ('influenza','SNOMED','616180000000000','Influenza with respiratory manifestations NOS'),
    ('influenza','SNOMED','61700007','Influenza with non-respiratory manifestation'),
    ('influenza','SNOMED','63039003','Influenza with respiratory manifestation other than pneumonia'),
    ('influenza','SNOMED','670550000000000','Influenza NOS'),
    ('influenza','SNOMED','699872005','Influenza A virus untyped strain present'),
    ('influenza','SNOMED','700349009','Influenza A virus subtype H10'),
    ('influenza','SNOMED','700350009','Influenza A virus subtype H10N8'),
    ('influenza','SNOMED','702482001','Influenza A H1N1 virus 2009 pandemic strain present'),
    ('influenza','SNOMED','707448003','Influenza caused by Influenza A virus subtype H7N9'),
    ('influenza','SNOMED','707902004','Ribonucleic acid of Influenza A virus H1N1'),
    ('influenza','SNOMED','707903009','Ribonucleic acid of Influenza A virus H1'),
    ('influenza','SNOMED','707904003','Ribonucleic acid of Influenza A virus H2'),
    ('influenza','SNOMED','707905002','Ribonucleic acid of Influenza A virus H3'),
    ('influenza','SNOMED','707906001','Ribonucleic acid of Influenza A virus H5 Asian lineage'),
    ('influenza','SNOMED','707907005','Ribonucleic acid of Influenza A virus H5'),
    ('influenza','SNOMED','707908000','Ribonucleic acid of Influenza A virus H5a'),
    ('influenza','SNOMED','707909008','Ribonucleic acid of Influenza A virus H5b'),
    ('influenza','SNOMED','707910003','Ribonucleic acid of Influenza A virus H6'),
    ('influenza','SNOMED','707911004','Ribonucleic acid of Influenza A virus H7'),
    ('influenza','SNOMED','707912006','Ribonucleic acid of Influenza A virus H9'),
    ('influenza','SNOMED','707913001','Ribonucleic acid of Influenza A virus hemagglutinin'),
    ('influenza','SNOMED','707914007','Ribonucleic acid of Influenza A virus matrix protein'),
    ('influenza','SNOMED','707915008','Ribonucleic acid of Influenza A virus N1'),
    ('influenza','SNOMED','707916009','Ribonucleic acid of Influenza A virus N2'),
    ('influenza','SNOMED','707917000','Ribonucleic acid of Influenza A virus neuraminidase'),
    ('influenza','SNOMED','707918005','Ribonucleic acid of Influenza A virus non-structural protein'),
    ('influenza','SNOMED','707919002','Ribonucleic acid of Influenza A virus nucleoprotein'),
    ('influenza','SNOMED','707920008','Ribonucleic acid of Influenza A virus polymerase A'),
    ('influenza','SNOMED','707921007','Ribonucleic acid of Influenza A virus polymerase B2'),
    ('influenza','SNOMED','707922000','Ribonucleic acid of Influenza A virus polymerase'),
    ('influenza','SNOMED','707923005','Ribonucleic acid of Influenza A virus'),
    ('influenza','SNOMED','707924004','Ribonucleic acid of Influenza virus A swine origin'),
    ('influenza','SNOMED','707925003','Ribonucleic acid of Influenza B virus'),
    ('influenza','SNOMED','707926002','Ribonucleic acid of Influenza C virus'),
    ('influenza','SNOMED','707927006','Ribonucleic acid of Influenza virus'),
    ('influenza','SNOMED','708119004','Influenza A virus subtype H7 present'),
    ('influenza','SNOMED','708120005','Influenza A virus subtype H9 present'),
    ('influenza','SNOMED','708527000','Influenza A virus subtype H5N8'),
    ('influenza','SNOMED','709361007','Antigen of Influenza A virus subtype H1'),
    ('influenza','SNOMED','709362000','Antigen of Influenza A virus subtype H3'),
    ('influenza','SNOMED','709436005','Antigen of Influenza virus A + B'),
    ('influenza','SNOMED','709437001','Antigen of Influenza virus A + B + C'),
    ('influenza','SNOMED','711128004','Influenza caused by influenza virus type A, avian, H3N2 strain'),
    ('influenza','SNOMED','711330007','Influenza A virus subtype H1N1 detected'),
    ('influenza','SNOMED','713083002','Influenza caused by Influenza A virus subtype H5'),
    ('influenza','SNOMED','713151006','Influenza B virus/Shanghai/361/2002'),
    ('influenza','SNOMED','715333003','Influenza A virus, not subtype H1N1'),
    ('influenza','SNOMED','715346008','Influenza A virus subtype H7N7'),
    ('influenza','SNOMED','715347004','Influenza A virus subtype H7N3'),
    ('influenza','SNOMED','715348009','Influenza A virus subtype H7N2'),
    ('influenza','SNOMED','715349001','Influenza A virus subtype H2N2'),
    ('influenza','SNOMED','715350001','Influenza A virus subtype H10N7'),
    ('influenza','SNOMED','715870001','Influenza A virus, not subtype H1 and not subtype H3'),
    ('influenza','SNOMED','719590007','Influenza caused by seasonal influenza virus'),
    ('influenza','SNOMED','719865001','Influenza caused by pandemic influenza virus'),
    ('influenza','SNOMED','720313003','Antigen to Swine influenza virus'),
    ('influenza','SNOMED','721863005','Antigen of Influenza virus surface protein'),
    ('influenza','SNOMED','722259006','Influenza B virus Malaysia lineage'),
    ('influenza','SNOMED','725894000','Influenza virus'),
    ('influenza','SNOMED','738276008','Disorder of central nervous system co-occurrent and due to infection with influenza virus'),
    ('influenza','SNOMED','74644004','Influenza with encephalopathy'),
    ('influenza','SNOMED','772807005','Influenza A virus subtype H3N8'),
    ('influenza','SNOMED','772809008','Influenza A virus subtype H1N2'),
    ('influenza','SNOMED','772810003','Influenza caused by Influenza A virus subtype H3N2'),
    ('influenza','SNOMED','772827006','Influenza A virus subtype H5N1'),
    ('influenza','SNOMED','772828001','Influenza caused by Influenza A virus subtype H5N1'),
    ('influenza','SNOMED','772835009','Detection of Influenza A virus using polymerase chain reaction technique'),
    ('influenza','SNOMED','772839003','Pneumonia caused by Influenza A virus'),
    ('influenza','SNOMED','78046005','Myocarditis caused by influenza virus'),
    ('influenza','SNOMED','781602008','Genus Deltainfluenzavirus'),
    ('influenza','SNOMED','781630008','Influenza D virus'),
    ('influenza','SNOMED','78431007','Influenza due to Influenza virus, type A, human'),
    ('influenza','SNOMED','81524006','Influenza caused by Influenza C virus'),
    ('influenza','SNOMED','817830000000000','Determination of Influenza A virus genotype by ribonucleic acid analysis'),
    ('influenza','SNOMED','817840000000000','Determination of Influenza B virus genotype by ribonucleic acid analysis'),
    ('influenza','SNOMED','83440004','Influenza with involvement of gastrointestinal tract'),
    ('influenza','SNOMED','84037004','Swine influenza'),
    ('influenza','SNOMED','840582007','Antigen of inactivated whole Influenza A virus subtype H5N1'),
    ('influenza','SNOMED','84113001','Influenzavirus C'),
    ('influenza','SNOMED','84512003','Influenzal pharyngitis'),
    ('influenza','SNOMED','866126000','Myelitis caused by Influenza A virus'),
    ('influenza','SNOMED','871771002','Antigen of Influenza A virus subtype H5N1'),
    ('influenza','SNOMED','88152000','Equine influenza')
  ],
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Influenza (Breathe recommended)
# MAGIC
# MAGIC https://www.hdruk.ac.uk/helping-with-health-data/health-data-research-hubs/breathe/ 

# COMMAND ----------

# --------------------------------------------------------------------------
# Influenza (Breathe recommended)
# --------------------------------------------------------------------------

codelist_influenza_breathe = spark.createDataFrame(
  [
    ('influenza_breathe','ICD10','A41.3','Sepsis due to Haemophilus influenzae'),
    ('influenza_breathe','ICD10','A49.2','Haemophilus influenzae infection, unspecified site'),
    ('influenza_breathe','ICD10','J09','Influenza due to certain identified influenza viruses'),
    ('influenza_breathe','ICD10','J10','Influenza due to other identified influenza virus'),
    ('influenza_breathe','ICD10','J10.0','Influenza with pneumonia, seasonal influenza virus identified'),
    ('influenza_breathe','ICD10','J10.1','Influenza with other respiratory manifestations, seasonal influenza virus identified'),
    ('influenza_breathe','ICD10','J10.8','Influenza with other manifestations, seasonal influenza virus identified'),
    ('influenza_breathe','ICD10','J11','Influenza due to unidentified influenza virus'),
    ('influenza_breathe','ICD10','J11.0','Influenza with pneumonia, virus not identified'),
    ('influenza_breathe','ICD10','J11.1','Influenza with other respiratory manifestations, virus not identified'),
    ('influenza_breathe','ICD10','J11.8','Influenza with other manifestations, virus not identified'),
    ('influenza_breathe','ICD10','J12.2','Parainfluenza virus pneumonia'),
    ('influenza_breathe','ICD10','J14','Pneumonia due to Haemophilus influenzae'),
    ('influenza_breathe','ICD10','J20.1','Acute bronchitis due to Haemophilus influenzae'),
    ('influenza_breathe','ICD10','J20.4','Acute bronchitis due to parainfluenza virus'),
    ('influenza_breathe','ICD10','Z25.1','Need for immunization against influenza'),
    ('influenza_breathe','SNOMED','1010051000006110','Haemophilus influenzae type B and meningitis C vaccination'),
    ('influenza_breathe','SNOMED','1024921000006116','Adverse reaction to *Influenza & Common Cold'),
    ('influenza_breathe','SNOMED','1048401000006110','Adverse reaction to Inactivated Influenza (Split Virion) Paediatric'),
    ('influenza_breathe','SNOMED','1072231000006110','Adverse reaction to Sanofi Pasteur Inactivated Influenza Split Virion'),
    ('influenza_breathe','SNOMED','1083051000006115','Adverse reaction to Inactivated Influenza (Split Virion)'),
    ('influenza_breathe','SNOMED','1083281000006116','Adverse reaction to Inactivated Influenza Vaccine, Surface Antigen'),
    ('influenza_breathe','SNOMED','1113881000006112','Adverse reaction to Uniflu Plus Gregovite C'),
    ('influenza_breathe','SNOMED','1116591000006118','Adverse reaction to Haemophilus Influenzae Type B Polysaccharide'),
    ('influenza_breathe','SNOMED','1118771000006112','Adverse reaction to *Influenza & Common Cold'),
    ('influenza_breathe','SNOMED','1119661000006117','Adverse reaction to Haemophilus influenzae type b polysaccharide, conjugated'),
    ('influenza_breathe','SNOMED','1120351000000115','Influenza A virus H1N1 subtype contact'),
    ('influenza_breathe','SNOMED','1122041000000117','Swine influenza contact'),
    ('influenza_breathe','SNOMED','1122831000000118','Suspected Influenza A virus subtype H1N1 infection'),
    ('influenza_breathe','SNOMED','1125021000006114','Adverse reaction to Haemophilus Influenzae B Polysaccaride Conjugated To Tetanus Protein'),
    ('influenza_breathe','SNOMED','1125031000006112','Adverse reaction to Haemophilus Influenzae B Polysaccaride Conjugated To Diphtheria Protein'),
    ('influenza_breathe','SNOMED','1126371000000114','Influenza A virus H1N1 subtype detected'),
    ('influenza_breathe','SNOMED','1126431000000117','Influenza A virus H1N1 subtype not detected'),
    ('influenza_breathe','SNOMED','1126551000000111','Local flu response centre notified'),
    ('influenza_breathe','SNOMED','1126621000000117','Influenza A virus H1N1 subtype close contact'),
    ('influenza_breathe','SNOMED','1128141000000118','Influenza A (H1N1) swine flu'),
    ('influenza_breathe','SNOMED','1131081000006113','Adverse reaction to Inactivated Influenza Vaccine'),
    ('influenza_breathe','SNOMED','1135701000000110','Possible influenza A virus H1N1 subtype'),
    ('influenza_breathe','SNOMED','1136091000000113','History of influenza vaccination'),
    ('influenza_breathe','SNOMED','1138151000000117','Advice given about Influenza A virus subtype H1N1 infection'),
    ('influenza_breathe','SNOMED','1143141000000112','Influenza A virus subtype H1N1 serology'),
    ('influenza_breathe','SNOMED','1143761000006119','Adverse reaction to Haemophilus Influenzae (Lyophilized Bacterial Lysates)'),
    ('influenza_breathe','SNOMED','1149291000000110','No response to influenza vaccination invitation'),
    ('influenza_breathe','SNOMED','1158771000000112','Consent given for influenza A subtype H1N1 vaccination'),
    ('influenza_breathe','SNOMED','1161531000000115','Advice given about swine flu by telephone'),
    ('influenza_breathe','SNOMED','11931191000006114','Influenza vaccination requested'),
    ('influenza_breathe','SNOMED','11934921000006113','Administration of first quadrivalent (QIV) inactivated seasonal influenza vaccination'),
    ('influenza_breathe','SNOMED','11934931000006111','Administration of first non adjuvanted trivalent (TIV) inactivated seasonal influenza vaccination'),
    ('influenza_breathe','SNOMED','11934951000006116','Administration of second non adjuvanted trivalent (TIV) inactivated seasonal influenza vaccination'),
    ('influenza_breathe','SNOMED','11934961000006119','Administration of adjuvanted trivalent (aTIV) inactivated seasonal influenza vaccination'),
    ('influenza_breathe','SNOMED','11934971000006114','First quadrivalent (QIV) inactivated seasonal influenza vaccination given by pharmacist'),
    ('influenza_breathe','SNOMED','11934981000006112','First non adjuvanted trivalent (TIV) inactivated seasonal influenza vaccination given by pharmacist'),
    ('influenza_breathe','SNOMED','11935001000006115','Second non adjuvanted trivalent (TIV) inactivated seasonal influenza vaccination given by pharmacist'),
    ('influenza_breathe','SNOMED','11935021000006113','Administration of first quadrivalent (QIV) inactivated seasonal influenza vaccination given by other healthcare provider'),
    ('influenza_breathe','SNOMED','11935031000006111','Administration of first non adjuvanted trivalent (TIV) inactivated seasonal influenza vaccination given by other healthcare provider'),
    ('influenza_breathe','SNOMED','11935061000006119','Administration of adjuvanted trivalent (aTIV) inactivated seasonal influenza vaccination given by other healthcare provider'),
    ('influenza_breathe','SNOMED','11935071000006114','Administration of second quadrivalent (QIV) inactivated seasonal influenza vaccination given by other healthcare provider'),
    ('influenza_breathe','SNOMED','11935081000006112','Adjuvanted trivalent (aTIV) inactivated seasonal influenza vaccination given by pharmacist'),
    ('influenza_breathe','SNOMED','11935091000006110','Administration of second non adjuvanted trivalent (TIV) inactivated seasonal influenza vaccination given by other healthcare provider'),
    ('influenza_breathe','SNOMED','11935101000006116','Second quadrivalent (QIV) inactivated seasonal influenza vaccination given by pharmacist'),
    ('influenza_breathe','SNOMED','11935111000006118','Administration of second quadrivalent (QIV) inactivated seasonal influenza vaccination'),
    ('influenza_breathe','SNOMED','11966421000006117','Adverse reaction to Influenza Vaccine Tetra Myl'),
    ('influenza_breathe','SNOMED','11989891000006112','Adverse reaction to Influenza Myl'),
    ('influenza_breathe','SNOMED','1206286018','Haemophilus influenzae septicaemia'),
    ('influenza_breathe','SNOMED','1227545013','[V]Flu - influenza vaccination'),
    ('influenza_breathe','SNOMED','1229740013','Influenza with bronchopneumonia'),
    ('influenza_breathe','SNOMED','1232627018','Pneumonia due to parainfluenza virus'),
    ('influenza_breathe','SNOMED','123962018','Influenza with encephalopathy'),
    ('influenza_breathe','SNOMED','1256621000033114','Influenza virus inactivated split (H1N1, H3N2-like, B/Brisbane/60/2008-like, strains)'),
    ('influenza_breathe','SNOMED','12589011000006116','Adverse reaction to Influenza virus inactivated split (H1N1, H3N2-like, B/Brisbane/60/2008-like, strains)'),
    ('influenza_breathe','SNOMED','12680721000006111','Hib (Haemophilus influenzae type b) and meningococcal group C vaccination'),
    ('influenza_breathe','SNOMED','12681331000006117','First diphtheria, tetanus, whooping cough, Hib (Haemophilus influenzae type b), polio and hepatitis B vaccination'),
    ('influenza_breathe','SNOMED','12681341000006110','Second diphtheria, tetanus, whooping cough, Hib (Haemophilus influenzae type b), polio and hepatitis B vaccination'),
    ('influenza_breathe','SNOMED','12681351000006112','Third diphtheria, tetanus, whooping cough, Hib (Haemophilus influenzae type b), polio and hepatitis B vaccination'),
    ('influenza_breathe','SNOMED','12715991000006110','[X]Influenza vaccine causing adverse effects in therapeutic use'),
    ('influenza_breathe','SNOMED','12721231000006111','Pneumonia or influenza NOS'),
    ('influenza_breathe','SNOMED','13002211000006112','Seasonal influenza vaccination given in school'),
    ('influenza_breathe','SNOMED','142934010','Influenza vaccination'),
    ('influenza_breathe','SNOMED','1484830012','Influenza vaccination contraindicated'),
    ('influenza_breathe','SNOMED','1485436015','Parainfluenza virus antibody level'),
    ('influenza_breathe','SNOMED','1485437012','Parainfluenza type 2 antibody level'),
    ('influenza_breathe','SNOMED','1485540014','Parainfluenza type 3 antibody level'),
    ('influenza_breathe','SNOMED','151462017','Haemophilus influenzae infection'),
    ('influenza_breathe','SNOMED','1783752014','Haemophilus influenzae B IgG level'),
    ('influenza_breathe','SNOMED','1784697017','Influenza A antigen level'),
    ('influenza_breathe','SNOMED','1784698010','Influenza B antigen level'),
    ('influenza_breathe','SNOMED','1792361000000119','Influenza A nucleic acid detection'),
    ('influenza_breathe','SNOMED','1803861000006110','Adverse reaction to Influenza Virus Live Attenuated'),
    ('influenza_breathe','SNOMED','1804181000006111','Adverse reaction to Influenza Vaccine (Live Attenuated)'),
    ('influenza_breathe','SNOMED','2395701000000111','Long term indication for seasonal influenza vaccination'),
    ('influenza_breathe','SNOMED','2397631000033114','Influenza Myl'),
    ('influenza_breathe','SNOMED','2608911000000114','Influenza due to zoonotic influenza virus'),
    ('influenza_breathe','SNOMED','2608951000000113','Influenza due to pandemic influenza virus'),
    ('influenza_breathe','SNOMED','2608991000000117','Influenza due to seasonal influenza virus'),
    ('influenza_breathe','SNOMED','2609031000000111','Influenza with pneumonia due to seasonal influenza virus'),
    ('influenza_breathe','SNOMED','2636251000000110','Parainfluenza group RNA (ribonucleic acid) detection assay'),
    ('influenza_breathe','SNOMED','265445015','Influenza vacc consent given'),
    ('influenza_breathe','SNOMED','2820795016','Influenza due to Influenza A virus subtype H1N1'),
    ('influenza_breathe','SNOMED','285858017','Has flu vaccination at home'),
    ('influenza_breathe','SNOMED','285859013','Hasflu vaccination at surgery'),
    ('influenza_breathe','SNOMED','288067017','[X]Haemophilus influenzae infection, unspecified'),
    ('influenza_breathe','SNOMED','296887012','Post influenza vaccination encephalitis'),
    ('influenza_breathe','SNOMED','301057016','Acute haemophilus influenzae laryngitis'),
    ('influenza_breathe','SNOMED','301108016','Acute haemophilus influenzae bronchitis'),
    ('influenza_breathe','SNOMED','301413010','Influenza with pneumonia, influenza virus identified'),
    ('influenza_breathe','SNOMED','301414016','Influenza with pneumonia NOS'),
    ('influenza_breathe','SNOMED','301415015','Influenza with other respiratory manifestation'),
    ('influenza_breathe','SNOMED','301416019','Influenza with laryngitis'),
    ('influenza_breathe','SNOMED','301417011','Influenza with pharyngitis'),
    ('influenza_breathe','SNOMED','301418018','Influenza with respiratory manifestations NOS'),
    ('influenza_breathe','SNOMED','301421016','Influenza with other manifestations'),
    ('influenza_breathe','SNOMED','301424012','Influenza with other manifestations NOS'),
    ('influenza_breathe','SNOMED','301431011','Pneumonia or influenza NOS'),
    ('influenza_breathe','SNOMED','3555871000006115','Parainfluenza virus pneumonia'),
    ('influenza_breathe','SNOMED','3555891000006119','Pneumonia due to parainfluenza virus'),
    ('influenza_breathe','SNOMED','3555901000006115','Parainfluenza pneumonia'),
    ('influenza_breathe','SNOMED','387051000000000','[X]Flu+oth respiratory manifestations,flu virus identified'),
    ('influenza_breathe','SNOMED','388291000000000','[X]H influenzae as cause/diseases classified/other chapters'),
    ('influenza_breathe','SNOMED','389901000000000','[X]Influenza+other manifestations,influenza virus identified'),
    ('influenza_breathe','SNOMED','396106019','Influenza NOS'),
    ('influenza_breathe','SNOMED','4063911000006112','Influenza like illness'),
    ('influenza_breathe','SNOMED','407721000000000','Haemophilus influenzae type B and meningitis C vaccination'),
    ('influenza_breathe','SNOMED','456021000000000','Acute bronchitis due to parainfluenza virus'),
    ('influenza_breathe','SNOMED','459620016','Influenza A antibody level'),
    ('influenza_breathe','SNOMED','459621017','Influenza B antibody level'),
    ('influenza_breathe','SNOMED','460150015','H/O: influenza vaccine allergy'),
    ('influenza_breathe','SNOMED','460160012','Influenza vaccination invitation letter sent'),
    ('influenza_breathe','SNOMED','4780481000006116','Acute parainfluenza virus bronchitis'),
    ('influenza_breathe','SNOMED','498121000000000','Haemophilus Influenzae B Polysaccaride Conjugated To Tetanus Protein'),
    ('influenza_breathe','SNOMED','498321000000000','Haemophilus Influenzae B Polysaccaride Conjugated To Diphtheria Protein'),
    ('influenza_breathe','SNOMED','546421000000000','Chest infection - influenza with pneumonia'),
    ('influenza_breathe','SNOMED','5791421000006116','Influenza split virion vaccine adverse reaction'),
    ('influenza_breathe','SNOMED','5791431000006118','Influenza surface antigen vaccine adverse reaction'),
    ('influenza_breathe','SNOMED','642021000000000','Inactivated Influenza (Split Virion)'),
    ('influenza_breathe','SNOMED','676781000000000','Influenza H1 virus detected'),
    ('influenza_breathe','SNOMED','676841000000000','Influenza H2 virus detected'),
    ('influenza_breathe','SNOMED','676901000000000','Influenza H3 virus detected'),
    ('influenza_breathe','SNOMED','676961000000000','Influenza H5 virus detected'),
    ('influenza_breathe','SNOMED','677021000000000','Influenza A virus, other or untyped strain detected'),
    ('influenza_breathe','SNOMED','677081000000000','Influenza B virus detected'),
    ('influenza_breathe','SNOMED','689941000000000','Human parainfluenza virus detected'),
    ('influenza_breathe','SNOMED','712231000000000','Sanofi Pasteur Inactivated Influenza Split Virion'),
    ('influenza_breathe','SNOMED','7267051000006117','Influenza A (H1N1)'),
    ('influenza_breathe','SNOMED','7267061000006115','Influenza caused by Influenza A virus subtype H1N1'),
    ('influenza_breathe','SNOMED','7293041000006114','Exposure to Influenza A virus subtype H1N1'),
    ('influenza_breathe','SNOMED','7293051000006111','Exposure to swine influenza virus'),
    ('influenza_breathe','SNOMED','762181000000000','Flu like illness'),
    ('influenza_breathe','SNOMED','7649691000006115','Haemophilus influenzae type b infection'),
    ('influenza_breathe','SNOMED','7649701000006115','Hib (Haemophilus influenzae type b) infection'),
    ('influenza_breathe','SNOMED','7696051000006119','First vaccination with Haemophilus influenzae type B and Neisseria meningitidis serotype C combination vaccine'),
    ('influenza_breathe','SNOMED','778711000000000','Influenza like illness'),
    ('influenza_breathe','SNOMED','778871000000000','Influenza with pneumonia'),
    ('influenza_breathe','SNOMED','808051000000000','H influenzae as cause/diseases classified/other chapters'),
    ('influenza_breathe','SNOMED','8190511000006118','Suspected influenza A virus subtype H1N1 infection'),
    ('influenza_breathe','SNOMED','843921000000000','*Influenza & Common Cold'),
    ('influenza_breathe','SNOMED','851321000000000','Haemophilus influenzae type b polysaccharide, conjugated'),
    ('influenza_breathe','SNOMED','880121000000000','Haemophilus Influenzae Type B Polysaccharide'),
    ('influenza_breathe','SNOMED','885251000000000','Influenza + pneumonia'),
    ('influenza_breathe','SNOMED','885261000000000','Influenza + encephalopathy'),
    ('influenza_breathe','SNOMED','885271000000000','Pneumonia/influenza NOS'),
    ('influenza_breathe','SNOMED','906821000000000','Haemophilus Influenzae (Lyophilized Bacterial Lysates)'),
    ('influenza_breathe','SNOMED','924501000000000','Influenza vaccination'),
    ('influenza_breathe','SNOMED','931801000000000','[X]Influenza vaccine causing adverse effects therapeutic use'),
    ('influenza_breathe','SNOMED','932221000000000','Adv react: influenza vaccine'),
    ('influenza_breathe','SNOMED','955561000000000','Adverse reaction to influenza vaccine'),
    ('influenza_breathe','SNOMED','966421000000000','Inactivated Influenza Virus Whole Virion Vero Cell Derived'),
    ('influenza_breathe','SNOMED','966521000000000','Inactivated Influenza Virus Split Virion Adjuvanted')
  ],
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md ## 5. Medications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Glucose lowering medications

# COMMAND ----------

# --------------------------------------------------------------------------
# Glucose lowering medications other than SGLT2i
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/060102/ 
# https://openprescribing.net/bnf/060101/ 
# # removed SGLT2i codes

# Acarbose (0601023A0)
# Albiglutide (0601023AS)
# Alogliptin (0601023AK)
# Alogliptin/metformin (0601023AJ)
# Chlorpropamide (0601021E0)
# Dulaglutide (0601023AQ)
# Exenatide (0601023Y0)
# Glibenclamide (0601021H0)
# Gliclazide (0601021M0)
# Glimepiride (0601021A0)
# Glipizide (0601021P0)
# Guar gum (0601023I0)
# Ins degludec/liraglutide (0601023AU)
# Linagliptin (0601023AE)
# Linagliptin/metformin (0601023AF)
# Liraglutide (0601023AB)
# Lixisenatide (0601023AI)
# Metformin hydrochloride (0601022B0)
# Metformin hydrochloride/pioglitazone (0601023W0)
# Metformin hydrochloride/rosiglitazone (0601023V0)
# Metformin hydrochloride/sitagliptin (0601023AD)
# Metformin hydrochloride/vildagliptin (0601023Z0)
# Nateglinide (0601023U0)
# Pioglitazone hydrochloride (0601023B0)
# Repaglinide (0601023R0)
# Rosiglitazone (0601023S0)
# Saxagliptin (0601023AC)
# Saxagliptin/metformin (0601023AH)
# Semaglutide (0601023AW)
# Sitagliptin (0601023X0)
# Tirzepatide (0601023AZ)
# Tolbutamide (0601021X0)
# Vildagliptin (0601023AA)
# Biphasic insulin aspart (0601012W0)
# Biphasic insulin lispro (0601012F0)
# Biphasic isophane insulin (0601012D0)
# Insulin aspart (0601011A0)
# Insulin degludec (0601012Z0)
# Insulin detemir (0601012X0)
# Insulin glargine (0601012V0)
# Insulin glargine/lixisenatide (0601012AB)
# Insulin glulisine (0601011P0)
# Insulin human (0601011R0)
# Insulin Lispro (0601011L0)
# Insulin zinc suspension (0601012G0)
# Isophane insulin (0601012S0)
# Protamine zinc insulin (0601012U0)
# Soluble insulin (0601011N0)

codelist_glucose_lowering = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AS")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AK")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AJ")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601021E0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AQ")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023Y0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601021H0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601021M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601021A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601021P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AU")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AE")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AF")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AB")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AI")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601022B0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AD")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023Z0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023B0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023S0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AC")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AH")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AW")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023X0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AZ")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601021X0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601023AA")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012D0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601011A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012Z0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012X0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012AB")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601011P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601011R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601011L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012G0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012S0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601012U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0601011N0")
  )
  .withColumn('name', f.lit('glucose_lowering'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_glucose_lowering.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_glucose_lowering)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ACE Inhibitors

# COMMAND ----------

# --------------------------------------------------------------------------
# Angiotensin-converting enzyme (ACE) inhibitors
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/020505/
# removed non ACE inhibitors


# Captopril (0205051F0)
# Cilazapril (0205051E0)
# Enalapril maleate (0205051I0)
# Enalapril maleate with diuretic (0205051H0)
# Fosinopril sodium (0205051J0)
# Imidapril hydrochloride (0205051W0)
# Lisinopril (0205051L0)
# Lisinopril with diuretic (0205051K0)
# Moexipril hydrochloride (0205051C0)
# Perindopril arginine (0205051Y0)
# Perindopril arginine with diuretic (0205051Z0)
# Perindopril erbumine (0205051M0)
# Perindopril erbumine with diuretic (0205051N0)
# Perindopril tosilate (0205051AA)
# Perindopril tosilate/indapamide (0205051AB)
# Perindopril with calcium channel blocker (0205051AC)
# Quinapril hydrochloride (0205051Q0)
# Quinapril hydrochloride with diuretic (0205051P0)
# Ramipril (0205051R0)
# Ramipril with calcium channel blocker (0205051S0)
# Trandolapril (0205051U0)
# Trandolapril with calcium channel blocker (0205051V0)


codelist_ace_inhibitor = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051E0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051H0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051J0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051Y0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051Z0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051N0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051AA")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051AB")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051AC")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051Q0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051S0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051V0")
  )
  .withColumn('name', f.lit('ace_inhibitor'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_ace_inhibitor.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_ace_inhibitor)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ARBs

# COMMAND ----------

# --------------------------------------------------------------------------
# Angiotensin receptor blockers (ARBs)
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/020505/
# removed non ARBs


# Candesartan cilexetil (0205052C0)
# Irbesartan (0205052I0)
# Irbesartan with diuretic (0205052A0)
# Losartan potassium (0205052N0)
# Losartan potassium with diuretic (0205052P0)
# Sacubitril/valsartan (0205052AE)
# Valsartan (0205052V0)
# Valsartan with diuretic (0205052X0)

codelist_arb = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052C0")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052I0")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052A0")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052N0")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052P0")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052AE")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052V0")
  | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052X0")
  )
  .withColumn('name', f.lit('arb'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_arb.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_arb)



# COMMAND ----------

# MAGIC %md
# MAGIC ### RAAS Inhibitors 

# COMMAND ----------

# --------------------------------------------------------------------------
# Renin-Angiotensin-Aldosterone System (RAAS) Inhibitors
# --------------------------------------------------------------------------

## ACE inhibitors
# https://openprescribing.net/bnf/020505/
# removed non ACE inhibitors

# Captopril (0205051F0)
# Cilazapril (0205051E0)
# Enalapril maleate (0205051I0)
# Enalapril maleate with diuretic (0205051H0)
# Fosinopril sodium (0205051J0)
# Imidapril hydrochloride (0205051W0)
# Lisinopril (0205051L0)
# Lisinopril with diuretic (0205051K0)
# Moexipril hydrochloride (0205051C0)
# Perindopril arginine (0205051Y0)
# Perindopril arginine with diuretic (0205051Z0)
# Perindopril erbumine (0205051M0)
# Perindopril erbumine with diuretic (0205051N0)
# Perindopril tosilate (0205051AA)
# Perindopril tosilate/indapamide (0205051AB)
# Perindopril with calcium channel blocker (0205051AC)
# Quinapril hydrochloride (0205051Q0)
# Quinapril hydrochloride with diuretic (0205051P0)
# Ramipril (0205051R0)
# Ramipril with calcium channel blocker (0205051S0)
# Trandolapril (0205051U0)
# Trandolapril with calcium channel blocker (0205051V0)

## ARBs
# https://openprescribing.net/bnf/020505/
# removed non ARBs

# Candesartan cilexetil (0205052C0)
# Irbesartan (0205052I0)
# Irbesartan with diuretic (0205052A0)
# Losartan potassium (0205052N0)
# Losartan potassium with diuretic (0205052P0)
# Sacubitril/valsartan (0205052AE)
# Valsartan (0205052V0)
# Valsartan with diuretic (0205052X0)


codelist_raas_inhibitor = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051E0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051H0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051J0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051Y0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051Z0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051N0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051AA")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051AB")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051AC")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051Q0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051S0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205051V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052N0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052AE")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0205052X0")
  )
  .withColumn('name', f.lit('raas_inhibitor'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_raas_inhibitor.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_raas_inhibitor)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Calcium channel blockers

# COMMAND ----------

# --------------------------------------------------------------------------
# Calcium channel blockers
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/020602/


# Amlodipine (0206020A0)
# Diltiazem hydrochloride (0206020C0)
# Felodipine (0206020F0)
# Isradipine (0206020I0)
# Lacidipine (0206020K0)
# Lercanidipine hydrochloride (0206020L0)
# Nicardipine hydrochloride (0206020Q0)
# Nifedipine (0206020R0)
# Nimodipine (0206020M0)
# Nisoldipine (0206020W0)
# Valsartan/amlodipine (0206020Z0)
# Verapamil hydrochloride (0206020T0)

codelist_calcium_channel_blocker = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020Q0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020Z0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0206020T0")
  )
  .withColumn('name', f.lit('calcium_channel_blocker'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_calcium_channel_blocker.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_calcium_channel_blocker)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beta Blockers

# COMMAND ----------

# --------------------------------------------------------------------------
# Beta blockers
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/0204/


# Acebutolol hydrochloride (0204000C0)
# Atenolol (0204000E0)
# Atenolol with calcium channel blocker (0204000U0)
# Atenolol with diuretic (0204000F0)
# Bisoprolol fumarate (0204000H0)
# Bisoprolol fumarate/aspirin (0204000AC)
# Carvedilol (020400080)
# Celiprolol hydrochloride (020400060)
# Co-prenozide (Oxprenolol hydrochloride/cyclopenthiazide) (0204000Y0)
# Co-tenidone (Atenolol/chlortalidone) (020400040)
# Labetalol hydrochloride (0204000I0)
# Metoprolol tartrate (0204000K0)
# Metoprolol tartrate with diuretic (0204000W0)
# Nadolol (0204000M0)
# Nebivolol (0204000AB)
# Oxprenolol hydrochloride (0204000N0)
# Pindolol (0204000P0)
# Pindolol with diuretic (020400010)
# Propranolol hydrochloride (0204000R0)
# Propranolol hydrochloride with diuretic (0204000Q0)
# Sotalol hydrochloride (0204000T0)
# Timolol (0204000V0)
# Timolol with diuretic (020400030)

codelist_beta_blocker = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000E0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000H0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000AC")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "020400080")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "020400060")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "Oxprenolol hydrochloride/cyclopenthiazide")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "Atenolol/chlortalidone")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000AB")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000N0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "020400010")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000Q0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000T0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0204000V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "020400030")
  )
  .withColumn('name', f.lit('beta_blocker'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_beta_blocker.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_beta_blocker)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diuretics

# COMMAND ----------

# --------------------------------------------------------------------------
# Diuretics
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/020201/ - Thiazides and related diuretics
# https://openprescribing.net/bnf/020202/ - Loop diuretics


# Bendroflumethiazide (0202010B0)
# Chlorothiazide (0202010D0)
# Chlortalidone (0202010F0)
# Cyclopenthiazide (0202010J0)
# Hydrochlorothiazide (0202010L0)
# Indapamide (0202010P0)
# Metolazone (0202010V0)
# Polythiazide (0202010X0)
# Xipamide (0202010Y0)
# Bumetanide (0202020D0)
# Furosemide (0202020L0)
# Torasemide (0202020U0)

codelist_diurectic = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010B0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010D0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010J0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010X0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202010Y0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202020D0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202020L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0202020U0")
  )
  .withColumn('name', f.lit('diuretic'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_diurectic.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
display(codelist_diurectic)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Statins

# COMMAND ----------

# --------------------------------------------------------------------------
# statin
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/0212/
# https://www.opencodelists.org/codelist/opensafely/statin-medication/2020-04-20/

# Atorvastatin (0212000B0)
# # # Cerivastatin (0212000C0) # withdrawn # No prescriptions found
# Fenofibrate/simvastatin (0212000AJ)
# Fluvastatin sodium (0212000M0)
# Pravastatin sodium (0212000X0)
# Rosuvastatin calcium (0212000AA)
# Simvastatin (0212000Y0)
# Simvastatin and ezetimibe (0212000AC)

codelist_statin = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000B0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000AJ')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000M0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000X0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000AA')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000Y0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000AC')
  )
  .withColumn('name', f.lit('statin'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_statin.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_statin.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Corticosteroids

# COMMAND ----------

# --------------------------------------------------------------------------
# Corticosteroids
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/010502/
# https://openprescribing.net/bnf/110401/
# https://openprescribing.net/bnf/100102/


# Beclometasone dipropionate (0105020G0)
# Budesonide (0105020A0)
# Hydrocortisone (0105020C0)
# Hydrocortisone acetate (0105020B0)
# Prednisolone (0105020F0)
# Prednisolone sodium metasulphobenzoate (0105020D0)
# Prednisolone sodium phosphate (0105020E0)
# Betamethasone sodium phosphate (1104010D0)
# Clobetasone butyrate (1104010F0)
# Dexamethasone (1104010I0)
# Fluorometholone (1104010K0)
# Hydrocortisone acetate (1104010M0)
# Loteprednol etabonate (1104010W0)
# Netilmicin/Dexamethasone (1104010X0)
# Prednisolone acetate (1104010R0)
# Prednisolone sodium phosphate (1104010S0)
# Rimexolone (1104010V0)
# Hydrocortisone acetate (1001022G0)
# Methylprednisolone acetate (1001022K0)
# Prednisolone acetate (1001022N0)
# Triamcinolone acetonide (1001022U0)
# Triamcinolone hexacetonide (1001022Y0)

codelist_corticosteroid = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020G0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020B0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020D0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0105020E0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010D0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010X0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010S0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1104010V0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1001022G0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1001022K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1001022N0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1001022U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "1001022Y0")
  )
  .withColumn('name', f.lit('corticosteroid'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_corticosteroid.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_corticosteroid.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Antispychotics

# COMMAND ----------

# --------------------------------------------------------------------------
# Antipsychotics
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/040201/


# Amisulpride (0402010A0)
# Aripiprazole (0402010AD)
# Benperidol (0402010B0)
# Cariprazine (0402010AJ)
# Chlorpromazine hydrochloride (0402010D0)
# Chlorprothixene (0402010F0)
# Clozapine (0402010C0)
# Flupentixol hydrochloride (0402010H0)
# Fluphenazine hydrochloride (0402010I0)
# Haloperidol (0402010J0)
# Levomepromazine hydrochloride (0402010L0)
# Levomepromazine maleate (0402010K0)
# Loxapine succinate (0402010M0)
# Lurasidone (0402010AI)
# Melperone hydrochloride (0402010AF)
# Olanzapine (040201060)
# Paliperidone (0402010AE)
# Pericyazine (0402010P0)
# Perphenazine (0402010Q0)
# Pimozide (0402010R0)
# Promazine hydrochloride (0402010S0)
# Quetiapine (0402010AB)
# Risperidone (040201030)
# Sulpiride (0402010U0)
# Thioridazine (0402010W0)
# Trifluoperazine (0402010X0)
# Ziprasidone hydrochloride (0402010AG)
# Zotepine (0402010AC)
# Zuclopenthixol acetate (040201010)
# Zuclopenthixol hydrochloride (0402010T0)


codelist_antipsychotic = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010A0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AD")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010B0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AJ")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010D0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010F0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010C0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010H0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010I0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010J0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010L0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010K0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010M0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AI")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AF")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "040201060")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AE")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010P0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010Q0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010R0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010S0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AB")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "040201030")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010U0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010W0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010X0")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AG")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010AC")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "040201010")
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == "0402010T0")
  )
  .withColumn('name', f.lit('antipsychotic'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_antipsychotic.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_antipsychotic.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### Blood pressure lowering

# COMMAND ----------

# # --------------------------------------------------------------------------
# # bp_lowering
# # --------------------------------------------------------------------------
# # 0204 -- beta blockers
# #   exclude 0204000R0 -- propranolol
# #   exclude 0204000Q0 -- propranolol
# # 020502 -- centrally acting antihypertensives
# #   exclude 0205020G -- guanfacine because it is only used for ADHD
# # 020504 -- alpha blockers
# # 020602 -- calcium channel blockers
# # 020203 -- potassium sparing diuretics
# # 020201 -- thiazide diuretics
# # 020501 -- vasodilator antihypertensives
# # 0205051 -- angiotensin-converting enzyme inhibitors
# # 0205052 -- angiotensin-II receptor antagonists
# #   exclude 0205052AE -- drugs for heart failure, not for hypertension
# # 0205053A0 -- aliskiren
# codelist_bp_lowering = (
#   pmeds_bnf
#   .where(
#     (
#       (f.substring(f.col('PrescribedBNFCode'), 1, 4) == '0204')
#         & ~(
#           (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0204000R0')
#             | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0204000Q0')
#         )
#     )
#     | (
#       (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020502')
#         & ~(
#           (f.substring(f.col('PrescribedBNFCode'), 1, 8) == '0205020G')
#         )
#     )
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020504')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020602')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020203')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020201')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020501')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 7) == '0205051')
#     | (
#        (f.substring(f.col('PrescribedBNFCode'), 1, 7) == '0205052')
#         & ~(
#            (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0205052AE')
#         )
#     )
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0205053A0')
#   )
#   .withColumn('name', f.lit('bp_lowering'))
#   .withColumn('terminology', f.lit('BNF'))
#   .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
#   .orderBy('name', 'terminology', 'code', 'term')    
# )

# # check
# tmpt = tab(codelist_bp_lowering.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
# print(codelist_bp_lowering.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### Metformin

# COMMAND ----------

# # --------------------------------------------------------------------------
# # metformin
# # --------------------------------------------------------------------------
# # https://openprescribing.net/bnf/060102/

# # Alogliptin/metformin (0601023AJ)
# # Canagliflozin/metformin (0601023AP)
# # Dapagliflozin/metformin (0601023AL)
# # Empagliflozin/metformin (0601023AR)
# # Linagliptin/metformin (0601023AF)
# # Metformin hydrochloride (0601022B0)
# # Metformin hydrochloride/pioglitazone (0601023W0)
# # Metformin hydrochloride/rosiglitazone (0601023V0)
# # Metformin hydrochloride/sitagliptin (0601023AD)
# # Metformin hydrochloride/vildagliptin (0601023Z0)
# # Saxagliptin/metformin (0601023AH)
# codelist_metformin = (
#   pmeds_bnf
#   .where(
#       (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AJ')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AP')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AL')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AR')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AF')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601022B0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023W0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023V0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AD')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023Z0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AH')    
#   )
#   .withColumn('name', f.lit('metformin'))
#   .withColumn('terminology', f.lit('BNF'))
#   .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
#   .orderBy('name', 'terminology', 'code', 'term')    
# )

# # check
# tmpt = tab(codelist_metformin.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
# print(codelist_metformin.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### Insulin

# COMMAND ----------

# # --------------------------------------------------------------------------
# # insulin
# # --------------------------------------------------------------------------
# #name	terminology	code	term
# #insulin	BNF	0601012W0	Biphasic insulin aspart
# #insulin	BNF	0601012F0	Biphasic insulin lispro
# #insulin	BNF	0601012D0	Biphasic isophane insulin
# #insulin	BNF	0601011A0	Insulin aspart
# #insulin	BNF	0601012Z0	Insulin degludec 
# #insulin	BNF	0601012X0	Insulin detemir
# #insulin	BNF	0601012V0	Insulin glargine
# #insulin	BNF	0601012AB	Insulin glargine/lixisenatide
# #insulin	BNF	0601011P0	Insulin glulisine
# #insulin	BNF	0601011R0	Insulin human
# #insulin	BNF	0601011L0	Insulin Lispro
# #insulin	BNF	0601012G0	Insulin zinc suspension
# #insulin	BNF	0601012S0	Isophane insulin
# #insulin	BNF	0601012U0	Protamine zinc insulin
# #insulin	BNF	0601011N0	Soluble insulin (Neutral insulin)

# tmp_codelist_insulin = (
#   pmeds_bnf
#   .where(
#       (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012W0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012F0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012D0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011A0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012Z0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012X0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012V0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012AB')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011P0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011R0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011L0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012G0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012S0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012U0')
#     | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011N0')
#   )
#   .withColumn('name', f.lit('insulin'))
#   .withColumn('terminology', f.lit('BNF'))
#   .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
#   .orderBy('name', 'terminology', 'code', 'term')    
# )

# tmp_insulin_icd = spark.createDataFrame(
#   [
#     ('insulin','ICD10','Y423','Insulin and oral hypoglycaemic [antidiabetetic drugs]')
#   ],
#   ['name', 'terminology', 'code', 'term']  
# )

# c = spark.createDataFrame(
#   [
#     ('insulin','SNOMED','67866001','Insulin (substance)'),
#     ('insulin','SNOMED','4700006','Beef insulin (substance)'),
#     ('insulin','SNOMED','10329000','Zinc insulin (substance)'),
#     ('insulin','SNOMED','706937006','Free insulin (substance)'),
#     ('insulin','SNOMED','69805005','Insulin pump, device (physical object)'),
#     ('insulin','SNOMED','67296003','Pork insulin (substance)'),
#     ('insulin','SNOMED','789480007','Insulin dose (observable entity)'),
#     ('insulin','SNOMED','246491008','Insulin used (attribute)'),
#     ('insulin','SNOMED','96367001','Human insulin (substance)'),
#     ('insulin','SNOMED','412210000','Insulin lispro (substance)'),
#     ('insulin','SNOMED','325072002','Insulin aspart (substance)')
#   ],
#   ['name', 'terminology', 'code', 'term']
# )

# codelist_insulin_tmp2 = (
#   tmp_codelist_insulin
#   .union(tmp_insulin_icd)
# )

# codelist_insulin = (
#     codelist_insulin_tmp2
#     .union(tmp_insulin_snomed)
# )


# COMMAND ----------

# MAGIC %md # 6. Combine

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
codelist_all = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (clist not in ['codelist_match', 'codelist_match_summ', 'codelist_match_stages_to_run', 'codelist_match_v2_test', 'codelist_tmp', 'codelist_all'])]):
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  codelist_tmp = globals()[clist]
  if(indx == 0):
    codelist_all = codelist_tmp
  else:
    # pre unionByName
    for col in [col for col in codelist_tmp.columns if col not in codelist_all.columns]:
      # print('  M - adding column: ' + col)
      codelist_all = codelist_all.withColumn(col, f.lit(None))
    for col in [col for col in codelist_all.columns if col not in codelist_tmp.columns]:
      # print('  C - adding column: ' + col)
      codelist_tmp = codelist_tmp.withColumn(col, f.lit(None))
      
    # unionByName  
    codelist_all = (
      codelist_all
      .unionByName(codelist_tmp)
    )
  
# order  
codelist = (
  codelist_all
  .orderBy('name', 'terminology', 'code')
)

# COMMAND ----------

# check
display(codelist)

# COMMAND ----------

tmpt = tab(codelist, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md # 7. Reformat

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
codelist = (
  codelist
  .withColumn('_code_old', f.col('code'))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
  .withColumn('_code_diff', f.when(f.col('code') != f.col('_code_old'), 1).otherwise(0))
)

# check
tmpt = tab(codelist, '_code_diff'); print()
print(codelist.where(f.col('_code_diff') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# tidy
codelist = codelist.drop('_code_old', '_code_diff')

# COMMAND ----------

# MAGIC %md # 8. Exclusions

# COMMAND ----------

# 20230424 Carmen advised:
# Exclude these:			
			
# name	terminology	code	term
# sbp	SNOMED	"775671000000104"	Post exercise systolic blood pressure response normal (finding)
# sbp	SNOMED	"707303003"	Post exercise systolic blood pressure response abnormal (finding)
# sbp	SNOMED	"707304009"	Post exercise systolic blood pressure response normal (finding)

# COMMAND ----------

# # check
# tmpt = tab(codelist, 'name', 'terminology'); print()

# # flag
# codelist = (
#   codelist
#   .withColumn('flag_exclusion', 
#               f.when((f.col('name') == 'sbp') & (f.col('code').isin(['775671000000104', '707303003', '707304009'])), 1)
#               .otherwise(0)
#              )
# )

# # check
# tmpt = tab(codelist, 'flag_exclusion')
# print(codelist.where(f.col('flag_exclusion') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# # filter
# codelist = (
#   codelist
#   .where(f.col('flag_exclusion') == 0)
# )

# # check
# tmpt = tab(codelist, 'name', 'terminology'); print()

# # tidy
# codelist = codelist.drop('flag_exclusion')

# # check
# print(codelist.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 9. Check

# COMMAND ----------

# check 
tmpt = tab(codelist, 'name', 'terminology')

# COMMAND ----------

# check
display(codelist)

# COMMAND ----------

# check
display(codelist.where(f.col('terminology') == 'ICD10'))

# COMMAND ----------

# check
# old = spark.table(f'{dbc}.{proj}_out_codelist_covariates')
# new = codelist
# key = ['name', 'terminology', 'code']
# file1, file2, file3, file3_differences = compare_files(old, new, key, warningError=0)

# COMMAND ----------

# tmpt = tab(file1, 'name'); print()
# tmpt = tab(file2, 'name', 'name_old_TWO'); print()

# COMMAND ----------

display(codelist)

# COMMAND ----------

# MAGIC %md # 10. Save

# COMMAND ----------

save_table(df=codelist, out_name=f'{proj}_out_codelist_covariates', save_previous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exporting codelist
# MAGIC

# COMMAND ----------

# # Exporting the codelist to a csv file

# codelist = spark.table(f'{dsa}.{proj}_out_codelist_covariates')
# codelist_exposures = spark.table(f'{dsa}.{proj}_out_codelist_exposures')

# # print(codelist.count())
# # print(codelist_exposures.count())

# covariates_1 = ["sglt2i","total_cholesterol", "eGFR","CKD", "HF","glucose_lowering", "ace_inhibitor", "arb","calcium_channel_blocker"]

# covariates_2 = ["antipsychotic","beta_blocker", "corticosteroid", "diuretic", "statin"]
 

# combined_codelist = codelist.select(["name", "terminology","code", "term"]).union(codelist_exposures)

# codelist_1 = (
#     combined_codelist
#     .filter(f.col('name').isin(covariates_1))
# )
# codelist_2 = (
#     combined_codelist
#     .filter(f.col('name').isin(covariates_2))
# )



# tab(codelist_1, "name");print();
# tab(codelist_2, "name");print();


# COMMAND ----------

