# Databricks notebook source
# MAGIC %md # CCU053-D00-master
# MAGIC
# MAGIC **Project** CCU053
# MAGIC
# MAGIC **Description** This notebook runs all data curation pipeline notebooks for the project in order.
# MAGIC
# MAGIC **Authors** Candidate 491922
# MAGIC
# MAGIC **Acknowledgements** 
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## list of notebooks
# MAGIC
# MAGIC ## data curation pipeline
# MAGIC - (D01-parameters) # called within each individual notebook
# MAGIC - D02a-codelist_exposure
# MAGIC - D02b-codelist_outcome
# MAGIC - D02c-codelist_covid
# MAGIC - D02d-codelist_covariate
# MAGIC - D03a-cohort
# MAGIC - D03b-cohort_inclusion
# MAGIC - D04-curated_data
# MAGIC - D05a-outcome
# MAGIC - D05b-case_control_matcching
# MAGIC - D06-exposure
# MAGIC - D08-covariates
# MAGIC - D09-combine
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md # Run Pipeline
# MAGIC

# COMMAND ----------

dbutils.notebook.run('CCU053-D02a-codelist_exposures', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D02b-codelist_outcomes', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D02c-codelist_covid', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D02d-codelist_covariates', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D02e-codelist_outcomes_flu', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D03a-cohort', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D03b-cohort_inclusions', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D04-curated_data', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D05a-outcome', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D05b-case_control_matching', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D06-exposure', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D07_covariates', 3600)

# COMMAND ----------

dbutils.notebook.run('CCU053-D08_combine', 3600)
