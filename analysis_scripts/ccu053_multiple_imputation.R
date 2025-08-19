
# Multiple imputation

install.packages("dbplyr")
install.packages("lubridate")
install.packages("readr")
install.packages("gt")
install.packages("huxtable")
install.packages("openxlsx")
install.packages("gtsummary")
install.packages("naniar")
install.packages("pheatmap")
install.packages("mice")

library(dplyr)
library(dbplyr)
library(glue)
library(DBI)
library(gtsummary)
library(tidyr)
library(survival)
library(lubridate)
library(readr)
library(gt)
library(huxtable)
library(openxlsx)
library(ggplot2)
library(scales) 
library(naniar)
library(reshape2)
library(pheatmap)
library(mice)
library(stringr)

library(DBI)
con <- dbConnect(
  odbc::odbc(),
  dsn = 'databricks',
  HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
  PWD = rstudioapi::askForPassword('Please enter Databricks PAT')
)

setwd("/db-mnt/databricks/rstudio_collab/CCU053")

# con <- DBI::dbConnect(odbc:::odbc(), "databricks")


#parameters
proj <- "ccu053"
dsa <- "dsa_391419_j3w9t_collab"
select_all <- glue("SELECT * FROM ",dsa,".")
select_all_proj <- glue("SELECT * FROM ",dsa,".",proj,"_")

combined_df <- dbGetQuery(con,sprintf(glue(select_all_proj,"out_combined")))

head(combined_df)


data <- combined_df %>% select(PERSON_ID, set, case, diabetes_type,
                               index_date, date_of_diagnosis, date_of_birth,
                               age_at_diagnosis, sex, ethnicity_5_group, 
                               region,imd_quintile,          
                               sglt_12_mos,sglt_3_mos_distinct,
                               sglt_6_mos_distinct, sglt_12_mos_distinct,
                               covid_offset,
                               bmi, hba1c, scr_egfr,
                               total_chol,hdl_chol,
                               hx_hf, raas_inhibitor,
                               antipsychotic,
                               beta_blocker,calcium_channel_blocker,
                               corticosteroid,diuretic,
                               glucose_lowering,statin)




# 
# 
# # To be removed
# imp_data <- imp_data %>%
#   filter(!is.na(PERSON_ID) & !is.na(set))

# # Create group variable based on hba1c
# data$hba1c_group <- cut(
#   data$hba1c,
#   breaks = c(30, 49, 58, 72, 131),
#   labels = c("[30-49)", "[49-58)", "[58-72)", "[72-131)"),
#   right = FALSE
# )
# 
# # Create group variable based on scr_egfr
# data$scr_egfr_group <- cut(
#   data$scr_egfr,
#   breaks = c(-Inf, 15, 30, 45, 60, Inf),
#   labels = c("<15", "[15-30)", "[30-45)", "[45-60)", ">60"),
#   right = FALSE
# )


data$recent_exposure <- ifelse(data$sglt_3_mos_distinct == 1, 1, 0)

data$non_recent_exposure <- ifelse(
  data$sglt_6_mos_distinct == 1 | data$sglt_12_mos_distinct == 1, 1, 0
)

data$sglt_exposure_group <- case_when(
  data$recent_exposure == 1 & data$non_recent_exposure == 1 ~ "both",
  data$recent_exposure == 1 & data$non_recent_exposure == 0 ~ "recent_only",
  data$recent_exposure == 0 & data$non_recent_exposure == 1 ~ "non_recent_only",
  TRUE ~ "none"
)

data$sglt_exposure_group <- as.factor(data$sglt_exposure_group)
data$sglt_exposure_group <- relevel(data$sglt_exposure_group, ref = "none")



data$ethnicity_5_group <- factor(data$ethnicity_5_group)
data$ethnicity_5_group <- relevel(data$ethnicity_5_group, ref = "White")
# 
# data$ethnicity_18_group <- factor(data$ethnicity_18_group)
# data$ethnicity_18_group <- relevel(data$ethnicity_18_group, ref = "British")

# data$scr_egfr_group <- factor(data$scr_egfr_group)
# data$scr_egfr_group <- relevel(data$scr_egfr_group, ref = "<15")
# 
# data$hba1c_group <- factor(data$hba1c_group)
# data$hba1c_group <- relevel(data$hba1c_group, ref = "[30-49)")

data$region <- factor(data$region)
data$region <- relevel(data$region, ref = "London")

data$case <- as.factor(data$case)

# data <- data %>% 
#   mutate(case = factor(case, levels = c(0, 1), labels = c("No", "Yes"))) 

data <- data %>%
  mutate(diabetes_type = factor(diabetes_type, levels = c("T1DM", "T2DM"), labels = c("Type 1", "Type 2")))

data <- data %>%
  mutate(sex = factor(sex, levels = c("F", "M"), labels = c("Female", "Male")))
data$sex <- relevel(data$sex, ref = "Male")

data <- data %>%
  mutate(imd_quintile = factor(imd_quintile,
                               levels = c(1, 2, 3, 4, 5),
                               labels = c("1 - Most deprived", "2", "3", "4", "5 - Least deprived")))

# data <- data %>%
#   mutate(bmi_group_1 = factor(bmi_group_1,
#                               levels = c("Underweight", "Healthy weight", "Overweight", "Obese")))


# Calculate the years of diabetes diagnosis
data <- data %>%
  mutate(years_since_diagnosis = as.numeric(difftime(index_date, date_of_diagnosis, units = "days")) / 365.25)

data <- data %>%
  mutate(year_of_birth = factor(year(date_of_birth)))

# imp_data$case <- as.factor(imp_data$case)
#imp_data$date_of_birth <- as.factor(format(imp_data$date_of_birth, "%Y-%m"))
# imp_data$ethnicity_5_group <- as.factor(imp_data$ethnicity_5_group)
# imp_data$region <- as.factor(imp_data$region)
# imp_data$imd_quintile <- as.factor(imp_data$imd_quintile)
data$sglt_12_mos <- as.factor(data$sglt_12_mos)
data$covid_offset <- as.factor(data$covid_offset)
# imp_data$bmi_group_1 <- as.factor(imp_data$bmi_group_1)
# imp_data$hba1c_group <- as.factor(imp_data$hba1c_group)
# imp_data$scr_egfr_group <- as.factor(imp_data$scr_egfr_group)
data$hx_hf <- as.factor(data$hx_hf)
data$raas_inhibitor <- as.factor(data$raas_inhibitor)
data$beta_blocker <- as.factor(data$beta_blocker)
data$calcium_channel_blocker <- as.factor(data$calcium_channel_blocker)
data$corticosteroid <- as.factor(data$corticosteroid)
data$diuretic <- as.factor(data$diuretic)
data$glucose_lowering <- as.factor(data$glucose_lowering)
data$antipsychotic <- as.factor(data$antipsychotic)
data$statin <- as.factor(data$statin)


str(data)


# method <- rep("pmm", ncol(data))
# names(method) <- names(data)
# method[c("PERSON_ID", "set", "date_of_diagnosis", "date_of_birth", "years_since_diagnosis")] <- ""
# 
# # Define predictor matrix and remove PERSON_ID and set
# pred <- make.predictorMatrix(imp_data)
# pred[, c("PERSON_ID", "set")] <- 0
# pred["PERSON_ID", ] <- 0
# pred["set", ] <- 0


method <- rep("pmm", ncol(data))
names(method) <- names(data)

# List of variables to exclude from imputation AND as predictors
vars_to_exclude <- c("PERSON_ID", "set", "date_of_diagnosis", "date_of_birth", 
                     "years_since_diagnosis", "sglt_3_mos_distinct",
                     "sglt_6_mos_distinct", "sglt_12_mos_distinct",
                     "recent_exposure", "non_recent_exposure", 
                     "sglt_exposure_group")



# Set method to "" to skip imputation
method[vars_to_exclude] <- ""

pred <- make.predictorMatrix(data)
# Set predictor matrix entries to 0
pred[, vars_to_exclude] <- 0
pred[vars_to_exclude, ] <- 0

method[names(data)]


data_t1dm <- data %>%
  filter(diabetes_type == 'Type 1')
data_t2dm <- data %>%
  filter(diabetes_type == 'Type 2')

##### SKIPPING
# # Using subset of data to assess MICE function works correctly
# test_data <- data_t2dm %>% dplyr::slice_sample(prop = 0.05)
# 
# test_imp <- mice(
#   test_data,
#   method = method[names(test_data)],
#   predictorMatrix = pred[names(test_data), names(test_data)],
#   m = 2,
#   maxit = 2,
#   seed = 101
# )
# 
# plot(test_imp)
# densityplot(test_imp)
# 
# # Using full data but only 2 imputations to assess if regression works
# data_t2dm_imp_2 <- mice(
#   data_t2dm,
#   method = method[names(data_t2dm)],
#   predictorMatrix = pred[names(data_t2dm), names(data_t2dm)],
#   m = 2,
#   maxit = 2,
#   seed = 101
# )
# 
# persistent_path <- "D:/PhotonUser/My Files/Home Folder/Data"
# saveRDS(my_data, file = file.path(persistent_path, "my_data.rds"))
# 
# saveRDS(data_t2dm_imp_2, "data_t2dm_imp_2.rds")
# saveRDS(data_t2dm_imp_2, file = "D:/PhotonUser/My Files/Home Folder/Data/my_data.rds")
# 
# saveRDS(data_t2dm_imp_2,
#         paste0("D:/PhotonUser/My Files/Home Folder/Data/data_t2dm_imp_2_", timestamp, ".rds"))
# 
###############
#Type 1 Diabetes

# library(mice)

# Define how many imputations you want
m_total <- 10

# Loop through and generate each imputed dataset separately
for (i in 1:m_total) {
  file_path <- paste0("imputations/t1dm_imputation_", i, ".rds")
  
  # Skip if this imputation already exists
  if (file.exists(file_path)) {
    message("Skipping imputation ", i, " (already exists)")
    next
  }
  
  # Run one imputation with 5 iterations
  imp_i <- mice(
    data_t1dm,
    m = 1,
    maxit = 5,
    method = method[names(data_t1dm)],
    predictorMatrix = pred[names(data_t1dm), names(data_t1dm)],
    seed = 100 + i
  )
  
  # Save the result
  saveRDS(imp_i, file_path)
  message("Saved imputation ", i)
}

###############
#Type 2 Diabetes

# library(mice)

# Define how many imputations you want
m_total <- 10

# Loop through and generate each imputed dataset separately
for (i in 1:m_total) {
  file_path <- paste0("imputations/t2dm_imputation_", i, ".rds")
  
  # Skip if this imputation already exists
  if (file.exists(file_path)) {
    message("Skipping imputation ", i, " (already exists)")
    next
  }
  
  # Run one imputation with 5 iterations
  imp_i <- mice(
    data_t2dm,
    m = 1,
    maxit = 5,
    method = method[names(data_t2dm)],
    predictorMatrix = pred[names(data_t2dm), names(data_t2dm)],
    seed = 100 + i
  )
  
  # Save the result
  saveRDS(imp_i, file_path)
  message("Saved imputation ", i)
}


#####################################################

diabetes_type_1 <- "t1dm"
diabetes_type_2 <- "t2dm"

m_total <- 10

# Load each imputed dataset into a list
imputed_list <- vector("list", m_total)

# From file location
for (i in 1:m_total) {
  file_path <- paste0("imputations/",diabetes_type_2, "_imputation_", i, ".rds")
  imputed_list[[i]] <- readRDS(file_path)
}


# Convert each mids object to a completed data.frame (just 1 imputation from each)
completed_list <- lapply(imputed_list, function(imp) {
  complete(imp, action = 1)
})



# Update the 10 imputed data sets with group variables 
imputed_list_transformed <- lapply(completed_list, function(data) {
  
  data$hba1c_group <- cut(data$hba1c,
                          breaks = c(30, 49, 58, 72, 131),
                          labels = c("30–49", "49–58", "58–72", "72–131"),
                          right = FALSE)
  
  data$hba1c_group <- relevel(data$hba1c_group, ref = "30–49")
  
  # data$scr_egfr_group <- cut(data$scr_egfr,
  #                            breaks = c(-Inf, 15, 30, 45, 60, Inf),
  #                            labels = c("<15", "[15-30)", "[30-45)", "[45-60)", ">60"),
  #                            right = FALSE)
  
  data$scr_egfr_group <- cut(
    data$scr_egfr,
    breaks = c(-Inf, 15, 30, 45, 60, 90, Inf),
    labels = c(
      "Stage 5 (eGFR <15)",
      "Stage 4 (eGFR 15–29)",
      "Stage 3b (eGFR 30–44)",
      "Stage 3a (eGFR 45–59)",
      "Stage 2 (eGFR 60–89)",
      "Stage 1 (eGFR 90+)"
    ),
    right = FALSE
  )
  
  data$scr_egfr_group <- relevel(data$scr_egfr_group, ref = "Stage 1 (eGFR 90+)")
  
  
  data <- data %>%
    mutate(
      bmi_group = case_when(
        is.na(bmi) | is.na(ethnicity_5_group) ~ NA_character_,
        
        # Ethnicity-adjusted BMI categories
        ethnicity_5_group %in% c("White", "Mixed or multiple", "Other ethnic group") & bmi <= 18.4 ~ "Underweight",
        ethnicity_5_group %in% c("White", "Mixed or multiple", "Other ethnic group") & bmi <= 24.9 ~ "Healthy weight",
        ethnicity_5_group %in% c("White", "Mixed or multiple", "Other ethnic group") & bmi <= 29.9 ~ "Overweight",
        ethnicity_5_group %in% c("White", "Mixed or multiple", "Other ethnic group")                 ~ "Obese",
        
        ethnicity_5_group %in% c("Asian or Asian British", "Black, Black British, Caribbean or African") & bmi <= 18.4 ~ "Underweight",
        ethnicity_5_group %in% c("Asian or Asian British", "Black, Black British, Caribbean or African") & bmi <= 22.9 ~ "Healthy weight",
        ethnicity_5_group %in% c("Asian or Asian British", "Black, Black British, Caribbean or African") & bmi <= 27.4 ~ "Overweight",
        ethnicity_5_group %in% c("Asian or Asian British", "Black, Black British, Caribbean or African")                 ~ "Obese",
        
        TRUE ~ NA_character_
      )
    )
  data$bmi_group <- as.factor(data$bmi_group)
  
  data$bmi_group <- relevel(data$bmi_group, ref = "Healthy weight")
  
  data$sglt_exposure_group <- as.factor(data$sglt_exposure_group)
  data$sglt_exposure_group <- relevel(data$sglt_exposure_group, ref = "none")
  
  data$case <- as.numeric(as.character(data$case))
  
  
  
  return(data)
})

# Use the first transformed dataset as the "original"
original_data <- imputed_list_transformed[[1]]
original_data$.imp <- 0
original_data$.id <- 1:nrow(original_data)

# Add .imp and .id to each imputed dataset
imputed_list_transformed <- lapply(seq_along(imputed_list_transformed), function(i) {
  df <- imputed_list_transformed[[i]]
  df$.imp <- i
  df$.id <- 1:nrow(df)
  return(df)
})

# Bind all into one long dataset
long_data <- bind_rows(original_data, dplyr::bind_rows(imputed_list_transformed))

# Reconstruct mids
imp_final <- as.mids(long_data)

timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
saveRDS(imp_final,
        paste0("combined_imp/imp_final_",diabetes_type_2,"_10_", timestamp, ".rds"))

##########################
# Run models 

# Model 1: Crude:
dataset_1 <- complete(imp_final, 1)
crude_model <- clogit(case ~ sglt_12_mos + strata(set), data = dataset_1)
summary(crude_model)

#unique(dataset_1$sglt_exposure_group)


# Model 2: Adjusted
model_2_models <- with(imp_final, clogit(
  formula = case ~ sglt_12_mos + covid_offset + hba1c_group +
    imd_quintile + ethnicity_5_group + region +
    bmi_group + scr_egfr_group + total_chol + hdl_chol + hx_hf +
    raas_inhibitor + antipsychotic + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin +
    strata(set)
))

timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
saveRDS(model_2_models,
        paste0("outputs/model_2_models_",diabetes_type_2,"_", timestamp, ".rds"))

pooled_model_2 <- pool(model_2_models)

#summary(pooled_model)

summary_model_2_imp_10 <- summary(pooled_model_2) %>%
  mutate(
    OR = exp(estimate),
    OR_lower = exp(estimate - 1.96 * std.error),
    OR_upper = exp(estimate + 1.96 * std.error)
  )

timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
write.csv(summary_model_2_imp_10,
          paste0("outputs/summary_model_2_",diabetes_type_2,"_imp_10", timestamp, ".csv"),
          row.names = FALSE)


# Model 3: Adjusted + interactions
model_3_models <- with(imp_final, clogit(
  formula = case ~ sglt_12_mos * covid_offset + sglt_12_mos * hba1c_group +
    imd_quintile + ethnicity_5_group + region +
    bmi_group + scr_egfr_group + total_chol + hdl_chol + hx_hf +
    raas_inhibitor + antipsychotic + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin +
    strata(set)
))


timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
saveRDS(model_3_models,
        paste0("outputs/model_3_models_",diabetes_type_2,"_", timestamp, ".rds"))


pooled_model_3 <- pool(model_3_models)

#summary(pooled_model_3)

summary_model_3_imp_10 <- summary(pooled_model_3) %>%
  mutate(
    OR = exp(estimate),
    OR_lower = exp(estimate - 1.96 * std.error),
    OR_upper = exp(estimate + 1.96 * std.error)
  )

timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
write.csv(summary_model_3_imp_10,
          paste0("outputs/summary_model_3_",diabetes_type_2,"_imp_10", timestamp, ".csv"),
          row.names = FALSE)


# Read each CSV and tag with metadata
mod2_t1dm <- read.csv("outputs/summary_model_2_t1dm_imp_1020250728_1100.csv")
mod2_t1dm$model <- "Model 2"
mod2_t1dm$diabetes_type <- "T1DM"

mod2_t2dm <- read.csv("outputs/summary_model_2_t2dm_imp_1020250728_1534.csv")
mod2_t2dm$model <- "Model 2"
mod2_t2dm$diabetes_type <- "T2DM"

mod3_t1dm <- read.csv("outputs/summary_model_3_t1dm_imp_1020250728_1111.csv")
mod3_t1dm$model <- "Model 3"
mod3_t1dm$diabetes_type <- "T1DM"

mod3_t2dm <- read.csv("outputs/summary_model_3_t2dm_imp_1020250728_1607.csv")
mod3_t2dm$model <- "Model 3"
mod3_t2dm$diabetes_type <- "T2DM"

# Combine into one data frame
all_models <- rbind(mod2_t1dm, mod2_t2dm, mod3_t1dm, mod3_t2dm)

# Optional: reorder columns to show metadata first
all_models <- all_models[, c("model", "diabetes_type", setdiff(names(all_models), c("model", "diabetes_type")))]

# Write to CSV
write.csv(all_models, "outputs/all_models_combined.csv", row.names = FALSE)



  mutate(model = "Model 3")
  
  
plot(imputed_list_transformed)
  
#################
# Imputation plots


m_total <- 10

# Load each imputed dataset into a list
imputed_list <- vector("list", m_total)

# From file location
for (i in 1:m_total) {
  file_path <- paste0("imputations/",diabetes_type_2, "_imputation_", i, ".rds")
  imputed_list[[i]] <- readRDS(file_path)
}

# Extract completed data from each mids object
imputed_dfs <- lapply(seq_along(imputed_list), function(i) {
  complete(imputed_list[[i]]) %>%
    mutate(.imp = i)
})

# Combine into one long-format data frame
long_df <- bind_rows(imputed_dfs)

###########
# Running multiple imputations in one session . 

# Run one imputation with 5 iterations
imp_t1dm <- mice(
  data_t1dm,
  m = 10,
  maxit = 5,
  method = method[names(data_t1dm)],
  predictorMatrix = pred[names(data_t1dm), names(data_t1dm)],
  seed = 101
)
# Save the result
saveRDS(imp_t1dm, "combined_imp/imp_singlesesh_t1dm.rds")

# Run one imputation with 5 iterations
imp_t2dm <- mice(
  data_t2dm,
  m = 10,
  maxit = 5,
  method = method[names(data_t2dm)],
  predictorMatrix = pred[names(data_t2dm), names(data_t2dm)],
  seed = 101
)
# Save the result
saveRDS(imp_t2dm, "combined_imp/imp_singlesesh_t2dm.rds")

###################
plot(imp_t1dm)

names(which(colSums(is.na(imp_t1dm$data)) > 0))
plot(imp_t1dm, y = "region")        # by name

densityplot(imp_t1dm)


##########################
# Sub-group analysis

# Make sure sglt_exposure_group is a factor
dataset_1$sglt_exposure_group <- factor(dataset_1$sglt_exposure_group, 
                                        levels = c("none", "recent_only", "non_recent_only", "both"))

# Run model
crude_model_group <- clogit(case ~ sglt_exposure_group + strata(set), data = dataset_1)

summary(crude_model_group)

# Model 2: Adjusted
model_2_models <- with(imp_final, clogit(
  formula = case ~ sglt_exposure_group + covid_offset + hba1c_group +
    imd_quintile + ethnicity_5_group + region +
    bmi_group + scr_egfr_group + total_chol + hdl_chol + hx_hf +
    raas_inhibitor + antipsychotic + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin +
    strata(set)
))

pooled_model_2 <- pool(model_2_models)

#summary(pooled_model)

x <- summary(pooled_model_2) %>%
  mutate(
    OR = exp(estimate),
    OR_lower = exp(estimate - 1.96 * std.error),
    OR_upper = exp(estimate + 1.96 * std.error)
  )

# timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
# write.csv(summary_model_2_imp_10,
#           paste0("outputs/summary_model_2_",diabetes_type_2,"_imp_10", timestamp, ".csv"),
#           row.names = FALSE)
