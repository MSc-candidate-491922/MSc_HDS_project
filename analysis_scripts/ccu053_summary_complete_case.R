# ---- Install packages ----
install.packages("dbplyr")
install.packages("lubridate")
install.packages("readr")
install.packages("gt")
install.packages("huxtable")
install.packages("openxlsx")
install.packages("gtsummary")
install.packages("naniar")
install.packages("webshot2")

# ---- Load Libraries ----
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
library(forcats)
library(webshot2)
library(stringr)
library(broom)
library(purrr)

# ============================================================================
# Connecting to db and loading data
# ============================================================================

con <- DBI::dbConnect(odbc:::odbc(), "databricks")

#parameters
proj <- "ccu053"
dsa <- "dsa_391419_j3w9t_collab"
select_all <- glue("SELECT * FROM ",dsa,".")
select_all_proj <- glue("SELECT * FROM ",dsa,".",proj,"_")

combined_df <- dbGetQuery(con,sprintf(glue(select_all_proj,"out_combined")))

names(combined_df)
head(combined_df)

data <- combined_df %>% select(PERSON_ID, set, case, index_date, diabetes_type,
                               date_of_diagnosis, age_at_diagnosis,        
                               date_of_birth,sex, ethnicity_5_group, ethnicity_18_group, region,imd_quintile,          
                               sglt_12_mos,sglt_3_mos_distinct,
                               sglt_6_mos_distinct,sglt_12_mos_distinct,
                               covid, covid_offset, 
                               bmi, bmi_group_1,
                               hba1c,hba1c_group,scr_egfr,
                               total_chol,hdl_chol,
                               hx_hf, scr_egfr_hx_ckd, raas_inhibitor,
                               antipsychotic,
                               beta_blocker,calcium_channel_blocker,
                               corticosteroid,diuretic,
                               glucose_lowering,statin)


# ============================================================================
# Formatting variables and defining factors
# ============================================================================


# Create group variable based on hba1c
data$hba1c_group <- cut(
  data$hba1c,
  breaks = c(30, 49, 58, 72, 131),
  labels = c("[30-49)", "[49-58)", "[58-72)", "[72-131)"),
  right = FALSE 
)

sum(is.na(data$hba1c_group))


# Create group variable based on scr_egfr
data$scr_egfr_group <- cut(
  data$scr_egfr,
  breaks = c(-Inf, 15, 30, 45, 60, Inf),
  labels = c("<15", "[15-30)", "[30-45)", "[45-60)", ">60"),
  right = FALSE 
)

sum(is.na(data$scr_egfr_group))


data$ethnicity_18_group <- factor(data$ethnicity_18_group)
data$ethnicity_18_group <- relevel(data$ethnicity_18_group, ref = "British")

data$ethnicity_5_group <- factor(data$ethnicity_5_group)
data$ethnicity_5_group <- relevel(data$ethnicity_5_group, ref = "White")

data$scr_egfr_group <- factor(data$scr_egfr_group)
data$scr_egfr_group <- relevel(data$scr_egfr_group, ref = "<15")

data$hba1c_group <- factor(data$hba1c_group)
data$hba1c_group <- relevel(data$hba1c_group, ref = "[30-49)")

data$region <- factor(data$region)
data$region <- relevel(data$region, ref = "London")


data <- data %>%
  mutate(case = factor(case, levels = c(0, 1), labels = c("No", "Yes"))) 

data <- data %>%
  mutate(diabetes_type = factor(diabetes_type, levels = c("T1DM", "T2DM"), labels = c("Type 1", "Type 2")))

data <- data %>%
  mutate(sex = factor(sex, levels = c("F", "M"), labels = c("Female", "Male")))

data <- data %>%
  mutate(imd_quintile = factor(imd_quintile,
                               levels = c(1, 2, 3, 4, 5),
                               labels = c("1 - Most deprived", "2", "3", "4", "5 - Least deprived")))

data <- data %>%
  mutate(bmi_group_1 = factor(bmi_group_1,
                              levels = c("Underweight", "Healthy weight", "Overweight", "Obese")))


# Calculate the age at index date and add it as a new column
data <- data %>%
  mutate(age_at_index_date = as.numeric(difftime(index_date, date_of_birth, units = "days")) / 365.25)

# Calculate the years of diabetes diagnosis
data <- data %>%
  mutate(years_since_diagnosis = as.numeric(difftime(index_date, date_of_diagnosis, units = "days")) / 365.25)

sum(is.na(data$PERSON_ID))


# ============================================================================
# Complete case filtering 
# ============================================================================

complete_case_filtering <- function(data) {
  
  # Count cases and controls before filtering
  cases_before <- sum(data$case == "Yes", na.rm = TRUE)
  cases_before_t1dm <- sum(data$case == "Yes" & data$diabetes_type =="Type 1", na.rm = TRUE)
  cases_before_t2dm <- sum(data$case == "Yes" & data$diabetes_type =="Type 2", na.rm = TRUE)
  controls_before <- sum(data$case == "No", na.rm = TRUE)
  controls_before_t1dm <- sum(data$case == "No" & data$diabetes_type =="Type 1", na.rm = TRUE)
  controls_before_t2dm <- sum(data$case == "No" & data$diabetes_type =="Type 2", na.rm = TRUE)
  
  # Identify sets to remove: any set with missing values in a case
  sets_to_remove <- unique(data$set[data$case == "Yes" & rowSums(is.na(data)) > 0])
  
  # Filter out records: exclude controls from removed sets and rows with missing values
  filtered_data <- data[!(data$set %in% sets_to_remove) & rowSums(is.na(data)) == 0, ]
  
  # Count cases and controls after filtering
  cases_after <- sum(filtered_data$case == "Yes", na.rm = TRUE)
  cases_after_t1dm <- sum(filtered_data$case == "Yes" & filtered_data$diabetes_type =="Type 1", na.rm = TRUE)
  cases_after_t2dm <- sum(filtered_data$case == "Yes" & filtered_data$diabetes_type =="Type 2", na.rm = TRUE)
  controls_after <- sum(filtered_data$case == "No", na.rm = TRUE)
  controls_after_t1dm <- sum(filtered_data$case == "No" & filtered_data$diabetes_type =="Type 1", na.rm = TRUE)
  controls_after_t2dm <- sum(filtered_data$case == "No" & filtered_data$diabetes_type =="Type 2", na.rm = TRUE)
  
  # Print counts before and after cleaning
  cat("Original number of rows:", nrow(data), "\n")
  cat("Number of cases before cleaning:", cases_before, " T1DM: ",cases_before_t1dm, " T2DM: ",cases_before_t2dm,  "\n")
  cat("Number of controls before cleaning:", controls_before, " T1DM: ",controls_before_t1dm, " T2DM: ",controls_before_t2dm, "\n\n")
  
  cat("Number of rows after cleaning:", nrow(filtered_data), "\n")
  cat("Number of cases after cleaning:", cases_after, " T1DM: ",cases_after_t1dm, " T2DM: ",cases_after_t2dm, "\n")
  cat("Number of controls after cleaning:", controls_after, " T1DM: ",controls_after_t1dm, " T2DM: ",controls_after_t2dm, "\n")
  
  # Return the cleaned data
  return(filtered_data)
}

# Create complete case data using the custom function
data_cc <- complete_case_filtering(data)

# ============================================================================
# Assessing number of matched controls for each case
# ============================================================================

# Custom funtion to produce a table of no of controls for each case
summarise_cases_by_control_count <- function(input_data) {
  
  # Step 1: Count the number of controls per set
  controls_per_set <- input_data %>%
    group_by(set) %>%
    summarise(num_controls = sum(case == "No"), .groups = "drop")
  
  # Step 2: Count number of cases per control count
  cases_per_control_count <- input_data %>%
    filter(case == "Yes") %>%
    left_join(controls_per_set, by = "set") %>%
    group_by(num_controls) %>%
    summarise(num_cases = n(), .groups = "drop")
  
  # Step 3: Calculate total cases and percentage
  total_cases <- sum(cases_per_control_count$num_cases)
  
  summary_df <- cases_per_control_count %>%
    mutate(percentage = (num_cases / total_cases) * 100)
  
  return(summary_df)
}

# ---- Assess number of controls for each case  ----

# For full data
summary_output_full <- summarise_cases_by_control_count(data)
print(summary_output_full)

# For complete case data
summary_output_cc <- summarise_cases_by_control_count(data_cc)
print(summary_output_cc)

# ---- Remove cases with zero matched controls from complete case data  ----


# Step 1: Count the number of controls per set
controls_per_set <- data_cc %>%
  group_by(set) %>%
  summarise(num_controls = sum(case == "No")) # Count number of controls

# Step 2: Remove only cases where there are no matched controls (num_controls == 0)
df_filtered <- data_cc %>%
  left_join(controls_per_set, by = "set") %>% 
  filter(!(case == "Yes" & num_controls == 0)) %>% # Remove cases with zero controls
  select(-num_controls)

# Step 3: Update complete case data with no zero-matched cases
data_cc <- df_filtered

# Check the number of controls for each case in the complete case 
# (This should have no cases with zero controls)
summary_output_cc <- summarise_cases_by_control_count(data_cc)
print(summary_output_cc)

# ============================================================================
# Summary statistics
# ============================================================================

# ---- Co-prevalence of covariates ----

covs_df <- data_cc %>%
  select(sglt_12_mos, covid_offset, hx_hf, antipsychotic, beta_blocker,
         calcium_channel_blocker, corticosteroid, diuretic, glucose_lowering,
         statin, raas_inhibitors)

# Convert to clean numeric matrix
covs_mat <- as.matrix(sapply(covs_df, as.numeric))

# Check shape
dim(covs_mat)  # Should be [n_rows, n_covariates]

# Now compute co-prevalence
co_prev_mat <- t(covs_mat) %*% covs_mat / nrow(covs_mat)

# Convert to long format for plotting
co_prev_df <- melt(co_prev_mat)
colnames(co_prev_df) <- c("Var1", "Var2", "CoPrevalence")

# Remove lower triangle
co_prev_df <- co_prev_df %>%
  mutate(order1 = as.numeric(factor(Var1, levels = colnames(co_prev_mat))),
         order2 = as.numeric(factor(Var2, levels = colnames(co_prev_mat)))) %>%
  filter(order1 <= order2)


label_map <- c(
  sglt_12_mos = "SGLT2i",
  covid_offset = "COVID",
  hx_hf = "Heart failure history",
  antipsychotic = "Antipsychotic",
  beta_blocker = "Beta-blocker",
  calcium_channel_blocker = "Calcium channel blocker",
  corticosteroid = "Corticosteroid",
  diuretic = "Diuretic",
  glucose_lowering = "Glucose-lowering",
  statin = "Statin",
  raas_inhibitors = "RAAS inhibitor"
)

co_prev_df <- co_prev_df %>%
  mutate(
    Var1 = recode(Var1, !!!label_map),
    Var2 = recode(Var2, !!!label_map)
  )

ggplot(co_prev_df, aes(x = Var2, y = Var1, fill = CoPrevalence)) +
  geom_tile(color = "white") +
  geom_text(aes(label = ifelse(CoPrevalence > 0, sprintf("%.2f", CoPrevalence), "")), size = 3) +
  scale_fill_gradient(low = "lightyellow", high = "purple", limits = c(0, max(co_prev_df$CoPrevalence))) +
  scale_x_discrete(limits = rev(unique(co_prev_df$Var2))) +  
  theme_minimal() +
  labs(
    title = "Co-prevalence of Binary Covariates",
    x = "",
    y = "",
    fill = "Co-prevalence"
  ) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    axis.text.y = element_text(hjust = 1)
  )

# ============================================================================
# Summary tables
# ============================================================================

# Extract data to be presented in summary tables
summary_data <- data  %>% 
  select(case, diabetes_type,age_at_diagnosis, age_at_index_date, years_since_diagnosis, sex, ethnicity_5_group, 
         region, imd_quintile,sglt_12_mos,covid_offset, bmi, bmi_group_1, hba1c, hba1c_group, scr_egfr, 
         scr_egfr_group, total_chol, hdl_chol, hx_hf, scr_egfr_hx_ckd, raas_inhibitor, antipsychotic,beta_blocker, 
         calcium_channel_blocker, corticosteroid, diuretic, glucose_lowering, statin)

summary_data_cc <- data_cc  %>% 
  select(case, diabetes_type,age_at_diagnosis, age_at_index_date, years_since_diagnosis, sex, ethnicity_5_group, 
         region, imd_quintile,sglt_12_mos,covid_offset, bmi, bmi_group_1, hba1c, hba1c_group, scr_egfr, 
         scr_egfr_group, total_chol, hdl_chol, hx_hf, scr_egfr_hx_ckd, raas_inhibitor, antipsychotic,beta_blocker, 
         calcium_channel_blocker, corticosteroid, diuretic, glucose_lowering, statin)

#summary_data$ethnicity <- summary_data$ethnicity_18_group
#summary_data_cc$ethnicity <- summary_data_cc$ethnicity_18_group
# 
# summary_data$ethnicity <- summary_data$ethnicity_5_group
# summary_data_cc$ethnicity <- summary_data_cc$ethnicity_5_group

head(summary_data)

# Updating categorical variables with missing data for summary tables only
summary_data <- summary_data %>%
  mutate(region = fct_explicit_na(region, na_level = "(missing)"),
         region = fct_relevel(region, "(missing)", after = Inf))  # moves "(missing)" to the end

summary_data <- summary_data %>%
  mutate(ethnicity_5_group = fct_explicit_na(ethnicity_5_group, na_level = "(missing)"),
         ethnicity_5_group = fct_relevel(ethnicity_5_group, "(missing)", after = Inf))  # moves "(missing)" to the end

summary_data <- summary_data %>%
  mutate(imd_quintile = fct_explicit_na(imd_quintile, na_level = "(missing)"),
         imd_quintile = fct_relevel(imd_quintile, "(missing)", after = Inf))  # moves "(missing)" to the end

summary_data <- summary_data %>%
  mutate(hba1c_group = fct_explicit_na(hba1c_group, na_level = "(missing)"),
         hba1c_group = fct_relevel(hba1c_group, "(missing)", after = Inf))  # moves "(missing)" to the end

summary_data <- summary_data %>%
  mutate(bmi_group_1 = fct_explicit_na(bmi_group_1, na_level = "(missing)"),
         bmi_group_1 = fct_relevel(bmi_group_1, "(missing)", after = Inf))  # moves "(missing)" to the end

summary_data <- summary_data %>%
  mutate(scr_egfr_group = fct_explicit_na(scr_egfr_group, na_level = "(missing)"),
         scr_egfr_group = fct_relevel(scr_egfr_group, "(missing)", after = Inf))  # moves "(missing)" to the end


# Filter data for different types of diabetes
summary_data_t1dm <- summary_data %>%
  filter(diabetes_type == 'Type 1')
summary_data_t2dm <- summary_data %>%
  filter(diabetes_type == 'Type 2')
# Filter data for different types of diabetes
summary_data_t1dm_cc <- summary_data_cc %>%
  filter(diabetes_type == 'Type 1')
summary_data_t2dm_cc <- summary_data_cc %>%
  filter(diabetes_type == 'Type 2')

# Set variable labels for the summary tables
variable_labels = list(diabetes_type ~ "Type of Diabetes, n (%)",
                       age_at_diagnosis ~ "Age at Diagnosis in years, median (IQR)",
                       age_at_index_date ~ "Age at Index Date in years, median (IQR)",
                       years_since_diagnosis ~ "Years Since Diagnosis, median (IQR)",
                       sex ~ "Sex,  n (%) ",                     
                       ethnicity_5_group ~ "Ethnicity,  n (%)",
                       region ~ "Region,  n (%)",
                       imd_quintile ~ "IMD quintile, n (%)",
                       sglt_12_mos ~ "SGLT2 Inhibitors within 12 mos, n (%)",
                       covid_offset ~ "Prior COVID-19 Infection within 6 mos, n (%)",
                       bmi ~ "BMI (mg/kg²), median (IQR)",
                       bmi_group_1 ~ "BMI Category,  n (%)",                     
                       hba1c ~ "HbA1c (mmol/mol), median (IQR)",
                       hba1c_group ~ "HbA1c category (mmol/mol), n (%)",
                       scr_egfr ~ "eGFR (mL/min/1.73m²), median (IQR)",
                       scr_egfr_group ~ "eGFR category (mL/min/1.73m²), n (%)",
                       total_chol ~ "Total Cholesterol (mmol/L), median (IQR)",
                       hdl_chol ~ "HDL Cholesterol (mmol/L), median (IQR)",
                       hx_hf ~ "History of Heart Failure, n (%)",
                       scr_egfr_hx_ckd ~ "History of Chronic Kidney Disease, calculated by eGFR, n (%)",
                       raas_inhibitor ~ "RAAS inhibitors",
                       antipsychotic ~ "Antipsychotics",
                       beta_blocker ~ "Beta blockers",
                       calcium_channel_blocker ~ "Calcium channel blockers",                     
                       corticosteroid ~ "Corticosteroids",
                       diuretic ~ "Diuretics",
                       glucose_lowering ~ "Glucose lowering medications",
                       statin ~ "Statins")


# Custom function to generate summary table
create_summary_table <- function(data) {
  summary_table <- data %>%
    tbl_summary(
      by = case,
      missing = "no",
      missing_text = "Missing",
      label = variable_labels,
      type = all_continuous() ~ "continuous2",
      statistic = all_continuous() ~ c(
        "Median (Q1, Q3)" = "{median} ({p25}, {p75})",
       # "Mean (SD)" = "{mean} ({sd})",
        "Missing" = "{N_miss} ({p_miss}%)"
      ),
      digits = list(
        all_categorical() ~ list(
          function(x) {
            count <- round(x / 5) * 5
            ifelse(count < 10, "--", count)
          },
          1  # percentage: 1 decimal place
        ),
        all_continuous2() ~ list(
          N_miss = function(x) {
            x_rounded <- round(x / 5) * 5
            ifelse(x_rounded < 10, "--", x_rounded)
          },
          p_miss = 1
        )
      )
    ) %>%
    bold_labels() %>%
    add_overall() %>%
    modify_spanning_header(c("stat_1", "stat_2") ~ "**Hospitalisation with DKA**") %>%
    modify_header(all_stat_cols() ~ "**{level}** \n\n **N = {round(n/5)*5}**")
  
  return(summary_table)
}


# Create summary tables for overall, diabetes type 1 and type 2

# Full dataset
summary_table_overall <- create_summary_table(summary_data)
summary_table_t1dm <- create_summary_table(summary_data_t1dm)
summary_table_t2dm <- create_summary_table(summary_data_t2dm)


# Complete-case analysis
summary_table_overall_cc <- create_summary_table(summary_data_cc)
summary_table_t1dm_cc <- create_summary_table(summary_data_t1dm_cc)
summary_table_t2dm_cc <- create_summary_table(summary_data_t2dm_cc)


# Printing tables
print(summary_table_overall)
print(summary_table_t1dm)
print(summary_table_t2dm)
print(summary_table_overall_cc)
print(summary_table_t1dm_cc)
print(summary_table_t2dm_cc)

## ---- Save summary tables as combined CSV ----

# Custom function to convert summary tables into tibbles
summarise_and_label <- function(tbl, label) {
  as_tibble(tbl) %>%
    mutate(summary_label = label) %>%
    relocate(summary_label)
}

# Convert summary tables in to tibbles and append to single tibble
combined_tbl <- bind_rows(
  summarise_and_label(summary_table_overall, "Overall"),
  summarise_and_label(summary_table_t1dm, "Type 1"),
  summarise_and_label(summary_table_t2dm, "Type 2")
)

combined_tbl_cc <- bind_rows(
  summarise_and_label(summary_table_overall_cc, "CC: Overall"),
  summarise_and_label(summary_table_t1dm_cc, "CC: Type 1"),
  summarise_and_label(summary_table_t2dm_cc, "CC: Type 2")
)

# Export tibble as CSV
write.csv(combined_tbl, "combined_summary.csv", row.names = FALSE)
write.csv(combined_tbl_cc, "combined_summary_complete_case.csv", row.names = FALSE)


#Save as png
filename <- paste0("summary_table_cov_overall_", format(Sys.time(), "%Y-%m-%d_%H-%M"), ".png")
summary_table_cov_overall |> as_gt() |> gt::gtsave(filename)






#############################################################################################

## CONDITIONAL LOGISTIC REGRESSION - Complete case

regression_data <- data_cc

str(combined_df)
head(combined_df)

regression_data$case <- ifelse(regression_data$case == "Yes", 1, 0)

# Re-leveling categorical variables for reference groups for regression
regression_data$ethnicity_18_group <- relevel(regression_data$ethnicity_18_group, ref = "British")
regression_data$ethnicity_5_group <- relevel(regression_data$ethnicity_5_group, ref = "White")
regression_data$scr_egfr_group <- relevel(regression_data$scr_egfr_group, ref = "<15")
regression_data$hba1c_group <- relevel(regression_data$hba1c_group, ref = "[30-49)")
regression_data$region <- relevel(regression_data$region, ref = "London")
regression_data$imd_quintile <- relevel(regression_data$imd_quintile, ref = "5 - Least deprived")
regression_data$bmi_group_1 <- relevel(regression_data$bmi_group_1, ref = "Healthy weight")





## Chosing between 5 or 18 ethnicity groups:
regression_data$ethncity <- regression_data$ethnicity_5_group
regression_data$ethnicity <- regression_data$ethnicity_18_group


regression_data_t1dm <- regression_data %>% filter(diabetes_type == 'Type 1') 
regression_data_t2dm <- regression_data %>% filter(diabetes_type == 'Type 2') 

##############

## ---- Uni-variable analysis ----

# Crude - overall
resClogit_univariable <- clogit(formula = case ~ sglt_12_mos + strata(set), data = regression_data)
#summary(resClogit_univariable)

# Crude - type 1
resClogit_t1dm_univariable <- clogit(formula = case ~ sglt_12_mos + strata(set), data = regression_data_t1dm)
#summary(resClogit_t1dm_univariable)

# Crude - type2
resClogit_t2dm_univariable <- clogit(formula = case ~ sglt_12_mos + strata(set), data = regression_data_t2dm)
#summary(resClogit_t2dm_univariable)

## ---- Adjusted analysis ----

# Overall
resClogit_adjusted_all <- clogit(
  formula = case ~ sglt_12_mos + covid_offset + imd_quintile + ethnicity + region
  + bmi_group_1 + hba1c_group + scr_egfr_group + total_chol + hdl_chol + hx_hf  + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data
)
# summary(resClogit_adjusted_all)

# Type 1
resClogit_adjusted_t1dm <- clogit(
  formula = case ~ sglt_12_mos + covid_offset + imd_quintile + ethnicity + region
  + bmi_group_1 + hba1c_group + scr_egfr_group + total_chol + hdl_chol + hx_hf  + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t1dm
)
# summary(resClogit_adjusted_t1dm)


# Type 2
resClogit_adjusted_t2dm <- clogit(
  formula = case ~ sglt_12_mos + covid_offset + imd_quintile + ethnicity + region
  + bmi_group_1 + hba1c_group + scr_egfr_group + total_chol + hdl_chol + hx_hf  + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t2dm
)
# summary(resClogit_adjusted_t2dm)

## ---- Adjusted with COVID interaction ----

# Overall
resClogit_adjusted_covid_int_all <- clogit(
  formula = case ~ sglt_12_mos * covid_offset + imd_quintile + ethnicity + region
  + bmi_group_1 + hba1c_group + scr_egfr_group + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data
)
# summary(resClogit_adjusted_covid_int_all)

# Type 1
resClogit_adjusted_covid_int_t1dm <- clogit(
  formula = case ~ sglt_12_mos * covid_offset + imd_quintile + ethnicity + region
  + bmi_group_1 + hba1c_group + scr_egfr + total_chol + hdl_chol + hx_hf  + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t1dm
)
# summary(resClogit_adjusted_covid_int_t1dm)

# Type 2
resClogit_adjusted_covid_int_t2dm <- clogit(
  formula = case ~ sglt_12_mos * covid_offset + imd_quintile + ethnicity + region
  + bmi_group_1 + hba1c_group + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t2dm
)
# summary(resClogit_adjusted_covid_int_t2dm)


## ---- Adjusted with COVID and HbA1c group interactionn ----

# Overall
resClogit_adjusted_covid_hba1c_int_all <- clogit(
  formula = case ~ sglt_12_mos * covid_offset + sglt_12_mos * hba1c_group + imd_quintile + ethnicity + region
  + bmi_group_1 + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data
)
#summary(resClogit_adjusted_covid_hba1c_int_all)

# Type 1
resClogit_adjusted_covid_hba1c_int_t1dm <- clogit(
  formula =case ~ sglt_12_mos * covid_offset + sglt_12_mos * hba1c_group + imd_quintile + ethnicity + region
  + bmi_group_1 + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t1dm
)
# summary(resClogit_adjusted_covid_hba1c_int_t1dm)

# Type 2
resClogit_adjusted_covid_hba1c_int_t2dm <- clogit(
  formula = case ~ sglt_12_mos * covid_offset + sglt_12_mos * hba1c_group + imd_quintile + ethnicity + region
  + bmi_group_1 + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t2dm
)
# summary(resClogit_adjusted_covid_hba1c_int_t2dm)


## ---- Adjusted with SLGTi * COVID * HbA1c_group interaction ----

# Overall
resClogit_adjusted_covid_hba1c_triple_int_all <- clogit(
  formula = case ~ sglt_12_mos * covid_offset * hba1c_group + imd_quintile + ethnicity + region
  + bmi_group_1 + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data
)
# summary(resClogit_adjusted_covid_hba1c_triple_int_all)

# Type 1
resClogit_adjusted_covid_hba1c_triple_int_t1dm <- clogit(
  formula = case ~ sglt_12_mos * covid_offset * hba1c_group + imd_quintile + ethnicity + region
  + bmi_group_1 + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t1dm
)
# summary(resClogit_adjusted_covid_hba1c_triple_int_t1dm)

# Type 2
resClogit_adjusted_covid_hba1c_triple_int_t2dm <- clogit(
  formula = case ~ sglt_12_mos * covid_offset * hba1c_group + imd_quintile + ethnicity + region
  + bmi_group_1 + scr_egfr + total_chol + hdl_chol + hx_hf + raas_inhibitor +
    antipsychotic  + beta_blocker + calcium_channel_blocker +
    corticosteroid + diuretic + glucose_lowering + statin + strata(set),
  data = regression_data_t2dm
)
# summary(resClogit_adjusted_covid_hba1c_triple_int_t2dm)



## ---- combine all model results into single file ----

# Custom function to combine results from all models
combine_clogit_models <- function(model_list) {
  
  
  combined_results <- imap_dfr(model_list, function(model, model_name) {
    tidy(model, exponentiate = TRUE, conf.int = TRUE) %>%
      mutate(
        `exp(-coef)` = exp(-estimate),
        model = model_name
      ) %>%
      select(model, term, `exp(coef)` = estimate, `exp(-coef)`, `lower .95` = conf.low, `upper .95` = conf.high)
  })
  
  return(combined_results)
}

# Create model list
model_list <- list(
  "univariable_all" = resClogit_univariable,
  "univariable_t1dm" = resClogit_t1dm_univariable,
  "univariable_t2dm" = resClogit_t2dm_univariable,
  "adjusted_all" = resClogit_adjusted_all,
  "adjusted_t1dm" = resClogit_adjusted_t1dm,
  "adjusted_t2dm" = resClogit_adjusted_t2dm,
  "adjusted_sglti*covid_all" = resClogit_adjusted_covid_int_all,
  "adjusted_sglti*covid_t1dm" = resClogit_adjusted_covid_int_t1dm,
  "adjusted_sglti*covid_t2dm" = resClogit_adjusted_covid_int_t2dm,
  "adjusted_sglti*covid_sglt*hba1c_all" = resClogit_adjusted_covid_hba1c_int_all,
  "adjusted_sglti*covid_sglt*hba1c_t1dm" = resClogit_adjusted_covid_hba1c_int_t1dm,
  "adjusted_sglti*covid_sglt*hba1c_t2dm" = resClogit_adjusted_covid_hba1c_int_t2dm,
  "adjusted_sglti*covid*hba1c_all" = resClogit_adjusted_covid_hba1c_triple_int_all,
  "adjusted_sglti*covid*hba1c_t1dm" = resClogit_adjusted_covid_hba1c_triple_int_t1dm,
  "adjusted_sglti*covid*hba1c_t2dm" = resClogit_adjusted_covid_hba1c_triple_int_t2dm
)

all_results <- combine_clogit_models(model_list)

write.csv(all_results, "combined_model_results_region18.csv", row.names = FALSE)


###############################################################################################################
###############################################################################################################
