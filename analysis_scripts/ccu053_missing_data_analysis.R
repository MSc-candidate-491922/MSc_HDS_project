#### Missing data analysis

install.packages("dbplyr")
install.packages("lubridate")
install.packages("readr")
install.packages("gt")
install.packages("huxtable")
install.packages("openxlsx")
install.packages("gtsummary")
install.packages("naniar")
install.packages("webshot2")
library(webshot2)



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




########
# Missing values analysis

nrow(data)


colSums(is.na(data))

data_clean <- data %>%
  filter(!is.na(PERSON_ID) & !is.na(set)) %>%
  select(PERSON_ID, set, case, sglt_12_mos, hba1c_group, ethnicity_18_group, region, imd_quintile, bmi_group_1, scr_egfr, total_chol, hdl_chol)

nrow(data_clean)


# Count NAs per column
na_summary <- data_clean %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "variable", values_to = "na_count")

ggplot(na_summary, aes(x = reorder(variable, -na_count), y = na_count)) +
  geom_bar(stat = "identity", fill = "darkorange") +
  labs(
    title = "Missing Value Count per Variable",
    x = "Variable",
    y = "Number of Missing Values"
  ) +
  scale_y_continuous(labels = comma) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


# Count NAs per variable per case group
na_by_case <- data_clean %>%
  group_by(case) %>%
  summarise(across(everything(), ~ sum(is.na(.)), .names = "na_{.col}")) %>%
  pivot_longer(cols = starts_with("na_"), 
               names_to = "variable", 
               values_to = "na_count") %>%
  mutate(variable = sub("na_", "", variable))  # clean variable names




# Plot
ggplot(na_by_case, aes(x = reorder(variable, -na_count), y = na_count, fill = factor(case))) +
  geom_bar(stat = "identity") +
  labs(
    title = "Missing Values per Variable by Case Status",
    x = "Variable",
    y = "Number of Missing Values",
    fill = "Case Status"
  ) +
  scale_y_continuous(labels = comma) +
  scale_fill_manual(values = c("0" = "steelblue", "1" = "firebrick"), labels = c("Control", "Case")) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

missing_summary_by_person <- data_clean %>%
  mutate(na_count = rowSums(is.na(select(., -PERSON_ID, -set)))) %>%
  group_by(PERSON_ID, set) %>%
  summarise(na_count = sum(na_count), .groups = "drop") %>%
  count(na_count)

ggplot(missing_summary_by_person, aes(x = factor(na_count), y = n)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(
    title = "Number of Missing Values per PERSON_ID + set Combination",
    x = "Number of Missing Values",
    y = "Count of Persons"
  ) +
  scale_y_continuous(labels = comma) +
  theme_minimal()


missing_summary_by_person <- data_clean %>%
  mutate(na_count = rowSums(is.na(select(., -PERSON_ID, -set)))) %>%
  group_by(PERSON_ID, set, case) %>%
  summarise(na_count = sum(na_count), .groups = "drop") %>%
  count(na_count, case)

# Plot with fill by case
ggplot(missing_summary_by_person, aes(x = factor(na_count), y = n, fill = factor(case))) +
  geom_bar(stat = "identity", position = "stack") +
  labs(
    title = "Number of Missing Values per PERSON_ID + set Combination",
    x = "Number of Missing Values",
    y = "Count of Persons",
    fill = "Case Status"
  ) +
  scale_y_continuous(labels = comma) +
  scale_fill_manual(values = c("0" = "steelblue", "1" = "firebrick"),
                    labels = c("Control", "Case")) +
  theme_minimal()

vis_miss(data_clean)          # Visualize missingness across dataset
miss_var_summary(data)  # Summary of missingness per variable

# Sample 5000 rows (adjust as needed)
data_sample <- data_clean %>% slice_sample(n = 5000)

vis_miss(data_sample)

data_clean %>%
  mutate(hba1c_missing = is.na(hba1c_group),
         bmi_missing = is.na(bmi_group_1),
         scr_egfr_missing = is.na(scr_egfr),
         total_chol_missing = is.na(total_chol),
         hdl_chol_missing = is.na(hdl_chol)) %>%
  group_by(case, sglt_12_mos) %>%
  summarise(
    hba1c_missing_rate = mean(hba1c_missing),
    bmi_missing_rate = mean(bmi_missing),
    scr_egfr_missing_rate = mean(scr_egfr_missing),
    total_chol_missing_rate = mean(total_chol_missing),
    hdl_chol_missing_rate = mean(hdl_chol_missing),
    n = n()
  )

########