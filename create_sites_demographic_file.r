sdr_data = NULL
race_data = NULL

combined_data = NULL
processed_data_list = NULL

# You first have to create a folder that contains all of the absentee files you wish to summarize
absentees_path <- "/Users/klausmayr/Desktop/VR/ev_sites/absentees"

absentees_list <- list.files(path = absentees_path, pattern = "\\.csv$", full.names = TRUE)

process_csv <- function(file_path) {
  print(paste0("Processing ", file_path))
  data <- fread(file_path)
  
  # Base filters applied to data
  base_filtered_data <- data %>%
    filter(ballot_req_type == "ONE-STOP") %>%
    filter(ballot_rtn_status == "ACCEPTED") %>%
    filter(!grepl("MAIL", site_name))

  # Creating initial dataframe with sumarized SDR data
  sdr_data <- base_filtered_data %>%
    group_by(county_desc, site_name, election_dt) %>%
    count(sdr) %>%
    mutate(site_total = sum(n)) %>%
    mutate(sdr_percent = round(n / site_total, 3)) %>%
    filter(sdr == 'Y') %>%
    select(-sdr, -site_total)
    
  # Adding summarized race data to the SDR dataframe.
  race_data <- base_filtered_data %>%
    mutate(race = if_else(race == "UNDESIGNATED", "UNDESIGNATED RACE", race)) %>%
    group_by(county_desc, site_name, election_dt) %>%
    count(race) %>%
    mutate(site_total = sum(n)) %>%
    mutate(race_percent = round(n / site_total, 3)) %>%
    # select(-n) %>%
    pivot_wider(names_from = race, values_from = c(race_percent, n), names_sep = "_") %>%
    left_join(sdr_data, by = c("site_name", "county_desc", "election_dt"), multiple = "all")
  
  
  # Adding ethnicity data. This one is conditional because some years don't include ethnicity data
  if ("ethnicity" %in% names(base_filtered_data)) {
    ethnicity_data <- base_filtered_data %>%
      mutate(ethnicity = if_else(ethnicity == "UNDESIGNATED", "UNDESIGNATED ETHNICITY", ethnicity)) %>%
      group_by(county_desc, site_name, election_dt) %>%
      count(ethnicity) %>%
      mutate(site_total_ethnicity = sum(n)) %>%
      mutate(ethnicity_percent = round(n / site_total_ethnicity, 3)) %>%
      # select(-n, -site_total_ethnicity) %>%
      pivot_wider(names_from = ethnicity, values_from = c(ethnicity_percent, n), names_sep = "_") %>%
      left_join(race_data, by = c("site_name", "county_desc", "election_dt"), multiple = "all")
  } else {
    # If there's no ethnicity column, just use race_data as is
    ethnicity_data <- race_data
  }


  # Add age data
  age_data <- base_filtered_data %>%
    group_by(county_desc, site_name, election_dt) %>%
    count(age_group = cut(age, breaks = c(18, 25, 40, 65, Inf),
                          labels = c("Age 18 - 25", "Age 26 - 40", "Age 41 - 65", "Age Over 66"))) %>%
    mutate(site_total_age = sum(n)) %>%
    mutate(age_percent = round(n / site_total_age, 3)) %>%
    # select(-n, -site_total_age) %>%
    pivot_wider(names_from = age_group, values_from = c(age_percent, n), names_sep = "_") %>%
    # rename('NO AGE' = 'NA') %>%
    rename_with(~ifelse(is.na(.), "NO AGE", .), .cols = everything()) %>%
    left_join(ethnicity_data, by = c("site_name", "county_desc", "election_dt"), multiple = "all")
  
  # Conditionally rename if 'NA' exists in the names of age_data
  if("NA" %in% names(age_data)) {
    age_data <- rename(age_data, `NO AGE` = `NA`)
  }

  # This next line will return the full dataframe created by the above processes when this function is called.
  return(age_data)
}

# This runs the above function on the list of absentee files, but just makes a list of each absentee file's dataframe
processed_data_list <- lapply(absentees_list, process_csv)

# This next step combines all of the absentee summary dataframes into one
combined_data <- bind_rows(processed_data_list)

# Adding an election date column that is formatted as YYYY-MM
combined_data$election <- mdy(combined_data$election_dt) %>% format("%Y-%m")

# Changing the name of the county_desc.x column to "County"
colnames(combined_data)[colnames(combined_data) == "county_desc.x"] <- "County"

# Export the new csv. I got in the habit of adding todays date to the file name to keep track when I update them.
write.csv(combined_data, file = "sites_demographics_[TODAYS_DATE].csv")
