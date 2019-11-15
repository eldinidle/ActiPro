#' @import plyr
#' @import data.table
#' @import progress
#' @import parallel
#' @import foreach
#' @import doParallel
#' @import iterators
#' @import RSQLite
#' @import DBI
#' @import eeptools
#' @import reldist
#' @import readxl
#' @import lubridate


#    ActiPro, an R package to process data from ActiGraph output
#    Copyright (C) 2018 Eldin Dzubur, PhD
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


#below code is just telling R to label the columns based on what mode the accelerometer data is in.
actigraph_mode_columns <- function(mode_integer) {
  mode_switch <- switch(mode_integer,
                        c("Activity"),
                        c("Activity", "Steps"),
                        c("Activity", "HR"),
                        c("Activity", "Steps", "HR"),
                        c("Activity", "Axis 2"),
                        c("Activity", "Axis 2", "Steps"),
                        c("Activity", "Axis 2", "HR"),
                        c("Activity", "Axis 2", "Steps", "HR"),
                        NA,
                        NA,
                        NA,
                        NA,
                        # 8-11 not possible, can't suppress axis 2
                        c("Activity", "Axis 2","Axis 3"),
                        c("Activity", "Axis 2","Axis 3", "Steps"),
                        c("Activity", "Axis 2","Axis 3", "HR"),
                        c("Activity", "Axis 2","Axis 3", "Steps", "HR"),
                        #
                        c("Activity","Lux"),
                        c("Activity", "Steps","Lux"),
                        c("Activity", "HR","Lux"),
                        c("Activity", "Steps", "HR","Lux"),
                        c("Activity", "Axis 2","Lux"),
                        c("Activity", "Axis 2", "Steps","Lux"),
                        c("Activity", "Axis 2", "HR","Lux"),
                        c("Activity", "Axis 2", "Steps", "HR","Lux"),
                        NA,
                        NA,
                        NA,
                        NA,
                        # 24-27 not possible, can't suppress axis 2
                        c("Activity", "Axis 2","Axis 3","Lux"),
                        c("Activity", "Axis 2","Axis 3", "Steps","Lux"),
                        c("Activity", "Axis 2","Axis 3", "HR","Lux"),
                        c("Activity", "Axis 2","Axis 3", "Steps", "HR","Lux"),
                        #
                        c("Activity","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Steps","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "HR","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Steps", "HR","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2", "Steps","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2", "HR","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2", "Steps", "HR","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        NA,
                        NA,
                        NA,
                        NA,
                        # 40-43 not possible, can't suppress axis 2
                        c("Activity", "Axis 2","Axis 3","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Axis 3", "Steps","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Axis 3", "HR","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Axis 3", "Steps", "HR","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        #
                        c("Activity","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Steps","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2", "Steps","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        NA,
                        NA,
                        NA,
                        NA,
                        # 56-59 not possible, can't suppress axis 2
                        c("Activity", "Axis 2","Axis 3","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Axis 3", "Steps","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Axis 3", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying"),
                        c("Activity", "Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")
  )
  return(mode_switch)
}

#' Convert dataset from Reach Lab to Actipro
#' @name reach_adapter
#'
#' @param rds_filepath file path of rds created by Reach Lab script
#'
#' @return A data.table containing the Actipro compatible dataset
#'
#'
#' @export
reach_adapter <- function(rds_filepath){
  reach_data <- as.data.table(
    readRDS(rds_filepath)
  )
  reach_data[, id := paste(id,"w",wave,sep="")]
  reach_data[, wave := NULL]
  reach_data[, did := NULL]
  reach_data[, timestamp := NULL]
  #reach_data[, person := NULL]
  #reach_data[, startdatetime := NULL]
  #reach_data[, stopdatetime := NULL]
  reach_data[, wear := !flag_nonwear & !flag_cut & !flag_ext]
  adapter_names <- c("id","epoch","age","fulltime","Activity", "Axis 2","Axis 3",
                     "Steps","pa","Incline Off","Incline Standing",
                     "Incline Sitting", "Incline Lying","Lux","nonwear",
                     "cut","ext","wear")
  setnames(reach_data,adapter_names)
  reach_data[, nonwear := !wear]
  reach_data[, HR := NA]
  reach_data[, fulldate := as.Date(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")]
  valid_days <- reach_data[cut == 0 & ext == 0,list(time = sum(wear, na.rm = TRUE), epoch = mean(epoch, na.rm = TRUE)), by = list(id,fulldate)]
  all_days <- reach_data[,.(dummy = 1),by = .(id,fulldate)][,dummy := NULL]
  valid_days <- merge(all_days,valid_days, by = c("id","fulldate"), all = TRUE)
  valid_days[is.na(epoch),epoch := 0]
  valid_days[, valid_day := ifelse(epoch == 30,as.integer(time >= 1200),
                                   ifelse(epoch == 60,as.integer(time >= 600),
                                          ifelse(epoch == 10,as.integer(time >= 3600),
                                          0)))]
  valid_days[, valid_day_sum := sum(valid_day), by = id]
  valid_days[, epoch := NULL]
  valid_days[, time := NULL]
  acc <- merge(x = reach_data, y= valid_days, by = c("id","fulldate") , all.x = TRUE)
  acc[, light := as.integer(pa == "lig" & wear & !ext & !cut)]
  acc[, mod := as.integer(pa == "mod" & wear & !ext & !cut)]
  acc[, vig := as.integer(pa == "vig" & wear & !ext & !cut)]
  acc[, sed := as.integer(pa == "sed" & wear & !ext & !cut)]
  acc[, ext := as.integer(ext)]
  acc[, cut := as.integer(cut)]
  acc[, divider := 60L/epoch]
  return(acc)
}

#' Produce novel sedentary features
#' @name sedentary_features
#'
#' @param acc_ageadjusted the file created by ActiPro
#' @param cut parameter indicating an epoch for manual cut; in this case, for manually coded sleep from REACH data
#' @param ext parameter indicating an epoch that exceeds standard bounds
#' @param valid_only indicates that computation will occur on non-valid days only, otherwise non-valid days will be discarded
#'
#' @return A data.table containing a set of novel statistical parameters based on Keadle
#'
#'
#' @export
sedentary_features <- function(acc_ageadjusted, cut = TRUE, ext = TRUE, valid_only = TRUE) {
  # Standardizing to minute epochs for relevant variables (sed)
  if(!cut){
    acc_ageadjusted[,cut := 0]
  }
  if(!ext){
    acc_ageadjusted[,ext := 0]
  }
  epoch_acc <- acc_ageadjusted[valid_day == 1 | valid_day == as.integer(valid_only),
                               list(id,fulldate,sed,fulltime,wear,age,divider,ext,cut)]
  epoch_acc[, hour := as.POSIXlt(fulltime)$hour]
  epoch_acc[, minute := as.POSIXlt(fulltime)$min]
  epoch_acc[ext == 1 | cut == 1, sed := 0]

  minute_acc <- epoch_acc[,
                          list(min_sed =  sum(sed, na.rm = TRUE),
                               min_wear = sum(wear, na.rm = TRUE),
                               mean_age = mean(age, na.rm = TRUE),
                               min_divider = mean(divider, na.rm = TRUE)),
                          by = list(id,
                                    fulldate,
                                    hour,
                                    minute)]
  minute_acc[, min_wear := min_wear/min_divider]
  minute_acc[, min_sed := min_sed/min_divider]

  # Creating day level variables, including sed_total
  day_acc <- minute_acc[, list(sed_total = sum(min_sed, na.rm = TRUE),
                               day_wear = sum(min_wear, na.rm = TRUE)),
                        by = list(id,fulldate)]

  # Some variables may have meaningful non-missing values,
  # so we create a distribution of all valid days to merge in
  temp_days <- day_acc[,list(id,fulldate)]

  # Output sed_total
  sed_total <- day_acc[, list(id,fulldate,sed_total)]
  sed_percent <- day_acc[, list(id, fulldate, sed_percent = sed_total/day_wear)]

  # Create sedentary bout sequence
  minute_acc[, non_sed := min_sed == 0]
  minute_acc[, true_sed := min_sed > 0]
  minute_acc[, unique_sed_bout := bout_sequence(true_sed,non_sed,"60", return_index = TRUE), by = .(id,fulldate)]
  minute_acc[, sed_bout_length := bout_sequence(true_sed,non_sed,"60"), by = .(id,fulldate)]
  minute_acc[, sed_bout_length := sed_bout_length/60L]

  # Identify breaks
  minute_acc[, index := .I]
  wear_delta <- minute_acc$non_sed[-1L] != minute_acc$non_sed[-length(minute_acc$non_sed)]
  delta <- data.table("index" = c(which(wear_delta), length(minute_acc$non_sed)))
  delta[, change := TRUE]
  minute_bouts <- merge(minute_acc,delta, by = c("index"), all = TRUE)
  minute_bouts[, sed_to_up := ifelse(change == TRUE & non_sed == TRUE,1,0)]

  # Create sed_breaks
  sed_breaks <- minute_bouts[, list(sed_breaks = sum(sed_to_up, na.rm = TRUE)),
                             by = list(id,fulldate)]

  # Merge sed_breaks and sed_total for sed_breaks_ratio
  temp_merge <- merge(sed_total,sed_breaks,by = c("id","fulldate"),all = TRUE)
  sed_breaks_ratio <- temp_merge[, list(id,fulldate,
                                        sed_breaks_ratio = sed_breaks/sed_total)]

  # Create length of sedentary events
  day_bouts <- minute_bouts[non_sed == FALSE,
                            list(sed_event_length = mean(sed_bout_length, na.rm = TRUE)),
                            by = list(id,fulldate,unique_sed_bout)]
  sed_event_length <- day_bouts[, list(sed_event_length = mean(sed_event_length, na.rm = TRUE)),
                                by = list(id,fulldate)]
  # Some sedentary events may have a length of 0 if participant was not sedentary.
  sed_event_length <- merge(temp_days,sed_event_length,by = c("id","fulldate"), all = TRUE)
  sed_event_length[is.na(sed_event_length), sed_event_length := 0]

  # Lock bouts to length exceeding 20, 60, and 120 minutes
  # Get total sedentary time for each bouts
  sed_total_over_20 <- minute_bouts[sed_bout_length >= (20) & non_sed == FALSE,
                                    list(sed_total_over_20 = sum(min_sed, na.rm = TRUE)),
                                    by = list(id,fulldate)]
  sed_total_over_20 <- merge(temp_days,sed_total_over_20,by = c("id","fulldate"), all = TRUE)
  sed_total_over_20[is.na(sed_total_over_20), sed_total_over_20 := 0]

  sed_total_over_60 <- minute_bouts[sed_bout_length >= (60) & non_sed == FALSE,
                                    list(sed_total_over_60 = sum(min_sed, na.rm = TRUE)),
                                    by = list(id,fulldate)]
  sed_total_over_60 <- merge(temp_days,sed_total_over_60,by = c("id","fulldate"), all = TRUE)
  sed_total_over_60[is.na(sed_total_over_60), sed_total_over_60 := 0]

  sed_total_over_120 <- minute_bouts[sed_bout_length >= (120) & non_sed == FALSE,
                                     list(sed_total_over_120 = sum(min_sed, na.rm = TRUE)),
                                     by = list(id,fulldate)]
  sed_total_over_120 <- merge(temp_days,sed_total_over_120,by = c("id","fulldate"), all = TRUE)
  sed_total_over_120[is.na(sed_total_over_120), sed_total_over_120 := 0]

  # Create unique index variable for fast counting
  day_bouts[, uniques := 1]
  unique_sed_total_over_20 <- minute_bouts[sed_bout_length >= (20) & non_sed == FALSE,
                                           list(length = mean(sed_bout_length, na.rm = TRUE)),
                                           by = list(id,fulldate,unique_sed_bout)]
  unique_sed_total_over_20[, uniques := 1]
  unique_sed_total_over_60 <- minute_bouts[sed_bout_length >= (60) & non_sed == FALSE,
                                           list(length = mean(sed_bout_length, na.rm = TRUE)),
                                           by = list(id,fulldate,unique_sed_bout)]
  unique_sed_total_over_60[, uniques := 1]
  unique_sed_total_over_120 <- minute_bouts[sed_bout_length >= (120) & non_sed == FALSE,
                                            list(length = mean(sed_bout_length, na.rm = TRUE)),
                                            by = list(id,fulldate,unique_sed_bout)]
  unique_sed_total_over_120[, uniques := 1]


  # Merge all totals to generate ratios
  count_day_bouts <- day_bouts[, list(count_day_bouts = sum(uniques)),
                               by = list(id, fulldate)]
  count_20_bouts <- unique_sed_total_over_20[, list(count_20_bouts = sum(uniques)),
                                             by = list(id, fulldate)]
  count_60_bouts <- unique_sed_total_over_60[, list(count_60_bouts = sum(uniques)),
                                             by = list(id, fulldate)]
  count_120_bouts <- unique_sed_total_over_120[, list(count_120_bouts = sum(uniques)),
                                               by = list(id, fulldate)]
  count_list <- list(temp_days,count_day_bouts, count_20_bouts, count_60_bouts, count_120_bouts)
  temp_merge <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), count_list)
  temp_merge[is.na(count_day_bouts), count_day_bouts := 0]
  temp_merge[is.na(count_20_bouts), count_20_bouts := 0]
  temp_merge[is.na(count_60_bouts), count_60_bouts := 0]
  temp_merge[is.na(count_120_bouts), count_120_bouts := 0]

  # Creating ratios
  sed_ratio_over_20 <- temp_merge[,list(id,fulldate,sed_ratio_over_20 = count_20_bouts/count_day_bouts)]
  sed_ratio_over_60 <- temp_merge[,list(id,fulldate,sed_ratio_over_60 = count_60_bouts/count_day_bouts)]
  sed_ratio_over_120 <- temp_merge[,list(id,fulldate,sed_ratio_over_120 = count_120_bouts/count_day_bouts)]

  # Use quantile() on previous day_bouts datatable
  all_quantiles <- day_bouts[,list(quantile = quantile(sed_event_length,probs = c(0.05,0.25,0.50,0.75,0.95)),
                                   label = c(0.05,0.25,0.50,0.75,0.95)),
                             by = list(id, fulldate)]
  sed_5_percentile <- all_quantiles[label == 0.05, list(id,fulldate,sed_5_percentile = quantile)]
  sed_25_percentile <- all_quantiles[label == 0.25, list(id,fulldate,sed_25_percentile = quantile)]
  sed_50_percentile <- all_quantiles[label == 0.50, list(id,fulldate,sed_50_percentile = quantile)]
  sed_75_percentile <- all_quantiles[label == 0.75, list(id,fulldate,sed_75_percentile = quantile)]
  sed_95_percentile <- all_quantiles[label == 0.95, list(id,fulldate,sed_95_percentile = quantile)]

  # Setting up analyses for alpha
  day_bouts[, min_event := min(sed_event_length), by = list(id,fulldate)]
  day_bouts[, m_def := log(sed_event_length/min_event)]
  sed_alpha <- day_bouts[, list(sed_alpha = 1+(1/mean(m_def, na.rm = TRUE))),
                         by = list(id, fulldate)]

  # Using reldist and gini
  sed_gini <- day_bouts[, list(sed_gini = gini(sed_event_length)), by = list(id,fulldate)]

  # Generate datatable with only shorter sed bouts, then create sed_bout_extralong variables
  sed_bouts <- day_bouts[sed_event_length < 30 & sed_event_length > 0 ]
  sed_bout_short_min <- sed_bouts[, list(sed_bout_short_min = sum(sed_event_length, na.rm = TRUE)),
                                      by = list(id,fulldate)]
  sed_bout_short_min <- merge(temp_days,sed_bout_short_min,by = c("id","fulldate"), all = TRUE)
  sed_bout_short_min[is.na(sed_bout_short_min), sed_bout_short_min := 0]

  sed_bout_short_count <- sed_bouts[, list(sed_bout_short_count = sum(uniques, na.rm = TRUE)),
                                        by = list(id,fulldate)]
  sed_bout_short_count <- merge(temp_days,sed_bout_short_count,by = c("id","fulldate"), all = TRUE)
  sed_bout_short_count[is.na(sed_bout_short_count), sed_bout_short_count := 0]


  # Generate datatable with only extra long sed bouts, then create sed_bout_extralong variables
  sed_bouts <- day_bouts[sed_event_length >= 60]
  sed_bout_extralong_min <- sed_bouts[, list(sed_bout_extralong_min = sum(sed_event_length, na.rm = TRUE)),
                                 by = list(id,fulldate)]
  sed_bout_extralong_min <- merge(temp_days,sed_bout_extralong_min,by = c("id","fulldate"), all = TRUE)
  sed_bout_extralong_min[is.na(sed_bout_extralong_min), sed_bout_extralong_min := 0]

  sed_bout_extralong_count <- sed_bouts[, list(sed_bout_extralong_count = sum(uniques, na.rm = TRUE)),
                                   by = list(id,fulldate)]
  sed_bout_extralong_count <- merge(temp_days,sed_bout_extralong_count,by = c("id","fulldate"), all = TRUE)
  sed_bout_extralong_count[is.na(sed_bout_extralong_count), sed_bout_extralong_count := 0]


  # Generate datatable with only long sed bouts, then create sed_bout_long variables
  sed_bouts <- day_bouts[sed_event_length < 60 & sed_event_length >= 30]
  sed_bout_long_min <- sed_bouts[, list(sed_bout_long_min = sum(sed_event_length, na.rm = TRUE)),
                                 by = list(id,fulldate)]
  sed_bout_long_min <- merge(temp_days,sed_bout_long_min,by = c("id","fulldate"), all = TRUE)
  sed_bout_long_min[is.na(sed_bout_long_min), sed_bout_long_min := 0]

  sed_bout_long_count <- sed_bouts[, list(sed_bout_long_count = sum(uniques, na.rm = TRUE)),
                                   by = list(id,fulldate)]
  sed_bout_long_count <- merge(temp_days,sed_bout_long_count,by = c("id","fulldate"), all = TRUE)
  sed_bout_long_count[is.na(sed_bout_long_count), sed_bout_long_count := 0]


  sed_features <- list(sed_total,
                       sed_percent,
                       sed_breaks,
                       sed_breaks_ratio,
                       sed_event_length,
                       sed_total_over_20,
                       sed_total_over_60,
                       sed_total_over_120,
                       sed_ratio_over_20,
                       sed_ratio_over_60,
                       sed_ratio_over_120,
                       sed_5_percentile,
                       sed_25_percentile,
                       sed_50_percentile,
                       sed_75_percentile,
                       sed_95_percentile,
                       sed_alpha,
                       sed_gini,
                       sed_bout_short_min,
                       sed_bout_short_count,
                       sed_bout_long_min,
                       sed_bout_long_count,
                       sed_bout_extralong_min,
                       sed_bout_extralong_count)

  return_sed <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), sed_features)

  return(return_sed)
}

#' Produce novel ancillary activity features
#'
#' @name ancillary_features
#'
#' @param acc_ageadjusted the file created by ActiPro
#' @param cut parameter indicating an epoch for manual cut; in this case, for manually coded sleep from REACH data
#' @param ext parameter indicating an epoch that exceeds standard bounds
#' @param valid_only indicates that computation will occur on non-valid days only, otherwise non-valid days will be discarded
#'
#' @return A data.table containing a set of novel statistical parameters based on Keadle
#'
#'
#' @export
ancillary_features <- function(acc_ageadjusted, cut = TRUE, ext = TRUE, valid_only = TRUE) {
  # Standardizing to minute epochs for relevant variables (Activity,Steps)
  if(!cut){
    acc_ageadjusted[,cut := 0]
  }
  if(!ext){
    acc_ageadjusted[,ext := 0]
  }
  epoch_acc <- acc_ageadjusted[valid_day == 1 | valid_day == as.integer(valid_only),
                               list(id,fulldate,Activity,Steps,fulltime,wear,age,divider,ext,cut)]
  epoch_acc[, hour := as.POSIXlt(fulltime)$hour]
  epoch_acc[, minute := as.POSIXlt(fulltime)$min]
  epoch_acc[ext == 1 | cut == 1, Activity := NA]
  epoch_acc[ext == 1 | cut == 1, Steps := NA]

  minute_acc <- epoch_acc[,
                          list(min_activity = sum(Activity, na.rm = TRUE),
                               min_steps =  sum(Steps),
                               min_wear = sum(wear, na.rm = TRUE),
                               mean_age = mean(age, na.rm = TRUE),
                               min_divider = mean(divider, na.rm = TRUE)),
                          by = list(id,
                                    fulldate,
                                    hour,
                                    minute)]
  minute_acc[, min_wear := min_wear/min_divider]

  # Generating MET minutes
  # Freedson MET equations on raw activity counts at min-epochs
  minute_acc[mean_age > 17, met_mins := 1.439008 + (0.000795 * min_activity)]
  minute_acc[mean_age < 18, met_mins := 2.757 + (0.0015 * min_activity) - (0.08957 * mean_age) - (0.000038 * min_activity * mean_age)]
  # Screen unrealistic MET values
  minute_acc[met_mins > 20, met_mins := NA]
  # Create anc_met_hrs at day-level
  anc_met_hrs <- minute_acc[, list(anc_met_hrs = sum(met_mins, na.rm = TRUE)/60), by=list(id,fulldate)]

  # Create binary stepping variable (step-mins)
  minute_acc[, min_bin_steps := ifelse(min_steps > 0,1,0)]

  # Create steps and wear time variable
  day_acc <- minute_acc[, list(anc_step_daily = sum(min_steps),
                               day_step_time = sum(min_bin_steps),
                               day_wear = sum(min_wear, na.rm = TRUE)), by = list(id,fulldate)]
  anc_step_daily <- day_acc[, list(id,fulldate,anc_step_daily)]
  # Compute anc_step_percent
  anc_step_percent <- day_acc[, list(id,fulldate,anc_step_percent = day_step_time/day_wear)]

  # Extract daily wear
  anc_wear <- day_acc[, list(id, fulldate,anc_wear = day_wear)]

  # Merge anc* variables
  anc_features <- list(anc_met_hrs,
                       anc_step_daily,
                       anc_step_percent,
                       anc_wear)

  return_anc <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), anc_features)

  return(return_anc)
}

#' Produce novel light activity features
#'
#' @name light_features
#'
#' @param acc_ageadjusted the file created by ActiPro
#' @param cut parameter indicating an epoch for manual cut; in this case, for manually coded sleep from REACH data
#' @param ext parameter indicating an epoch that exceeds standard bounds
#' @param valid_only indicates that computation will occur on non-valid days only, otherwise non-valid days will be discarded
#'
#' @return A data.table containing a set of novel statistical parameters based on Keadle
#'
#'
#' @export
light_features <- function(acc_ageadjusted, cut = TRUE, ext = TRUE, valid_only = TRUE) {
  # Standardizing to minute epochs for relevant variables (sed)
  if(!cut){
    acc_ageadjusted[,cut := 0]
  }
  if(!ext){
    acc_ageadjusted[,ext := 0]
  }
  epoch_acc <- acc_ageadjusted[valid_day == 1 | valid_day == as.integer(valid_only),
                               list(id,fulldate,light,fulltime,wear,age,divider,ext,cut)]
  epoch_acc[, hour := as.POSIXlt(fulltime)$hour]
  epoch_acc[, minute := as.POSIXlt(fulltime)$min]
  epoch_acc[ext == 1 | cut == 1, light := 0]

  minute_acc <- epoch_acc[,
                          list(min_light =  sum(light, na.rm = TRUE),
                               min_wear = sum(wear, na.rm = TRUE),
                               mean_age = mean(age, na.rm = TRUE),
                               min_divider = mean(divider, na.rm = TRUE)),
                          by = list(id,
                                    fulldate,
                                    hour,
                                    minute)]
  minute_acc[, min_wear := min_wear/min_divider]
  minute_acc[, min_light := min_light/min_divider]

  # Creating day level variables, including light_total
  day_acc <- minute_acc[, list(light_total = sum(min_light, na.rm = TRUE),
                               day_wear = sum(min_wear, na.rm = TRUE)),
                        by = list(id,fulldate)]

  # Some variables may have meaningful non-missing values,
  # so we create a distribution of all valid days to merge in
  temp_days <- day_acc[,list(id,fulldate)]

  # Output light_total
  light_total <- day_acc[, list(id,fulldate,light_total)]
  #####sed_percent <- day_acc[, list(id, fulldate, sed_percent = sed_total/day_wear)]

  # Create light bout sequence
  minute_acc[, non_light := min_light == 0]
  minute_acc[, true_light := min_light > 0]
  minute_acc[, unique_light_bout := bout_sequence(true_light,non_light,"60", return_index = TRUE), by = .(id,fulldate)]
  minute_acc[, light_bout_length := bout_sequence(true_light,non_light,"60"), by = .(id,fulldate)]
  minute_acc[, light_bout_length := light_bout_length/60L]

  #### Identify breaks
  minute_acc[, index := .I]
  wear_delta <- minute_acc$non_sed[-1L] != minute_acc$non_sed[-length(minute_acc$non_sed)]
  delta <- data.table("index" = c(which(wear_delta), length(minute_acc$non_sed)))
  delta[, change := TRUE]
  minute_bouts <- merge(minute_acc,delta, by = c("index"), all = TRUE)
  minute_bouts[, sed_to_up := ifelse(change == TRUE & non_light == TRUE,1,0)]

  # Create length of light events
  day_bouts <- minute_bouts[non_light == FALSE,
                            list(light_event_length = mean(light_bout_length, na.rm = TRUE)),
                            by = list(id,fulldate,unique_light_bout)]
  light_event_length <- day_bouts[, list(light_event_length = mean(light_event_length, na.rm = TRUE)),
                                  by = list(id,fulldate)]
  # Some light events may have a length of 0 if participant was not sedentary.
  light_event_length <- merge(temp_days,light_event_length,by = c("id","fulldate"), all = TRUE)
  light_event_length[is.na(light_event_length), light_event_length := 0]

  # Lock bouts to length exceeding 5, 10, and 30 minutes
  # Get total light time for each bouts
  light_total_over_5 <- minute_bouts[light_bout_length >= (5) & non_light == FALSE,
                                     list(light_total_over_5 = sum(min_light, na.rm = TRUE)),
                                     by = list(id,fulldate)]
  light_total_over_5 <- merge(temp_days,light_total_over_5,by = c("id","fulldate"), all = TRUE)
  light_total_over_5[is.na(light_total_over_5), light_total_over_5 := 0]

  light_total_over_10 <- minute_bouts[light_bout_length >= (10) & non_light == FALSE,
                                      list(light_total_over_10 = sum(min_light, na.rm = TRUE)),
                                      by = list(id,fulldate)]
  light_total_over_10 <- merge(temp_days,light_total_over_10,by = c("id","fulldate"), all = TRUE)
  light_total_over_10[is.na(light_total_over_10), light_total_over_10 := 0]

  light_total_over_30 <- minute_bouts[light_bout_length >= (30) & non_light == FALSE,
                                      list(light_total_over_30 = sum(min_light, na.rm = TRUE)),
                                      by = list(id,fulldate)]
  light_total_over_30 <- merge(temp_days,light_total_over_30,by = c("id","fulldate"), all = TRUE)
  light_total_over_30[is.na(light_total_over_30), light_total_over_30 := 0]

  # Create unique index variable for fast counting
  day_bouts[, uniques := 1]
  unique_light_total_over_5 <- minute_bouts[light_bout_length >= (5) & non_light == FALSE,
                                            list(length = mean(light_bout_length, na.rm = TRUE)),
                                            by = list(id,fulldate,unique_light_bout)]
  unique_light_total_over_5[, uniques := 1]
  unique_light_total_over_10 <- minute_bouts[light_bout_length >= (10) & non_light == FALSE,
                                             list(length = mean(light_bout_length, na.rm = TRUE)),
                                             by = list(id,fulldate,unique_light_bout)]
  unique_light_total_over_10[, uniques := 1]
  unique_light_total_over_30 <- minute_bouts[light_bout_length >= (30) & non_light == FALSE,
                                             list(length = mean(light_bout_length, na.rm = TRUE)),
                                             by = list(id,fulldate,unique_light_bout)]
  unique_light_total_over_30[, uniques := 1]


  # Merge all totals to generate ratios
  # Also, creates output variables
  count_day_bouts <- day_bouts[, list(count_day_bouts = sum(uniques)),
                               by = list(id, fulldate)]
  light_events <- count_day_bouts[, list(id, fulldate, light_events = count_day_bouts)]
  light_events <- merge(temp_days,light_events,by = c("id","fulldate"), all = TRUE)
  light_events[is.na(light_events), light_events := 0]


  count_5_bouts <- unique_light_total_over_5[, list(count_5_bouts = sum(uniques)),
                                             by = list(id, fulldate)]
  count_10_bouts <- unique_light_total_over_10[, list(count_10_bouts = sum(uniques)),
                                               by = list(id, fulldate)]
  count_30_bouts <- unique_light_total_over_30[, list(count_30_bouts = sum(uniques)),
                                               by = list(id, fulldate)]
  count_list <- list(temp_days,count_day_bouts, count_5_bouts, count_10_bouts, count_30_bouts)
  temp_merge <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), count_list)
  temp_merge[is.na(count_day_bouts), count_day_bouts := 0]
  temp_merge[is.na(count_5_bouts), count_5_bouts := 0]
  temp_merge[is.na(count_10_bouts), count_10_bouts := 0]
  temp_merge[is.na(count_30_bouts), count_30_bouts := 0]

  # Creating bout counts
  light_boutcount_over_5 <- temp_merge[,list(id,fulldate,light_boutcount_over_5 = count_5_bouts/1)]
  light_boutcount_over_10 <- temp_merge[,list(id,fulldate,light_boutcount_over_10 = count_10_bouts/1)]
  light_boutcount_over_30 <- temp_merge[,list(id,fulldate,light_boutcount_over_30 = count_30_bouts/1)]

  # Creating ratios
  light_ratio_over_5 <- temp_merge[,list(id,fulldate,light_ratio_over_5 = count_5_bouts/count_day_bouts)]
  light_ratio_over_10 <- temp_merge[,list(id,fulldate,light_ratio_over_10 = count_10_bouts/count_day_bouts)]
  light_ratio_over_30 <- temp_merge[,list(id,fulldate,light_ratio_over_30 = count_30_bouts/count_day_bouts)]

  # Use quantile() on previous day_bouts datatable
  all_quantiles <- day_bouts[,list(quantile = quantile(light_event_length,probs = c(0.05,0.25,0.50,0.75,0.95)),
                                   label = c(0.05,0.25,0.50,0.75,0.95)),
                             by = list(id, fulldate)]
  light_5_percentile <- all_quantiles[label == 0.05, list(id,fulldate,light_5_percentile = quantile)]
  light_25_percentile <- all_quantiles[label == 0.25, list(id,fulldate,light_25_percentile = quantile)]
  light_50_percentile <- all_quantiles[label == 0.50, list(id,fulldate,light_50_percentile = quantile)]
  light_75_percentile <- all_quantiles[label == 0.75, list(id,fulldate,light_75_percentile = quantile)]
  light_95_percentile <- all_quantiles[label == 0.95, list(id,fulldate,light_95_percentile = quantile)]

  # Setting up analyses for alpha
  day_bouts[, min_event := min(light_event_length), by = list(id,fulldate)]
  day_bouts[, m_def := log(light_event_length/min_event)]
  light_alpha <- day_bouts[, list(light_alpha = 1+(1/mean(m_def, na.rm = TRUE))),
                           by = list(id, fulldate)]

  # Using reldist and gini
  light_gini <- day_bouts[, list(light_gini = gini(light_event_length)), by = list(id,fulldate)]


  sed_features <- list(light_total,
                       light_events,
                       light_event_length,
                       light_boutcount_over_5,
                       light_boutcount_over_10,
                       light_boutcount_over_30,
                       light_ratio_over_5,
                       light_ratio_over_10,
                       light_ratio_over_30,
                       light_total_over_5,
                       light_total_over_10,
                       light_total_over_30,
                       light_5_percentile,
                       light_25_percentile,
                       light_50_percentile,
                       light_75_percentile,
                       light_95_percentile,
                       light_alpha,
                       light_gini)

  return_light <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), sed_features)

  return(return_light)
}

#' Produce novel MVPA features
#'
#' @name mvpa_features
#' @param cut parameter indicating an epoch for manual cut; in this case, for manually coded sleep from REACH data
#' @param ext parameter indicating an epoch that exceeds standard bounds
#' @param valid_only indicates that computation will occur on non-valid days only, otherwise non-valid days will be discarded
#' @param acc_ageadjusted the file created by ActiPro
#'
#' @return A data.table containing a set of novel statistical parameters based on Keadle
#'
#'
#'
#' @export
mvpa_features <- function(acc_ageadjusted, cut = TRUE, ext = TRUE, valid_only = TRUE) {
  # Standardizing to minute epochs for relevant variables (mvpa and Activity)
  if(!cut){
    acc_ageadjusted[,cut := 0]
  }
  if(!ext){
    acc_ageadjusted[,ext := 0]
  }
  epoch_acc <- acc_ageadjusted[valid_day == 1 | valid_day == as.integer(valid_only),
                               list(id,fulldate,Activity,
                                    mvpa = as.integer(mod == 1 | vig == 1),
                                    fulltime,wear,age,divider,ext,cut)]
  epoch_acc[, hour := as.POSIXlt(fulltime)$hour]
  epoch_acc[, minute := as.POSIXlt(fulltime)$min]
  epoch_acc[ext == 1 | cut == 1, mvpa := 0]

  minute_acc <- epoch_acc[,
                          list(min_mvpa =  sum(mvpa, na.rm = TRUE),
                               min_wear = sum(wear, na.rm = TRUE),
                               mean_age = mean(age, na.rm = TRUE),
                               min_activity = sum(Activity, na.rm = TRUE),
                               min_divider = mean(divider, na.rm = TRUE)),
                          by = list(id,
                                    fulldate,
                                    hour,
                                    minute)]
  minute_acc[, min_wear := min_wear/min_divider]
  minute_acc[, min_mvpa := min_mvpa/min_divider]

  # Generating MET minutes
  # Freedson MET equations on raw activity counts at min-epochs
  minute_acc[mean_age > 17, met_mins := 1.439008 + (0.000795 * min_activity)]
  minute_acc[mean_age < 18, met_mins := 2.757 + (0.0015 * min_activity) - (0.08957 * mean_age) - (0.000038 * min_activity * mean_age)]
  # Screen unrealistic MET values
  minute_acc[met_mins > 20, met_mins := NA]
  # Create anc_met_hrs at day-level
  ####anc_met_hrs <- minute_acc[, list(anc_met_hrs = sum(met_mins, na.rm = TRUE)/60), by=list(id,fulldate)]


  # Creating day level variables, including mvpa_total and met max
  day_acc <- minute_acc[, list(mvpa_total = sum(min_mvpa, na.rm = TRUE),
                               day_wear = sum(min_wear, na.rm = TRUE)),
                        by = list(id,fulldate)]

  # Some variables may have meaningful non-missing values,
  # so we create a distribution of all valid days to merge in
  temp_days <- day_acc[,list(id,fulldate)]

  # Output mvpa_total
  mvpa_total <- day_acc[, list(id,fulldate,mvpa_total)]

  # Output mvpa_met_max
  mvpa_met_max <- minute_acc[min_mvpa > 0,
                             list(mvpa_met_max = max(met_mins, na.rm = TRUE)),
                             by = list(id,fulldate)]
  ####mvpa_percent <- day_acc[, list(id, fulldate, mvpa_percent = mvpa_total/day_wear)]

  # Create MVPA bout sequence
  minute_acc[, non_mvpa := min_mvpa == 0]
  minute_acc[, true_mvpa := min_mvpa > 0]
  minute_acc[, unique_mvpa_bout := bout_sequence(true_mvpa,non_mvpa,"60", return_index = TRUE), by = .(id,fulldate)]
  minute_acc[, mvpa_bout_length := bout_sequence(true_mvpa,non_mvpa,"60"), by = .(id,fulldate)]
  minute_acc[, mvpa_bout_length := mvpa_bout_length/60L]

  # Integrating new 2-minute threshold variables
  minute_acc[, mvpa_new := ifelse((mvpa_bout_length <= 2
                                   & non_mvpa == 1),1
                                  ,true_mvpa)]
  minute_acc[, mvpa_new_break := as.integer(!mvpa_new)]
  minute_acc[, mvpa_new_length := bout_sequence(mvpa_new, mvpa_new_break, "60"), by = .(id,fulldate)]
  minute_acc[, mvpa_new_length := mvpa_new_length/60L]
  minute_acc[, mvpa_new_index := bout_sequence(mvpa_new, mvpa_new_break, "60", return_index = TRUE), by = .(id,fulldate)]
  minute_acc[, mvpa_guideline_bout := as.integer(mvpa_new_length >= 10 & mvpa_new == 1)]
  minute_acc[, mvpa_sporadic_bout := as.integer(mvpa_bout_length < 10 & mvpa_guideline_bout == 0 & true_mvpa == 1)]


  # Identify breaks
  minute_acc[, index := .I]
  wear_delta <- minute_acc$non_mvpa[-1L] != minute_acc$non_mvpa[-length(minute_acc$non_mvpa)]
  delta <- data.table("index" = c(which(wear_delta), length(minute_acc$non_mvpa)))
  delta[, change := TRUE]
  minute_bouts <- merge(minute_acc,delta, by = c("index"), all = TRUE)
  minute_bouts[, mvpa_to_up := ifelse(change == TRUE & non_mvpa == TRUE,1,0)]

  # Create mvpa_breaks
  ####mvpa_breaks <- minute_bouts[, list(mvpa_breaks = sum(mvpa_to_up, na.rm = TRUE)),
  ####                            by = list(id,fulldate)]

  # Merge mvpa_breaks and mvpa_total for mvpa_breaks_ratio
  ####temp_merge <- merge(mvpa_total,mvpa_breaks,by = c("id","fulldate"),all = TRUE)
  ####mvpa_breaks_ratio <- temp_merge[, list(id,fulldate,
  ####                                       mvpa_breaks_ratio = mvpa_breaks/mvpa_total)]


  # Create length of MVPA events - general,sporadic and guideline
  day_bouts <- minute_bouts[non_mvpa == FALSE,
                            list(mvpa_event_length = mean(mvpa_bout_length, na.rm = TRUE)),
                            by = list(id,fulldate,unique_mvpa_bout)]
  sporadic_day_bouts <- minute_bouts[mvpa_sporadic_bout == 1,
                                     list(mvpa_event_length = mean(mvpa_bout_length, na.rm = TRUE),
                                          met_mins = sum(met_mins, na.rm = TRUE)),
                                     by = list(id,fulldate,unique_mvpa_bout)]
  guideline_day_bouts <- minute_bouts[mvpa_guideline_bout == 1,
                                      list(mvpa_event_length = mean(mvpa_new_length, na.rm = TRUE),
                                           met_mins = sum(met_mins, na.rm = TRUE)),
                                      by = list(id,fulldate,mvpa_new_index)]

  mvpa_guideline_length <- guideline_day_bouts[, list(mvpa_guideline_length = mean(mvpa_event_length, na.rm = TRUE)),
                                               by = list(id,fulldate)]
  # Some MVPA events may have a length of 0 if participant was not in MVPA.
  mvpa_guideline_length <- merge(temp_days,mvpa_guideline_length,by = c("id","fulldate"), all = TRUE)
  mvpa_guideline_length[is.na(mvpa_guideline_length), mvpa_guideline_length := 0]


  # Creating unique sporadic events, total time, and met_hrs
  sporadic_day_bouts[, uniques := 1]
  mvpa_sporadic_events <- sporadic_day_bouts[,list(mvpa_sporadic_events = sum(uniques)),
                                             by = list(id, fulldate)]
  mvpa_sporadic_events <- merge(temp_days,mvpa_sporadic_events,by = c("id","fulldate"), all = TRUE)
  mvpa_sporadic_events[is.na(mvpa_sporadic_events), mvpa_sporadic_events := 0]
  mvpa_sporadic_min <- sporadic_day_bouts[,list(mvpa_sporadic_min = sum(mvpa_event_length)),
                                          by = list(id, fulldate)]
  mvpa_sporadic_min <- merge(temp_days,mvpa_sporadic_min,by = c("id","fulldate"), all = TRUE)
  mvpa_sporadic_min[is.na(mvpa_sporadic_min), mvpa_sporadic_min := 0]
  mvpa_sporadic_met_hrs <- sporadic_day_bouts[,list(mvpa_sporadic_met_hrs = sum(met_mins)/60L),
                                              by = list(id, fulldate)]
  mvpa_sporadic_met_hrs <- merge(temp_days,mvpa_sporadic_met_hrs,by = c("id","fulldate"), all = TRUE)
  mvpa_sporadic_met_hrs[is.na(mvpa_sporadic_met_hrs), mvpa_sporadic_met_hrs := 0]

  guideline_day_bouts[, uniques := 1]
  mvpa_guideline_bouts <- guideline_day_bouts[,list(mvpa_guideline_bouts = sum(uniques)),
                                              by = list(id, fulldate)]
  mvpa_guideline_bouts <- merge(temp_days,mvpa_guideline_bouts,by = c("id","fulldate"), all = TRUE)
  mvpa_guideline_bouts[is.na(mvpa_guideline_bouts), mvpa_guideline_bouts := 0]
  mvpa_guideline_min <- guideline_day_bouts[,list(mvpa_guideline_min = sum(mvpa_event_length)),
                                            by = list(id, fulldate)]
  mvpa_guideline_min <- merge(temp_days,mvpa_guideline_min,by = c("id","fulldate"), all = TRUE)
  mvpa_guideline_min[is.na(mvpa_guideline_min), mvpa_guideline_min := 0]
  mvpa_guideline_met_hrs <- guideline_day_bouts[,list(mvpa_guideline_met_hrs = sum(met_mins)/60L),
                                                by = list(id, fulldate)]
  mvpa_guideline_met_hrs <- merge(temp_days,mvpa_guideline_met_hrs,by = c("id","fulldate"), all = TRUE)
  mvpa_guideline_met_hrs[is.na(mvpa_guideline_met_hrs), mvpa_guideline_met_hrs := 0]

  # Lock bouts to length exceeding 2, 5, 10 for general
  # Lock bouts to length exceeding 10 and 20 for guideline
  # Get total MVPA time for each bout
  ####mvpa_total_over_2 <- minute_bouts[mvpa_bout_length >= (2) & non_mvpa == FALSE,
  ####                                   list(mvpa_total_over_2 = sum(min_mvpa, na.rm = TRUE)),
  ####                                   by = list(id,fulldate)]
  ####mvpa_total_over_2 <- merge(temp_days,mvpa_total_over_20,by = c("id","fulldate"), all = TRUE)
  ####mvpa_total_over_2[is.na(mvpa_total_over_20), mvpa_total_over_20 := 0]

  ####mvpa_total_over_5 <- minute_bouts[mvpa_bout_length >= (5) & non_mvpa == FALSE,
  ####                                   list(mvpa_total_over_5 = sum(min_mvpa, na.rm = TRUE)),
  ####                                   by = list(id,fulldate)]
  ####mvpa_total_over_5 <- merge(temp_days,mvpa_total_over_60,by = c("id","fulldate"), all = TRUE)
  ####mvpa_total_over_5[is.na(mvpa_total_over_60), mvpa_total_over_60 := 0]

  ####mvpa_total_over_10 <- minute_bouts[mvpa_bout_length >= (10) & non_mvpa == FALSE,
  ####                                    list(mvpa_total_over_10 = sum(min_mvpa, na.rm = TRUE)),
  ####                                    by = list(id,fulldate)]
  ####mvpa_total_over_10 <- merge(temp_days,mvpa_total_over_120,by = c("id","fulldate"), all = TRUE)
  ####mvpa_total_over_10[is.na(mvpa_total_over_120), mvpa_total_over_120 := 0]

  # Create unique index variable for fast counting
  day_bouts[, uniques := 1]
  unique_mvpa_total_over_2 <- minute_bouts[mvpa_bout_length >= (2) & non_mvpa == FALSE,
                                           list(length = mean(mvpa_bout_length, na.rm = TRUE)),
                                           by = list(id,fulldate,unique_mvpa_bout)]
  unique_mvpa_total_over_2[, uniques := 1]
  unique_mvpa_total_over_5 <- minute_bouts[mvpa_bout_length >= (5) & non_mvpa == FALSE,
                                           list(length = mean(mvpa_bout_length, na.rm = TRUE)),
                                           by = list(id,fulldate,unique_mvpa_bout)]
  unique_mvpa_total_over_5[, uniques := 1]
  unique_mvpa_total_over_10 <- minute_bouts[mvpa_bout_length >= (10) & non_mvpa == FALSE,
                                            list(length = mean(mvpa_bout_length, na.rm = TRUE)),
                                            by = list(id,fulldate,unique_mvpa_bout)]
  unique_mvpa_total_over_10[, uniques := 1]
  unique_mvpa_total_over_20 <- minute_bouts[mvpa_bout_length >= (20) & non_mvpa == FALSE,
                                            list(length = mean(mvpa_bout_length, na.rm = TRUE)),
                                            by = list(id,fulldate,unique_mvpa_bout)]
  unique_mvpa_total_over_20[, uniques := 1]
  unique_guide_total_over_10 <- minute_bouts[mvpa_new_length >= (10) & mvpa_guideline_bout == TRUE,
                                             list(length = mean(mvpa_new_length, na.rm = TRUE)),
                                             by = list(id,fulldate,mvpa_new_index)]
  unique_guide_total_over_10[, uniques := 1]
  unique_guide_total_over_20 <- minute_bouts[mvpa_new_length >= (20) & mvpa_guideline_bout == TRUE,
                                             list(length = mean(mvpa_new_length, na.rm = TRUE)),
                                             by = list(id,fulldate,mvpa_new_index)]
  unique_guide_total_over_20[, uniques := 1]


  # Merge all totals to generate ratios
  count_day_bouts <- day_bouts[, list(count_day_bouts = sum(uniques)),
                               by = list(id, fulldate)]
  count_2_bouts <- unique_mvpa_total_over_2[, list(count_2_bouts = sum(uniques)),
                                            by = list(id, fulldate)]
  count_5_bouts <- unique_mvpa_total_over_5[, list(count_5_bouts = sum(uniques)),
                                            by = list(id, fulldate)]
  count_10_bouts <- unique_mvpa_total_over_10[, list(count_10_bouts = sum(uniques)),
                                              by = list(id, fulldate)]
  count_20_bouts <- unique_mvpa_total_over_20[, list(count_20_bouts = sum(uniques)),
                                              by = list(id, fulldate)]
  count_10_long_bouts <- unique_guide_total_over_10[, list(count_10_long_bouts = sum(uniques)),
                                                    by = list(id, fulldate)]
  count_20_long_bouts <- unique_guide_total_over_20[, list(count_20_long_bouts = sum(uniques)),
                                                    by = list(id, fulldate)]
  count_list <- list(temp_days,count_day_bouts, count_2_bouts, count_5_bouts, count_10_bouts, count_20_bouts,
                     count_10_long_bouts,count_20_long_bouts)
  temp_merge <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), count_list)
  temp_merge[is.na(count_day_bouts), count_day_bouts := 0]
  temp_merge[is.na(count_2_bouts), count_2_bouts := 0]
  temp_merge[is.na(count_5_bouts), count_5_bouts := 0]
  temp_merge[is.na(count_10_bouts), count_10_bouts := 0]
  temp_merge[is.na(count_20_bouts), count_20_bouts := 0]
  temp_merge[is.na(count_10_long_bouts), count_10_long_bouts := 0]
  temp_merge[is.na(count_20_long_bouts), count_20_long_bouts := 0]

  # Creating ratios
  mvpa_ratio_over_2 <- temp_merge[,list(id,fulldate,mvpa_ratio_over_2 = count_2_bouts/count_day_bouts)]
  mvpa_ratio_over_5 <- temp_merge[,list(id,fulldate,mvpa_ratio_over_5 = count_5_bouts/count_day_bouts)]
  mvpa_ratio_over_10 <- temp_merge[,list(id,fulldate,mvpa_ratio_over_10 = count_10_bouts/count_day_bouts)]
  mvpa_ratio_over_20 <- temp_merge[,list(id,fulldate,mvpa_ratio_over_20 = count_20_bouts/count_day_bouts)]
  mvpa_ratio_long_10 <- temp_merge[,list(id,fulldate,mvpa_ratio_long_10 = count_10_long_bouts/count_day_bouts)]
  mvpa_ratio_long_20 <- temp_merge[,list(id,fulldate,mvpa_ratio_long_20 = count_20_long_bouts/count_day_bouts)]

  # Creating bout counts
  mvpa_boutcount_over_2 <- temp_merge[,list(id,fulldate,mvpa_boutcount_over_2 = count_2_bouts/1)]
  mvpa_boutcount_over_5 <- temp_merge[,list(id,fulldate,mvpa_boutcount_over_5 = count_5_bouts/1)]
  mvpa_boutcount_over_10 <- temp_merge[,list(id,fulldate,mvpa_boutcount_over_10 = count_10_bouts/1)]
  mvpa_boutcount_over_20 <- temp_merge[,list(id,fulldate,mvpa_boutcount_over_20 = count_20_bouts/1)]
  mvpa_boutcount_long_10 <- temp_merge[,list(id,fulldate,mvpa_boutcount_long_10 = count_10_long_bouts/1)]
  mvpa_boutcount_long_20 <- temp_merge[,list(id,fulldate,mvpa_boutcount_long_20 = count_20_long_bouts/1)]

  #Gini and alpha for MVPA
  mvpa_gini <- day_bouts[, list(mvpa_gini = gini(mvpa_event_length)), by = list(id,fulldate)]
  day_bouts[, min_event := min(mvpa_event_length), by = list(id,fulldate)]
  day_bouts[, m_def := log(mvpa_event_length/min_event)]
  mvpa_alpha <- day_bouts[, list(mvpa_alpha = 1+(1/mean(m_def, na.rm = TRUE))),
                           by = list(id, fulldate)]



  mvpa_features <- list(mvpa_total,
                        mvpa_guideline_min,
                        mvpa_sporadic_min,
                        mvpa_sporadic_events,
                        mvpa_sporadic_met_hrs,
                        mvpa_ratio_over_2,
                        mvpa_ratio_over_5,
                        mvpa_ratio_over_10,
                        mvpa_ratio_over_20,
                        mvpa_ratio_long_10,
                        mvpa_ratio_long_20,
                        mvpa_boutcount_over_2,
                        mvpa_boutcount_over_5,
                        mvpa_boutcount_over_10,
                        mvpa_boutcount_over_20,
                        mvpa_boutcount_long_10,
                        mvpa_boutcount_long_20,
                        mvpa_met_max,
                        mvpa_guideline_met_hrs,
                        mvpa_guideline_bouts,
                        mvpa_guideline_length,
                        mvpa_alpha,
                        mvpa_gini)

  return_mvpa <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), mvpa_features)

  return(return_mvpa)

}

bout_sequence <- function(acc_stream, break_stream, epoch, return_index = FALSE, return_position = FALSE){ #, min_bout_length
  acc_raw <- data.table("acc_stream" = as.integer(acc_stream), "break_stream" = as.integer(break_stream))
  epoch <- as.integer(epoch)
  bins <- nrow(acc_raw)

  wear_delta <- acc_raw$break_stream[-1L] != acc_raw$break_stream[-length(acc_raw$break_stream)]
  delta <- data.table("index" = c(which(wear_delta), length(acc_raw$break_stream)))

  delta[, lag := shift(index,1L,NA_integer_,type = "lag")]
  delta[, lead := shift(index,1L,NA_integer_,type = "lead")]
  delta[, reps := index - lag]
  delta[, change := lead - index]
  delta[, length := epoch*change]
  delta[, reference := index + 1L]
  delta[.N,change:= delta[.N,index] - bins]
  delta[.N,lead := nrow(acc_raw)]
  delta[1,reps := delta[1,index]]

  delta[, c("reflag","relength","rechange") := shift(.SD,1L,NA_integer_,type = "lag"),.SDcols = c("reference", "length", "change")]
  delta[, index_ref := .I]

  delta[1,reflag := delta[2,reflag] - delta[1,index]]
  delta[1,relength := delta[1,reps] * epoch]
  delta[1,rechange := delta[1,reps]]

  acc_raw[, acc_var_length := rep(delta[,relength],delta[,rechange])]
  acc_raw[, index := rep(delta[,index_ref],delta[,rechange])]
  acc_raw[, position := rep(delta[,reflag],delta[,rechange])]

  if(return_index){
    return(acc_raw[,index])
  } else if (return_position){
    return(acc_raw[,position])
  } else {
    return(acc_raw[,acc_var_length])
  }
}

## Function
# Input: ActiGraph File
# Output: ActiGraph MetaData DataFrame
actigraph_metadata <- function(file_location) {
  meta <- fread(file_location, nrows= 9, stringsAsFactors = FALSE, select = "V1", header = FALSE, fill = TRUE, sep = ",")
  meta_t <- t(meta)

  meta1 <- unlist(strsplit(meta_t[1], " "))
  meta2 <- unlist(strsplit(meta_t[2], " "))
  meta3 <- unlist(strsplit(meta_t[3], " "))
  meta4 <- unlist(strsplit(meta_t[4], " "))
  meta5 <- unlist(strsplit(meta_t[5], " "))
  meta6 <- unlist(strsplit(meta_t[6], " "))
  meta7 <- unlist(strsplit(meta_t[7], " "))
  meta8 <- unlist(strsplit(meta_t[8], " "))
  meta9 <- unlist(strsplit(meta_t[9], " "))


  meta_actigraph <-    ifelse(length(which(meta1 == "ActiGraph", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "ActiGraph", arr.ind = TRUE)+1],"NA")
  meta_actilife <-     ifelse(length(which(meta1 == "ActiLife", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "ActiLife", arr.ind = TRUE)+1],"NA")
  meta_firmware <-     ifelse(length(which(meta1 == "Firmware", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "Firmware", arr.ind = TRUE)+1],"NA")
  meta_format <-       ifelse(length(which(meta1 == "format", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "format", arr.ind = TRUE)+1],"NA")
  meta_filter <-       ifelse(length(which(meta1 == "Filter", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "Filter", arr.ind = TRUE)+1],"NA")

  meta_serial <-       meta2[length(meta2)]
  meta_start_time <-   meta3[length(meta3)]
  meta_start_date <-   meta4[length(meta4)]
  meta_epoch <-        meta5[length(meta5)]
  meta_dl_time <-      meta6[length(meta6)]
  meta_dl_date <-      meta7[length(meta7)]
  meta_mem_address <-  meta8[length(meta8)]

  meta_voltage <-      ifelse(length(which(meta9 == "Voltage:", arr.ind = TRUE)) > 0,
                              meta9[which(meta9 == "Voltage:", arr.ind = TRUE)+1],"NA")
  meta_mode <-         ifelse(length(which(meta9 == "Mode", arr.ind = TRUE)) > 0,
                              meta9[which(meta9 == "Mode", arr.ind = TRUE)+2],"NA")

  metadata <- data.table(meta_actigraph, meta_actilife, meta_firmware,
                         meta_format, meta_filter, meta_serial, meta_start_time,
                         meta_start_date, meta_epoch, meta_dl_time, meta_dl_date,
                         meta_mem_address, meta_voltage, meta_mode,
                         stringsAsFactors = FALSE)

  return(metadata)
}


actigraph_raw <- function(file_location, dataTable = FALSE, metaData = TRUE) {
  if(dataTable){
    if(metaData){
      raw <- tryCatch(
        {fread(file_location, skip=10, header = T)},
        error=function(cond) {
          message("Using READ CSV Instead")
          as.data.table(read_csv(file_location, skip = 10, col_names = TRUE))
        }, finally={})
      raw$Date <- NULL
      raw$Time <- NULL
      raw$Vector.Magnitude <- NULL
      raw$VectorMagnitude <- NULL
      raw$`Vector Magnitude` <- NULL
    } else {
      read_raw <- tryCatch(
        {fread(file_location, header = T)},
        error=function(cond) {
          message("Using READ CSV Instead")
          as.data.table(read_csv(file_location))
        }, finally={})
      raw <- data.table("Activity" = read_raw$Activity)
    }
  } else {
    raw <- tryCatch(
      {fread(file_location, skip=10, header = F)},
      error=function(cond){
        message("Using READ CSV Instead")
        as.data.table(read_csv(file_location, skip = 10))
      }, finally={})
    raw$Vector.Magnitude <- NULL
    raw$VectorMagnitude <- NULL
    raw$`Vector Magnitude` <- NULL
  }
  if(metaData){
    metadata <- as.data.table(actigraph_metadata(file_location))

    start <- paste(metadata$meta_start_date," ",metadata$meta_start_time, sep="")
    format <- data.table(unlist(strsplit(metadata$meta_format,"/")), stringsAsFactors = FALSE)
    if(metadata$meta_format == "NA"){
      format <- data.table(c("m","d","Y"),stringsAsFactors =  FALSE)
    }
    for(i in 1:nrow(format)){
      if(tolower(format[i,1]) == "m" || tolower(format[i,1]) == "mm"){
        format[i,1 := "m"]
      } else if(tolower(format[i,1]) == "d" || tolower(format[i,1]) == "dd"){
        format[i,1 := "d"]
      } else if(tolower(format[i,1]) == "yy" || tolower(format[i,1]) == "yyyy" || tolower(format[i,1]) == "y"){
        format[i,1 := "Y"]
      }
    }
  } else {
    format <- data.table(c("m","d","Y"),stringsAsFactors =  FALSE)
  }

  #raw[, fulltime := NULL]
  if(metaData){
    if(dataTable){
      dtstarttable <- tryCatch(
        {fread(file_location, skip=10, nrows = 1, header = T)},
        error=function(cond) {
          message("Using READ CSV Instead")
          as.data.table(read_csv(file_location, skip=10, n_max = 1, col_names = T))
        }, finally={})
      dtStart <- paste(dtstarttable$Date,dtstarttable$Time)
      raw[1,fulltime := as.POSIXct(dtStart,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = "America/Los_Angeles")]
    } else {
      raw[1,fulltime := as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = "America/Los_Angeles")]
    }
  } else {
    dtstarttable <- tryCatch(
      {fread(file_location, nrows = 1, header = T)},
      error=function(cond) {
        message("Using READ CSV Instead")
        as.data.table(read_csv(file_location, n_max = 1, col_names = T))
      }, finally={})
    start <- paste(dtstarttable$Date,dtstarttable$Time)
    raw[1,fulltime := as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = "America/Los_Angeles")]
  }

  if(metaData){
    epoch <- data.table(unlist(strsplit(metadata$meta_epoch,":")), stringsAsFactors = FALSE)
    epoch_hour <- as.integer(epoch[1,1])
    epoch_minute <- as.integer(epoch[2,1])
    epoch_second <- as.integer(epoch[3,1])

    epoch <- (epoch_hour*60*60)+(epoch_minute*60)+epoch_second
  } else {
    dtstarttable <- tryCatch(
      {fread(file_location, nrows = 2, header = T)},
      error=function(cond) {
        message("Using READ CSV Instead")
        as.data.table(read_csv(file_location, n_max = 2, col_names = T))
      }, finally={})
    dtNext <- paste(dtstarttable$Date[2],dtstarttable$Time[2])
    raw[2, fulltime := as.POSIXct(dtNext,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = "America/Los_Angeles")]
    epoch <- raw$fulltime[2]-raw$fulltime[1]
  }

  start_time <- as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = "America/Los_Angeles")
  raw[, fulltime := start_time+epoch*(.I-1)]

  if(metaData){
    mode <- as.integer(metadata[1,meta_mode])+1L
  } else {
    mode <- 1L
  }
  rows <- actigraph_mode_columns(mode)

  setnames(raw,c(rows,"fulltime"))

  return(raw)
}

acc_nonwear_agd <- function(file_location, nhanes = TRUE){
  agd <- dbConnect(SQLite(), file_location)
  acc_raw <- as.data.table(dbReadTable(agd,"data"))
  settings <- as.data.table(dbReadTable(agd,"settings"))
  acc_raw$fulltime <- as.POSIXct(acc_raw$dataTimestamp/(10000000),origin = "0001-01-01 00:00:00", tz = "America/Los_Angeles")
  acc_raw[,1:=NULL]
  mode_integer <- as.integer(settings[settingName == "modenumber", settingValue])
  rows <- actigraph_mode_columns(mode_integer+1)
  setnames(acc_raw,c(rows,"fulltime"))
  epoch <- as.integer(settings[settingName == "epochlength", settingValue])

  if(epoch == 30){
    nhanes_break <- 50
  } else if(epoch == 60){
    nhanes_break <- 100
  } else if(epoch == 10){
    nhanes_break <- 100/6
  } else {
    print("Epoch not supported")
    stop()
  }

  if(!nhanes) {
    nhanes_break <- 0
  }

  acc_raw[, non_wear := as.integer(Activity == 0)]
  acc_raw[, non_wear_break := as.integer(!non_wear)]
  acc_raw[, non_wear_length := bout_sequence(acc_raw[,non_wear],acc_raw[,non_wear_break],epoch)]
  acc_raw[, non_wear_new := ifelse((non_wear_length <= 120
                                    & non_wear == 0
                                    & Activity < nhanes_break),1
                                   ,non_wear)]
  acc_raw[, non_wear_new_break := as.integer(!non_wear_new)]
  acc_raw[, non_wear_length_new := bout_sequence(non_wear_new, non_wear_new_break, epoch)]
  acc_raw[, non_wear_bout := as.integer(non_wear_length_new > 3600 & non_wear_new == 1)]
  setnames(acc_raw,"non_wear_bout","nonwear")
  acc_raw[ , c("non_wear","non_wear_break",
               "non_wear_length", "non_wear_new",
               "non_wear_new_break","non_wear_length_new") := NULL]
  acc_raw[, wear := as.integer(!nonwear)]

  acc_raw[, fulldate := as.Date(as.character(as.POSIXct(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")))]
  valid_days <- data.table(ddply(acc_raw,~fulldate,summarise,time=sum(wear)))
  if(epoch == 30){
    valid_days[, valid_day := as.integer(time > 1200)]
  } else if(epoch == 60){
    valid_days[, valid_day := as.integer(time > 600)]
  } else if(epoch == 10){
    valid_days[, valid_day := as.integer(time > 3600)]
  }
  valid_days[, valid_day_sum := sum(valid_day)]
  setnames(valid_days,"time","valid_day_length")
  acc <- merge(x = acc_raw, y= valid_days, by = "fulldate" , all.x = TRUE)
  acc[,epoch := epoch]

  return(acc)
}

acc_nonwear <- function(file_location, nhanes = TRUE, dataTable = FALSE, metaData = TRUE){
  if(metaData){
    acc_metadata <- actigraph_metadata(file_location)
  }
  acc_raw <- actigraph_raw(file_location, dataTable, metaData)

  if(metaData){
    epoch <- data.table(unlist(strsplit(acc_metadata$meta_epoch,":")), stringsAsFactors = FALSE)
    epoch_hour <- as.integer(epoch[1,1])
    epoch_minute <- as.integer(epoch[2,1])
    epoch_second <- as.integer(epoch[3,1])

    epoch <- (epoch_hour*60*60)+(epoch_minute*60)+epoch_second
  } else {
    epoch <- acc_raw$fulltime[2]-acc_raw$fulltime[1]
  }

  if(epoch == 30){
    nhanes_break <- 50
  } else if(epoch == 60){
    nhanes_break <- 100
  } else {
    print("Epoch not supported")
    stop()
  }

  if(!nhanes) {
    nhanes_break <- 0
  }

  acc_raw[, non_wear := as.integer(Activity == 0)]
  acc_raw[, non_wear_break := as.integer(!non_wear)]
  acc_raw[, non_wear_length := bout_sequence(acc_raw[,non_wear],acc_raw[,non_wear_break],epoch)]
  acc_raw[, non_wear_new := ifelse((non_wear_length <= 120
                                    & non_wear == 0
                                    & Activity < nhanes_break),1
                                   ,non_wear)]
  acc_raw[, non_wear_new_break := as.integer(!non_wear_new)]
  acc_raw[, non_wear_length_new := bout_sequence(non_wear_new, non_wear_new_break, epoch)]
  acc_raw[, non_wear_bout := as.integer(non_wear_length_new > 3600 & non_wear_new == 1)]
  setnames(acc_raw,"non_wear_bout","nonwear")
  acc_raw[ , c("non_wear","non_wear_break",
               "non_wear_length", "non_wear_new",
               "non_wear_new_break","non_wear_length_new") := NULL]
  acc_raw[, wear := as.integer(!nonwear)]

  acc_raw[, fulldate := as.Date(as.character(as.POSIXct(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")))]
  valid_days <- data.table(ddply(acc_raw,~fulldate,summarise,time=sum(wear)))
  if(epoch == 30){
    valid_days[, valid_day := as.integer(time > 1200)]
  } else if(epoch == 60){
    valid_days[, valid_day := as.integer(time > 600)]
  }
  valid_days[, valid_day_sum := sum(valid_day)]
  setnames(valid_days,"time","valid_day_length")
  acc <- merge(x = acc_raw, y= valid_days, by = "fulldate" , all.x = TRUE)
  acc[,epoch := epoch]

  return(acc)
}

#' Produce an activity dataset from raw data
#'
#' @param folder_location location of .csv or agd files
#' @param age_data_file two column .csv file containing id and age
#' @param nhanes_nonwear = TRUE, use NHANES non-wear thresholds
#' @param id_length = 7, length of id variable in age_data_file
#' @param agdFile = FALSE, agd file, not csv, if TRUE, overrides \code{dataTable} and \code{metaData} settings
#' @param dataTable = FALSE, datatable format, post-processed with ActiLife
#' @param metaData = TRUE, presence of metadata in headers of csv file
#'
#' @return A data.table containing a variety of activity fields and wear time
#'
#'
#' @export
acc_ageadjusted <- function(folder_location, age_data_file, nhanes_nonwear = TRUE, id_length = 7, agdFile = FALSE, dataTable = FALSE, metaData = TRUE){
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  if(!agdFile){
    message("No AGD File")
    file_locations <- list.files(folder_location, full.names = TRUE, pattern = "\\.csv$")
    file_ids <- list.files(folder_location, full.names = FALSE, pattern = "\\.csv$")
    acc_vars<- c("Activity", "Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")

    acc_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                     , total = length(file_ids)/(cores-1), clear = FALSE, width = 60)

    acc_full <- foreach(i=1:length(file_ids), .combine=rbind,.packages=c("readr","data.table","plyr","progress","ActiPro")) %dopar% {
      acc_hold <- NULL
      acc_hold <- acc_nonwear(file_locations[i], nhanes = nhanes_nonwear, dataTable, metaData)
      for(var in acc_vars){
        if(is.na(match(var,colnames(acc_hold)))){
          acc_hold[,var] <- NA_integer_
        }
      }
      acc_hold$file_id <- file_ids[i]
      acc_progress$tick()
      acc_hold
    }
  }
  if(agdFile){
    message("AGD File")
    file_locations <- list.files(folder_location, full.names = TRUE, pattern = "\\.agd$")
    file_ids <- list.files(folder_location, full.names = FALSE, pattern = "\\.agd$")
    acc_vars<- c("Activity", "Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")

    acc_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                     , total = length(file_ids)/(cores-1), clear = FALSE, width = 60)

    acc_full <- foreach(i=1:length(file_ids), .combine=rbind,.packages=c("readr","data.table","plyr","progress","ActiPro","DBI","RSQLite")) %dopar% {
      acc_hold <- NULL
      acc_hold <- acc_nonwear_agd(file_locations[i], nhanes = nhanes_nonwear)
      for(var in acc_vars){
        if(is.na(match(var,colnames(acc_hold)))){
          acc_hold[,var] <- NA_integer_
        }
      }
      acc_hold$file_id <- file_ids[i]
      acc_progress$tick()
      acc_hold
    }
  }

  acc_full[, id := tolower(substr(file_id,1,id_length))]

  acc_full_age <- process_age(age_data_file, acc_full)


  stopCluster(cl)
  return(acc_full_age)
}

process_age <- function(age_data_file, acc_full){
  age_data <- fread(age_data_file, stringsAsFactors = FALSE, colClasses=c(rep("character",2)))
  age_data_date <- nchar(age_data[1,2]) == 10
  if(age_data_date){
    colnames(age_data) <- c("id","dob")
    temp_age <- acc_full[, .SD[1], id][,.(id,fulldate)]
    age_data <- merge(x = temp_age, y = age_data, by = "id", all = FALSE)
    age_data[, agedate := as.Date(as.character(as.POSIXct(dob, origin = "1970-01-01", tz = "America/Los_Angeles")))]
    age_data[, age := age_calc(agedate, enddate=fulldate,units = "years")]
    age_data <- age_data[,.(id,age)]
  } else {
    colnames(age_data) <- c("id","age")
    age_data[, age := as.integer(age)]
  }
  age_data[, id := tolower(id)]
  age_data[age > 17 , age := 18L] # adult support override

  age <- as.integer(c(6,7,8,9,10,11,12,13,14,15,16,17,18))
  div_mod <- as.integer(c(1400, 1515,1638,1770,1910,2059,2220,2393,2580,2781,3000,3239,2020))
  div_vig <- as.integer(c(3758,3947,4147,4360,4588,4832,5094,5375,5679,6007,6363,6751,5999))
  age_acc <- data.table(age,div_mod,div_vig)

  age_merge <- merge(x = age_data, y= age_acc, by = "age" , all = FALSE)

  age_merge[age > 17, div_mod := 2020L]
  age_merge[age > 17, div_vig := 5999L]

  acc_full_age <- merge(x = acc_full, y= age_merge, by = "id" , all = FALSE)

  acc_full_age[, divider := ifelse(epoch == 30, 2,ifelse(epoch == 10, 6,1))]

  acc_full_age[, sed := as.integer(wear == 1 & Activity < (100/divider))]
  acc_full_age[, vig := as.integer(wear == 1 & sed != 1 & Activity > (div_vig/divider))]
  acc_full_age[, mod := as.integer(wear == 1 & sed != 1 & vig != 1 & Activity > (div_mod/divider))]
  acc_full_age[, light := as.integer(wear == 1 & sed != 1 & mod != 1 & vig != 1)]

  return(acc_full_age)
}

#' Generates vector of continuous MVPA bouts
#'
#' @param acc_ageadjusted An ActiPro data.table derived from Actigraph Actilife data
#' @param min_break Length of time for a grace period during a bout, probably 0
#' @param act_break Grace period bout restriction, upper level
#' @param bout_length Minimum bout length, in seconds
#'
#' @return A vector that fits in the \code{acc_ageadjusted} data.table
#'
#'
#' @export
mvpa_bouts <- function(acc_ageadjusted, min_break, act_break, bout_length) {
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  mvpa_acc <- acc_ageadjusted[,c("Activity","mod","vig","id","fulltime","fulldate","epoch")]
  mvpa_acc[, mvpa := as.integer(mod == 1 | vig == 1)]
  mvpa_acc[, mod := NULL]
  mvpa_acc[, vig := NULL]
  mvpa_acc[, mvpa_break := as.integer(!mvpa)]

  mvpa_ids <- unique(mvpa_acc$id)

  mvpa_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                    , total = length(mvpa_ids)/(cores-1), clear = FALSE, width = 60)

  mvpa_proc  <- foreach(i=1:length(mvpa_ids), .combine=rbind,.packages=c("plyr","progress","ActiPro")) %dopar% {
    mvpa_hold <- mvpa_acc[id == mvpa_ids[i]]
    epoch <- mvpa_hold[1,epoch]

    bout <- bout_sequence(mvpa_hold$mvpa,mvpa_hold$mvpa_break, epoch)
    mvpa_hold[, mvpa_length := bout]
    mvpa_hold[, mvpa_new := ifelse((mvpa_length <= min_break
                                    & mvpa == 0
                                    & Activity < act_break),1,mvpa)]

    mvpa_hold[, mvpa_new_break := as.integer(!mvpa_new)]

    bout_new <- bout_sequence(mvpa_hold$mvpa_new,mvpa_hold$mvpa_new_break, epoch)
    mvpa_hold[, mvpa_length_new := bout_new]
    mvpa_hold[, mvpa_bout := as.integer(mvpa_length_new > bout_length & mvpa_new == 1)]
    mvpa_progress$tick()
    mvpa_hold
  }
  stopCluster(cl)
  return(mvpa_proc[, mvpa_bout])
}

#' Returns a datatable to merge accelerometer data back into EMA data
#'
#' @param ema_file A csv file with three columns, id, fulltime, and index
#' @param activity_data An \code{acc_ageadjusted} data.table
#' @param time_stubs Time windows to use
#' @param activity_types Types of activity from \code{acc_ageadjusted} data.table
#'
#' @return A data.table with windows appended to ema_stubs file
#'
#'
#' @export
ema_acc <- function(ema_file, activity_data,
                    time_stubs = c("15","30","60","120"),
                    activity_types = c("VALID","NONVALID","MOD","VIG","SED","LIGHT","MVPA","MVPA_BOUT","ACTIVITY")){
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)
  ema_stubs <- fread(ema_file, colClasses = c("character","character","integer"),
                     col.names = c("ID","FULLTIME","ACC_STABLE_STUB"))
  ema_stubs[, ID := tolower(ID)]
  ema_stubs[, time := as.POSIXct(FULLTIME,format="%Y-%m-%d %H:%M:%S",origin="1970-01-01", tz = "America/Los_Angeles")]

  keycols <- c("ID","time")
  setorderv(ema_stubs,keycols)
  setkeyv(ema_stubs,keycols)

  keycols <- c("id","fulltime")
  setorderv(activity_data,keycols)
  setkeyv(activity_data,keycols)

  ema_stubs2 <- ema_stubs
  ema_stubs2[, es_date := as.IDate(fulltime, tz = "America/Los_Angeles")]
  ema_stubs2[, es_time := as.ITime(fulltime, tz = "America/Los_Angeles")]
  keycols <- c("ID","es_date","es_time")
  setorderv(ema_stubs2,keycols)
  setkeyv(ema_stubs2,keycols)

  activity_data2 <- activity_data
  activity_data2[, ad_date := as.IDate(fulltime, tz = "America/Los_Angeles")]
  activity_data2[, ad_time := as.ITime(fulltime, tz = "America/Los_Angeles")]
  keycols <- c("id","ad_date","ad_time")
  setorderv(activity_data2,keycols)
  setkeyv(activity_data2,keycols)


  type_var <- function(type_switch){
    return(switch(type_switch,
                  VALID = "wear",
                  NONVALID = "nonwear",
                  MOD = "mod",
                  VIG = "vig",
                  SED = "sed",
                  LIGHT = "light",
                  MET = "met",
                  MVPA = "mvpa",
                  MVPA_BOUT = "mvpa_bout",
                  ACTIVITY = "Activity"))
  }

  ema_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                   , total = (length(activity_types)*length(time_stubs)*3), clear = FALSE, width = 60)

  for (ts in time_stubs) {
    for (type in activity_types){
      print(ts)
      print(type)
      print("BEFORE")
      before <- paste(type,"_",ts,"_BEFORE", sep="")
      acc_vector <- foreach(i=1:ema_stubs[,.N], .combine=rbind,.packages=c("data.table")) %dopar% {
        hold <- activity_data2[.(ema_stubs2[i,ID],ema_stubs2[i,es_date])]
        setkey(hold,ad_time)
        hold[between(ad_time,
                     ema_stubs2[i,es_time]-(60L*as.integer(ts)),
                     ema_stubs2[i,es_time]+1L, incbounds = FALSE),
             sum(eval(parse(text = type_var(type))), na.rm = TRUE)/mean(divider,na.rm = TRUE)]
        #hold[ad_time > ema_stubs2[i,es_time]-(60L*as.integer(ts)) &
        #   ad_time <= ema_stubs2[i,es_time],
        #   sum(eval(parse(text = type_var(type))))/mean(divider)]
      }
      ema_stubs[, eval(before) := acc_vector]
      ema_progress$tick()
    }
  }

  for (ts in time_stubs) {
    for (type in activity_types){
      print(ts)
      print(type)
      print("AFTER")
      after <- paste(type,"_",ts,"_AFTER", sep="")
      acc_vector <- foreach(i=1:ema_stubs[,.N], .combine=rbind,.packages=c("data.table")) %dopar% {
        hold <- activity_data2[.(ema_stubs2[i,ID],ema_stubs2[i,es_date])]
        setkey(hold,ad_time)
        hold[between(ad_time,
                     ema_stubs2[i,es_time]-1,
                     ema_stubs2[i,es_time]+(60L*as.integer(ts)), incbounds = FALSE),
             sum(eval(parse(text = type_var(type))), na.rm = TRUE)/mean(divider,na.rm = TRUE)]
        #hold[ad_time > ema_stubs2[i,es_time]-(60L*as.integer(ts)) &
        #   ad_time <= ema_stubs2[i,es_time],
        #   sum(eval(parse(text = type_var(type))))/mean(divider)]
      }
      ema_stubs[, eval(after) := acc_vector]
      ema_progress$tick()
    }
  }

  for (ts in time_stubs) {
    for (type in activity_types){
      print(ts)
      print(type)
      print("WINDOW")
      window <- paste(type,"_",as.character(as.integer(ts)*2),"_WINDOW", sep="")
      acc_vector <- foreach(i=1:ema_stubs[,.N], .combine=rbind,.packages=c("data.table")) %dopar% {
        hold <- activity_data2[.(ema_stubs2[i,ID],ema_stubs2[i,es_date])]
        setkey(hold,ad_time)
        hold[between(ad_time,
                     ema_stubs2[i,es_time]-(60L*as.integer(ts)),
                     ema_stubs2[i,es_time]+(60L*as.integer(ts)), incbounds = FALSE),
             sum(eval(parse(text = type_var(type))), na.rm = TRUE)/mean(divider,na.rm = TRUE)]
        #hold[ad_time > ema_stubs2[i,es_time]-(60L*as.integer(ts)) &
        #   ad_time <= ema_stubs2[i,es_time],
        #   sum(eval(parse(text = type_var(type))))/mean(divider)]
      }
      ema_stubs[, eval(window) := acc_vector]
      ema_progress$tick()
    }
  }

  stopCluster(cl)

  return(ema_stubs)
}

#' Returns a datatable to merge accelerometer data back into EMA data
#'
#' @param ema_file A csv file with three columns, id, fulltime, and index in that specific order
#' @param activity_data An \code{acc_ageadjusted} data.table
#' @param time_stubs Time windows to use
#' @param activity_types Types of activity from \code{acc_ageadjusted} data.table
#' @param cut_dataset_start the number of timea to split the ema dataset, this might be needed for very large studies or slow computers
#'
#' @return A data.table with windows appended to ema_stubs file
#'
#'
#' @export
ema_acc_fast <- function(ema_file, activity_data,
                         time_stubs = c("15","30","60","120"),
                         activity_types = c("VALID","NONVALID","MOD","VIG","SED","LIGHT","MVPA","MVPA_BOUT","ACTIVITY"),
                         cut_dataset_start = 0){
  if(is.null(activity_data) | nrow(activity_data) == 0){
    return(NULL)
  }
  ema_stubs <- fread(ema_file, colClasses = c("character","character","integer"),
                     col.names = c("id","FULLTIME","ACC_STABLE_STUB"))
  ema_stubs[, id := tolower(id)]
  activity_data[, tester := 1L]
  activity_data_ids <- activity_data[, lapply(.SD, first) , by = "id",.SDcols = c("tester")]
  activity_data_ids[, tester := NULL]
  ema_stubs <- merge(ema_stubs, activity_data_ids, by = "id", all = FALSE)
  ema_stubs[, time := as.POSIXct(FULLTIME,format="%Y-%m-%d %H:%M:%S",origin="1970-01-01", tz = "America/Los_Angeles")]
  if(cut_dataset_start == 0){
    ema_stubs[, split := 0]
  } else {
    ema_stubs[, split := as.integer((substring(id,cut_dataset_start)))]
  }
  ema_datasets <- split(ema_stubs, by = "split")

  expand_sec <- "60 sec" # paste(as.character(min(activity_data$epoch, na.rm = TRUE)),"sec")
  multiplier_sec <- 1 #as.integer(60L/min(activity_data$epoch))

  keycols <- c("id","fulltime")
  setorderv(activity_data,keycols)
  setkeyv(activity_data,keycols)

  activity_data[, hour := as.POSIXlt(fulltime)$hour]
  activity_data[, minute := as.POSIXlt(fulltime)$min]
  minute_acc <- activity_data[,
                          list(Activity =  sum(Activity, na.rm = TRUE),
                               light =  sum(light, na.rm = TRUE),
                               mod =  sum(mod, na.rm = TRUE),
                               vig =  sum(vig, na.rm = TRUE),
                               sed =  sum(sed, na.rm = TRUE),
                               wear = sum(wear, na.rm = TRUE),
                               nonwear = sum(nonwear, na.rm = TRUE),
                               mvpa_bout =  sum(mvpa_bout, na.rm = TRUE),
                               mvpa =  sum(mvpa, na.rm = TRUE),
                               divider = mean(divider, na.rm = TRUE)),
                          by = list(id,
                                    fulldate,
                                    hour,
                                    minute)]
  divided_acc <- minute_acc[, list(
    id = id,
    fulldate = fulldate,
    fulltime = as.POSIXct(paste(as.character(fulldate)," ",
                      sprintf("%02d",hour),":",
                      sprintf("%02d",minute),":00",sep=""),
                      format="%Y-%m-%d %H:%M:%S",origin="1970-01-01", tz = "America/Los_Angeles"),
    Activity =  Activity/divider,
    light =  light/divider,
    mod =  mod/divider,
    vig =  vig/divider,
    sed =  sed/divider,
    wear = wear/divider,
    nonwear = nonwear/divider,
    mvpa_bout =  mvpa_bout/divider,
    mvpa =  mvpa/divider,
    divider = 1L)]
  activity_data2 <- divided_acc
  activity_data2[, ad_date := as.IDate(fulltime, format="%Y-%m-%d %H:%M:%S")]
  activity_data2[, ad_time := as.ITime(fulltime, format="%Y-%m-%d %H:%M:%S")]
  keycols <- c("id","ad_date","ad_time")
  setorderv(activity_data2,keycols)
  setkeyv(activity_data2,keycols)
  setorderv(activity_data2,c("id","fulltime"))
  setkeyv(activity_data2,c("id","fulltime"))

  dataset_return <- NULL
  for(ema_stubs in ema_datasets){
    keycols <- c("id","time")
    setorderv(ema_stubs,keycols)
    setkeyv(ema_stubs,keycols)

    ema_stubs2 <- ema_stubs
    ema_stubs2[, es_date := as.IDate(FULLTIME, format="%Y-%m-%d %H:%M:%S")]
    ema_stubs2[, es_time := as.ITime(FULLTIME, format="%Y-%m-%d %H:%M:%S")]
    keycols <- c("id","es_date","es_time")
    setorderv(ema_stubs2,keycols)
    setkeyv(ema_stubs2,keycols)


    type_var <- function(type_switch){
      return(switch(type_switch,
                    VALID = "wear",
                    NONVALID = "nonwear",
                    MOD = "mod",
                    VIG = "vig",
                    SED = "sed",
                    LIGHT = "light",
                    #MET = "met",
                    MVPA = "mvpa",
                    MVPA_BOUT = "mvpa_bout",
                    ACTIVITY = "Activity"))
    }

    #ema_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
    #                                 , total = (length(activity_types)*length(time_stubs)*3), clear = FALSE, width = 60)

    for (ts in time_stubs) {
      ema_stubs2[, expand_times := (as.integer(ts)*multiplier_sec)]
      ema_stubs2[, low_time := as.POSIXct(round_date(time - as.integer(ts)*60L,expand_sec) + 60L/multiplier_sec, tz = "America/Los_Angeles")]
      ema_stubs2[, high_time := as.POSIXct(round_date(time,expand_sec) , tz = "America/Los_Angeles")]
      pre_expand <- ema_stubs2[!is.na(time), .SD[rep(1:.N, expand_times)]]
      expand <- pre_expand[, fulltime := seq(low_time, high_time, by = expand_sec),
                                                                       by = .(ACC_STABLE_STUB,low_time, high_time)][]
      setorderv(expand,c("id","fulltime"))
      setkeyv(expand,c("id","fulltime"))
      for (type in activity_types){
        print(ts)
        print(type)
        print("BEFORE")
        before <- paste(type,"_",ts,"_BEFORE", sep="")
        act_var <- paste("i.",type_var(type),sep="")
        merged <- expand[activity_data2, nomatch = 0, with = FALSE, j =
                           c('id','fulltime',act_var,'divider','id','ACC_STABLE_STUB')]
        merged[, return_var := (get(act_var))]
        return <- merged[, .(add_var = sum(return_var/divider, na.rm = TRUE)), by=.(id, ACC_STABLE_STUB)]
        #setnames(return,"add_var",eval(before))
        ema_stubs[return, on = c('ACC_STABLE_STUB'), eval(before) := i.add_var]
      #  ema_progress$tick()
      }
    }

    for (ts in time_stubs) {
      ema_stubs2[, expand_times := (as.integer(ts)*multiplier_sec)]
      ema_stubs2[, low_time := as.POSIXct(round_date(time,expand_sec), tz = "America/Los_Angeles")]
      ema_stubs2[, high_time := as.POSIXct(round_date(time + as.integer(ts)*60L,expand_sec) - 60L/multiplier_sec, tz = "America/Los_Angeles")]
      pre_expand <- ema_stubs2[!is.na(time), .SD[rep(1:.N, expand_times)]]
      expand <- pre_expand[, fulltime := seq(low_time, high_time, by = expand_sec),
                           by = .(ACC_STABLE_STUB,low_time, high_time)][]

      #ema_stubs2[, expand_times := (as.integer(ts)*multiplier_sec)]
      #ema_stubs2[, low_time := as.POSIXct(round(time,"min"), tz = "America/Los_Angeles")]
      #ema_stubs2[, high_time := as.POSIXct(round(time + as.integer(ts)*60L,"min"), tz = "America/Los_Angeles")]
      #expand <- ema_stubs2[!is.na(time), .SD[rep(1:.N, expand_times)]][,
      #                                                                 fulltime := seq(low_time , high_time, by = expand_sec),
      #                                                                 by = .(ACC_STABLE_STUB,low_time, high_time)][]

      setorderv(expand,c("id","fulltime"))
      setkeyv(expand,c("id","fulltime"))
      for (type in activity_types){
        print(ts)
        print(type)
        print("AFTER")
        before <- paste(type,"_",ts,"_AFTER", sep="")
        act_var <- paste("i.",type_var(type),sep="")
        merged <- expand[activity_data2, nomatch = 0, with = FALSE, j =
                           c('id','fulltime',act_var,'divider','id','ACC_STABLE_STUB')]
        merged[, return_var := (get(act_var))]
        return <- merged[, .(add_var = sum(return_var/divider, na.rm = TRUE)), by=.(id, ACC_STABLE_STUB)]
        #setnames(return,"add_var",eval(before))
        ema_stubs[return, on = c('ACC_STABLE_STUB'), eval(before) := i.add_var]
      #  ema_progress$tick()
      }
    }
#
#     for (ts in time_stubs) {
#       ema_stubs2[, expand_times := (as.integer(ts)*2L*multiplier_sec)]
#       ema_stubs2[, low_time := as.POSIXct(round_date(time - as.integer(ts)*60L,expand_sec), tz = "America/Los_Angeles")]
#       ema_stubs2[, high_time := as.POSIXct(round_date(time + as.integer(ts)*60L,expand_sec), tz = "America/Los_Angeles")]
#       pre_expand <- ema_stubs2[!is.na(time), .SD[rep(1:.N, expand_times)]]
#       expand <- pre_expand[, fulltime := seq(low_time, high_time, by = expand_sec),
#                            by = .(ACC_STABLE_STUB,low_time, high_time)][]
#
#       #ema_stubs2[, expand_times := (as.integer(ts)*2L*multiplier_sec)]
#       #ema_stubs2[, low_time := as.POSIXct(round(time - as.integer(ts)*60L,"min"), tz = "America/Los_Angeles")]
#       #ema_stubs2[, high_time := as.POSIXct(round(time + as.integer(ts)*60L,"min"), tz = "America/Los_Angeles")]
#       #expand <- ema_stubs2[!is.na(time), .SD[rep(1:.N, expand_times)]][,
#                                                                       # fulltime := seq(low_time , high_time, by = expand_sec),
#                                                                      #  by = .(ACC_STABLE_STUB,low_time, high_time)][]
#       setorderv(expand,c("id","fulltime"))
#       setkeyv(expand,c("id","fulltime"))
#       for (type in activity_types){
#         print(ts)
#         print(type)
#         print("WINDOW")
#         before <- paste(type,"_",ts,"_WINDOW", sep="")
#         act_var <- paste("i.",type_var(type),sep="")
#         merged <- expand[activity_data2, nomatch = 0, with = FALSE, j =
#                            c('id','fulltime',act_var,'divider','id','ACC_STABLE_STUB')]
#         #merged[, return_var := (get(act_var))]
#         return <- merged[, .(add_var = sum(return_var/divider, na.rm = TRUE)), by=.(id, ACC_STABLE_STUB)]
#         #setnames(return,"add_var",eval(before))
#         ema_stubs[return, on = c('ACC_STABLE_STUB'), eval(before) := i.add_var]
#       #  ema_progress$tick()
#       }
#     }

    dataset_return <- cbind(list(dataset_return,ema_stubs))
  }

  if(cut_dataset_start == 0){
    dataset_return <- as.data.table(dataset_return[2,1])
  }

  return(dataset_return)
}

actiwatch_nonwear <- function(acc_raw, epoch = 30, nonwear_break_value = 100, valid_day_minutes = 600){
  nhanes_break <- nonwear_break_value/(60/epoch)

  acc_raw[, non_wear := as.integer(Activity == 0)]
  acc_raw[, non_wear_break := as.integer(!non_wear)]
  acc_raw[, non_wear_length := bout_sequence(acc_raw[,non_wear],acc_raw[,non_wear_break],epoch)]
  acc_raw[, non_wear_new := ifelse((non_wear_length <= 120
                                    & non_wear == 0
                                    & Activity < nhanes_break),1
                                   ,non_wear)]
  acc_raw[, non_wear_new_break := as.integer(!non_wear_new)]
  acc_raw[, non_wear_length_new := bout_sequence(non_wear_new, non_wear_new_break, epoch)]
  acc_raw[, non_wear_bout := as.integer(non_wear_length_new > 3600 & non_wear_new == 1)]
  setnames(acc_raw,"non_wear_bout","nonwear")
  acc_raw[ , c("non_wear","non_wear_break",
               "non_wear_length", "non_wear_new",
               "non_wear_new_break","non_wear_length_new") := NULL]
  acc_raw[, wear := as.integer(!nonwear)]

  acc_raw[, fulldate := as.Date(as.character(as.POSIXct(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")))]
  valid_days <- data.table(ddply(acc_raw,~fulldate,summarise,time=sum(wear, na.rm = TRUE)))

  valid_days[, valid_day := as.integer(time > valid_day_minutes*(60/epoch))]

  valid_days[, valid_day_sum := sum(valid_day, na.rm = TRUE)]
  setnames(valid_days,"time","valid_day_length")
  acc <- merge(x = acc_raw, y= valid_days, by = "fulldate" , all.x = TRUE)
  acc[,epoch := epoch]
  return(acc)
}


#' Convert dataset from ActiWatch to Actipro
#' @name actiwatch_ageadjusted
#'
#' @param actiwatch_data actual actiwatch data using Actiwatch read helpers from ActiPro
#' @param age_data_file age data file for varying ages for cutpoints
#'
#' @return A data.table containing the Actipro compatible dataset
#'
#'
#' @export
actiwatch_ageadjusted <- function(actiwatch_data, age_data_file = NULL){
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  idlist <- unique(actiwatch_data$id)
  nonwear_return <- foreach(idvar = idlist, .combine = rbind,
                            .packages = c("data.table","plyr","ActiPro")) %dopar% {
                              actiwatch_nonwear(actiwatch_data[id == idvar])
                            }
  if(is.null(age_data_file)){
    return(nonwear_return)
  } else {
    return(process_age(age_data_file,nonwear_return))
  }
}

#' Convert dataset from ActiWatch XSLX to Actiwatch R
#' @name actiwatch_xlsx_merged_reader
#'
#' @param actiwatch_filepath filepath of actiwatch data, as multiple XLSX sheets
#'
#' @return A data.table containing Actiwatch data, raw
#'
#'
#' @export
actiwatch_xlsx_merged_reader <- function(actiwatch_filepath){
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  pre_return <- foreach(int_sheet = 1:length(excel_sheets(actiwatch_filepath)), .combine = rbind,
                        .packages = c("readxl")) %dopar% {
                          pre_read <- read_excel(actiwatch_filepath, sheet = int_sheet, col_names = FALSE, col_types = "text")
                          if(nrow(pre_read)>0){
                            locate_identity <- which(pre_read[1] == "Identity:")
                            subject_id <- as.character(pre_read[2][locate_identity,])
                            count_lines <- which(pre_read[1] == "Line")
                            last_line <- count_lines[length(count_lines)] - 1
                            begin_epochs <- last_line + 1
                            return_read <- read_excel(actiwatch_filepath, sheet = int_sheet, col_names = FALSE, skip = begin_epochs)
                            name_read <- read_excel(actiwatch_filepath, sheet = int_sheet, col_names = TRUE, skip = last_line)
                            cellnames <- colnames(name_read)
                            colnames(return_read) <- cellnames
                            return_read$id <- subject_id
                            return_read
                          }
                        }
  return_data <- as.data.table(pre_return)
  keep_idta <- return_data[, .(id, date = Date, time = Time, Activity)]
  keep_idta[, c("dropthis", "time") := tstrsplit(time, " ", fixed=TRUE)]
  keep_idta[, dropthis := NULL]
  keep_idta[,fulltime := as.POSIXct(paste(as.character(date),as.character(time),sep=" "),
                                    format="%Y-%m-%d %H:%M:%S",origin="1970-01-01", tz = "America/Los_Angeles")]
  dummy_activity_vars <- c("Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")
  for(i in dummy_activity_vars){
    keep_idta[, eval(i) := NA]
  }
  keep_idta[, Activity := as.integer(Activity)]

}

#' Convert dataset from ActiWatch XSLX to Actiwatch R
#' @name actiwatch_mergedcsv_reader
#'
#' @param actiwatch_filepath filepath of actiwatch data, as a long CSV file
#'
#' @return A data.table containing Actiwatch data, raw
#'
#'
#' @export
actiwatch_mergedcsv_reader <- function(actiwatch_csv_filepath, drop_suffix = 4, id_length = 3){
  existing_csv <- fread(actiwatch_csv_filepath)
  keep_list <- c("id","date","time","activity")
  keep_idta <- existing_csv[, .(id, date, time, Activity = activity)]
  keep_idta[, c("month", "day", "year") := tstrsplit(date, "/", fixed=TRUE)]
  keep_idta[, date := paste(sprintf("%02d",as.integer(month)),sprintf("%02d",as.integer(day)),
                            sprintf("%04d",as.integer(year)), sep = "/")]
  keep_idta[, c("hour", "min", "sec") := tstrsplit(time, ":", fixed=TRUE)]
  keep_idta[, time := paste(sprintf("%02d",as.integer(hour)),sprintf("%02d",as.integer(min)),
                            sprintf("%02d",as.integer(sec)), sep = ":")]
  keep_idta[, c("month", "day", "year") := NULL]
  keep_idta[, c("hour", "min", "sec") := NULL]
  dummy_activity_vars <- c("Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")
  for(i in dummy_activity_vars){
    keep_idta[, eval(i) := NA]
  }
  keep_idta[,fulltime := as.POSIXct(paste(as.character(date),as.character(time),sep=" "),
                                    format="%m/%d/%Y %H:%M:%S",origin="1970-01-01", tz = "America/Los_Angeles")]
  keep_idta[, id := substr(id,drop_suffix+1,drop_suffix+id_length)]
  return(keep_idta)
}
