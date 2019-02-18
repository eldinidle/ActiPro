.onLoad <- function(libname, pkgname){
  declare_vars <- c("ACC_STABLE_STUB","Activity","FULLTIME","HR","ID","Steps","acc_var_length","ad_date",
                    "ad_time","age","agedate","change","day_step_time","day_wear","did","divider","dob",
                    "dummy","epoch","es_date","es_time","expand_times","ext","file_id","flag_cut","flag_ext",
                    "flag_nonwear","fulldate","fulltime","high_time","i","i.add_var","index","index_ref",
                    "label","lag","lead","light","light_bout_length","low_time","m_def","mean_age","met_mins",
                    "meta_mode","min_activity","min_bin_steps","min_divider","min_event","min_light","min_mvpa",
                    "min_sed","min_steps","min_wear","mod","mvpa","mvpa_bout","mvpa_bout_length","mvpa_break",
                    "mvpa_event_length","mvpa_guideline_bout","mvpa_length","mvpa_length_new","mvpa_new",
                    "mvpa_new_break","mvpa_new_index","mvpa_new_length","mvpa_sporadic_bout","mvpa_to_up",
                    "non_light","non_mvpa","non_sed","non_wear","non_wear_bout","non_wear_break",
                    "non_wear_length","non_wear_length_new","non_wear_new","non_wear_new_break",
                    "nonwear","pa","position","quantile","read_csv","rechange","reference",
                    "reflag","relength","reps","return_var","sed","sed_bout_length","sed_to_up",
                    "settingName","settingValue","tester","time","timestamp","true_light","true_mvpa",
                    "true_sed","unique_light_bout","unique_mvpa_bout","unique_sed_bout","uniques",
                    "valid_day","valid_day_sum","vig","wave","wear")

  if(getRversion() >= "2.15.1")  utils::globalVariables(declare_vars)
}

