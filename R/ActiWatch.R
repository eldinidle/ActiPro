# library(readxl)
# library(tibble)
# library(dplyr)
# library(foreach)
# library(doParallel)
# library(data.table)
# library(plyr)
##library(ActiPro)
#
# actiwatch_filepath = "/Users/eldin/Downloads/FRESH Actigraph Data_01.24.19.xlsx"
# actiwatch_csv_filepath = "/Users/eldin/Downloads/shirlene_is_a_jerk.csv"
# age_data_file = "/Users/eldin/Downloads/agedata.csv"
# ema_file = "/Users/eldin/Downloads/ematomerge.csv"
#
# acc_agd <- actiwatch_ageadjusted(actiwatch_mergedcsv_reader(actiwatch_csv_filepath), age_data_file)
# acc_agd$mvpa <- as.integer(acc_agd$mod == 1 | acc_agd$vig == 1)
# acc_agd$mvpa_bout <- mvpa_bouts(acc_agd, 0, 0, 600)
#
# anc <- ancillary_features(acc_agd, FALSE, FALSE, FALSE)
# sed <- sedentary_features(acc_agd, FALSE, FALSE, FALSE)
# light <- light_features(acc_agd, FALSE, FALSE, FALSE)
# mvpa <- mvpa_features(acc_agd, FALSE, FALSE, FALSE)
# features <- list(anc,sed,light,mvpa)
# day_level <- Reduce(function(...) merge(..., by = c("id","fulldate"), all = T), features)
#
#
# ema_acc_merge <- ema_acc_fast(ema_file, acc)
# write.csv(ema_acc_merge, "/Users/eldin/Downloads/datatomerge.csv")

#
# actiwatch_ageadjusted <- function(actiwatch_data, age_data_file = NULL){
#   cores=detectCores()
#   if(cores[1]>2){
#     cl <- makeCluster(cores[1]-1)
#   } else {
#     cores <- 2
#     cl <- makeCluster(1)
#   }
#   registerDoParallel(cl)
#
#   idlist <- unique(actiwatch_data$id)
#   nonwear_return <- foreach(idvar = idlist, .combine = rbind,
#                         .packages = c("data.table","plyr","A")) %dopar% {
#                           actiwatch_nonwear(actiwatch_data[id == idvar])
#                         }
#   if(is.null(age_data_file)){
#     return(nonwear_return)
#   } else {
#     return(process_age(age_data_file,nonwear_return))
#   }
# }
#
# actiwatch_nonwear <- function(acc_raw, epoch = 30, nonwear_break_value = 100, valid_day_minutes = 600){
#   nhanes_break <- nonwear_break_value/(60/epoch)
#
#   acc_raw[, non_wear := as.integer(Activity == 0)]
#   acc_raw[, non_wear_break := as.integer(!non_wear)]
#   acc_raw[, non_wear_length := bout_sequence(acc_raw[,non_wear],acc_raw[,non_wear_break],epoch)]
#   acc_raw[, non_wear_new := ifelse((non_wear_length <= 120
#                                     & non_wear == 0
#                                     & Activity < nhanes_break),1
#                                    ,non_wear)]
#   acc_raw[, non_wear_new_break := as.integer(!non_wear_new)]
#   acc_raw[, non_wear_length_new := bout_sequence(non_wear_new, non_wear_new_break, epoch)]
#   acc_raw[, non_wear_bout := as.integer(non_wear_length_new > 3600 & non_wear_new == 1)]
#   setnames(acc_raw,"non_wear_bout","nonwear")
#   acc_raw[ , c("non_wear","non_wear_break",
#                "non_wear_length", "non_wear_new",
#                "non_wear_new_break","non_wear_length_new") := NULL]
#   acc_raw[, wear := as.integer(!nonwear)]
#
#   acc_raw[, fulldate := as.Date(as.character(as.POSIXct(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")))]
#   valid_days <- data.table(ddply(acc_raw,~fulldate,summarise,time=sum(wear, na.rm = TRUE)))
#
#   valid_days[, valid_day := as.integer(time > valid_day_minutes*(60/epoch))]
#
#   valid_days[, valid_day_sum := sum(valid_day, na.rm = TRUE)]
#   setnames(valid_days,"time","valid_day_length")
#   acc <- merge(x = acc_raw, y= valid_days, by = "fulldate" , all.x = TRUE)
#   acc[,epoch := epoch]
#   return(acc)
# }

#
# process_age <- function(age_data_file, acc_full){
#   age_data <- fread(age_data_file, stringsAsFactors = FALSE, colClasses=c(rep("character",2)))
#   age_data_date <- nchar(age_data[1,2]) == 10
#   if(age_data_date){
#     colnames(age_data) <- c("id","dob")
#     temp_age <- acc_full[, .SD[1], id][,.(id,fulldate)]
#     age_data <- merge(x = temp_age, y = age_data, by = "id", all = FALSE)
#     age_data[, agedate := as.Date(as.character(as.POSIXct(dob, origin = "1970-01-01", tz = "America/Los_Angeles")))]
#     age_data[, age := age_calc(agedate, enddate=fulldate,units = "years")]
#     age_data <- age_data[,.(id,age)]
#   } else {
#     colnames(age_data) <- c("id","age")
#     age_data[, age := as.integer(age)]
#   }
#   age_data[, id := tolower(id)]
#   age_data[age > 17 , age := 18L] # adult support override
#
#   age <- as.integer(c(6,7,8,9,10,11,12,13,14,15,16,17,18))
#   div_mod <- as.integer(c(1400, 1515,1638,1770,1910,2059,2220,2393,2580,2781,3000,3239,2020))
#   div_vig <- as.integer(c(3758,3947,4147,4360,4588,4832,5094,5375,5679,6007,6363,6751,5999))
#   age_acc <- data.table(age,div_mod,div_vig)
#
#   age_merge <- merge(x = age_data, y= age_acc, by = "age" , all = FALSE)
#
#   age_merge[age > 17, div_mod := 2020L]
#   age_merge[age > 17, div_vig := 5999L]
#
#   acc_full_age <- merge(x = acc_full, y= age_merge, by = "id" , all = FALSE)
#
#   acc_full_age[, divider := ifelse(epoch == 30, 2,ifelse(epoch == 10, 6,1))]
#
#   acc_full_age[, sed := as.integer(wear == 1 & Activity < (100/divider))]
#   acc_full_age[, vig := as.integer(wear == 1 & sed != 1 & Activity > (div_vig/divider))]
#   acc_full_age[, mod := as.integer(wear == 1 & sed != 1 & vig != 1 & Activity > (div_mod/divider))]
#   acc_full_age[, light := as.integer(wear == 1 & sed != 1 & mod != 1 & vig != 1)]
#
#   return(acc_full_age)
# }
#
# bout_sequence <- function(acc_stream, break_stream, epoch, return_index = FALSE, return_position = FALSE){ #, min_bout_length
#   acc_raw <- data.table("acc_stream" = as.integer(acc_stream), "break_stream" = as.integer(break_stream))
#   epoch <- as.integer(epoch)
#   bins <- nrow(acc_raw)
#
#   wear_delta <- acc_raw$break_stream[-1L] != acc_raw$break_stream[-length(acc_raw$break_stream)]
#   delta <- data.table("index" = c(which(wear_delta), length(acc_raw$break_stream)))
#
#   delta[, lag := shift(index,1L,NA_integer_,type = "lag")]
#   delta[, lead := shift(index,1L,NA_integer_,type = "lead")]
#   delta[, reps := index - lag]
#   delta[, change := lead - index]
#   delta[, length := epoch*change]
#   delta[, reference := index + 1L]
#   delta[.N,change:= delta[.N,index] - bins]
#   delta[.N,lead := nrow(acc_raw)]
#   delta[1,reps := delta[1,index]]
#
#   delta[, c("reflag","relength","rechange") := shift(.SD,1L,NA_integer_,type = "lag"),.SDcols = c("reference", "length", "change")]
#   delta[, index_ref := .I]
#
#   delta[1,reflag := delta[2,reflag] - delta[1,index]]
#   delta[1,relength := delta[1,reps] * epoch]
#   delta[1,rechange := delta[1,reps]]
#
#   acc_raw[, acc_var_length := rep(delta[,relength],delta[,rechange])]
#   acc_raw[, index := rep(delta[,index_ref],delta[,rechange])]
#   acc_raw[, position := rep(delta[,reflag],delta[,rechange])]
#
#   if(return_index){
#     return(acc_raw[,index])
#   } else if (return_position){
#     return(acc_raw[,position])
#   } else {
#     return(acc_raw[,acc_var_length])
#   }
# }
