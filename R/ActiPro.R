require("plyr")
require("progress")
require("parallel")
require("foreach")
require("doParallel")
require("iterators")
require("data.table")



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

## Function
# Input: Acc Stream Vector, Break Acc Stream Vector, Epoch Length
# Output: Bout Sequence Vector
dt_bout_sequence <- function(acc_stream, break_stream, epoch){ #, min_bout_length
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
  #delta[, reflag := shift(reference,1L,NA_integer_,type = "lag")]
  #delta[, relength := shift(length,1L,NA_integer_,type = "lag")]
  #delta[, rechange := shift(change,1L,NA_integer_,type = "lag")]

  delta[1,reflag := delta[2,reflag] - delta[1,index]]
  delta[1,relength := delta[1,reps] * epoch]
  delta[1,rechange := delta[1,reps]]

  acc_raw[, acc_var_length := rep(delta[,relength],delta[,rechange])]

  return(acc_raw[,acc_var_length])
}

## Function
# Input: Acc Stream Vector, Break Acc Stream Vector, Epoch Length
# Output: Bout Sequence Vector
bout_sequence <- function(acc_stream, break_stream, epoch){ #, min_bout_length
  acc_raw <- data.frame(acc_stream, break_stream)

  wear_delta <- acc_raw$break_stream[-1L] != acc_raw$break_stream[-length(acc_raw$break_stream)]
  delta <- data.frame(c(which(wear_delta), length(acc_raw$break_stream)))
  colnames(delta) <- c("index")
  delta$lag <- as.integer(sapply(1:nrow(delta), function(x) delta$index[x-1]))
  delta$lead <- as.integer(sapply(1:nrow(delta), function(x) delta$index[x+1]))
  delta$reps <- delta$index - delta$lag
  delta$change <- delta$lead - delta$index
  delta$length <- epoch*delta$change
  delta$reference <- delta$index + 1
  delta$change[nrow(delta)] <- delta$index[nrow(delta)] - nrow(acc_raw)
  delta$lead[nrow(delta)] <- nrow(acc_raw)
  delta$reps[1] <- delta$index[1]

  delta$reflag <- as.integer(sapply(1:nrow(delta), function(x) delta$reference[x-1]))
  delta$relength <- as.integer(sapply(1:nrow(delta), function(x) delta$length[x-1]))
  delta$rechange <- as.integer(sapply(1:nrow(delta), function(x) delta$change[x-1]))
  delta$reflag[1] <- delta$reflag[2]-delta$index[1]
  delta$relength[1] <- delta$reps[1] * epoch
  delta$rechange[1] <- delta$reps[1]

  acc_raw$acc_var_length <- rep(delta$relength, delta$rechange)

  return(acc_raw$acc_var_length)
}
## Function
# Input: ActiGraph File
# Output: ActiGraph MetaData DataFrame
dt_actigraph_metadata <- function(file_location) {
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
## Function
# Input: ActiGraph File
# Output: ActiGraph MetaData DataFrame
actigraph_metadata <- function(file_location) {
  meta <- read.csv(file_location, nrows = 9, header = F)
  meta_t <- data.frame(t(meta), stringsAsFactors = FALSE)

  meta1 <- data.frame(strsplit(meta_t[1,1], " "), stringsAsFactors = FALSE)
  meta2 <- data.frame(strsplit(meta_t[1,2], " "), stringsAsFactors = FALSE)
  meta3 <- data.frame(strsplit(meta_t[1,3], " "), stringsAsFactors = FALSE)
  meta4 <- data.frame(strsplit(meta_t[1,4], " "), stringsAsFactors = FALSE)
  meta5 <- data.frame(strsplit(meta_t[1,5], " "), stringsAsFactors = FALSE)
  meta6 <- data.frame(strsplit(meta_t[1,6], " "), stringsAsFactors = FALSE)
  meta7 <- data.frame(strsplit(meta_t[1,7], " "), stringsAsFactors = FALSE)
  meta8 <- data.frame(strsplit(meta_t[1,8], " "), stringsAsFactors = FALSE)
  meta9 <- data.frame(strsplit(meta_t[1,9], " "), stringsAsFactors = FALSE)


  meta_actigraph <-    ifelse(length(which(meta1 == "ActiGraph", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "ActiGraph", arr.ind = TRUE)[1,1]+1,1],"NA")
  meta_actilife <-     ifelse(length(which(meta1 == "ActiLife", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "ActiLife", arr.ind = TRUE)[1,1]+1,1],"NA")
  meta_firmware <-     ifelse(length(which(meta1 == "Firmware", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "Firmware", arr.ind = TRUE)[1,1]+1,1],"NA")
  meta_format <-       ifelse(length(which(meta1 == "format", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "format", arr.ind = TRUE)[1,1]+1,1],"NA")
  meta_filter <-       ifelse(length(which(meta1 == "Filter", arr.ind = TRUE)) > 0,
                              meta1[which(meta1 == "Filter", arr.ind = TRUE)[1,1]+1,1],"NA")

  meta_serial <-       meta2[nrow(meta2),1]
  meta_start_time <-   meta3[nrow(meta3),1]
  meta_start_date <-   meta4[nrow(meta4),1]
  meta_epoch <-        meta5[nrow(meta5),1]
  meta_dl_time <-      meta6[nrow(meta6),1]
  meta_dl_date <-      meta7[nrow(meta7),1]
  meta_mem_address <-  meta8[nrow(meta8),1]

  meta_voltage <-      meta9[which(meta9 == "Voltage:", arr.ind = TRUE)[1,1]+1,1]
  meta_mode <-         meta9[which(meta9 == "Mode", arr.ind = TRUE)[1,1]+2,1]

  metadata <- data.frame(meta_actigraph, meta_actilife, meta_firmware,
                         meta_format, meta_filter, meta_serial, meta_start_time,
                         meta_start_date, meta_epoch, meta_dl_time, meta_dl_date,
                         meta_mem_address, meta_voltage, meta_mode,
                         stringsAsFactors = FALSE)

  return(metadata)
}

dt_actigraph_raw <- function(file_location, dataTable = FALSE, metaData = TRUE) {
  if(dataTable){
    if(metaData){
      raw <- fread(file_location, skip=10, header = T)
      raw$Date <- NULL
      raw$Time <- NULL
      raw$Vector.Magnitude <- NULL
      raw$VectorMagnitude <- NULL
    } else {
      read_raw <- fread(file_location, header = T)
      raw <- data.table("Activity" = read_raw$Activity)
    }
  } else {
    raw <- fread(file_location, skip=10, header = F)
  }
  if(metaData){
    metadata <- as.data.table(dt_actigraph_metadata(file_location))

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
      dtStart <- paste(fread(file_location, skip=10, nrows = 1, header = T)$Date,fread(file_location, skip=10, nrows = 1, header = T)$Time)
      raw[1,fulltime := as.POSIXct(dtStart,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())]
    } else {
      raw[1,fulltime := as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())]
    }
  } else {
    start <- paste(fread(file_location, nrows = 1, header = T)$Date,fread(file_location, nrows = 1, header = T)$Time)
    raw[1,fulltime] <- as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())
  }

  if(metaData){
    epoch <- data.table(unlist(strsplit(metadata$meta_epoch,":")), stringsAsFactors = FALSE)
    epoch_hour <- as.integer(epoch[1,1])
    epoch_minute <- as.integer(epoch[2,1])
    epoch_second <- as.integer(epoch[3,1])

    epoch <- (epoch_hour*60*60)+(epoch_minute*60)+epoch_second
  } else {
    dtNext <- paste(fread(file_location, nrows = 2, header = T)$Date[2],fread(file_location, nrows = 2, header = T)$Time[2])
    raw[2, fulltime] <- as.POSIXct(dtNext,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())
    epoch <- raw$fulltime[2]-raw$fulltime[1]
  }

  start_time <- as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""))
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

## Function
# Input: ActiGraph File
# Output: ActiGraph Raw DataFrame
actigraph_raw <- function(file_location, dataTable = FALSE, metaData = TRUE) {
  if(dataTable){
    if(metaData){
      raw <- read.csv(file_location, skip=10, header = T)
      raw$Date <- NULL
      raw$Time <- NULL
      raw$Vector.Magnitude <- NULL
      raw$VectorMagnitude <- NULL
    } else {
      read_raw <- read.csv(file_location, header = T)
      raw <- data.frame("Activity" = read_raw$Activity)
    }
  } else {
    raw <- read.csv(file_location, skip=10, header = F)
  }
  if(metaData){
    metadata <- actigraph_metadata(file_location)

    start <- paste(metadata$meta_start_date," ",metadata$meta_start_time, sep="")
    format <- data.frame(strsplit(metadata$meta_format,"/"), stringsAsFactors = FALSE)
    if(metadata$meta_format == "NA"){
      format <- data.frame(c("m","d","Y"),stringsAsFactors =  FALSE)
    }
    for(i in 1:nrow(format)){
      if(tolower(format[i,1]) == "m" || tolower(format[i,1]) == "mm"){
        format[i,1] <- "m"
      } else if(tolower(format[i,1]) == "d" || tolower(format[i,1]) == "dd"){
        format[i,1] <- "d"
      } else if(tolower(format[i,1]) == "yy" || tolower(format[i,1]) == "yyyy" || tolower(format[i,1]) == "y"){
        format[i,1] <- "Y"
      }
    }
  } else {
    format <- data.frame(c("m","d","Y"),stringsAsFactors =  FALSE)
  }

  raw$fulltime <- NA
  if(metaData){
    if(dataTable){
      dtStart <- paste(read.csv(file_location, skip=10, nrows = 1, header = T)$Date,read.csv(file_location, skip=10, nrows = 1, header = T)$Time)
      raw$fulltime[1] <- as.POSIXct(dtStart,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())
    } else {
      raw$fulltime[1] <- as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())
    }
  } else {
    start <- paste(read.csv(file_location, nrows = 1, header = T)$Date,read.csv(file_location, nrows = 1, header = T)$Time)
    raw$fulltime[1] <- as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())
  }

  if(metaData){
    epoch <- data.frame(strsplit(metadata$meta_epoch,":"), stringsAsFactors = FALSE)
    epoch_hour <- as.integer(epoch[1,1])
    epoch_minute <- as.integer(epoch[2,1])
    epoch_second <- as.integer(epoch[3,1])

    epoch <- (epoch_hour*60*60)+(epoch_minute*60)+epoch_second
  } else {
    dtNext <- paste(read.csv(file_location, nrows = 2, header = T)$Date[2],read.csv(file_location, nrows = 2, header = T)$Time[2])
    raw$fulltime[2] <- as.POSIXct(dtNext,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""), tz = Sys.timezone())
    epoch <- raw$fulltime[2]-raw$fulltime[1]
  }

  start_time <- as.POSIXct(start,format=paste("%",format[1,1],"/%",format[2,1],"/%",format[3,1]," %H:%M:%S", sep=""))
  raw$fulltime <- sapply(1:nrow(raw), function(x) start_time+epoch*(x-1))

  if(metaData){
    mode <- as.integer(metadata$meta_mode)+1
  }
  else {
    mode <- 1
  }
  rows <- actigraph_mode_columns(mode)

  colnames(raw) <- c(rows,"fulltime")

  return(raw)
}


dt_acc_nonwear <- function(file_location, nhanes = TRUE, dataTable = FALSE, metaData = TRUE){
  if(metaData){
    acc_metadata <- dt_actigraph_metadata(file_location)
  }
  acc_raw <- dt_actigraph_raw(file_location, dataTable, metaData)

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
  acc_raw[, non_wear_length := dt_bout_sequence(acc_raw[,non_wear],acc_raw[,non_wear_break],epoch)]
  acc_raw[, non_wear_new := ifelse((non_wear_length <= 120
                                    & non_wear == 0
                                    & Activity < nhanes_break),1
                                   ,non_wear)]
  acc_raw[, non_wear_new_break := as.integer(!non_wear_new)]
  acc_raw[, non_wear_length_new := dt_bout_sequence(non_wear_new, non_wear_new_break, epoch)]
  acc_raw[, non_wear_bout := as.integer(non_wear_length_new > 3600 & non_wear_new == 1)]
  setnames(acc_raw,"non_wear_bout","nonwear")
  acc_raw[ , c("non_wear","non_wear_break",
               "non_wear_length", "non_wear_new",
               "non_wear_new_break","non_wear_length_new") := NULL]
  acc_raw[, wear := as.integer(!nonwear)]

  acc_raw[, fulldate := as.Date(as.character(as.POSIXct(fulltime, origin = "1970-01-01", tz = Sys.timezone())))]
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


acc_nonwear <- function(file_location, nhanes = TRUE, dataTable = FALSE, metaData = TRUE){
  if(metaData){
    acc_metadata <- actigraph_metadata(file_location)
  }
  acc_raw <- actigraph_raw(file_location, dataTable, metaData)

  if(metaData){
  epoch <- data.frame(strsplit(acc_metadata$meta_epoch,":"), stringsAsFactors = FALSE)
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

  acc_raw$non_wear <- ifelse(acc_raw$Activity == 0,1,0)
  acc_raw$non_wear_break <- as.integer(!acc_raw$non_wear)
  acc_raw$non_wear_length <- bout_sequence(acc_raw$non_wear,acc_raw$non_wear_break, epoch)
  acc_raw$non_wear_new <- sapply(1:nrow(acc_raw), function(x) ifelse((acc_raw$non_wear_length[x] <= 120
                                                                      && acc_raw$non_wear[x] == 0
                                                                      && acc_raw$Activity[x] < nhanes_break),1
                                                                     ,acc_raw$non_wear[x]))
  acc_raw$non_wear_new_break <- as.integer(!acc_raw$non_wear_new)
  acc_raw$non_wear_length_new <- bout_sequence(acc_raw$non_wear_new,acc_raw$non_wear_new_break, epoch)
  acc_raw$non_wear_bout <- as.integer(acc_raw$non_wear_length_new > 3600 & acc_raw$non_wear_new == 1)
  acc_raw$nonwear <- acc_raw$non_wear_bout
  acc_proc <- acc_raw[ , -which(names(acc_raw) %in% c("non_wear","non_wear_break",
                                                    "non_wear_length", "non_wear_new",
                                                    "non_wear_new_break","non_wear_length_new",
                                                    "non_wear_bout"))]
  acc_proc$wear <- as.integer(!acc_proc$nonwear)

  acc_proc$fulldate <- as.Date(as.character(as.POSIXct(acc_proc$fulltime, origin = "1970-01-01", tz = Sys.timezone())))
  valid_days <- data.frame(ddply(acc_proc,~fulldate,summarise,time=sum(wear)))
  if(epoch == 30){
    valid_days$valid_day <- as.integer(valid_days$time > 1200)
  } else if(epoch == 60){
    valid_days$valid_day <- as.integer(valid_days$time > 600)
  }
  valid_days$valid_day_sum <- sum(valid_days$valid_day)
  valid_days <- valid_days[,-2]
  acc <- join(acc_proc,valid_days, by="fulldate", type = "left")
  acc$epoch <- epoch

  return(acc)
}


dt_acc_ageadjusted <- function(folder_location, age_data_file, nhanes_nonwear = TRUE, id_length = 7, dataTable = FALSE, metaData = TRUE){
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  file_locations <- list.files(folder_location, full.names = TRUE, pattern = "\\.csv$")
  file_ids <- list.files(folder_location, full.names = FALSE, pattern = "\\.csv$")
  acc_vars<- c("Activity", "Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")

  acc_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                   , total = length(file_ids)/(cores-1), clear = FALSE, width = 60)

  acc_full <- foreach(i=1:length(file_ids), .combine=rbind,.packages=c("data.table","plyr","progress","ActiPro")) %dopar% {
    acc_hold <- NULL
    acc_hold <- dt_acc_nonwear(file_locations[i], nhanes = nhanes_nonwear, dataTable, metaData)
    for(var in acc_vars){
      if(is.na(match(var,colnames(acc_hold)))){
        acc_hold[,var] <- NA_integer_
      }
    }
    acc_hold$file_id <- file_ids[i]
    acc_progress$tick()
    acc_hold
  }

  acc_full[, id := tolower(substr(file_id,1,id_length))]

  age_data <- fread(age_data_file, stringsAsFactors = FALSE, colClasses=c(rep("character",2)))
  colnames(age_data) <- c("id","age")
  age_data[, age := as.integer(age)]
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

  acc_full_age[, divider := ifelse(epoch == 30, 2,1)]

  acc_full_age[, sed := as.integer(wear == 1 & Activity < (100/divider))]
  acc_full_age[, vig := as.integer(Activity > (div_vig/divider))]
  acc_full_age[, mod := as.integer(vig != 1 & Activity > (div_mod/divider))]
  acc_full_age[, light := as.integer(wear == 1 & sed != 1 & mod != 1 & vig != 1)]

  acc_full_age[, string_time := as.character(as.POSIXct(fulltime, origin = "1970-01-01", tz = Sys.timezone()))]

  stopCluster(cl)
  return(acc_full_age)
}


## Function
# Input: Folder of accelerometer files, age data file (ID and age only), NHANES yes/no

acc_ageadjusted <- function(folder_location, age_data_file, nhanes_nonwear = TRUE, id_length = 7, dataTable = FALSE, metaData = TRUE){
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  file_locations <- list.files(folder_location, full.names = TRUE, pattern = "\\.csv$")
  file_ids <- list.files(folder_location, full.names = FALSE, pattern = "\\.csv$")
  acc_vars<- c("Activity", "Axis 2","Axis 3", "Steps", "HR","Lux","Incline Off","Incline Standing", "Incline Sitting", "Incline Lying")

  acc_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                   , total = length(file_ids)/(cores-1), clear = FALSE, width = 60)

  acc_full <- foreach(i=1:length(file_ids), .combine=rbind,.packages=c("plyr","progress","ActiPro")) %dopar% {
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
  acc_full$id <- tolower(substr(acc_full$file_id,1,id_length))

  age_data <- read.csv(age_data_file, stringsAsFactors = FALSE, colClasses=c(rep("character",2)))
  colnames(age_data) <- c("id","age")
  age_data$age <- as.integer(age_data$age)
  age_data$id <- tolower(age_data$id)
  age_data$age[age_data$age > 17] <- 18 #Currently handles all adults as equals


  age <- c(6,7,8,9,10,11,12,13,14,15,16,17,18)
  div_mod <- c(1400, 1515,1638,1770,1910,2059,2220,2393,2580,2781,3000,3239,2020)
  div_vig <- c(3758,3947,4147,4360,4588,4832,5094,5375,5679,6007,6363,6751,5999)
  age_acc <- data.frame(age,div_mod,div_vig)

  age_merge <- join(age_data,age_acc, by="age", type = "inner")

  age_merge$div_mod <- sapply(1:nrow(age_merge), function(x) {
    if(age_merge$age[x]>17){
      2020
    } else {
      age_merge$div_mod[x]
    }
  })
  age_merge$div_vig <- sapply(1:nrow(age_merge), function(x) {
    if(age_merge$age[x]>17){
      5999
    } else {
      age_merge$div_vig[x]
    }
  })

  acc_full_age <- join(acc_full,age_merge, by="id", type = "inner")
  acc_full_age$divider <- ifelse(acc_full_age$epoch==30,2,1)

  acc_full_age$sed <- as.integer(acc_full_age$wear == 1 & acc_full_age$Activity < (100/acc_full_age$divider))
    #ifelse(acc_full_age$wear == 1 && acc_full_age$Activity < (100/acc_full_age$divider),1,0)
  acc_full_age$vig <- as.integer(acc_full_age$Activity > (acc_full_age$div_vig/acc_full_age$divider))
    #ifelse(acc_full_age$Activity > (acc_full_age$div_vig/acc_full_age$divider),1,0)
  acc_full_age$mod <- as.integer(acc_full_age$vig != 1 & acc_full_age$Activity > (acc_full_age$div_mod/acc_full_age$divider))
    #ifelse(acc_full_age$vig != 1,
     #                          ifelse(acc_full_age$Activity > (acc_full_age$div_mod/acc_full_age$divider),1,0),0)
  acc_full_age$light <- as.integer(acc_full_age$wear == 1 & acc_full_age$sed != 1 & acc_full_age$mod != 1 & acc_full_age$vig != 1)
    #ifelse(acc_full_age$sed != 1,
     #                          ifelse(acc_full_age$mod != 1,
      #                                ifelse(acc_full_age$vig != 1,1,0),0),0)

  acc_full_age$string_time <- as.character(as.POSIXct(acc_full_age$fulltime, origin = "1970-01-01", tz = Sys.timezone()))

  stopCluster(cl)
  return(acc_full_age)
}


dt_mvpa_bouts <- function(acc_ageadjusted, min_break, act_break, bout_length) {
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


mvpa_bouts <- function(acc_ageadjusted, min_break, act_break, bout_length) {
  cores=detectCores()
  if(cores[1]>2){
    cl <- makeCluster(cores[1]-1)
  } else {
    cores <- 2
    cl <- makeCluster(1)
  }
  registerDoParallel(cl)

  mvpa_acc <- acc_ageadjusted[,c("mod","vig","id","fulltime","fulldate","epoch")]
  mvpa_acc$mvpa <- as.integer(mvpa_acc$mod == 1 | mvpa_acc$vig == 1)
  mvpa_acc$mod <- NULL
  mvpa_acc$vig <- NULL
  mvpa_acc$mvpa_break <- as.integer(!mvpa_acc$mvpa)

  mvpa_ids <- unique(mvpa_acc$id)

  mvpa_progress <- progress_bar$new(format = "Processing [:bar] :percent eta: :eta elapsed time :elapsed"
                                   , total = length(mvpa_ids)/(cores-1), clear = FALSE, width = 60)

  mvpa_proc  <- foreach(i=1:length(mvpa_ids), .combine=rbind,.packages=c("plyr","progress","ActiPro")) %dopar% {
    mvpa_hold <- subset(mvpa_acc,mvpa_acc$id == mvpa_ids[i])
    epoch <- mvpa_hold$epoch[1]

    mvpa_hold$mvpa_length <- bout_sequence(mvpa_hold$mvpa,mvpa_hold$mvpa_break, epoch)
    mvpa_hold$mvpa_new <- sapply(1:nrow(mvpa_hold), function(x) ifelse((mvpa_hold$mvpa_length[x] <= min_break
                                                                        && mvpa_hold$mvpa[x] == 0
                                                                        && mvpa_hold$Activity[x] < act_break),1
                                                                       ,mvpa_hold$mvpa[x]))

    mvpa_hold$mvpa_new_break <- as.integer(!mvpa_hold$mvpa_new)
    mvpa_hold$mvpa_length_new <- bout_sequence(mvpa_hold$mvpa_new,mvpa_hold$mvpa_new_break, epoch)
    mvpa_hold$mvpa_bout <- as.integer(mvpa_hold$mvpa_length_new > bout_length & mvpa_hold$mvpa_new == 1)
    mvpa_progress$tick()
    mvpa_hold
  }
  stopCluster(cl)
  return(mvpa_proc$mvpa_bout)
}
