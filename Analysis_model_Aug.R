## GSA WB2 project, INSITE Data Analysis team
## Overall integration, code taken from Analysis_model.R and adding into database each time data is taken ** 

## We have not created a modular program and created a data processing sequence in this special case

### Author: Karthik Srinivasan
## 17th August 2015
# Function:  Integrate wearable IEQ & HRV (1 minute data based on Participant ID and Timestamp)
### This module should ideally integrate the entire data in the following steps:
###### Prepare IEQ & HRV data as required
##### (a) IEQ + HRV = 1min
##    (b) 1 min + Participants = part
###   (c) part + space = part_space
####  (d) part_space + psy = data_all
### Code should be generic to include modifications and updates in the future
#### Take all the variables and then subset at each stage (remove items of psy measures)

## One added thing is aggregation to whatever levels

Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre7') 
library(rJava)
library(xlsx)
library(reshape)
library(xts)
library(memisc)
library(DataCombine)
library(RMySQL)
library(MASS)
library(klaR)
library(psych)
library(Hmisc)
library(multilevel)
library(lme4)
library(nortest)
library(lmtest)
library(nlme)
library(catnet)
library(e1071)
library(randomForest)
library(rpart)


setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Aclima Wearnode\\processed\\Wearnodes_CO2_Pr_Temp_RH_Sound_minutely")
ieq_part_1 <- list()
temp = list.files(pattern="*.csv")

### Read the files into list of dataframes ##

system.time(
  for (i in 1:length(temp)) 
  {
    all_content = readLines(temp[i])
    skip_second = all_content[-c(1,2)]
    ieq_part_1[[i]] <- read.csv(textConnection(skip_second), header = FALSE)
  }
)

device_ids <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\device_info_2.xlsx",sheetName = "device_info")

### label column

for (i in 1: nrow(device_ids))
{
  ieq_part_1[[i]]$device_ID <- rep(device_ids[i,2],nrow(ieq_part_1[[i]]))
  ieq_part_1[[i]]$WB2_ID <- rep(device_ids[i,1],nrow(ieq_part_1[[i]]))
}


ieq_CO2_press <- ieq_part_1[[1]]

for(i in 2:length(temp))
{
  ieq_CO2_press <- rbind(ieq_CO2_press,ieq_part_1[[i]])
}

### Mention the column names carefully:

colnames(ieq_CO2_press) <- c("Timestamp","Pressure","Relative_humidity","Temperature","CO2","Sound","device_ID","WB2_ID")
ieq_CO2_press$Timestamp <- as.POSIXct(ieq_CO2_press$Timestamp,tz="GMT")
write.csv(ieq_CO2_press,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_july_1min.csv')

#### HRV dataset #########################################################
##################

append_files <- function()
{
  phy_part_1 <- list()
  temp = list.files(pattern="*.xls")
  
  for (i in 1:length(temp)) 
  {
    phy_part_1[[i]] <- read.xlsx(temp[i], 2)
    colnames(phy_part_1[[i]])[1] <- c("Timestamp")
  }
  
  p_ids <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\participant_IDs.csv",header=F)
  
  for (i in 1: nrow(p_ids))
  {
    phy_part_1[[i]]$P_ID <- rep(p_ids[i,1],nrow(phy_part_1[[i]]))
  }
  
  phy_all <- phy_part_1[[1]]
  
  for (i in 2: length(phy_part_1))
  {
    phy_all <- rbind(phy_all,phy_part_1[[i]])
  }
  
  return (phy_all)
  
}

# Better to run this in R rather than R Studio - Takes too much time

setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\5min")
system.time(  
  phy_5min <- append_files()
)

setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\Moving_window")
system.time(
  phy_5minmw <- append_files()
)

## The 5 minute data is flattened/time-expanded to 1 minute duration

#write.csv(phy_5minmw,"Moving_window.csv")
#write.csv(phy_5min,"5min.csv")

phy_1 <-phy_5min
phy_1$P_ID<-as.factor(phy_1$P_ID)
phy_part <- split(phy_1,phy_1$P_ID)

phy_part_min <- list()

to_min <- function(data_in){
  data_o <- TimeExpand(data_in,TimeVar='Timestamp',by='min')
  h <- data_o
  h[,!colnames(h)%in% c("Timestamp")] <-na.locf(h[,!colnames(h)%in% c("Timestamp")])
  h$P_ID<-as.factor(h$P_ID)
  data_out <- h 
  return(data_out)
}

for(i in 1:length(phy_part))
{
  phy_part_min[[i]] <- to_min(phy_part[[i]])
}

phy_1mmm <- phy_part_min[[1]]

for (i in 2: length(phy_part_min))
{
  phy_1mmm <- rbind(phy_1mmm,phy_part_min[[i]])
}

phy_min <- as.data.frame(cbind(rownames(phy_1mmm),phy_1mmm))
phy_min$Timestamp <- as.POSIXct(phy_min$Timestamp)

phy_2 <-phy_5minmw
phy_2$Timestamp <- as.POSIXct(phy_2$Timestamp)

write.csv(phy_2,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch5\\phy_5min.csv")
write.csv(phy_min,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch5\\phy_5min_exp.csv")

phy_2_b <- phy_2
phy_min_b <- phy_min

### Additional phy data, just append to current dataset:

phy_2 <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch5\\phy_5min.csv")
phy_min <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch5\\phy_5min_exp.csv")

phy_min$Timestamp <- as.POSIXct(phy_min$Timestamp)
phy_2$Timestamp <- as.POSIXct(phy_2$Timestamp)

###### hrv-ieq integration ####
###############################

## The P_ID is updated periodically from the Finished_participants sheet maintained by Casey Lindberg

hrv_ieq_int <- function(ieq_in, phy_out)
{
  P_ID_lookup <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Participant_information.xlsx",sheetName = "Finished_participants")
  
  # ieq_in <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min.csv')
  # phy_out <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\5min_time_expanded\\phy_5min.csv")
  
  
  ieq_in$Date <- as.Date(ieq_in$Timestamp)
  
  ## Assign P_ID based on lookup (Participant on a particular day)
  ieq_part <- merge(ieq_in,P_ID_lookup,by = c("WB2_ID","Date"))
  
  # ieq_part_trim <- na.omit(ieq_part)
  #ieq_d <- ieq_part_trim[,colnames(ieq_part_trim)%in% c("ID","CO2","Timestamp","WB2_ID","device_ID")]
  #ieq_d <- ieq_part[,colnames(ieq_part)%in% c("ID","CO2","Pressure","Temperature","Sound","Timestamp","Date","WB2_ID","Base_location","Day.")]
  ieq_d <- ieq_part
  ieq_dat <- ieq_d[order(ieq_d$ID,ieq_d$Timestamp),]
  
  #write.csv(ieq_dat,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min_participant.csv')
  #ieq_part_trim2 <- subset(ieq_part,! ieq_part$CO2 =="NA") 
  ## Join IEQ and Phy based on timestamp and P_ID
  #ieq_dat$Timestamp <- as.POSIXct( strptime(ieq_dat$Timestamp, "%m/%d/%Y %H:%M"))
  phy_out$Timestamp <- as.POSIXct(phy_out$Timestamp)
  
  phy_out$Timestamp <- round(phy_out$Timestamp , "mins")
  ieq_dat$Timestamp <- round(ieq_dat$Timestamp , "mins")
  
  phy_out$P_ID <- as.factor(phy_out$P_ID)
  ieq_dat$ID <- as.factor(ieq_dat$ID)
  
  phy_ieq <- merge(ieq_dat,phy_out,by.x = c("Timestamp","ID"),by.y = c("Timestamp","P_ID") )
  phy_ieq <- phy_ieq[order(phy_ieq$ID,phy_ieq$Timestamp),]
  
  return(phy_ieq)
}

phy_ieq_mw <- hrv_ieq_int(ieq_in = ieq_CO2_press,phy_out = phy_2)
phy_ieq_min <- hrv_ieq_int(ieq_in = ieq_CO2_press,phy_out = phy_min)

write.csv(phy_ieq_mw,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\july_integration\\HRV_IEQ_july_mw.csv')
write.csv(phy_ieq_min,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\july_integration\\HRV_IEQ_july_1min.csv')

gsa_p1 <- dbConnect(MySQL(),
                    user = 'root',
                    password = 'insite123',
                    host = 'localhost',
                    dbname='IEQUA')

dbWriteTable(conn = gsa_p1, name = 'phy_ieq_mw', value = as.data.frame(phy_ieq_mw))
dbWriteTable(conn = gsa_p1, name = 'phy_ieq_min', value = as.data.frame(phy_ieq_min))

d = fetch(dbSendQuery(gsa_p1, "SHOW COLUMNS FROM phy_ieq_mw;"),n=-1)

dbSendQuery(gsa_p1, "ALTER TABLE phy_ieq_mw MODIFY COLUMN `Timestamp` TIMESTAMP;")
dbSendQuery(gsa_p1, "ALTER TABLE phy_ieq_mw MODIFY COLUMN `ID` varchar(3);")
dbSendQuery(gsa_p1, "ALTER TABLE phy_ieq_mw ADD CONSTRAINT  pk_ID_timestamp PRIMARY KEY(ID,Timestamp);")

dbSendQuery(gsa_p1, "ALTER TABLE phy_ieq_min MODIFY COLUMN `Timestamp` TIMESTAMP;")
dbSendQuery(gsa_p1, "ALTER TABLE phy_ieq_min MODIFY COLUMN `ID` varchar(3);")
dbSendQuery(gsa_p1, "ALTER TABLE phy_ieq_min ADD CONSTRAINT  pk_ID_timestamp PRIMARY KEY(ID,Timestamp);")

#dbListTables(gsa_p1)

dbDisconnect(gsa_p1)

### 22nd July
# phy_ieq <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\HRV_IEQ_july_1min.csv')
phy_ieq$ID <- as.factor(phy_ieq$ID)
phy_ieq$Timestamp <- as.POSIXct(phy_ieq$Timestamp)
phy_ieq$Date <- as.Date(phy_ieq$Date)

## Filter out as per the pickup, drop-off, and removed time duration ##

time_in_filter <- function(data_in)
{
  part_time <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\pickup_drop_of_sensor_times.xlsx',sheetName="all_p")
  part_df <- data.frame(Date = part_time$Date, ID = part_time$ID,
                        Time_in = part_time$Time_in,Time_out = part_time$Time_out,
                        Tin1 = part_time$Tin1,Tout1 = part_time$Tout1,Tin2 = part_time$Tin2,Tout2 = part_time$Tout2) 
  
  convert_time <- function(var_in)
  {  
    d <- as.POSIXct(strptime(var_in, "%H:%M"))
    return(d)
  }
  
  for(j in 3:ncol(part_df))           ### Convert all the time variables from char to time format
  {
    part_df[,j] <- convert_time(part_df[,j])
  }
  
  part_df$ID <- as.factor(part_df$ID)
  
  # write.csv(part_df,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\pickup_drop_times.csv')
  
  #part_wears <- merge(phy_ieq,part_df,by=c("ID","Date"),all.x=TRUE)
  part_wears <- merge(data_in,part_df,by=c("ID","Date"),all.x=TRUE)
  part_wears <- part_wears[order(part_wears$ID,part_wears$Timestamp),]
  
  # write.csv(part_wears,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\time_filters_completed_data.csv')
  #part_wears$Timestamp <- as.POSIXct(part_wears$Timestamp)
  
  part_wears$Time <- format(part_wears$Timestamp,"%H:%M")
  part_wears$Time <- as.POSIXct(strptime(part_wears$Time, "%H:%M"))
  
  #part_wears$Time <- as.POSIXct(strptime(part_wears$Timestamp, "%H:%M"))
  #part_wears$Time <- as.POSIXct(part_wears$Timestamp, format="%H:%M")
  
  d1 <- subset(part_wears,part_wears$Time_in < part_wears$Time & part_wears$Time_out > part_wears$Time)
  
  ## Too high SDNN  ####
  #data_pruned <- subset(d1,d1$SDNN < 300 & d1$SDNN > 0) ## Filters as well as removes NA
  #data_outliers <- subset(d1,d1$SDNN >= 300)
  #data_missing <- subset(d1,d1$SDNN <= 0 | is.na(d1$SDNN))
  
  return(d1)
}

phy_ieq_mw_f <- time_in_filter(phy_ieq_mw)
phy_ieq_min_f <- time_in_filter(phy_ieq_min)


#append_other_info <- function(d1){
  ## Generate ToD and DoW
d1 <- phy_ieq_mw_f

  ToD_DoW_gen <- function(data_in)
  {
    t <- data_in
    #t$ToD <- t$Time
    for(i in 1:nrow(t))
    {
      if(t$Time[i] < as.POSIXct(strptime("8:00","%H:%M"))){t$ToD[i] = "Early_Morning" } else
        if (t$Time[i] >=  as.POSIXct(strptime("8:00","%H:%M")) & t$Time[i] <  as.POSIXct(strptime("12:00","%H:%M"))){t$ToD[i] = "Morning" } else  
          if (t$Time[i] >=  as.POSIXct(strptime("12:00","%H:%M")) & t$Time[i] <  as.POSIXct(strptime("15:00","%H:%M"))){t$ToD[i] = "Afternoon" } else
            if (t$Time[i] >=  as.POSIXct(strptime("15:00","%H:%M")) & t$Time[i] <  as.POSIXct(strptime("18:00","%H:%M"))){t$ToD[i] = "Evening" } else    
              if (t$Time[i] >=  as.POSIXct(strptime("18:00","%H:%M")) & t$Time[i] <  as.POSIXct(strptime("20:00","%H:%M"))){t$ToD[i] = "Late_Evening" } else {t$ToD[i] = "Night" }
    }
    # t$DoW <- weekdays(as.Date(t$Timestamp)) ## as.Date screws it up!!!
    t$DoW <- weekdays(t$Timestamp) 
    return(t)
  }
  
  data_time <- ToD_DoW_gen(d1)
  data_time$Time <- format(data_time$Time,"%H:%M")
  
  ## (a) Merge participant information 
Part_demo_t <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\Psy\\Psy_intake_work_doc.xlsx",sheetName = "All_trim")

P_ID_BL <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\Participant_information.xlsx",sheetName = "BL")
P_ID_BL <- na.omit(P_ID_BL)

Part_demo <- merge(Part_demo_t,P_ID_BL,by = c("ID"),all.x=TRUE)

write.csv(Part_demo,"Participant_demographics.csv")
temp <- merge(Part_demo_t,P_ID_BL,by = c("ID"),all.y=TRUE)

dbWriteTable(conn = gsa_p1, name = 'Part_demo', value = as.data.frame(Part_demo))
dbSendQuery(gsa_p1, "ALTER TABLE Part_demo MODIFY COLUMN `ID` varchar(3);")
dbSendQuery(gsa_p1, "ALTER TABLE Part_demo ADD CONSTRAINT  pk_ID PRIMARY KEY(ID);")

## Find aggregates for BL based on


# Right way: Instead of merging it here, join it in mySQL, and extract it back here 
data_with_demo <- merge(data_time,Part_demo,by = c("ID"),all.x=TRUE)
  
  ### (b) Merge psy information
  
  #psy <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey_july.xlsx',sheetName="All")
  
psy <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey_july_trim.csv')
  
  psy_wwn <- psy
  
  psy_wwn_1 <- subset(psy_wwn,Form != "Missing")
  psy_wwn_1$Timestamp <- round(as.POSIXct(strptime(psy_wwn_1$Form_start_date, "%m/%d/%Y %H:%M")),"mins")  #
  psy_wwn_1$ID <- as.factor(psy_wwn_1$ID)
  psy_wwn_2 <- psy_wwn_1[order(psy_wwn_1$ID,psy_wwn_1$Timestamp),]

## Store this in the database similar to IEQ_HRV:
dbWriteTable(conn = gsa_p1, name = 'esm_data', value = as.data.frame(psy_wwn_2))
dbSendQuery(gsa_p1, "ALTER TABLE esm_data MODIFY COLUMN `Timestamp` TIMESTAMP;")
dbSendQuery(gsa_p1, "ALTER TABLE esm_data MODIFY COLUMN `ID` varchar(3);")
dbSendQuery(gsa_p1, "ALTER TABLE esm_data ADD CONSTRAINT  pk_ID_timestamp PRIMARY KEY(ID,Timestamp);")

data_dem2 <- data_with_demo[order(data_with_demo$ID,data_with_demo$Timestamp),] 
  
  # Merge psy and data_with_demo
  ### Left join, since many participants data are not present in the hrv_ieq sheets yet
  
  #hip <- merge(data_with_demo,psy_wwn_2,by = c("Timestamp","ID"),all.x=TRUE,all.y=TRUE)  
  data_with_demo_caffeine <- merge(data_dem2,psy_wwn_2,by = c("Timestamp","ID"),all.x=TRUE)
  
  #write.csv(data_with_demo_caffeine,"temp.csv")
  
  ### Pad the psy data till last 30 minutes (Remember for HLM model, space included will reduce the dof because of these null values
  ### Better to have lesser datapoints rather than inaccurate space mapping: 
  ### LOGIC: person can be in same place as stated for lastt 30 minutes before survey rather than whole duration
  
  data_with_demo_caffeine <- data_with_demo_caffeine[order(data_with_demo_caffeine$ID, data_with_demo_caffeine$Timestamp),]
data_padded <- data_with_demo_caffeine
data_padded$flag <- NA
 
system.time(
  for (i in 1:nrow(data_padded))
  {
    if (!is.na(data_padded$Current_task[i]))
    {
      #for (t in 38:42)   ### These are the columns with psy variables that 'need' padding
      for (r in 1:15)
      {
      if (i > r)       ### Logic begins only after 30 minutes
      {
            #if(!is.na(data_padded$caffeine[i-r]) | data_padded$ID[i-r] != data_padded$ID[i] )
            # | difftime data_padded$Timestamp[100],data_padded$Timestamp[10])) > 10
        if(data_padded$ID[i-r] != data_padded$ID[i]  | data_padded$Date[i-r] != data_padded$Date[i] | data_padded$ID[i+r] != data_padded$ID[i]  | data_padded$Date[i+r] != data_padded$Date[i])  { 
          data_padded$flag[i] <- "flag" 
                                                        } 
        else
        {
          if(is.na(data_padded$Current_task[i-r]))
          { 
          for (t in (ncol(data_padded) - ncol(psy_wwn_2) +1):ncol(data_padded))   ### These are the columns with psy variables that 'need' padding
            {         
             data_padded[i-r,t] <- data_padded[i,t] 
              #data_padded$flag[i-r] <- "OK"   ## Pads the previous 30 minutes
            
           #   if(is.na(data_padded[i+r,t]))
           #   {
              data_padded[i+r,t] <- data_padded[i,t]
              #data_padded$flag[i+r] <- "OK"   ## Pads the current 30 minutes
           #   }
            }
          }
        }
      }
    }
  }
}
)
  
  ## FLAW IN THIS CODE, as disconnected points also get padded (that is, need to check for subsequent points > 30 minutes
  ## difftime condition)
#data_padded_old <- data_padded 

write.csv(data_padded,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\psy_august_padded.csv')
  
  ## Adding spatial data information 
  
#  spatial_char <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\Spatial_char.xlsx",sheetName = "All_july")
spatial_c <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\Spatial_char.xlsx",sheetName = "August")
spatial_c$space <- tolower(spatial_c$Space)

space_clusters <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\Spatial_char.xlsx",sheetName = "Space_clusters")
space_clusters$space <- tolower(space_clusters$Space)

space_label <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\Spatial_char.xlsx",sheetName = "Room_type_clean")

char_space <- merge(spatial_c,space_clusters,by.x = c("space"),by.y = c("space"),all.x=TRUE)

spatial_char <- merge(char_space,space_label,by.x = c("Room_type"),by.y = c("Room_type"),all.x=TRUE)

dbWriteTable(conn = gsa_p1, name = 'spatial_char', value = as.data.frame(spatial_char))
dbSendQuery(gsa_p1, "ALTER TABLE spatial_char MODIFY COLUMN `space` varchar(25);")
dbSendQuery(gsa_p1, "ALTER TABLE spatial_char ADD CONSTRAINT  pk_space PRIMARY KEY(space);")

  data_padded$Space <- data_padded$space
  data_padded$space <- tolower(data_padded$Space)
  
  data_with_space <- merge(data_padded,spatial_char,by.x = c("space"),by.y = c("space"),all.x=TRUE)
  data_with_space <- data_with_space[order(data_with_space$ID, data_with_space$Timestamp),]
  
#  return(data_with_space) }

#d1 <- phy_ieq_mw_f
#d1 <- phy_ieq_min_f

write.csv(data_with_space,na="",'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\archive\\july_integration\\17thAug_mw.csv')

data_full <- data_with_space

data_full$SDNN <- data_full[,c('SDNN..milisec..')]
data_full$nHF <- data_full[,c('HF....')]
data_full$Base_location <- data_full[,c('Base_location.x')]

data_full$Base_location <- factor(data_full$Base_location)
levels(data_full$Base_location)[levels(data_full$Base_location)=="G000"] <- "G0"

data_f <- subset(data_full,data_full$SDNN < 180 & data_full$SDNN > 0) ## Filters as well as r

#data_high <- subset(data_full,data_full$SDNN > 180) ## Filters as well as r
## Most of it is 115 on a fine Thursday 
# write.csv(data_f,"temp.csv")

discreta <- function(data_input)
{
  # data_input$Timestamp <- as.POSIXct(strptime(data_input$Timestamp, "%m/%d/%Y %H:%M"))
  
  data_mod <- data_input
  
  for (i in 1: nrow(data_mod) )
  {
    if(!is.na(data_mod$Temperature[i]))
    {  
      if (data_mod$Temperature[i] < 16 ) 
      {
        data_mod$Temperature_feel[i] <- "Cold"
      } else if (data_mod$Temperature[i] >= 16 && data_mod$Temperature[i] < 18 ) 
      {
        data_mod$Temperature_feel[i] <- "Moderate"
      } else if (data_mod$Temperature[i] >= 18 && data_mod$Temperature[i] < 22 ) 
      {
        data_mod$Temperature_feel[i] <- "Ideal" 
      } else if (data_mod$Temperature[i] >= 22 && data_mod$Temperature[i] < 24 ) 
      {
        data_mod$Temperature_feel[i] <- "Warm" 
      } else if (data_mod$Temperature[i] >= 24) 
      {
        data_mod$Temperature_feel[i] <- "Hot" 
      }
    }
    else { data_mod$Temperature_feel[i] <- NA}
  }
  
  data_mod$Temperature_feel <- as.factor(data_mod$Temperature_feel)
  
  data_in <- data_mod
  data_in$ID <- as.factor(data_in$ID)
  
  ## Discretize SDNN as a new variale:
  
  for(i in 1:nrow(data_in))
  {
    if(is.na(data_in$Age[i])){data_in$Age_gr[i] = NA} else
    {  
      if(data_in$Age[i] < 30){data_in$Age_gr[i]= "twenties" } else
        if(data_in$Age[i] >= 30 & data_in$Age[i] < 40){data_in$Age_gr[i]= "thirties" } else
          if(data_in$Age[i] >= 40 & data_in$Age[i] < 50){data_in$Age_gr[i]= "forties" } else
            if(data_in$Age[i] >= 50 & data_in$Age[i] < 60){data_in$Age_gr[i]= "fifties" } else
            {data_in$Age_gr[i]= "senior" }   
    }
  }    
  
  for(i in 1:nrow(data_in))
  {
    if(is.na(data_in$BMI[i])){data_in$BMI_gr[i] = NA} else
    { 
      if(data_in$BMI[i] < 18.5){data_in$BMI_gr[i]= "underweight" } else
        if(data_in$BMI[i] >= 18.5 & data_in$BMI[i] < 25){data_in$BMI_gr[i]= "normal" } else
          if(data_in$BMI[i] >= 25 & data_in$BMI[i] < 30){data_in$BMI_gr[i]= "overweight" } else
          {data_in$BMI_gr[i]= "obese" } 
    }
  } 
  
  return(data_in)
}

## FOr temperature, Age, BMI

system.time( all_variables <- discreta(data_f) )

all_variables$ToD <- factor(all_variables$ToD)
all_variables$DoW <- factor(all_variables$DoW)

levels(all_variables$ToD)[levels(all_variables$ToD)=="Afternoon"] <- "Mid-day"

names(all_variables)[names(all_variables)=="temperature"] <- "Perceived_temperature"
names(all_variables)[names(all_variables)=="Space"] <- "NN_space"

dbWriteTable(conn = gsa_p1, name = 'all_variables', value = as.data.frame(all_variables))
write.csv(all_variables,na="","Aug_data.csv")

## Take only what is needed & impute them

Val_arousal <- fa(all_variables[,colnames(dat)%in% c('tense','content','sad','alert','tired'	'happy','upset','calm')],
                  nfactors=2, rotate="varimax",fm="pa")

all_variables$space <- factor(all_variables$space)

data_var <- data.frame(Focus = all_variables$focused, Productive = all_variables$productive, ID = all_variables$ID, Cluster = all_variables$Cluster,
                       Base_location = all_variables$Base_location, Gender= all_variables$Gender,
                       ToD = all_variables$ToD,DoW = all_variables$DoW, Alcohol = all_variables$Alcohol,
                       BMI = all_variables$BMI_gr, Age = all_variables$Age_gr,
                       Health = all_variables$Health_condition,Current_task = all_variables$Current_task,
                       White_noise = all_variables$white.noise,  Window_distance = all_variables$Window_dist,
                       CO2=all_variables$CO2, Sound=all_variables$Sound, Pressure=all_variables$Pressure, 
                       Relative_humidity=all_variables$Relative_humidity, Temperature=all_variables$Temperature_feel,
                       SDNN =all_variables$SDNN,Room_type = all_variables$Space_label,
                       nHF = all_variables$nHF, space = all_variables$space,
                       Valence =  - round(Val_arousal$scores[,1],2),Arousal = round(Val_arousal$scores[,2],2),
                       Ethnicity = all_variables$Ethnicity)


## Here, basically, we have emotions, space info, person info, time info, hrv, ieq & we are inferring the following:
## MANOVA for BL,Room_type, Current_task on HRV, Arousal/Valence
## HLM for HRV & Arousal/Valence due to IEQ
## Focus specifically on CO2, Noise, temperature

# Brownie for Output behavior of specific participant impacting Noise, 

levels(data_var$ToD)[levels(data_var$ToD)=="Mid-day"] <- "Midday"

data_var$ToD <- as.character(data_var$ToD)
data_var$ToD <- factor(data_var$ToD)

data_back <- data_var

system.time(
  for(j in 1:ncol(data_var))
  {
    if(is.factor(data_var[,j]))
    {
      data_var[,j] <- factor(data_var[,j], levels = c(levels(data_var[,j]), "Absent_data"))  
      for(k in 1:nrow(data_var))
      {
        if(is.na(data_var[k,j]))
        {
          data_var[k,j] <- "Absent_data"
        }
      }
    data_var[,j] <- as.character(data_var[,j])  
    data_var[,j] <- factor(data_var[,j])  
    }
  }
)

write.csv(data_back,na="","Selected_Aug_no_absent_label.csv")
write.csv(data_var,na="","Selected_Aug_data.csv")

### Aggregate statistics ##

agg_function <- function(data_agg)
{
desc_list <- list()

for(j in 1:ncol(data_agg))
{
  if(is.factor(data_agg[,j]))
  {
  desc_list[[j]] <- list(Variable = colnames(data_agg)[j],table(data_agg[,j]))
  }
  else  
  {
  desc_list[[j]] <- data.frame(Variable = colnames(data_agg)[j],Mean = mean(data_agg[,j],na.rm=T),SD = sd(data_agg[,j],na.rm=T))
  }
}
return(desc_list)
}

data_agg_all <- agg_function(data_back)
data_agg_4400 <- agg_function(data_back[data_back$Base_location =="4400",])
data_agg_ROB <- agg_function(data_back[data_back$Base_location =="ROB",])
data_agg_G0 <- agg_function(data_back[data_back$Base_location =="G0",])


data_allvar_agg_all <- agg_function(all_variables)
data_allvar_agg_4400 <- agg_function(all_variables[all_variables$Base_location =="4400",])
data_allvar_agg_ROB <- agg_function(all_variables[all_variables$Base_location =="ROB",])
data_allvar_agg_G0 <- agg_function(all_variables[data_back$Base_location =="G0",])

data_allvar_agg_4400[[8]]
data_allvar_agg_4400[[26]]

t <- cbind(all_variables$Happy,all_variables$Productive,all_variables$Tired,all_variables$Focused,t$Base_location)
data_pall <- agg_function(t)

datas <- data_var

datas[,colnames(datas)%in% c('CO2','Sound','Pressure','Relative_humidity')] <- scale(as.data.frame(datas[,colnames(datas)%in% c('CO2','Sound','Pressure','Relative_humidity')]), center=TRUE, scale = FALSE)
datas[,colnames(datas)%in% c('CO2','Sound','Pressure','Relative_humidity')]  <- round(impute(datas[,colnames(datas)%in% c('CO2','Sound','Pressure','Relative_humidity')],mean),2)

## Null models:

Null.model1<-lme(SDNN~1,random=~1|ID,data=datas,na.action = na.exclude)

summary(Null.model1)
ICC1(aov(SDNN~ID,data=datas))  ## 43%

Null.model2<-lme(SDNN~1,random=~1|space,data=datas,na.action = na.exclude)
summary(Null.model2)
ICC1(aov(SDNN~space,data=datas)) # 35% 

Null.model3<-lme(SDNN~1,random=~1|Room_type,data=datas,na.action = na.exclude)
summary(Null.model3)
ICC1(aov(SDNN~Room_type,data=datas)) # 10% 

Null.model4<-lme(SDNN~1,random=~1|Base_location,data=datas, control=list(opt="optim"),na.action = na.exclude)
summary(Null.model4)
ICC1(aov(SDNN~Base_location,data=datas))  # 0.3% , but we still keep it

Null.model5<-lme(SDNN~1,random=~1|Current_task,data=datas,na.action = na.exclude)
summary(Null.model5)
ICC1(aov(SDNN~Current_task,data=datas)) # 2.5% 

Null.model6<-lme(SDNN~1,random=~1|Cluster,data=datas,na.action = na.exclude)
summary(Null.model6)
ICC1(aov(SDNN~Cluster,data=datas)) # 13.7% 


# Use datas for the outputs (imputation is more dangerous for the outputs)

'''
model.SDNN <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound
                 + Base_location + Gender + ToD + DoW  + BMI + Age,
                 control=list(opt="optim"),  random=list(ID=~ 1+ CO2 + Temperature + Sound 
                        + Age + BMI + Gender   
                        +   Pressure+Relative_humidity),data=datas,na.action = na.exclude)
summary(model.SDNN)


model.SDNN.imp <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound
                  + Base_location + Gender + ToD + DoW  + BMI + Age + Cluster + Alcohol + Current_task 
                  + White_noise + Window_distance + Room_type,
                  control=list(opt="optim"),  random=list(ID=~ 1+ CO2 + Temperature + Sound 
                                                          +Relative_humidity),data=data_imp,na.action = na.exclude)
summary(model.SDNN.imp)
'''
## Step 1: Add one by one random effect and see if AIC reduces!

+ CO2 + Temperature + Sound + Relative_humidity + Age + BMI 

model.1 <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound + Base_location,
                  control=list(opt="optim"),  random=list(ID=~ 1),data=datas,na.action = na.exclude)
summary(model.1)

# AIC = 319168.3

# CO2 = 318735
# Pressure = 318283
# Temperature = 318516
# Relative_humidity = 318671
# Sound = 318578

## Adding all random effects other than CO2 ##

system.time(
model.2 <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound + Base_location
              + Age + BMI, control=list(opt="optim"),  random=list(ID=~ 1 + Pressure + Temperature +
                                              Relative_humidity + Sound),data=datas,na.action = na.exclude)
)

summary(model.2)
# AIC = 317150
# Time = 242 seconds

system.time(
  model.3 <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound 
  + Cluster + Room_type + Current_task + BMI, control=list(opt="optim"),  
  random=list(ID=~ 1 + Pressure + Temperature + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)

# AIC = 316954
# Time = 556 seconds 
sink("Random-effects model results.txt")
summary(model.3)
unlink("Random-effects model results.txt")

system.time(
  model.4b <- lme(fixed = SDNN~1, control=list(opt="optim"),  
                 random=list(ID=~ 1),data=datas,na.action = na.exclude)
)
summary(model.4b)

system.time(
  model.4a <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound +
                   ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                 random=list(ID=~ 1 + Pressure + Temperature + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4a)

system.time(
  model.4 <- lme(fixed = SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound +
               ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                 random=list(ID=~ 1 + Pressure + Temperature + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4)

# AIC = 316375
# Time =  967 seconds 

system.time(
  model.5 <- lme(fixed = SDNN~Pressure + CO2 + Relative_humidity + Sound +
                Ethnicity  +   ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                 random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.5)

system.time(
  model.4nHF <- lme(fixed = nHF~Pressure + CO2 + Relative_humidity + Sound +
                   ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                 random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4nHF)
# 2000 seconds

system.time(
  model.4best <- lme(fixed = SDNN~Pressure + CO2 + Relative_humidity + Sound +
                   ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                 random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4best)


system.time(
  model.4Valence <- lme(fixed = Valence~Pressure + CO2 + Temperature + Relative_humidity + Sound +
                      ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                    random=list(ID=~ 1 + Pressure +  CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4Valence)

system.time(
  model.4Arousal <- lme(fixed = Arousal~Pressure + CO2 + Temperature + Relative_humidity + Sound +
                          ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                        random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4Arousal)

system.time(
  model.4Focus <- lme(fixed = Focus~Pressure + CO2 + Temperature + Relative_humidity + Sound +
                          ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                        random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4Focus)

system.time(
  model.4Productivity <- lme(fixed = Productive~Pressure + CO2 + Temperature + Relative_humidity + Sound +
                          ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                        random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4Productivity)

datas$happy <- all_variables$happy
datas$LF.HF <- all_variables$LF.HF
datas$tired <- all_variables$tired
datas$alert <- all_variables$alert

system.time(
  model.4Happy <- lme(fixed =  happy ~ Pressure + CO2 + Temperature + Relative_humidity + Sound +
                        ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                      random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4Happy)

system.time(
  model.4LF.HF <- lme(fixed =  LF.HF ~ Pressure + CO2 + Relative_humidity + Sound +
                        ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                      random=list(ID=~ 1 +  Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4LF.HF)


system.time(
  model.4tired <- lme(fixed =  tired ~ Pressure + CO2 + Temperature + Relative_humidity + Sound +
                        ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                      random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4tired)


system.time(
  model.4alert <- lme(fixed =  alert ~ Pressure + CO2 + Temperature + Relative_humidity + Sound +
                        ToD + DoW + Room_type + Current_task, control=list(opt="optim"),  
                      random=list(ID=~ 1 + Pressure + CO2 + Relative_humidity + Sound),data=datas,na.action = na.exclude)
)
summary(model.4alert)



# AIC = 316375
# Time =  967 seconds 

## Not of much use, so doing CDA instead

anova.SDNN <- anova(lm(SDNN ~ Base_location + Current_task+Room_type + Cluster + ToD + DoW +  Age + BMI + ID,datas))

anova.SDNN2 <- anova(lm(SDNN ~ Room_type + Cluster + ToD + DoW + Base_location + Current_task +  Age + BMI,datas))

anova.Valence <- anova(Valence ~ Base_location + Current_task+Room_type + Cluster + ToD + DoW +  Age + BMI,datas)
summary.aov(anova.Valence) 

anova.Arousal <- aov(Arousal ~ Base_location + Current_task+Room_type + Cluster + ToD + DoW +  Age + BMI,datas)
summary(anova.Arousal) 

### Discretize SDNN #############

sata_sdnn <- datas[,c('SDNN','nHF')]

sata_hrv_p <- split(sata_sdnn,datas$ID)

for(i in 1:length(sata_hrv_p))
{
  sata_hrv_p[[i]] <- cnDiscretize(sata_hrv_p[[i]],3,mode="quantile")
}  

sata_hrv_t <- sata_hrv_p[[1]]

for (i in 2:length(sata_hrv_p))
{
  sata_hrv_t <- rbind(sata_hrv_t,sata_hrv_p[[i]])
}


for(col in 1:ncol(sata_hrv_t))
{
  sata_hrv_t[,col] <- as.factor(sata_hrv_t[,col])
  levels(sata_hrv_t[,col]) <- c("High stress","Stress","Low stress")
}

datas$SDNN_d <- sata_hrv_t$SDNN
datas$nHF_d <- sata_hrv_t$nHF

write.csv(datas,"ML_dataset.csv")

impute_all <- function(data_in)
{
for(j in 1:ncol(data_in))
  {
    if(is.factor(data_in[,j]))
    {
      data_in[,j] <- factor(data_in[,j], levels = c(levels(data_in[,j]), "Absent_data"))  
      for(k in 1:nrow(data_in))
      {
        if(is.na(data_in[k,j]))
        {
          data_in[k,j] <- "Absent_data"
        }
      }
      data_in[,j] <- as.character(data_in[,j])  
      data_in[,j] <- factor(data_in[,j])  
    }
    else
    {
      data_in[,j]  <- as.numeric(round(impute(data_in[,j],mean),2))
    }  
    
  }
return(data_in)
}

system.time( data2 <- impute_all(datas) )

write.csv(data2,"ML_dataset_all_imputed.csv")

##### Use the ML methods ##### NB, RF, Perceptron (?), SVM, Logistic regression


datasd <- datas[,colnames(datas)%in% c('SDNN_d','Base_location','Gender','ToD','DoW',
        'CO2','Sound','Pressure','Relative_humidity','Temperature',
        'Room_type','BMI','Age','Current_task','White_noise')]


## Create training and test dataset  ####

'''
data_in <- datas[,colnames(datas)%in% c("Cluster","Base_location","Gender","ToD","DoW","Alcohol","BMI","Age",
                                        "Health","Current_task","White_noise","Window_distance","CO2",  
                                        "Sound","Pressure","Relative_humidity","Temperature","Room_type"
)]
data_out <- datas$SDNN_d
'''

set.seed(123)

smpsize <- floor(0.8 * nrow(datasd))
mltr  <- sample(seq_len(nrow(data_in)),size = smpsize,replace=FALSE)

mltrain <- as.data.frame(data_in[mltr,])
mltest <- as.data.frame(data_in[-mltr,])

write.csv(mltrain,"trainingset.csv")
write.csv(mltest,"testset.csv")


system.time( 
  nb <- naiveBayes(SDNN_d~.,data=datasd)
)
nb.predict.sdnn <- predict(nb,datasd)
table(nb.predict.sdnn,datasd$SDNN_d)

dt <- rpart(SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound + 
     ToD + DoW,data=datas)
dt.predict.Bill <- predict(dt,bntest)

data_rf <- na.omit(datas)

rf <- randomForest(SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound
                    + ToD + DoW,data=datas)
rf.predict.Bill <- predict(rf,bntest)

rf2 <- randomForest(SDNN~Pressure + CO2 + Temperature + Relative_humidity + Sound
            Age + BMI  + ToD + DoW,data=datas)

svm <- svm(data_out~.,data=data_in)
svm.predict.Bill <- predict(svm,bntest)




