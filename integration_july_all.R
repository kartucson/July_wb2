## We have not created a modular program and created a data processing sequence in this special case

### Author: Karthik Srinivasan
## 20th July 2015
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

colnames(ieq_CO2_press) <- c("Timestamp","Pressure","CO2","Temperature","Sound","device_ID","WB2_ID")

ieq_CO2_press$Timestamp <- as.POSIXct(ieq_CO2_press$Timestamp,tz="GMT")

#write.csv(ieq_CO2_press,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_july_1min.csv')

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

write.csv(phy_2,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch4\\phy_5min.csv")
#write.csv(phy_min,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch3\\phy_5min_exp.csv")

phy_2_b <- phy_2
phy_min_b <- phy_min

### Additional phy data, just append to current dataset:

phy_2 <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch4\\phy_5min.csv")
phy_min <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch3\\phy_5min_exp.csv")

phy_min$Timestamp <- as.POSIXct(phy_min$Timestamp)
phy_2$Timestamp <- as.POSIXct(phy_2$Timestamp)

###### hrv-ieq integration ####
###############################

## The P_ID is updated periodically from the Finished_participants sheet maintained by Casey Lindberg

P_ID_lookup <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Participant_information.xlsx",sheetName = "Finished_participants")

# ieq_in <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min.csv')
# phy_out <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\5min_time_expanded\\phy_5min.csv")

ieq_in <- ieq_CO2_press
ieq_in$Date <- as.Date(ieq_in$Timestamp)

## Assign P_ID based on lookup (Participant on a particular day)
ieq_part <- merge(ieq_in,P_ID_lookup,by = c("WB2_ID","Date"))

# ieq_part_trim <- na.omit(ieq_part)
#ieq_d <- ieq_part_trim[,colnames(ieq_part_trim)%in% c("ID","CO2","Timestamp","WB2_ID","device_ID")]

ieq_d <- ieq_part[,colnames(ieq_part)%in% c("ID","CO2","Pressure","Temperature","Sound","Timestamp","Date","WB2_ID","Base_location","Day.")]
ieq_dat <- ieq_d[order(ieq_d$ID,ieq_d$Timestamp),]

#write.csv(ieq_dat,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min_participant.csv')
#ieq_part_trim2 <- subset(ieq_part,! ieq_part$CO2 =="NA") 

## Join IEQ and Phy based on timestamp and P_ID

phy_out <- phy_2

colnames(ieq_dat)
colnames(phy_out)

#ieq_dat$Timestamp <- as.POSIXct( strptime(ieq_dat$Timestamp, "%m/%d/%Y %H:%M"))
phy_out$Timestamp <- as.POSIXct(phy_out$Timestamp)

phy_out$Timestamp <- round(phy_out$Timestamp , "mins")
ieq_dat$Timestamp <- round(ieq_dat$Timestamp , "mins")

phy_out$P_ID <- as.factor(phy_out$P_ID)
ieq_dat$ID <- as.factor(ieq_dat$ID)

phy_ieq <- merge(ieq_dat,phy_out,by.x = c("Timestamp","ID"),by.y = c("Timestamp","P_ID") )
phy_ieq <- phy_ieq[order(phy_ieq$ID,phy_ieq$Timestamp),]

write.csv(phy_ieq,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\HRV_IEQ_july_1min.csv')

### 22nd July

phy_ieq <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\HRV_IEQ_july_1min.csv')

phy_ieq$ID <- as.factor(phy_ieq$ID)
phy_ieq$Timestamp <- as.POSIXct(phy_ieq$Timestamp)
phy_ieq$Date <- as.Date(phy_ieq$Date)

## Filter out as per the pickup, drop-off, and removed time duration ##

part_time <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\pickup_drop_of_sensor_times.xlsx',sheetName="all_p")
#colnames(part_time)

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

part_wears <- merge(phy_ieq,part_df,by=c("ID","Date"),all.x=TRUE)

part_wears <- part_wears[order(part_wears$ID,part_wears$Timestamp),]

# write.csv(part_wears,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\time_filters_completed_data.csv')
##
#part_wears$Timestamp <- as.POSIXct(part_wears$Timestamp)
part_wears$Time <- format(part_wears$Timestamp,"%H:%M")
part_wears$Time <- as.POSIXct(strptime(part_wears$Time, "%H:%M"))

#part_wears$Time <- as.POSIXct(strptime(part_wears$Timestamp, "%H:%M"))
#part_wears$Time <- as.POSIXct(part_wears$Timestamp, format="%H:%M")

d1 <- subset(part_wears,part_wears$Time_in < part_wears$Time & part_wears$Time_out > part_wears$Time)

## Too high SDNN  ####
data_pruned <- subset(d1,d1$SDNN < 180 & d1$SDNN > 0) ## Filters as well as removes NA
data_outliers <- subset(d1,d1$SDNN >= 180)
data_missing <- subset(d1,d1$SDNN <= 0 | is.na(d1$SDNN))

#write.csv(data_pruned,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\all_data_june17.csv')
### Check the HLM model in the next module hlm_june.R

## To work this out later
d2 <- subset(d1,!(d1$Tin1 > d1$Time & d1$Tout1 < d1$Time) | is.na(d1$Tin1) | is.na(d1$Tout1))
d2 <- subset(d1,d1$Tin1 < d1$Time | d1$Tout1 > d1$Time)
d3 <- subset(d2,!(d2$Tin2 < d2$Time & d2$Tout2 > d2$Time)) 

## Generate ToD and DoW

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

Part_demo <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\Psy_intake_work_doc.xlsx",sheetName = "All_trim")

data_with_demo <- merge(data_time,Part_demo,by = c("ID"),all.x=TRUE)

### (b) Merge psy information

#psy <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey_july.xlsx',sheetName="All")

psy <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey_july_trim.csv')

psy_wwn <- psy

psy_wwn_1 <- subset(psy_wwn,Form != "Missing")
psy_wwn_1$Timestamp <- round(as.POSIXct(strptime(psy_wwn_1$Form_start_date, "%m/%d/%Y %H:%M")),"mins")  #
psy_wwn_1$ID <- as.factor(psy_wwn_1$ID)
psy_wwn_2 <- psy_wwn_1[order(psy_wwn_1$ID,psy_wwn_1$Timestamp),]

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

for (i in 1:nrow(data_padded))
{
  if (!is.na(data_padded$caffeine[i]))
  {
    #for (t in 38:42)   ### These are the columns with psy variables that 'need' padding
    for (t in (ncol(data_padded) - ncol(psy_wwn_2) +1):ncol(data_padded))   ### These are the columns with psy variables that 'need' padding
    { 
      for (r in 1:30)
      {
        if (i > r)       ### Logic begins only after 30 minutes
        {
          #if(!is.na(data_padded$caffeine[i-r]) | data_padded$ID[i-r] != data_padded$ID[i] )
          # | difftime data_padded$Timestamp[100],data_padded$Timestamp[10])) > 10
          if(data_padded$ID[i-r] != data_padded$ID[i]  )
          { data_padded$flag[i-r] <- "flag" } 
          else
          { data_padded[i-r,t] <- data_padded[i,t] 
            data_padded$flag[i-r] <- "OK"   ## Pads the previous 30 minutes
          }
        }
      }
    }
  }
}

## FLAW IN THIS CODE, as disconnected points also get padded (that is, need to check for subsequent points > 30 minutes
## difftime condition)

write.csv(data_padded,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\psy_july_padded.csv')

## Adding spatial data information 

spatial_char <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Spatial_char.xlsx",sheetName = "All_july")
spatial_char$space <- tolower(spatial_char$Space)

data_padded$Space <- data_padded$space
data_padded$space <- tolower(data_padded$Space)

data_with_space <- merge(data_padded,spatial_char,by.x = c("space"),by.y = c("space"),all.x=TRUE)

data_with_space <- data_with_space[order(data_with_space$ID, data_with_space$Timestamp),]

write.csv(data_with_space,na="",'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\data_wb2_C1_all.csv')

## Need to clean 'space' from both the datasets before merging manually!!

### DATA PRUNING ####

## Remove P106 as of now: 16th June 2015

#data_with_space_old <- data_with_space
#data_with_space <- subset(data_with_space,data_with_space$ID != 106)

### Sheet with time of pickup, drop and interim breaks ####

#Keep only Morning, afternoon and evening for filtering office hours
#data_in_office <- subset(data_with_space,data_with_space$ToD == "Morning" | data_with_space$ToD == "Afternoon" | data_with_space$ToD == "Evening")


