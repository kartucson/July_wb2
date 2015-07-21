## We have not created a modular program and created a data processing sequence in this special case

### Author: Karthik Srinivasan
## 17th June 2015
# Function:  Integrate wearable IEQ & HRV (1 minute data based on Participant ID and Timestamp)
### This module should ideally integrate the entire data in the following steps:
###### Prepare IEQ & HRV data as required
##### (a) IEQ + HRV = 1min
##    (b) 1 min + Participants = part
###   (c) part + space = part_space
####  (d) part_space + psy = data_all
### Code should be generic to include modifications and updates in the future
#### Take all the variables and then subset at each stage (remove items of psy measures)

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

setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Aclima Wearnode\\processed\\temp_pressure_co2_minutely")
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

colnames(ieq_CO2_press) <- c("Timestamp","Pressure","CO2","Temperature","device_ID","WB2_ID")

ieq_CO2_press$Timestamp <- as.POSIXct(ieq_CO2_press$Timestamp,tz="GMT")

#write.csv(ieq_CO2_press,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_temp_1min.csv')

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

#write.csv(phy_2,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch3\\phy_5min.csv")
#write.csv(phy_min,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch3\\phy_5min_exp.csv")

phy_2_b <- phy_2
phy_min_b <- phy_min

### Additional phy data, just append to current dataset:

phy_2 <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\batch3\\phy_5min.csv")
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

ieq_d <- ieq_part[,colnames(ieq_part)%in% c("ID","CO2","Pressure","Temperature","Timestamp","Date","WB2_ID","Base_location","Day.")]
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

#write.csv(phy_ieq,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\HRV_IEQ_1min.csv')

## Conservative dataset (removing nulls) 
#write.csv(na.omit(phy_ieq),'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\HRV_IEQ_NotNull.csv')

## Analyze data with SDNN > 180
# Note phy_part has no data with SDNN > 180

#Add two variables: Day of week and Time & Time of day for convenience

phy_ieq$Time <- format(phy_ieq$Timestamp,"%H:%M")
phy_ieq$Time <- as.POSIXct(strptime(phy_ieq$Time, "%H:%M"))

#phy_ieq$Time_seconds <- as.numeric(phy_ieq$Time) - min(as.numeric(phy_ieq$Time))

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

data_time <- ToD_DoW_gen(phy_ieq)
data_time$Time <- format(data_time$Time,"%H:%M")

## (a) Merge participant information 

Part_demo <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\Psy_intake_work_doc.xlsx",sheetName = "trim")

#Part_demo_trunc <- Part_demo[,colnames(Part_demo) %in% c("ID","Gender","Ethnicity","Age",    
#                          "Experience","BMI")]

data_with_demo <- merge(data_time,Part_demo,by = c("ID"),all.x=TRUE)

### (b) Merge psy information

psy <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey.xlsx',1)
psy_wwn<- psy[,colnames(psy)%in% c("ID","Form","Form_start_date","space","preoccupation","closest_space_num","caffeine")]

psy_wwn_1 <- subset(psy_wwn,Form != "Missing")
psy_wwn_1$Timestamp <- round(as.POSIXct(psy_wwn_1$Form_start_date),"mins")  #
psy_wwn_1$ID <- as.factor(psy_wwn_1$ID)
psy_wwn_2 <- psy_wwn_1[order(psy_wwn_1$ID,psy_wwn_1$Timestamp),]

# Merge psy and data_with_demo
### Left join, since many participants data are not present in the hrv_ieq sheets yet

#hip <- merge(data_with_demo,psy_wwn_2,by = c("Timestamp","ID"),all.x=TRUE,all.y=TRUE)  
data_with_demo_caffeine <- merge(data_with_demo,psy_wwn_2,by = c("Timestamp","ID"),all.x=TRUE)

### Pad the psy data till last 30 minutes (Remember for HLM model, space included will reduce the dof because of these null values
### Better to have lesser datapoints rather than inaccurate space mapping: 
### LOGIC: person can be in same place as stated for lastt 30 minutes before survey rather than whole duration

data_with_demo_caffeine <- data_with_demo_caffeine[order(data_with_demo_caffeine$ID, data_with_demo_caffeine$Timestamp),]
data_padded <- data_with_demo_caffeine
data_padded$flag <- "z"

for (i in 1:nrow(data_padded))
{
  if (!is.na(data_padded$caffeine[i]))
  {
    #for (t in 38:42)   ### These are the columns with psy variables that 'need' padding
    for (t in 43:48)   ### These are the columns with psy variables that 'need' padding
    { 
      for (r in 1:30)
      {
        if (i > r)       ### Logic begins only after 30 minutes
        {
          if(!is.na(data_padded$caffeine[i-r]) | data_padded$ID[i-r] != data_padded$ID[i] )
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

### Extreme padding: Pad all the values to reduce missingness:

data_padded_interim <- data_padded

colnames(data_padded[,43:48])

data_padded$space_a <- data_padded$space
data_padded$preoccupation_a <- data_padded$preoccupation
data_padded$caffeine_a <- data_padded$caffeine

for (i in 1:nrow(data_padded))
{
  if (!is.na(data_padded$caffeine[i]))
  {
    #for (t in 38:42)   ### These are the columns with psy variables that 'need' padding
    for (t in 50:52)   ### These are the columns with psy variables that 'need' padding
    { 
      for (r in 1:100)
      {
        if (i > r)       ### Logic begins only after 30 minutes
        {
          if(!is.na(data_padded$caffeine[i-r]) | data_padded$ID[i-r] != data_padded$ID[i] )
          { data_padded$flag_a[i-r] <- "flag" } 
          else
          { data_padded[i-r,t] <- data_padded[i,t] 
            #data_padded$flag_a[i-r] <- "OK"   ## Pads the previous time_duration
          }
        }
      }
    }
  }
}



#write.csv(data_padded,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\psy_padded.csv')

## Adding spatial data information 

spatial_char <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Spatial_char.xlsx",sheetName = "V2")

data_with_space <- merge(data_padded,spatial_char,by.x = c("space"),by.y = c("Space"),all.x=TRUE)

#write.csv(data_with_space,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\with_space.csv')

### DATA PRUNING ####

## Remove P106 as of now: 16th June 2015

data_with_space_old <- data_with_space
data_with_space <- subset(data_with_space,data_with_space$ID != 106)


### Sheet with time of pickup, drop and interim breaks ####

#Keep only Morning, afternoon and evening for filtering office hours
data_in_office <- subset(data_with_space,data_with_space$ToD == "Morning" | data_with_space$ToD == "Afternoon" | data_with_space$ToD == "Evening")

## A more accurate method is to filter out as per the pickup, drop-off, and removed time duration ##

part_time <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\pickup_drop_till_138.xlsx',1)
#colnames(part_time)

part_df <- data.frame(Date = part_time$Day, ID = part_time$ID,BL = part_time$BL,Time_in = part_time$Time_in,Time_out = part_time$Time_out,Tin1 = part_time$Tin1,Tout1 = part_time$Tout1,Tin2 = part_time$Tin2,Tout2 = part_time$Tout2) 

convert_time <- function(var_in)
{  
  d <- as.POSIXct(strptime(var_in, "%H:%M"))
  return(d)
}

for(j in 4:ncol(part_df))           ### Convert all the time variables from char to time format
{
  part_df[,j] <- convert_time(part_df[,j])
}

part_df$ID <- as.factor(part_df$ID)

part_wears <- merge(data_in_office,part_df,by=c("ID","Date"))

part_wears <- part_wears[order(part_wears$ID,part_wears$Timestamp),]

# write.csv(part_wears,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\time_filters_completed_data.csv')
##

part_wears$Time <- as.POSIXct(strptime(part_wears$Time, "%H:%M"))

d1 <- subset(part_wears,part_wears$Time_in < part_wears$Time & part_wears$Time_out > part_wears$Time)
## (11291/14290 are to be seen here)

## Too high SDNN  ####
data_pruned <- subset(d1,d1$SDNN < 180 & d1$SDNN > 0)

data_outliers <- subset(d1,d1$SDNN >= 180)

#write.csv(data_pruned,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\all_data_june17.csv')

### Check the HLM model in the next module hlm_june.R

## To work this out later
d2 <- subset(d1,!(d1$Tin1 > d1$Time & d1$Tout1 < d1$Time))
d2 <- subset(d1,d1$Tin1 < d1$Time | d1$Tout1 > d1$Time)
& !(d1$Tin2 < d1$Time & d1$Tout2 > d1$Time) 
data_filtered_time

# Use na.action = na.exclude in the regression model (and then smoothen the prediction as much as possible)
# datetime <- as.POSIXlt(paste(yr, mo, dy, hr, mn), format = "%Y %m %d %H %M"))
