### Author: Karthik Srinivasan
## 1st June 2015
# Function:  Integrate wearable IEQ & HRV (1 minute data based on Participant ID and Timestamp)

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

P_ID_lookup <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Participant_information.xlsx",sheetName = "Finished_participants")

ieq_in <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min.csv')

phy_out <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Phy\\5min_time_expanded\\phy_5min.csv")

ieq_in$Date <- as.Date(ieq_in$Timestamp)

#temp3 <- ieq_in[sort(ieq_in$Timestamp),]

## Assign P_ID based on lookup (Participant on a particular day)
ieq_part <- merge(ieq_in,P_ID_lookup,by = c("WB2_ID","Date"))

#temp<- merge(ieq_in,P_ID_lookup,by.x = c("WB2_ID","device_ID"),by.x = c("WB2_ID","Aclima_node"),join="inner")

ieq_part_trim <- na.omit(ieq_part)

ieq_d <- ieq_part_trim[,colnames(ieq_part_trim)%in% c("ID","CO2","Timestamp","WB2_ID","device_ID")]

ieq_dat <- ieq_d[order(ieq_d$ID,ieq_d$Timestamp),]
write.csv(ieq_dat,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min_participant.csv')

#ieq_part_trim2 <- subset(ieq_part,! ieq_part$CO2 =="NA") 

## Join IEQ and Phy based on timestamp and P_ID

ieq_dat <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min_participant.csv')

colnames(ieq_dat)
colnames(phy_out)

ieq_dat$Timestamp <- as.POSIXct( strptime(ieq_dat$Timestamp, "%m/%d/%Y %H:%M"))
phy_out$Timestamp <- as.POSIXct(phy_out$Timestamp)

phy_out$Timestamp <- round(phy_out$Timestamp , "mins")
ieq_dat$Timestamp <- round(ieq_dat$Timestamp , "mins")

phy_out$P_ID <- as.factor(phy_out$P_ID)
ieq_dat$ID <- as.factor(ieq_dat$ID)

phy_ieq <- merge(ieq_dat,phy_out,by.x = c("Timestamp","ID"),by.y = c("Timestamp","P_ID") )

temp <- merge(ieq_dat,phy_out,by.x = c("Timestamp","ID"),by.y = c("Timestamp","P_ID") )

phy_ieq <- phy_ieq[order(phy_ieq$ID,phy_ieq$Timestamp),]

write.csv(phy_ieq,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\HRV_IRQ_1min.csv')

phy_ieq_NN <- na.omit(phy_ieq) 
write.csv(phy_ieq_NN,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\HRV_IRQ_NotNull.csv')


mayjune <- dbConnect(MySQL(),
                    user = 'root',
                    password = 'insite112',
                    host = 'localhost',
                    dbname='IEQUA')

dbWriteTable(conn = mayjune, name = 'ieq_phy_with_null', value = as.data.frame(phy_ieq_NN))

dbWriteTable(conn = mayjune, name = 'ieq_phy_no_null', value = as.data.frame(phy_ieq))

dbDisconnect(mayjune)


# Null data
describe(phy_ieq_with_null)

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

## Keep only Morning, afternoon and evening

data_work <- subset(data_time,data_time$ToD == "Morning" | data_time$ToD == "Afternoon" | data_time$ToD == "Evening")
write.csv(data_work,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\worktime_1min.csv')

## Append participant information 

Part_demo <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\Psy_intake_work_doc.xlsx",1)

Part_demo_trunc <- Part_demo[,colnames(Part_demo) %in% c("ID","Gender","Ethnicity","Age",
"Experience","BMI")]

data_work_with_demo <- merge(data_work,Part_demo_trunc,by = c("ID"))

## Discretize stuff 
write.csv(data_work_with_demo,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\worktime_1min_withdemo.csv')

#####################
## For Bayesian #####
#####################

data_BN_c <- data_work_with_demo[,colnames(data_work_with_demo) %in% c("ID","Timestamp",
                                      "CO2","SDNN","Activity","Time","ToD","DoW","Ethnicity",
                                      "Age","Experience","BMI","Gender")]

# data_BN_c$SDNN <- data_work_with_demo$SDNN

# Discretize #

bin_var <- function(var)
    {
      cutoff<-quantile(var,(0:4)/4)
      var_cat <- vector()
      for(i in 1:length(var))
      {
        if (var[i] < cutoff[2]) {var_cat[i] ="Very_Low"} else 
          if (var[i] < cutoff[3] & var[i] >= cutoff[2]) {var_cat[i] ="Low"} else 
            if (var[i] < cutoff[4] & var[i] >= cutoff[3]) {var_cat[i] ="High"} else
            {var_cat[i] ="Very_High"}     
      }
      return(var_cat)
    }

### Another alternative is to use the discretize function (same thing) with breaks = 3, "interval" method for median


data_BN_num <- data_BN_c[,colnames(data_BN_c)%in% c("CO2","Activity","SDNN")]

data_discrete <- data_BN_num

for (j in 1:ncol(data_BN_num))
{
  data_discrete[,j] <- as.factor(bin_var(data_BN_num[,j]))   
}

## Age ##

for(i in 1:nrow(data_BN_c))
{
  if(data_BN_c$Age[i] < 30){data_BN_c$Age_gr[i]= "twenties" } else
    if(data_BN_c$Age[i] >= 30 & data_BN_c$Age[i] < 40){data_BN_c$Age_gr[i]= "thirties" } else
    if(data_BN_c$Age[i] >= 40 & data_BN_c$Age[i] < 50){data_BN_c$Age_gr[i]= "forties" } else
    if(data_BN_c$Age[i] >= 50 & data_BN_c$Age[i] < 60){data_BN_c$Age_gr[i]= "fifties" } else
    {data_BN_c$Age_gr[i]= "senior" }   
}    

## BMI ##

'''
http://www.nhlbi.nih.gov/health/educational/lose_wt/BMI/bmicalc.htm
Underweight = <18.5
Normal weight = 18.5–24.9 
Overweight = 25–29.9 
Obesity = BMI of 30
'''

for(i in 1:nrow(data_BN_c))
{
  if(data_BN_c$BMI[i] < 18.5){data_BN_c$BMI_gr[i]= "underweight" } else
    if(data_BN_c$BMI[i] >= 18.5 & data_BN_c$BMI[i] < 25){data_BN_c$BMI_gr[i]= "normal" } else
      if(data_BN_c$BMI[i] >= 25 & data_BN_c$BMI[i] < 30){data_BN_c$BMI_gr[i]= "overweight" } else
        {data_BN_c$BMI_gr[i]= "obese" }   
} 

data_BN <- data.frame(data_discrete,ToD = data_BN_c$ToD,DoW = data_BN_c$DoW,Ethnicity = data_BN_c$Ethnicity
                      ,Gender = data_BN_c$Gender,Age_group = data_BN_c$Age_gr,BMI_lev = data_BN_c$BMI_gr)    

write.csv(data_BN,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\data_BN.csv')
