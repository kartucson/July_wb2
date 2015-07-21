### Author: Karthik Srinivasan
## 14th June 2015
# Function:  Integrate IEQ-HRV with Psy data (1 minute data and 1 hour survey Participant ID and Timestamp)

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

## Take current file, but later may need the updated file here

hrv_ieq <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\worktime_1min_withdemo.csv')

psy <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey.xlsx',1)

psy_wwn<- psy[,colnames(psy)%in% c("ID","Form","Form_start_date","space","closest_space_num","caffeine")]
## Add the psy later

#t <- as.POSIXct(hrv_ieq$Timestamp,tz="gmt")

hrv_ieq$Timestamp <- as.POSIXct(hrv_ieq$Timestamp,tz="gmt")

psy_wwn_1 <- subset(psy_wwn,Form != "Missing")

psy_wwn_1$Timestamp <- round(as.POSIXct(psy_wwn_1$Form_start_date),"mins")

psy_wwn_1$ID <- as.factor(psy_wwn_1$ID)
hrv_ieq$ID <- as.factor(hrv_ieq$ID)

psy_wwn_2 <- psy_wwn_1[order(psy_wwn_1$ID,psy_wwn_1$Timestamp),]

hrv_ieq_2 <- hrv_ieq[order(hrv_ieq$ID,hrv_ieq$Timestamp),]

# Merge psy and hrv_ieq

hip <- merge(hrv_ieq_2,psy_wwn_2,by = c("Timestamp","ID"),all.x=TRUE,all.y=TRUE)  

write.csv(hip,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\psy_hrv_ieq.csv')

### Yet to do: pad (for previous 15 minutes, as caffeine consumption values as current value!)


## Work ahead (15 June)
## Joined dataset has 4095 - 3925 values as only Psy and not HRV/IEQ data. Investigate
### Logic to add values to the PSy columns (last 5 minutes ?)
#### Or Psy factors can vary only across Level -3 (hours) and be fixed factors {obviously}
##### How to define levels in Hours, 

