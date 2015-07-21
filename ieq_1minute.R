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

setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Aclima Wearnode\\processed\\processed_CO2_Pressure_minutely")

ieq_part_1 <- list()

temp = list.files(pattern="*.csv")

  for (i in 1:length(temp)) 
  {
  all_content = readLines(temp[i])
  skip_second = all_content[-c(1,2)]
  ieq_part_1[[i]] <- read.csv(textConnection(skip_second), header = FALSE)
  }

device_ids <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\device_info.xlsx",sheetName = "device_info")

for (i in 1: nrow(device_ids))
{
  ieq_part_1[[i]]$device_ID <- rep(device_ids[i,2],nrow(ieq_part_1[[i]]))
  ieq_part_1[[i]]$WB2_ID <- rep(device_ids[i,1],nrow(ieq_part_1[[i]]))
}


ieq_CO2 <- ieq_part_1[[1]]

  for(i in 2:length(temp))
  {
  ieq_CO2 <- rbind(ieq_CO2,ieq_part_1[[i]])
  }

colnames(ieq_CO2) <- c("Timestamp","CO2","device_ID","WB2_ID")

#ieq_CO2_b <- ieq_CO2 

ieq_CO2$Timestamp <- as.POSIXct(ieq_CO2$Timestamp,tz="GMT")

ieq_CO2_EST_timezone <- ieq_CO2 

###### NOTE! #########################
##### FOr PAST PARTICIPANTS ONLY #####
ieq_CO2_EST_timezone$Timestamp <- ieq_CO2$Timestamp - 4*60*60
#############   HENCEFORTH    ##########################
ieq_CO2_EST_timezone$Timestamp <- ieq_CO2$Timestamp 
#######################################

## For EST, we subtract 4 hours. Note: For HRV dataset, it is done already (don't know how?, but did verify with log data)

#### DOMAIN-WISE, the data doesnt map well with the participant log. 
## Awaiting confirmation from Aclima (dated: 2nd June) and then, we re-run the code
# Till then, assume sanctity of data and merge for the sake of establishing the process  

write.csv(ieq_CO2_EST_timezone,'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\IEQ\\IEQ_CO2_1min.csv')
