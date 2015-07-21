### Take 5minutes moving average and 5 minutes data and prepare datasets

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

# Append files

append_files <- function()
{
  phy_part_1 <- list()
  temp = list.files(pattern="*.xls")

  for (i in 1:length(temp)) 
    {
      phy_part_1[[i]] <- read.xlsx(temp[i], 2)
      colnames(phy_part_1[[i]])[1] <- c("Timestamp")
    }
  
  p_ids <- read.csv("C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\participant_IDs.csv",header=F)
  
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

setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\5min")
phy_5min <- append_files()

setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\Moving_window")
phy_5minmw <- append_files()

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

phy_2 <-phy_5minmw
phy_2$Timestamp <- as.POSIXct(phy_2$Timestamp)

### LOGIC to discard bad data based on NULL values

write.csv(phy_2,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\5min_time_expanded\\phy_5min.csv")
write.csv(phy_min,"C:\\Users\\karthik\\Google Drive\\GSA DATA\\movisens sensor\\Processed data\\5min_time_expanded\\phy_5min_exp.csv")
