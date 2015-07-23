### Psy analysis ###

## Take all Psy data files & append 

# Factor analysis followed by
## MANOVA 
## Emotions <- Rooms
## Emotions <- IEQ
## Emotions <- HRV

### Subject participant number # 

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

library(FactoMineR)
library(psych)

psy <- read.xlsx('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey_july.xlsx',sheetName="All")

#psy_wwn<- psy[,colnames(psy)%in% c("ID","Form","Form_start_date","space","preoccupation","closest_space_num","caffeine")]

psy_wwn_1 <- subset(psy,Form != "Missing")
psy_wwn_1$Timestamp <- round(as.POSIXct(psy_wwn_1$Form_start_date),"mins")  #
psy_wwn_1$ID <- as.factor(psy_wwn_1$ID)
psy_wwn_2 <- psy_wwn_1[order(psy_wwn_1$ID,psy_wwn_1$Timestamp),]

psy_in <- psy_wwn_2

colnames(psy_in)

fit <- fa(psy_in[,27:34], nfactors=2, rotate="varimax",fm="pa")
fit # print results
plot(fit)
text(fit$scores[,1],fit$scores[,2],cex= 0.7, pos=3)


load <- fit$loadings[,1:2] 
plot(load,type="n",xlab="Component 1",ylab = "Component 2") # set up plot 
text(load,labels=names(psy_in[,27:34]),cex=0.8)
abline(h = 0, v = 0, col = "gray60")

write.csv(round(fit$scores,2),'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\Factor_scores.csv')

P_ID_lookup <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\hourly_survey_july.xlsx",sheetName="P_ID_lookup")

psy_in_loc <- merge(psy_in,P_ID_lookup,by = c("ID"))

## MANOVA of Component 1, Component 2, Focus, Productivity on Rooms, base locations, subject to participant
##
### Means of wings, rooms for Psy, (HRV ?)
## Call at 10:30 pm tomorrow, like today
### Why loss of participants (so many?)
#### Timings of participants in office in pickup_drop_till_138.xlsx

psy_fac <- cbind(psy_in_loc,round(fit$scores,2))

## PA2 is arousal and PA1 is valence

# Dataframe with Psy and groups

data_psy <- data.frame(Focus = psy_fac$focused, Productive = psy_fac$productive, Valence = psy_fac$PA1,
                       Arousal = psy_fac$PA2,ID = psy_fac$ID, room = psy_fac$space, BL = psy_fac$Base_location )

data_psy_in <- scale(as.data.frame(data_psy[,1:4]), center=TRUE, scale = TRUE)

write.csv(data_psy,"temp.csv")
#data_scale <- scale(as.data.frame(data_psy), center=TRUE, scale = TRUE)

data_manova <- manova(data_psy_in ~ data_psy[,5] + data_psy[,6] + data_psy[,7])

data_manova.2 <- manova(data_psy_in ~ data_psy[,7] + data_psy[,5])

summary.aov( data_manova.2)

data_m <- data.frame(data_psy_in,Base = data_psy[,7],ID = data_psy[,5])

write.csv(data_m,"Base_locations_psy_comparison.csv")

means.ID <- aggregate(cbind(Focus,Productive,Valence,Arousal) ~ ID, data_m, mean)  ## Mean of all groups
means.ID[,2:5] <- round(means.ID[,2:5],2)

counts.ID <- aggregate(cbind(Focus,Productive,Valence,Arousal) ~ ID, data_m, FUN = function(x){NROW(x)})  ## count of all groups

means.BL <- aggregate(cbind(Focus,Productive,Valence,Arousal) ~ Base, data_m, mean)  ## Mean of all groups
means.BL[,2:5] <- round(means.BL[,2:5],2)

counts.BL <- aggregate(cbind(Focus,Productive,Valence,Arousal) ~ Base, data_m, FUN = function(x){NROW(x)})  ## count of all groups

write.csv(data.frame(means.BL,count = counts.BL[,2]),"Base_locations_psy_comparison_results.csv")

means.all.ID <- aggregate(cbind(focused,productive,tense,content,
                                sad,alert,tired,happy,upset,calm) ~ ID, psy_fac, mean)  ## Mean of all groups
means.all.ID[,2:11] <- round(means.all.ID[,2:11],2)

write.csv(means.all.ID,"Mean_psy__of_participants.csv")

Part_demo <- read.xlsx("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\Psy_intake_work_doc.xlsx",sheetName = "trim")

data_with_demo <- merge(means.all.ID,Part_demo,by = c("ID"),all.x=TRUE)

write.csv(data_with_demo,"Mean_psy_with_big5.csv")

plot_preoc <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\working_files\\all_data_june17.csv')

plot(plot_preoc$preoccupation_a,plot_preoc$SDNN..milisec..)

plot(sata_or$preoccupation,sata_or$SDNN)
