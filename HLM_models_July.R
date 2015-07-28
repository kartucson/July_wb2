### HLM model & Data mining and Bayesian networks

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
library(bayesm)
library(multilevel)
library(lme4)
library(nortest)
library(lmtest)
library(nlme)

library(catnet)

#data_full <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\wb2_analysis_dataset.csv')
data_full <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\july_integration\\data_wb2_all.csv')

## What all this dataset contains:
# (a) Wearable (2)
# HRv (4)
# Activity
# Survey (each hour): Caffeine space info spread across previous 30 minutes observations
# Participant intake/exit survey - demographic and big-5
# Spatial info of current space in which participant is
## 

### To do PCA to reduce the Space variables
## what about categorical space variables ???

### Taking a more conservative dataset for initial analysis

data_backup <- data_full
data_pruned <- subset(data_full,data_full$SDNN < 180 & data_full$SDNN > 0)

### Scale the continuous variables ##

#

discreta <- function(data_input)
{
  data_input$Timestamp <- as.POSIXct(strptime(data_input$Timestamp, "%m/%d/%Y %H:%M"))
  
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
  
  ## THere is a disconnect here, do we add caffeine in the data_con list or not? 
  ## We seem to do the mean imputation anyways in that case 
  
  #data_con <- data.frame(Pressure = data_in$Pressure,
  #    Sound = data_in$Sound, CO2 = data_in$CO2, Activity = data_in$Activity)
  # Dont center the outcomes: SDNN = data_in$SDNN, nHF = data_in$nHF ,LFHF = data_in$LFHF)
  
  ## A dangerous propositiom if personal level variables (SDNN, RMSSD) discretized based on Quartiles
  #data_cons <- data_con
  #data_cons <- as.data.frame(scale(data_con, scale=F, center=T))
  
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
  
  ######### SDNN #########
  
  data_sdnn <- data_in[,c('SDNN','nHF')]
  
  data_hrv_p <- split(data_sdnn,data_in$ID)
  
  for(i in 1:length(data_hrv_p))
  {
    #data_hrv_p[[i]] <- cnDiscretize(data_hrv_p[[i]],2,mode="quantile")
    data_hrv_p[[i]] <- cnDiscretize(data_hrv_p[[i]],3,mode="quantile")
  }  
  
  data_hrv_t <- data_hrv_p[[1]]
  
  for (i in 2:length(data_hrv_p))
  {
    data_hrv_t <- rbind(data_hrv_t,data_hrv_p[[i]])
  }
  
  
  for(col in 1:ncol(data_hrv_t))
  {
    data_hrv_t[,col] <- as.factor(data_hrv_t[,col])
    #levels(data_hrv_t[,col]) <- c("Very_Low","Low","High","Very_High")
    #levels(data_hrv_t[,col]) <- c("Low","High")
    #levels(data_hrv_t[,col]) <- c("Low","Medium","High")
    levels(data_hrv_t[,col]) <- c("High stress","Stress","No Stress")
  }

#data_out <- data.frame(data_cons, data_in[,!colnames(data_in)%in% colnames(data_cons)],data_hrv_t)
#data_out <- data.frame(data_cons,data_in,data_hrv_t)
 data_out <- data.frame(data_in,data_hrv_t)
return(data_out)
}

system.time( data_no_filter <- discreta(data_full) )
system.time( data_filter <- discreta(data_pruned)  )

plot(data_no_filter$Base_location,data_hrv_t[,1])

plot(data_no_filter$Base_location,data_no_filter$SDNN,
     sub = paste("4400 mean = ",round(mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="4400")$SDNN)),2)
                 , ", G0 mean = ",
    round( mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="G0")$SDNN)),2)
                 , ", ROB mean = ",
    round( mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="ROB")$SDNN))) , 2)
)

plot(data_no_filter$Base_location,data_no_filter$SDNN,
     sub = paste("4400 mean = ",round(mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="4400")$SDNN)),2)
                 , ", G0 mean = ",
                 round( mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="G0")$SDNN)),2)
                 , ", ROB mean = ",
                 round( mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="ROB")$SDNN))) , 2)
,ylim = c(0,2000)
)


histogram(subset(data_no_filter,data_no_filter$Base_location=="G0")$SDNN,breaks=500)
histogram(subset(data_no_filter,data_no_filter$Base_location=="4400")$SDNN,breaks=500,xlim=c(0,1000))
histogram(subset(data_no_filter,data_no_filter$Base_location=="ROB")$SDNN,breaks=500)

mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="G0")$SDNN))
mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="4400")$SDNN))
mean(na.omit(subset(data_no_filter,data_no_filter$Base_location=="ROB")$SDNN))

median(na.omit(subset(data_no_filter,data_no_filter$Base_location=="G0")$SDNN))
median(na.omit(subset(data_no_filter,data_no_filter$Base_location=="4400")$SDNN))
median(na.omit(subset(data_no_filter,data_no_filter$Base_location=="ROB")$SDNN))

#### With filter ####################

plot(data_filter$Base_location,data_filter$SDNN,
     sub = paste("4400 mean = ",round(mean(na.omit(subset(data_filter,data_filter$Base_location=="4400")$SDNN)),2)
                 , ", G0 mean = ",
                 round( mean(na.omit(subset(data_filter,data_filter$Base_location=="G0")$SDNN)),2)
                 , ", ROB mean = ",
                 round( mean(na.omit(subset(data_filter,data_filter$Base_location=="ROB")$SDNN))) , 2)
)

histogram(subset(data_filter,data_filter$Base_location=="G0")$SDNN,breaks=500)
histogram(subset(data_filter,data_filter$Base_location=="4400")$SDNN,breaks=500)
histogram(subset(data_filter,data_filter$Base_location=="ROB")$SDNN,breaks=500)

mean(na.omit(subset(data_filter,data_filter$Base_location=="G0")$SDNN))
mean(na.omit(subset(data_filter,data_filter$Base_location=="4400")$SDNN))
mean(na.omit(subset(data_filter,data_filter$Base_location=="ROB")$SDNN))

median(na.omit(subset(data_filter,data_filter$Base_location=="G0")$SDNN))
median(na.omit(subset(data_filter,data_filter$Base_location=="4400")$SDNN))
median(na.omit(subset(data_filter,data_filter$Base_location=="ROB")$SDNN))

#### Casey's wish for 300 as cap ###

data_300 <- subset(data_full,data_full$SDNN < 300 & data_full$SDNN > 0)

plot(data_300$Base_location,data_300$SDNN        )

histogram(subset(data_300,data_300$Base_location=="G0")$SDNN,breaks=500)
histogram(subset(data_300,data_300$Base_location=="4400")$SDNN,breaks=500)
histogram(subset(data_300,data_300$Base_location=="ROB")$SDNN,breaks=500)


  ## HLM  ###
#Levels are ID, Space, 

data_ins <- data_no_filter

## Selectively filter

datas <- data_ins[,colnames(data_ins) %in% c("space","ID","Pressure","CO2",
      "Temperature_feel","Sound","Base_location",
"SDNN","Activity","ToD","DoW","Gender",
"Ethnicity","Age_gr","BMI_gr","wing.cluster",
"Health_condition","Alcohol",
"Current_task","caffeine","cigarette",
"Room.type","white.noise","Natural_light")]

Null.model1<-lme(SDNN~1,random=~1|ID,data=datas, control=list(opt="optim"),na.action = na.exclude)

summary(Null.model1)
VarCorr(Null.model1)
ICC1(aov(SDNN~ID,data=datas))  ## 18%

Null.model2<-lme(SDNN~1,random=~1|space,data=datas, control=list(opt="optim"),na.action = na.exclude)
summary(Null.model2)
ICC1(aov(SDNN~space,data=datas)) # 6% 

Null.model2b<-lme(SDNN~1,random=~1|wing.cluster,data=datas, control=list(opt="optim"),na.action = na.exclude)
summary(Null.model2b)
ICC1(aov(SDNN~wing.cluster,data=datas)) # 6%

Null.model3<-lme(SDNN~1,random=~1|Base_location,data=datas, control=list(opt="optim"),na.action = na.exclude)
summary(Null.model3)
ICC1(aov(SDNN~Base_location,data=datas))  # 2%

## Zero should not lie inside the CI of SD for levels to be useful ##
intervals(Null.model2) 
intervals(Null.model1)
intervals(Null.model3)

## SO null effects are significant

attach(datas)
c("space","ID","Pressure","CO2",
 "Temperature_feel","Sound","Base_location",
 "SDNN","Activity","ToD","DoW","Gender",
 "Ethnicity","Age_gr","BMI_gr",
 "Health_condition","Alcohol",
 "Current_task","caffeine","cigarette",
 "Room.type","white.noise","Natural_light")]

a <- lm(SDNN~. -space,data=datas )
### Not much use as > 40,000 deleted for missingness

## Pairwise correlation of variables and SDNN

data_ins$space <- as.factor(data_ins$space)

detach(data_ins)
attach(data_ins)

### One-way ANOVA for one continuous and one categorical, correlation for two continuous and 
## Chisquared test for two categorical as follows:

cor(data_ins$ID,data_ins$SDNN)

summary(lm(SDNN~ID))
### 115 is the most different, 103 is also different 

summary(lm(SDNN~Base_location))
# G0 is different 

summary(lm(SDNN~space,data=data_ins))
# space 3046, 3334, 4344, 4459,4461 make differences

#chisq.test(table(survey$Smoke, survey$Exer)) 

chisq.test(table(data_ins$space, data_ins$ID)) 
chisq.test(table(data_ins$Base_location, data_ins$ID)) 
chisq.test(table(data_ins$space, data_ins$Base_location)) 

## Enough, now check or random effects on these variables, in a hierarchical manner
# Null model 
### Predictor of interest
#### Covariates
### Check variance co-variance components (for random-effects) like previous paper (Thayer2007) 

colnames(data_ins)

model.2 <- lme(SDNN ~ Pressure + CO2 + Window_dist)

## Since too many missing values, impute with mean for continuous and 'z' for discrete variables
## to avoid data loss in modeling

data_imp <- data_ins

k_count <- data_ins

system.time(
  for(j in 1:ncol(data_imp))
  {
    if(is.numeric(data_imp[,j]))
    {
      for(i in 1:nrow(data_imp))
      {
        if(is.na(data_imp[i,j]))
        {
          data_imp[i,j] <- round(mean(data_imp[,j],na.rm=T),2)
          #k_count[i,j] <- c("something") 
        } else
        {
          #k_count[i,j] <- c("nothing")  
        }  
      }
    }
  }
)

'''
for(i in 1:nrow(data_imp))
{
if(is.na(data_imp$Temperature[i]))
{
  data_imp$Temperature[i] <- round(mean(data_imp$Temperature,na.rm=T),2)
  #k_count[i,j] <- c("something") 
}
}
'''

data_imp2 <- data_imp 

system.time(
  for(j in 1:ncol(data_imp))
  {
    if(is.factor(data_imp[,j]))
    {
      data_imp[,j] <- factor(data_imp[,j], levels = c(levels(data_imp[,j]), "missing"))  
      for(k in 1:nrow(data_imp))
      {
        if(is.na(data_imp[k,j]))
        {
          data_imp[k,j] <- "missing"
          #  k_count[k,j] <- c("character is empty")
        }
        #else {
        # k_count[k,j] <- c("character is full")  
        #}  
      }
    }
  }
)

#write.csv(data_imp,"imputed_data_15june.csv")
write.csv(data_imp,"imputed_data_17june.csv")
write.csv(data_imp2,"imputed_data_17june_cat_bl.csv")

## HLM

# Use data_imp2 to discretize # 
## Different classification and 10 fold cross validation  

data_a <- data_imp2

model.0 <- lme(fixed = SDNN~1, random=list(Base_location =~ 1,ID=~1, wing.cluster=~1),data=data_a,control=list(opt="optim"),method = "REML" )

model.1 <- lme(fixed = SDNN~Pressure + CO2 + Window_dist, random=list(Base_location =~ 1, 
                                                                      ID=~1, wing.cluster=~1),data=data_a,control=list(opt="optim"),method = "REML" )

model.2 <- lme(fixed = SDNN~Pressure + CO2 +Window_dist , random=list(Base_location =~ 1, ID=~1, wing.cluster=~Pressure + CO2+Window_dist),data=data_a,control=list(opt="optim"),method = "REML" )
summary(model.2)

model.3 <- lme(fixed = SDNN~Pressure + CO2 , random=list(Base_location =~ 1, ID=~1, wing.cluster=~Pressure + CO2+Window_dist),data=data_a,control=list(opt="optim"),method = "REML" )
summary(model.3)

model.4 <- lme(fixed = SDNN~Pressure*CO2 + Activity + ToD + DoW + Gender + Ethnicity , random=list(Base_location =~ 1, ID=~BMI , wing.cluster=~Pressure + CO2+Window_dist),data=data_a,control=list(opt="optim"),method = "REML" )
summary(model.4)

model.5 <- lme(fixed = SDNN~Pressure*CO2 + ToD + DoW + caffeine + preoccupation + white.noise + Activity, random=list(Base_location =~ 1, ID=~BMI, wing.cluster=~Window_dist + white.noise),data=data_a,control=list(opt="optim"),method = "REML" )
summary(model.5)

model.6 <- lme(fixed = SDNN~Pressure + CO2 + ToD + DoW + caffeine + preoccupation + white.noise, random=list(Base_location =~ 1, ID=~1+BMI+ Activity, wing.cluster=~1 + white.noise),data=data_a,control=list(opt="optim"),method = "REML" )
summary(model.6)



