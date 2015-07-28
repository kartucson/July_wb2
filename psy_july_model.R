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

#psy_in <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\wb2_analysis_dataset.csv')
psy_in <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\july_integration\\data_wb2_all.csv')
colnames(psy_in)

fit <- fa(psy_in[,60:67], nfactors=2, rotate="varimax",fm="pa")
fit # print results

load <- fit$loadings[,1:2] 
plot(load,type="n",xlab="Component 1",ylab = "Component 2") # set up plot 
text(load,labels=names(psy_in[,60:67]),cex=0.8)
abline(h = 0, v = 0, col = "gray60")

write.csv(round(fit$scores,2),'C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\Factor_scores.csv')

## MANOVA of Component 1, Component 2, Focus, Productivity on Rooms, base locations, subject to participant
##
### Means of wings, rooms for Psy, (HRV ?)
## Call at 10:30 pm tomorrow, like today
### Why loss of participants (so many?)
#### Timings of participants in office in pickup_drop_till_138.xlsx

system.time( psy_dis <- discreta(psy_in) )

psy_fac <- cbind(psy_dis,round(fit$scores,2))

## PA2 is arousal and PA1 is valence

# Dataframe with Psy and groups

data_psy <- data.frame(Focus = psy_fac$focused, Productive = psy_fac$productive, Valence = psy_fac$PA1,
                       Arousal = psy_fac$PA2,ID = psy_fac$ID, space = psy_fac$wing.cluster,
                       BL = psy_fac$Base_location, Gender= psy_fac$Gender,
                       ToD = psy_fac$ToD,DoW = psy_fac$DoW, Alcohol = psy_fac$Alcohol,
                       BMI = psy_fac$BMI_gr, Age = psy_fac$Age_gr,
                       Health = psy_fac$Health_condition,Current_task = psy_fac$Current_task,
                       caffeine = psy_fac$caffeine,Natural_light = psy_fac$Natural_light,
                       White_noise = psy_fac$white.noise)

data_psy_in <- scale(as.data.frame(data_psy[,1:4]), center=TRUE, scale = TRUE)

# data_p <- as.data.frame(na.omit(data_psy))

#data_in_out_t <- data.frame(data_psy_in, data_psy[,!colnames(data_psy)%in% colnames(data_psy_in)])

data_in_out_t <- data.frame(data_psy_in, data_psy)

data_in_out <- na.omit(data_in_out_t)

write.csv(data_psy,"temp.csv")

write.csv(data_in_out,"temp.csv")
#data_scale <- scale(as.data.frame(data_psy), center=TRUE, scale = TRUE)
#data_mat <- data.frame(Y = data_in_out[,1:4], X = data_in_out[,5:18])
# data_x <- data_in_out[,5:18]

### 

data_manova <- manova(cbind(Focus,Productive,Valence,Arousal) ~ ID + space +
      Gender+ToD+DoW+Alcohol+BMI+Age+Health+
        Current_task+caffeine+Natural_light+White_noise + BL,data_in_out)

summary.aov( data_manova)


data_manova2 <- manova(cbind(Focus,Productive,Valence,Arousal) ~ space +
                         Gender+ToD+DoW+BMI+Age+Health+
                         Current_task+caffeine+Natural_light+White_noise + BL +Alcohol 
                        ,data_in_out)

ToD+DoW+Alcohol+BMI+Age+Health+
  Current_task+caffeine+Natural_light+White_noise + BL

summary.aov( data_manova2)

### NN: ## Just the group means of Males, Age_group, BMI,
### Base_locations, wing.cluster, ID --> All psy
setwd("C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\Psy\\July\\output_sheets")

data_pre_screen <- data.frame(data_psy_in, psy_fac)

write.csv(data_pre_screen,"Base_locations_psy_comparison.csv")

#data_num <- data_pre_screen[,c(1:8,12,14,25,29:33,35:48,22,23)]

data_num <- data_pre_screen[,c(1:4,8:11,13:17,29:33,35:48,22,25,23)]

means.ID <- aggregate(as.matrix(data_num) ~ ID, data_pre_screen, mean,na.action=na.exclude)  ## Mean of all groups
means.ID[,2:ncol(means.ID)] <- round(means.ID[,2:ncol(means.ID)],2)

counts.ID <- aggregate(as.matrix(data_num) ~ ID, data_pre_screen, FUN = function(x){NROW(x)})  ## count of all groups

write.csv(data.frame(means.ID,count = counts.ID[,2]),"Mean_for_participants.csv")

means.BL <- aggregate(as.matrix(data_num) ~ Base_location, data_pre_screen, mean)  ## Mean of all groups
means.BL[,2:ncol(means.BL)] <- round(means.BL[,2:ncol(means.BL)],2)

counts.BL <- aggregate(as.matrix(data_num) ~ Base_location, data_pre_screen, FUN = function(x){NROW(x)})  ## count of all groups

#length(levels(data_pre_screen$ID))

write.csv(data.frame(means.BL,count = counts.BL[,2]),"Base_locations_psy_comparison_results.csv")

participants_dist <- aggregate(ID ~ Base_location + Gender, data_pre_screen, FUN = function(x){length(unique(x))})  ## count of all groups

means.wing <- aggregate(as.matrix(data_num) ~ wing.cluster, data_pre_screen, mean)  ## Mean of all groups
means.wing[,2:ncol(means.wing)] <- round(means.wing[,2:ncol(means.wing)],2)

counts.wing <- aggregate(as.matrix(data_num) ~ wing.cluster, data_pre_screen, FUN = function(x){NROW(x)})  ## count of all groups

write.csv(data.frame(means.wing,count = counts.wing[,2]),"Mean_wing_locations.csv")


####################################################### Data Mining #################

'''
Formula <- formula(paste("y ~ ", 
                         paste(PredictorVariables, collapse=" + ")))
lm(Formula, Data)
'''
library(randomForest)
library(rpart)
library(tree)

OUTPUTS = data_in_out[,1:4]
INPUTS = data_in_out[,5:18]

Formula <- formula(paste("data_in_out[,2]~",paste(colnames(data_in_out)[5:18],collapse="+"))) 

system.time( fit2 <- randomForest(Formula,   data=data_in_out) )
print(fit2) # view results 
importance(fit2) # importance of each predictor

system.time( r.SDNN <- rpart(SDNN~., data= data_cl_imputed, method="anova") )
plot(r.SDNN, uniform=TRUE, 
     main="Regression tree ")
text(r.SDNN, use.n=TRUE, all=TRUE, cex=.8)
as.data.frame(r.SDNN$variable.importance)

c_tr <- tree(SDNN~.,data=data_cl_imputed,method="anova") 
cv.model <- cv.tree(c_tr)
summary(c_tr)

library(e1071)
## OR ksvm ?

data_cl_imputed <- data_cl_imputed[,!colnames(data_cl_imputed)%in%c("Smoking")]
system.time( svr_sdnn <- svm(SDNN ~ .,  data = data_cl_imputed) )


Cross validation to find the best tree:
  cv.model <- cv.tree(tree.model)

ICC1(aov(SDNN~Base_location,data=data_cl_imputed))  ## 0.68 %


###### IEQ, Focus in HLM model ######

datap_full <- read.csv('C:\\Users\\karthik\\Google Drive\\GSA DATA\\Data integration\\wb2_analysis_dataset.csv')


