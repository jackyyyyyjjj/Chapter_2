rm (list = ls(all=TRUE))
graphics.off()
library(stringr)
script_path <- rstudioapi::getActiveDocumentContext()$path
hash = "(.*)/"
setwd(str_extract(script_path, hash)) 

library('dplyr')
library('RSQLite')
library('DBI')
conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app_merger.sqlite")
# kk <- dbGetQuery(conn, "SELECT A.id,A.publisher_url,A.publisher_id,A.publisher_email,A.publisher_name,
# B.id,B.publisher_url,B.publisher_id,B.publisher_email,B.publisher_name 
#                  from AppMonstaPlayStoreDetails20150817 A JOIN AppMonstaPlayStoreDetails20160815 B ON 
#                  A.id = B.id")
gc(T)
dbDisconnect(conn)
# Datafull <- kk
# ####A is the data from 2015, B is the data from 2016
# colnames(Datafull) <- c('A.id','A.publisher_url','A.publisher_id','A.publisher_email','A.publisher_name',
#                         'B.id','B.publisher_url','B.publisher_id','B.publisher_email','B.publisher_name')
scoreinclude <- dbGetQuery(conn, "SELECT Id16,Id17, publisher_name16, publisher_name17,publisher_email16,publisher_email17,IndexN, simscore from similarityTable1617")


rawdata <- dbGetQuery(conn, "SELECT Id16,Id17, publisher_name16, publisher_name17,publisher_email16,publisher_email17,IndexN from duplicateTable1617")


scoreinclude$CRITERIA <- paste(scoreinclude$publisher_name16,scoreinclude$publisher_name17,sep = '-')
rawdata$CRITERIA <- paste(rawdata$publisher_name16,rawdata$publisher_name17,sep = '-')
#clean the repeated ids
scoreinclude1 <- scoreinclude %>%
  distinct(Id16, .keep_all =TRUE)

rawdata1 <- rawdata %>%
  distinct(Id16, .keep_all =TRUE)



#Collect the data that the similarity score is higher than 0.2. We assume that the data with higer score which means that it is different 
#publisher name
#delete the duplicated ids
scoreinclude0.2 <- scoreinclude1[scoreinclude1$simscore>0.22,]
scoresmall0.2 <- scoreinclude1[scoreinclude1$simscore<=0.22,]


# The publisher names mmaybe the same.but the apps that they have will be lots different,
# that is the reason, we delete the same criteria, at the same time,keep a copy file of duplicate data, then we run the follow code
# to get the all apps, which happened merger situation
Merger_data <- rawdata1[rawdata1$CRITERIA %in% scoreinclude0.2$CRITERIA,]


# GET ALL THE developer's own apps
N.16 <- rawdata1[rawdata1$publisher_name16 %in% scoreinclude0.2$publisher_name16,]
#Find out all the merger activities in the database
duplicate_data16 <- table(N.16$publisher_name16)

Fre_data16 <- as.data.frame(duplicate_data16) 

colnames(Fre_data16)[1] <- 'publisher_name16'
colnames(Fre_data16)[2] <- 'Ownapps16'

#find out all the developers' app data inside 2017
N.17 <- rawdata1[rawdata1$publisher_name17 %in% scoreinclude0.2$publisher_name17,]
duplicate_data17 <- table(N.17$publisher_name17)

Fre_data17 <- as.data.frame(duplicate_data17) 

colnames(Fre_data17)[1] <- 'publisher_name17'
colnames(Fre_data17)[2] <- 'Ownapps17'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data1 <- merge(Merger_data,Fre_data16,by='publisher_name16')
Merger_data1 <- merge(Merger_data1,Fre_data17,by='publisher_name17')

### Now I can check how the increase and the decrease

UP_DEV <- Merger_data1[Merger_data1$Ownapps17 > Merger_data1$Ownapps16,]
DOWN_DEV <- Merger_data1[Merger_data1$Ownapps17 < Merger_data1$Ownapps16,]
EQU_DEV <- Merger_data1[Merger_data1$Ownapps17 == Merger_data1$Ownapps16,]


### IF i MERGE THE DIFFERENT NAME


#Get the number of apps owned by the app owners in 2016, the app owners exist in 2015
N.1617 <- rawdata1[rawdata1$publisher_name17 %in% scoreinclude0.2$publisher_name16,]
duplicate_data1617 <- table(N.1617$publisher_name17)
Fre_data1617 <- as.data.frame(duplicate_data1617) 
Fre_data1617.A <- Fre_data1617
colnames(Fre_data1617.A)[1] <- 'publisher_name16' #GOT THE SMAE DV IN 2016 AND IN 2017
colnames(Fre_data1617.A)[2] <- 'OWNAPP16BASE17'
Merger_data2 <- merge(Merger_data1,Fre_data1617.A,by.x ='publisher_name16',all.x = TRUE)

#Get the number of apps owned by the app owners in 2016, the app owners exist in 2017
N.1716 <- rawdata1[rawdata1$publisher_name16 %in% scoreinclude0.2$publisher_name17,]
duplicate_data1716 <- table(N.1716$publisher_name16)
Fre_data1716 <- as.data.frame(duplicate_data1716) 
Fre_data1716.A <- Fre_data1716
colnames(Fre_data1716.A)[1] <- 'publisher_name17' #GOT THE SMAE DV IN 2015 AND IN 2016
colnames(Fre_data1716.A)[2] <- 'OWNAPP17BASE16'
Merger_data3 <- merge(Merger_data2,Fre_data1716.A,by.x ='publisher_name17',all.x = TRUE)

#delete the same publisher's name, because the doc2vec will not work all the time. HAHA
Merger_data4 <- Merger_data3[!Merger_data3$publisher_name17 == Merger_data3$publisher_name16,]


# Get the all the appowner's data info from the very original data --------
conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
Srawdata1 <- dbGetQuery(conn, "SELECT id,date, downloads, publisher_name from AppMonstaPlayStoreDetails20160815")
Srawdata2 <- dbGetQuery(conn, "SELECT id,date, downloads, publisher_name from AppMonstaPlayStoreDetails20170814")

raw16 <- Srawdata1[Srawdata1$publisher_name %in% Merger_data4$publisher_name16,]
raw_data16<- table(raw16$publisher_name)
raw_data16all <- as.data.frame(raw_data16) 
colnames(raw_data16all)[1] <- 'publisher_name16'
colnames(raw_data16all)[2] <- 'Ownapps16all'

raw17 <- Srawdata2[Srawdata2$publisher_name %in% Merger_data4$publisher_name17,]
raw_data17 <- table(raw17$publisher_name)
raw_data17all <- as.data.frame(raw_data17) 
colnames(raw_data17all)[1] <- 'publisher_name17'
colnames(raw_data17all)[2] <- 'Ownapps17all'


Merger_data5 <- merge(Merger_data4,raw_data16all,by.x ='publisher_name16',all.x = TRUE)
Merger_data6 <- merge(Merger_data5,raw_data17all,by.x ='publisher_name17',all.x = TRUE)
#again get the cross data
raw1617 <- Srawdata1[Srawdata2$publisher_name %in% Merger_data4$publisher_name16,]
raw_data1617 <- table(raw1617$publisher_name)
raw_data1617all <- as.data.frame(raw_data1617) 
colnames(raw_data1617all)[1] <- 'publisher_name16'
colnames(raw_data1617all)[2] <- 'Ownapps1617all'

raw1716 <- Srawdata2[Srawdata1$publisher_name %in% Merger_data4$publisher_name17,]
raw_data1716 <- table(raw1716$publisher_name)
raw_data1716all <- as.data.frame(raw_data1716) 
colnames(raw_data1716all)[1] <- 'publisher_name17'
colnames(raw_data1716all)[2] <- 'Ownapps1716all'


Merger_data7 <- merge(Merger_data6,raw_data1617all,by.x ='publisher_name16',all.x = TRUE)
Merger_data8 <- merge(Merger_data7,raw_data1716all,by.x ='publisher_name17',all.x = TRUE)

#I will use the criteria to distint the data

Merger_data9 <- Merger_data8 %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(Merger_data9$publisher_name17)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>2,]

Merger_data10 <- Merger_data8[Merger_data8$publisher_name17 %in% raw_EXTR2$Var1,]
Notsure_nermge <- Merger_data8[!Merger_data8$publisher_name17 %in% raw_EXTR2$Var1,]

write.csv(Merger_data10,'Merger_data1617.csv')

# ####Maybe check the publisher email is a better way
# Datafull_DIFF.emid <- Datafull[Datafull$A.publisher_email!=Datafull$B.publisher_email,]
# dbDisconnect(conn)



conn <- dbConnect(RSQLite::SQLite(), "topapps.sqlite")
top <- dbGetQuery(conn, "SELECT Id17 from duplicateTable1718")
dbDisconnect(conn)
exist15 <- Srawdata1[Srawdata1$id %in% top$Id17,]
exist16 <- Srawdata2[Srawdata2$id %in% top$Id17,]

exist16 <- read.csv('exist16.csv')
Srawdata1 <- dbGetQuery(conn, "SELECT A.* ,B.* from AppMonstaPlayStoreDetails20160815 A JOIN Topapps B ON A.id = B.id")
Srawdata2 <- Srawdata1[,-c(32,33)]
write.csv(Srawdata2,'exist16all.csv')
Srawdata1 <- readRDS('Srawdata1.Rds')
# SELECT A.id,A.publisher_url,A.publisher_id,A.publisher_email,A.publisher_name,'
#                                           'B.id,B.publisher_url,B.publisher_id,B.publisher_email,B.publisher_name from AppMonstaPlayStoreDetails20170814 A '
#                                           'JOIN AppMonstaPlayStoreDetails20180813 B ON A.id = B.id

existmerger <- Merger_data10[Merger_data10$Id17 %in% top$Id17,]
write.csv(existmerger,'existmerger1718.csv')
write.csv(exist15,'exist15.csv')
write.csv(exist16,'exist16.csv')



