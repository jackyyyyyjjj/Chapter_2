rm (list = ls(all=TRUE))
graphics.off()
library(stringr)
script_path <- rstudioapi::getActiveDocumentContext()$path
hash = "(.*)/"
setwd(str_extract(script_path, hash)) 

library('dplyr')
library('RSQLite')
library('DBI')
gc(T)
conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app_merger.sqlite")
scoreinclude <- dbGetQuery(conn, "SELECT Id17,Id18, publisher_name17, publisher_name18,publisher_email17,publisher_email18,IndexN, simscore from similarityTable1718")
rawdata <- dbGetQuery(conn, "SELECT Id17,Id18, publisher_name17, publisher_name18,publisher_email17,publisher_email18,IndexN from duplicateTable1718")
dbDisconnect(conn)

# Get the all the appowner's data info from the very original data --------
conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")
Srawdata1 <- dbGetQuery(conn, "SELECT * from AppMonstaPlayStoreDetails20170814")
Srawdata2 <- dbGetQuery(conn, "SELECT * from AppMonstaPlayStoreDetails20180813")
dbDisconnect(conn)

conn <- dbConnect(RSQLite::SQLite(), "topapps.sqlite")
top <- dbGetQuery(conn, "SELECT Id17 from duplicateTable1718")
dbDisconnect(conn)

colnames(top) <- c('Id17')





scoreinclude$CRITERIA <- paste(scoreinclude$publisher_name17,scoreinclude$publisher_name18,sep = '-')
rawdata$CRITERIA <- paste(rawdata$publisher_name17,rawdata$publisher_name18,sep = '-')
#clean the repeated ids
scoreinclude1 <- scoreinclude %>%
  distinct(Id17, .keep_all =TRUE)

rawdata1 <- rawdata %>%
  distinct(Id17, .keep_all =TRUE)

# Srawdata1.cl <- Srawdata1 %>%
#   distinct(id, .keep_all =TRUE)
# 
# Srawdata2.cl <- Srawdata2 %>%
#   distinct(id, .keep_all =TRUE)

#Collect the data that the similarity score is higher than 0.2. We assume that the data with higer score which means that it is different 
#publisher name
#delete the duplicated ids
scoreinclude0.2<- scoreinclude1[scoreinclude1$simscore>0.22,]
scoresmall0.2 <- scoreinclude1[scoreinclude1$simscore<=0.22,]


# The publisher names mmaybe the same.but the apps that they have will be lots different,
# that is the reason, we delete the same criteria, at the same time,keep a copy file of duplicate data, then we run the follow code
# to get the all apps, which happened merger situation
Merger_data <- rawdata1[rawdata1$CRITERIA %in% scoreinclude0.2$CRITERIA,]


# GET ALL THE developer's own apps
N.17 <- Srawdata1[Srawdata1$publisher_name %in% scoreinclude0.2$publisher_name17,]
#Find out all the merger activities in the database
duplicate_data17 <- table(N.17$publisher_name)

Fre_data17 <- as.data.frame(duplicate_data17) 

colnames(Fre_data17)[1] <- 'publisher_name17'
colnames(Fre_data17)[2] <- 'Ownapps17all'

#find out all the developers' app data inside 2018
N.18 <- Srawdata2[Srawdata2$publisher_name %in% scoreinclude0.2$publisher_name18,]
duplicate_data18 <- table(N.18$publisher_name)

Fre_data18 <- as.data.frame(duplicate_data18) 

colnames(Fre_data18)[1] <- 'publisher_name18'
colnames(Fre_data18)[2] <- 'Ownapps18all'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data1 <- merge(scoreinclude0.2,Fre_data17,by='publisher_name17')
Merger_data1 <- merge(Merger_data1,Fre_data18,by='publisher_name18')



#get the data from rawdata, because in this dataset the ids are the same
# GET ALL THE developer's own apps
N.17 <- rawdata1[rawdata1$publisher_name17 %in% scoreinclude0.2$publisher_name17,]
#Find out all the merger activities in the database
duplicate_data17 <- table(N.17$publisher_name17)

Fre_data17 <- as.data.frame(duplicate_data17) 

colnames(Fre_data17)[1] <- 'publisher_name17'
colnames(Fre_data17)[2] <- 'Ownapps17'

#find out all the developers' app data inside 2018
N.18 <- rawdata1[rawdata1$publisher_name18 %in% scoreinclude0.2$publisher_name18,]
duplicate_data18 <- table(N.18$publisher_name18)

Fre_data18 <- as.data.frame(duplicate_data18) 

colnames(Fre_data18)[1] <- 'publisher_name18'
colnames(Fre_data18)[2] <- 'Ownapps18'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data2 <- merge(scoreinclude0.2,Fre_data17,by='publisher_name17')
Merger_data2 <- merge(Merger_data2,Fre_data18,by='publisher_name18')

seldata <- Merger_data2[,c(3,10,11)]
seldata2 <- merge(Merger_data1,seldata,by='Id17',all.x = T) 


### IF i MERGE THE DIFFERENT NAME





#again get the cross data
raw1718 <- Srawdata2[Srawdata2$publisher_name %in% seldata2$publisher_name17,]
raw_data1718 <- table(raw1718$publisher_name)
raw_data1718all <- as.data.frame(raw_data1718) 
colnames(raw_data1718all)[1] <- 'publisher_name17'
colnames(raw_data1718all)[2] <- 'Ownapps1718all'

raw1817 <- Srawdata1[Srawdata1$publisher_name %in% seldata2$publisher_name18,]
raw_data1817 <- table(raw1817$publisher_name)
raw_data1817all <- as.data.frame(raw_data1817) 
colnames(raw_data1817all)[1] <- 'publisher_name18'
colnames(raw_data1817all)[2] <- 'Ownapps1817all'


Merger_data7 <- merge(seldata2,raw_data1718all,by.x ='publisher_name17',all.x = TRUE)
Merger_data8 <- merge(Merger_data7,raw_data1817all,by.x ='publisher_name18',all.x = TRUE)
#DELETE THE SAME ID IN 2017 AND 2018
Merger_dataK <- Merger_data8[Merger_data8$publisher_name18!=Merger_data8$publisher_name17,]
Merger_data8 <- Merger_dataK 
#I will use the criteria to distint the data

Merger_data9 <- Merger_data8 %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(Merger_data9$publisher_name18)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>=2,]
raw_EXTR3 <- raw_EXTR[raw_EXTR$Freq==1,]

Merger_data1718 <- Merger_data8[Merger_data8$publisher_name18 %in% raw_EXTR2$Var1,]
Notsure_nermge <- Merger_data8[!Merger_data8$publisher_name18 %in% raw_EXTR2$Var1,]
Merger_data10 <-Merger_data8
#Here I can use the notsure merger to run all, then final step,we can separate them based on merger10
write.csv(Merger_data1718,'./1718/Merger_data1718.csv')
write.csv(Notsure_nermge,'./1718/Notsure_nermge.csv')

# ####Maybe check the publisher email is a better way
# Datafull_DIFF.emid <- Datafull[Datafull$A.publisher_email!=Datafull$B.publisher_email,]
# dbDisconnect(conn)
# library(openxlsx)
# text < read.csv('C1_X.csv')
# renderTable({head(painters,input$num)})



#check the top list app in the two raw datasets
exist17 <- Srawdata1[Srawdata1$id %in% top$Id17,]
exist18 <- Srawdata2[Srawdata2$id %in% top$Id17,]

existmerger_top <- Merger_data10[Merger_data10$Id17 %in% top$Id17,]
existmerger_oth <- Merger_data10[!(Merger_data10$Id17 %in% top$Id17),]
# write.csv(exist17,'exist17.csv')
# write.csv(exist18,'exist18.csv')


# Now I took 2017 as an example to analysis -------------------------------
#I need to extract all the apps in merger between 17 and 18, then according this to find the how many top apps owned by the publishers
top_merger <- exist17[exist17$id %in% existmerger_top$Id17,]
Count_app_top <- table(top_merger$publisher_name) 
Count_app_top <- as.data.frame(Count_app_top)
colnames(Count_app_top)[1] <- 'publisher_name17'
colnames(Count_app_top)[2] <- 'Count_app_top17'

apps_all <- existmerger_top[existmerger_top$publisher_name17 %in% Count_app_top$publisher_name17,]
apps_all1 <- apps_all %>%distinct(publisher_name17, .keep_all =TRUE)
apps_all17 <- merge(apps_all1,Count_app_top,by='publisher_name17')


# Now I need to get the same thing in 2018 --------------------------------
#I need to extract all the apps in merger between 17 and 18, then according this to find the how many top apps owned by the publishers
# top_merger18 <- exist18all[exist18all$publisher_name %in% existmerger_top$publisher_name18,]
# Count_app_top18 <- table(top_merger18$publisher_name) 
# Count_app_top18 <- as.data.frame(Count_app_top18)
# colnames(Count_app_top18)[1] <- 'publisher_name18'
# colnames(Count_app_top18)[2] <- 'Count_app_top18'

top_merger18 <- exist18[exist18$id %in% existmerger_top$Id18,]
Count_app_top18 <- table(top_merger18$publisher_name) 
Count_app_top18 <- as.data.frame(Count_app_top18)
colnames(Count_app_top18)[1] <- 'publisher_name18'
colnames(Count_app_top18)[2] <- 'Count_app_top18'

apps_all <- existmerger_top[existmerger_top$publisher_name18 %in% Count_app_top18$publisher_name18,]
apps_all1 <- apps_all %>%distinct(publisher_name18, .keep_all =TRUE)
apps_all18 <- merge(apps_all1,Count_app_top18,by='publisher_name18')

apps_all18 <- apps_all18[,c(1,16)]

Final_app_count2 <- merge(apps_all17,apps_all18,by='publisher_name18',all.x = T)




# write.csv(Final_app_count,'./1718/Final_app_count.csv')
write.csv(Final_app_count2,'./1718/Final_app_count2.csv')


# Now check the original data which may help check the right rate ---------




cMerger_data7 <- merge(Merger_data10,Final_app_count2[,c(1,17)],by.x ='publisher_name18',all.x = TRUE)
cMerger_data8 <- merge(cMerger_data7,Final_app_count2[,c(2,16)],by.x ='publisher_name17',all.x = TRUE)

cMerger_data8$topornot <- ifelse(cMerger_data8$Id17 %in%top$Id17,1,0)


# objaim <- Final_app_count2[,c(8:10,17:18)]
# 
# BestR <- merge(cMerger_data8,objaim,by = 'CRITERIA',all.x = T)

write.csv(cMerger_data8,'./1718/BestR.csv')


# now step2 ---------------------------------------------------------------
BestR <- cMerger_data8
# all app details merger app we can search ------------------------------------------
Pick17data <- Srawdata1[Srawdata1$publisher_name %in% BestR$publisher_name17,]
Pick18data <- Srawdata2[Srawdata2$publisher_name %in% BestR$publisher_name18,]
write.csv(Pick17data,'./1718/Pick17data.csv')
write.csv(Pick18data,'./1718/Pick18data.csv')
sectdata <- BestR[is.na(BestR$Ownapps1718all),]
write.csv(sectdata,'./1718/sectdata.csv')

#Use the criteria to check whether it is real mergerhha

sectdata1 <- sectdata %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(sectdata$publisher_name17)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>=2,]
raw_EXTR3 <- raw_EXTR[raw_EXTR$Freq==1,]


Mergerfin <- sectdata[sectdata$publisher_name17 %in% raw_EXTR2$Var1,]
Mergerfin$Newapp17 <- Mergerfin$Ownapps17all-Mergerfin$Ownapps17
Mergerfin$Newapp18 <- Mergerfin$Ownapps18all-Mergerfin$Ownapps18
Notsure_nermge <- sectdata[!sectdata$publisher_name17 %in% raw_EXTR2$Var1,]
Notsure_nermge$Newapp17 <- Notsure_nermge$Ownapps17all-Notsure_nermge$Ownapps17
Notsure_nermge$Newapp18 <- Notsure_nermge$Ownapps18all-Notsure_nermge$Ownapps18
write.csv(Mergerfin,'./1718/Mergerfin17.csv')
write.csv(Notsure_nermge,'./1718/Notsure_nermge17.csv')



conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")
dbRemoveTable(conn,"Mergerfin")
dbRemoveTable(conn,"Notsure_nermge")
dbExistsTable(conn,"Notsure_nermge")
dbWriteTable(conn,"Mergerfin",Mergerfin,append=TRUE)
dbWriteTable(conn,"Notsure_nermge",Notsure_nermge,append=TRUE)
dbDisconnect(conn)


conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")

Detapp17 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20170814 A JOIN Mergerfin B ON A.id = B.id17")
Detapp18 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20180813 A JOIN Mergerfin B ON A.id = B.id17")
dbDisconnect(conn)
# SELECT A.* ,B.* from AppMonstaPlayStoreDetails18170814 A JOIN Topapps B ON A.id = B.id



# Detapp17 <- Srawdata1[Srawdata1$id %in% Mergerfin$Id17,]
# Detapp18 <- Srawdata2[Srawdata2$id %in% Mergerfin$Id18,]

colnames(Detapp17) <- paste(colnames(Detapp17),'17',sep='')
colnames(Detapp18) <- paste(colnames(Detapp18),'18',sep='')
colnames(Mergerfin)[3] <- 'id17'
colnames(Mergerfin)[4] <- 'id18'
Mergerfin2 <- merge(Mergerfin,Detapp17,by ='id17',all.x = T)
Mergerfin3 <- merge(Mergerfin2,Detapp18,by='id18',all.x = T)

Mergerfin3 <- Mergerfin3 %>%distinct(id17, .keep_all =TRUE)
Mergerfin3 <- Mergerfin3[order(Mergerfin3$publisher_name17.x,decreasing = T),]


write.csv(Mergerfin3,'./1718/Mergerfin3.csv')

conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")

Detapp17 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20170814 A JOIN Notsure_nermge B ON A.id = B.id17")
Detapp18 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20180813 A JOIN Notsure_nermge B ON A.id = B.id17")
dbDisconnect(conn)

colnames(Detapp17) <- paste(colnames(Detapp17),'17',sep='')
colnames(Detapp18) <- paste(colnames(Detapp18),'18',sep='')
colnames(Notsure_nermge)[3] <- 'id17'
colnames(Notsure_nermge)[4] <- 'id18'
Notsure_nermge2 <- merge(Notsure_nermge,Detapp17,by='id17',all.x = T)
Notsure_nermge3 <- merge(Notsure_nermge2,Detapp18,by='id18',all.x = T)

Notsure_nermge3 <- Notsure_nermge3 %>%distinct(id17, .keep_all =TRUE)
Notsure_nermge3 <- Notsure_nermge3[order(Notsure_nermge3$publisher_name17.x,decreasing = T),]

write.csv(Notsure_nermge3,'./1718/Notsure_nermge3.csv')














# # select the data based on 2017 data --------------------------------------
# 
# 
# # collect top app details data --------------------------------------------
# conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
# Srawdata1 <- dbGetQuery(conn, "SELECT A.* ,B.* from AppMonstaPlayStoreDetails20150817 A JOIN Topapps B ON A.id = B.id")
# dbDisconnect(conn)
# Srawdata2 <- Srawdata1[,-c(32,33)]
# write.csv(Srawdata1,'exist15all.csv')
# 
# conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
# Srawdata1 <- dbGetQuery(conn, "SELECT A.* ,B.* from AppMonstaPlayStoreDetails20160815 A JOIN Topapps B ON A.id = B.id")
# Srawdata2 <- Srawdata1[,-c(32,33)]
# write.csv(Srawdata2,'exist16all.csv')
# 
# conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
# Srawdata1 <- dbGetQuery(conn, "SELECT A.* ,B.* from AppMonstaPlayStoreDetails20170814 A JOIN Topapps B ON A.id = B.id")
# dbDisconnect(conn)
# Srawdata2 <- Srawdata1[,-c(32,33)]
# write.csv(Srawdata1,'exist17all.csv')
# 
# conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
# Srawdata1 <- dbGetQuery(conn, "SELECT A.* ,B.* from AppMonstaPlayStoreDetails20180813 A JOIN Topapps B ON A.id = B.id")
# dbDisconnect(conn)
# Srawdata2 <- Srawdata1[,-c(32,33)]
# write.csv(Srawdata1,'exist18all.csv')
# 
# conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
# Srawdata1 <- dbGetQuery(conn, "SELECT A.* ,B.* from AppMonstaPlayStoreDetails20190812 A JOIN Topapps B ON A.id = B.id")
# dbDisconnect(conn)
# Srawdata2 <- Srawdata1[,-c(32,33)]
# write.csv(Srawdata1,'exist19all.csv')




