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
scoreinclude <- dbGetQuery(conn, "SELECT Id18,Id19, publisher_name18, publisher_name19,publisher_email18,publisher_email19,IndexN, simscore from similarityTable1819")
rawdata <- dbGetQuery(conn, "SELECT Id18,Id19, publisher_name18, publisher_name19,publisher_email18,publisher_email19,IndexN from duplicateTable1819")
dbDisconnect(conn)


conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")
Srawdata1 <- dbGetQuery(conn, "SELECT * from AppMonstaPlayStoreDetails20180813")
Srawdata2 <- dbGetQuery(conn, "SELECT * from AppMonstaPlayStoreDetails20190812")
dbDisconnect(conn)

conn <- dbConnect(RSQLite::SQLite(), "topapps.sqlite")
top <- dbGetQuery(conn, "SELECT Id17 from duplicateTable1718")
dbDisconnect(conn)


colnames(top) <- c('Id18')



scoreinclude$CRITERIA <- paste(scoreinclude$publisher_name18,scoreinclude$publisher_name19,sep = '-')
rawdata$CRITERIA <- paste(rawdata$publisher_name18,rawdata$publisher_name19,sep = '-')
#clean the repeated ids
scoreinclude1 <- scoreinclude %>%
  distinct(Id18, .keep_all =TRUE)

rawdata1 <- rawdata %>%
  distinct(Id18, .keep_all =TRUE)

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
N.18 <- Srawdata1[Srawdata1$publisher_name %in% scoreinclude0.2$publisher_name18,]
#Find out all the merger activities in the database
duplicate_data18 <- table(N.18$publisher_name)

Fre_data18 <- as.data.frame(duplicate_data18) 

colnames(Fre_data18)[1] <- 'publisher_name18'
colnames(Fre_data18)[2] <- 'Ownapps18all'

#find out all the developers' app data inside 2019
N.19 <- Srawdata2[Srawdata2$publisher_name %in% scoreinclude0.2$publisher_name19,]
duplicate_data19 <- table(N.19$publisher_name)

Fre_data19 <- as.data.frame(duplicate_data19) 

colnames(Fre_data19)[1] <- 'publisher_name19'
colnames(Fre_data19)[2] <- 'Ownapps19all'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data1 <- merge(scoreinclude0.2,Fre_data18,by='publisher_name18')
Merger_data1 <- merge(Merger_data1,Fre_data19,by='publisher_name19')



#get the data from rawdata, because in this dataset the ids are the same
# GET ALL THE developer's own apps
N.18 <- rawdata1[rawdata1$publisher_name18 %in% scoreinclude0.2$publisher_name18,]
#Find out all the merger activities in the database
duplicate_data18 <- table(N.18$publisher_name18)

Fre_data18 <- as.data.frame(duplicate_data18) 

colnames(Fre_data18)[1] <- 'publisher_name18'
colnames(Fre_data18)[2] <- 'Ownapps18'

#find out all the developers' app data inside 2019
N.19 <- rawdata1[rawdata1$publisher_name19 %in% scoreinclude0.2$publisher_name19,]
duplicate_data19 <- table(N.19$publisher_name19)

Fre_data19 <- as.data.frame(duplicate_data19) 

colnames(Fre_data19)[1] <- 'publisher_name19'
colnames(Fre_data19)[2] <- 'Ownapps19'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data2 <- merge(scoreinclude0.2,Fre_data18,by='publisher_name18')
Merger_data2 <- merge(Merger_data2,Fre_data19,by='publisher_name19')

seldata <- Merger_data2[,c(3,10,11)]
seldata2 <- merge(Merger_data1,seldata,by='Id18',all.x = T) 


### IF i MERGE THE DIFFERENT NAME





#again get the cross data
raw1819 <- Srawdata2[Srawdata2$publisher_name %in% seldata2$publisher_name18,]
raw_data1819 <- table(raw1819$publisher_name)
raw_data1819all <- as.data.frame(raw_data1819) 
colnames(raw_data1819all)[1] <- 'publisher_name18'
colnames(raw_data1819all)[2] <- 'Ownapps1819all'

raw1918 <- Srawdata1[Srawdata1$publisher_name %in% seldata2$publisher_name19,]
raw_data1918 <- table(raw1918$publisher_name)
raw_data1918all <- as.data.frame(raw_data1918) 
colnames(raw_data1918all)[1] <- 'publisher_name19'
colnames(raw_data1918all)[2] <- 'Ownapps1918all'


Merger_data7 <- merge(seldata2,raw_data1819all,by.x ='publisher_name18',all.x = TRUE)
Merger_data8 <- merge(Merger_data7,raw_data1918all,by.x ='publisher_name19',all.x = TRUE)
#DELETE THE SAME ID IN 2018 AND 2019
Merger_dataK <- Merger_data8[Merger_data8$publisher_name19!=Merger_data8$publisher_name18,]
Merger_data8 <- Merger_dataK 
#I will use the criteria to distint the data

Merger_data9 <- Merger_data8 %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(Merger_data9$publisher_name19)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>=2,]
raw_EXTR3 <- raw_EXTR[raw_EXTR$Freq==1,]

Merger_data1819 <- Merger_data8[Merger_data8$publisher_name19 %in% raw_EXTR2$Var1,]
Notsure_nermge <- Merger_data8[!Merger_data8$publisher_name19 %in% raw_EXTR2$Var1,]
Merger_data10 <-Merger_data8
#Here I can use the notsure merger to run all, then final step,we can separate them based on merger10
write.csv(Merger_data1819,'./1819/Merger_data1819.csv')
write.csv(Notsure_nermge,'./1819/Notsure_nermge.csv')

# ####Maybe check the publisher email is a better way
# Datafull_DIFF.emid <- Datafull[Datafull$A.publisher_email!=Datafull$B.publisher_email,]
# dbDisconnect(conn)
# library(openxlsx)
# text < read.csv('C1_X.csv')
# renderTable({head(painters,input$num)})


conn <- dbConnect(RSQLite::SQLite(), "topapps.sqlite")
top <- dbGetQuery(conn, "SELECT Id18 from duplicateTable1819")
dbDisconnect(conn)
#check the top list app in the two raw datasets
exist18 <- Srawdata1[Srawdata1$id %in% top$Id18,]
exist19 <- Srawdata2[Srawdata2$id %in% top$Id18,]

existmerger_top <- Merger_data10[Merger_data10$Id18 %in% top$Id18,]
existmerger_oth <- Merger_data10[!(Merger_data10$Id18 %in% top$Id18),]
# write.csv(exist18,'exist18.csv')
# write.csv(exist19,'exist19.csv')


# Now I took 2018 as an example to analysis -------------------------------
#I need to extract all the apps in merger between 18 and 19, then according this to find the how many top apps owned by the publishers
top_merger <- exist18[exist18$id %in% existmerger_top$Id18,]
Count_app_top <- table(top_merger$publisher_name) 
Count_app_top <- as.data.frame(Count_app_top)
colnames(Count_app_top)[1] <- 'publisher_name18'
colnames(Count_app_top)[2] <- 'Count_app_top18'

apps_all <- existmerger_top[existmerger_top$publisher_name18 %in% Count_app_top$publisher_name18,]
apps_all1 <- apps_all %>%distinct(publisher_name18, .keep_all =TRUE)
apps_all18 <- merge(apps_all1,Count_app_top,by='publisher_name18')


# Now I need to get the same thing in 2019 --------------------------------
#I need to extract all the apps in merger between 18 and 19, then according this to find the how many top apps owned by the publishers
# top_merger19 <- exist19all[exist19all$publisher_name %in% existmerger_top$publisher_name19,]
# Count_app_top19 <- table(top_merger19$publisher_name) 
# Count_app_top19 <- as.data.frame(Count_app_top19)
# colnames(Count_app_top19)[1] <- 'publisher_name19'
# colnames(Count_app_top19)[2] <- 'Count_app_top19'

top_merger19 <- exist19[exist19$id %in% existmerger_top$Id19,]
Count_app_top19 <- table(top_merger19$publisher_name) 
Count_app_top19 <- as.data.frame(Count_app_top19)
colnames(Count_app_top19)[1] <- 'publisher_name19'
colnames(Count_app_top19)[2] <- 'Count_app_top19'

apps_all <- existmerger_top[existmerger_top$publisher_name19 %in% Count_app_top19$publisher_name19,]
apps_all1 <- apps_all %>%distinct(publisher_name19, .keep_all =TRUE)
apps_all19 <- merge(apps_all1,Count_app_top19,by='publisher_name19')

apps_all19 <- apps_all19[,c(1,16)]

Final_app_count2 <- merge(apps_all18,apps_all19,by='publisher_name19',all.x = T)




# write.csv(Final_app_count,'./1819/Final_app_count.csv')
write.csv(Final_app_count2,'./1819/Final_app_count2.csv')


# Now check the original data which may help check the right rate ---------




cMerger_data7 <- merge(Merger_data10,Final_app_count2[,c(1,17)],by.x ='publisher_name19',all.x = TRUE)
cMerger_data8 <- merge(cMerger_data7,Final_app_count2[,c(2,16)],by.x ='publisher_name18',all.x = TRUE)

cMerger_data8$topornot <- ifelse(cMerger_data8$Id18 %in%top$Id18,1,0)


# objaim <- Final_app_count2[,c(8:10,18:19)]
# 
# BestR <- merge(cMerger_data8,objaim,by = 'CRITERIA',all.x = T)

write.csv(cMerger_data8,'./1819/BestR.csv')


# now step2 ---------------------------------------------------------------
BestR <- cMerger_data8
# all app details merger app we can search ------------------------------------------
Pick18data <- Srawdata1[Srawdata1$publisher_name %in% BestR$publisher_name18,]
Pick19data <- Srawdata2[Srawdata2$publisher_name %in% BestR$publisher_name19,]
write.csv(Pick18data,'./1819/Pick18data.csv')
write.csv(Pick19data,'./1819/Pick19data.csv')
sectdata <- BestR[is.na(BestR$Ownapps1819all),]
write.csv(sectdata,'./1819/sectdata.csv')

#Use the criteria to check whether it is real mergerhha

sectdata1 <- sectdata %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(sectdata$publisher_name18)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>=2,]
raw_EXTR3 <- raw_EXTR[raw_EXTR$Freq==1,]


Mergerfin <- sectdata[sectdata$publisher_name18 %in% raw_EXTR2$Var1,]
Mergerfin$Newapp18 <- Mergerfin$Ownapps18all-Mergerfin$Ownapps18
Mergerfin$Newapp19 <- Mergerfin$Ownapps19all-Mergerfin$Ownapps19
Notsure_nermge <- sectdata[!sectdata$publisher_name18 %in% raw_EXTR2$Var1,]
Notsure_nermge$Newapp18 <- Notsure_nermge$Ownapps18all-Notsure_nermge$Ownapps18
Notsure_nermge$Newapp19 <- Notsure_nermge$Ownapps19all-Notsure_nermge$Ownapps19
write.csv(Mergerfin,'./1819/Mergerfin18.csv')
write.csv(Notsure_nermge,'./1819/Notsure_nermge18.csv')


conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")
dbRemoveTable(conn,"Mergerfin")
dbRemoveTable(conn,"Notsure_nermge")
dbExistsTable(conn,"Notsure_nermge")
dbWriteTable(conn,"Mergerfin",Mergerfin,append=TRUE)
dbWriteTable(conn,"Notsure_nermge",Notsure_nermge,append=TRUE)
dbDisconnect(conn)


conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")

Detapp18 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20180813 A JOIN Mergerfin B ON A.id = B.id18")
Detapp19 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20190812 A JOIN Mergerfin B ON A.id = B.id18")
dbDisconnect(conn)
# SELECT A.* ,B.* from AppMonstaPlayStoreDetails18170814 A JOIN Topapps B ON A.id = B.id



# Detapp17 <- Srawdata1[Srawdata1$id %in% Mergerfin$Id17,]
# Detapp18 <- Srawdata2[Srawdata2$id %in% Mergerfin$Id18,]

colnames(Detapp18) <- paste(colnames(Detapp18),'18',sep='')
colnames(Detapp19) <- paste(colnames(Detapp19),'19',sep='')
# Detapp19 <- Detapp19[,-c(42,43)]
colnames(Mergerfin)[3] <- 'id18'
colnames(Mergerfin)[4] <- 'id19'
Mergerfin2 <- merge(Mergerfin,Detapp18,by ='id18',all.x = T)
Mergerfin3 <- merge(Mergerfin2,Detapp19,by='id19',all.x = T)

Mergerfin3 <- Mergerfin3 %>%distinct(id18, .keep_all =TRUE)
Mergerfin3 <- Mergerfin3[order(Mergerfin3$publisher_name18.x,decreasing = T),]


write.csv(Mergerfin3,'./1819/Mergerfin3.csv')

conn <- dbConnect(RSQLite::SQLite(), "E:/Data/Appmonsta_NoDead/Aug15/appmonsta_app.sqlite")

Detapp18 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20180813 A JOIN Notsure_nermge B ON A.id = B.id18")
Detapp19 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20190812 A JOIN Notsure_nermge B ON A.id = B.id18")
dbDisconnect(conn)

colnames(Detapp18) <- paste(colnames(Detapp18),'18',sep='')
colnames(Detapp19) <- paste(colnames(Detapp19),'19',sep='')
colnames(Notsure_nermge)[3] <- 'id18'
colnames(Notsure_nermge)[4] <- 'id19'
Notsure_nermge2 <- merge(Notsure_nermge,Detapp18,by='id18',all.x = T)
Notsure_nermge3 <- merge(Notsure_nermge2,Detapp19,by='id19',all.x = T)

Notsure_nermge3 <- Notsure_nermge3 %>%distinct(id18, .keep_all =TRUE)
Notsure_nermge3 <- Notsure_nermge3[order(Notsure_nermge3$publisher_name18.x,decreasing = T),]

write.csv(Notsure_nermge3,'./1819/Notsure_nermge3.csv')


kk <- as.data.frame(table(Notsure_nermge3$publisher_name18.x))
