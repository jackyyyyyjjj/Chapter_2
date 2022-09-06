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
conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app_merger.sqlite")
scoreinclude <- dbGetQuery(conn, "SELECT Id20,Id21, publisher_name20, publisher_name21,publisher_email20,publisher_email21,IndexN, simscore from similarityTable2021")
rawdata <- dbGetQuery(conn, "SELECT Id20,Id21, publisher_name20, publisher_name21,publisher_email20,publisher_email21,IndexN from duplicateTable2021")
dbDisconnect(conn)

# Get the all the appowner's data info from the very original data --------
conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
Srawdata1 <- dbGetQuery(conn, "SELECT * from AppMonstaPlayStoreDetails20200820")
Srawdata2 <- dbGetQuery(conn, "SELECT * from AppMonstaPlayStoreDetails20210816")
dbDisconnect(conn)

conn <- dbConnect(RSQLite::SQLite(), "topapps.sqlite")
top <- dbGetQuery(conn, "SELECT Id17 from duplicateTable1718")
dbDisconnect(conn)
colnames(top) <- c('Id20')

scoreinclude$CRITERIA <- paste(scoreinclude$publisher_name20,scoreinclude$publisher_name21,sep = '-')
rawdata$CRITERIA <- paste(rawdata$publisher_name20,rawdata$publisher_name21,sep = '-')
#clean the repeated ids
scoreinclude1 <- scoreinclude %>%
  distinct(Id20, .keep_all =TRUE)

rawdata1 <- rawdata %>%
  distinct(Id20, .keep_all =TRUE)

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
N.20 <- Srawdata1[Srawdata1$publisher_name %in% scoreinclude0.2$publisher_name20,]
#Find out all the merger activities in the database
duplicate_data20 <- table(N.20$publisher_name)

Fre_data20 <- as.data.frame(duplicate_data20) 

colnames(Fre_data20)[1] <- 'publisher_name20'
colnames(Fre_data20)[2] <- 'Ownapps20all'

#find out all the developers' app data inside 2021
N.21 <- Srawdata2[Srawdata2$publisher_name %in% scoreinclude0.2$publisher_name21,]
duplicate_data21 <- table(N.21$publisher_name)

Fre_data21 <- as.data.frame(duplicate_data21) 

colnames(Fre_data21)[1] <- 'publisher_name21'
colnames(Fre_data21)[2] <- 'Ownapps21all'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data1 <- merge(scoreinclude0.2,Fre_data20,by='publisher_name20')
Merger_data1 <- merge(Merger_data1,Fre_data21,by='publisher_name21')



#get the data from rawdata, because in this dataset the ids are the same
# GET ALL THE developer's own apps
N.20 <- rawdata1[rawdata1$publisher_name20 %in% scoreinclude0.2$publisher_name20,]
#Find out all the merger activities in the database
duplicate_data20 <- table(N.20$publisher_name20)

Fre_data20 <- as.data.frame(duplicate_data20) 

colnames(Fre_data20)[1] <- 'publisher_name20'
colnames(Fre_data20)[2] <- 'Ownapps20'

#find out all the developers' app data inside 2021
N.21 <- rawdata1[rawdata1$publisher_name21 %in% scoreinclude0.2$publisher_name21,]
duplicate_data21 <- table(N.21$publisher_name21)

Fre_data21 <- as.data.frame(duplicate_data21) 

colnames(Fre_data21)[1] <- 'publisher_name21'
colnames(Fre_data21)[2] <- 'Ownapps21'


#Merge the data with Merger_data
#From this data set, it shows that the developers will not provide all the apps to others,only sell part of it
Merger_data2 <- merge(scoreinclude0.2,Fre_data20,by='publisher_name20')
Merger_data2 <- merge(Merger_data2,Fre_data21,by='publisher_name21')

seldata <- Merger_data2[,c(3,10,11)]
seldata2 <- merge(Merger_data1,seldata,by='Id20',all.x = T) 


### IF i MERGE THE DIFFERENT NAME





#again get the cross data
raw2021 <- Srawdata2[Srawdata2$publisher_name %in% seldata2$publisher_name20,]
raw_data2021 <- table(raw2021$publisher_name)
raw_data2021all <- as.data.frame(raw_data2021) 
colnames(raw_data2021all)[1] <- 'publisher_name20'
colnames(raw_data2021all)[2] <- 'Ownapps2021all'

raw2120 <- Srawdata1[Srawdata1$publisher_name %in% seldata2$publisher_name21,]
raw_data2120 <- table(raw2120$publisher_name)
raw_data2120all <- as.data.frame(raw_data2120) 
colnames(raw_data2120all)[1] <- 'publisher_name21'
colnames(raw_data2120all)[2] <- 'Ownapps2120all'


Merger_data7 <- merge(seldata2,raw_data2021all,by.x ='publisher_name20',all.x = TRUE)
Merger_data8 <- merge(Merger_data7,raw_data2120all,by.x ='publisher_name21',all.x = TRUE)
#DELETE THE SAME ID IN 2020 AND 2021
Merger_dataK <- Merger_data8[Merger_data8$publisher_name21!=Merger_data8$publisher_name20,]
Merger_data8 <- Merger_dataK 
#I will use the criteria to distint the data I think here I only need to use the pubisher_name 20 not the 21 haha

Merger_data9 <- Merger_data8 %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(Merger_data9$publisher_name20)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>=2,]
raw_EXTR3 <- raw_EXTR[raw_EXTR$Freq==1,]

Merger_data2021 <- Merger_data8[Merger_data8$publisher_name20 %in% raw_EXTR2$Var1,]
Notsure_nermge <- Merger_data8[!Merger_data8$publisher_name20 %in% raw_EXTR2$Var1,]
Merger_data10 <-Merger_data8
#Here I can use the notsure merger to run all, then final step,we can separate them based on merger10
write.csv(Merger_data2021,'./2021/Merger_data2021.csv')
write.csv(Notsure_nermge,'./2021/Notsure_nermge.csv')

# ####Maybe check the publisher email is a better way
# Datafull_DIFF.emid <- Datafull[Datafull$A.publisher_email!=Datafull$B.publisher_email,]
# dbDisconnect(conn)
# library(openxlsx)
# text < read.csv('C1_X.csv')
# renderTable({head(painters,input$num)})



#check the top list app in the two raw datasets
exist20 <- Srawdata1[Srawdata1$id %in% top$Id20,]
exist21 <- Srawdata2[Srawdata2$id %in% top$Id20,]

existmerger_top <- Merger_data10[Merger_data10$Id20 %in% top$Id20,]
existmerger_oth <- Merger_data10[!(Merger_data10$Id20 %in% top$Id20),]
# write.csv(exist20,'exist20.csv')
# write.csv(exist21,'exist21.csv')


# Now I took 2020 as an example to analysis -------------------------------
#I need to extract all the apps in merger between 20 and 21, then according this to find the how many top apps owned by the publishers
top_merger <- exist20[exist20$id %in% existmerger_top$Id20,]
Count_app_top <- table(top_merger$publisher_name) 
Count_app_top <- as.data.frame(Count_app_top)
colnames(Count_app_top)[1] <- 'publisher_name20'
colnames(Count_app_top)[2] <- 'Count_app_top20'

apps_all <- existmerger_top[existmerger_top$publisher_name20 %in% Count_app_top$publisher_name20,]
apps_all1 <- apps_all %>%distinct(publisher_name20, .keep_all =TRUE)
apps_all20 <- merge(apps_all1,Count_app_top,by='publisher_name20')


# Now I need to get the same thing in 2021 --------------------------------
#I need to extract all the apps in merger between 20 and 21, then according this to find the how many top apps owned by the publishers
# top_merger21 <- exist21all[exist21all$publisher_name %in% existmerger_top$publisher_name21,]
# Count_app_top21 <- table(top_merger21$publisher_name) 
# Count_app_top21 <- as.data.frame(Count_app_top21)
# colnames(Count_app_top21)[1] <- 'publisher_name21'
# colnames(Count_app_top21)[2] <- 'Count_app_top21'

top_merger21 <- exist21[exist21$id %in% existmerger_top$Id21,]
Count_app_top21 <- table(top_merger21$publisher_name) 
Count_app_top21 <- as.data.frame(Count_app_top21)
colnames(Count_app_top21)[1] <- 'publisher_name21'
colnames(Count_app_top21)[2] <- 'Count_app_top21'

apps_all <- existmerger_top[existmerger_top$publisher_name21 %in% Count_app_top21$publisher_name21,]
apps_all1 <- apps_all %>%distinct(publisher_name21, .keep_all =TRUE)
apps_all21 <- merge(apps_all1,Count_app_top21,by='publisher_name21')

apps_all21 <- apps_all21[,c(1,16)]

Final_app_count2 <- merge(apps_all20,apps_all21,by='publisher_name21',all.x = T)




# write.csv(Final_app_count,'./2021/Final_app_count.csv')
write.csv(Final_app_count2,'./2021/Final_app_count2.csv')


# Now check the original data which may help check the right rate ---------




cMerger_data7 <- merge(Merger_data10,Final_app_count2[,c(1,17)],by.x ='publisher_name21',all.x = TRUE)
cMerger_data8 <- merge(cMerger_data7,Final_app_count2[,c(2,16)],by.x ='publisher_name20',all.x = TRUE)

cMerger_data8$topornot <- ifelse(cMerger_data8$Id20 %in%top$Id20,1,0)


# objaim <- Final_app_count2[,c(8:10,20:21)]
# 
# BestR <- merge(cMerger_data8,objaim,by = 'CRITERIA',all.x = T)

write.csv(cMerger_data8,'./2021/BestR.csv')


# now step2 ---------------------------------------------------------------
BestR <- cMerger_data8
# all app details merger app we can search ------------------------------------------
Pick20data <- Srawdata1[Srawdata1$publisher_name %in% BestR$publisher_name20,]
Pick21data <- Srawdata2[Srawdata2$publisher_name %in% BestR$publisher_name21,]
write.csv(Pick20data,'./2021/Pick20data.csv')
write.csv(Pick21data,'./2021/Pick21data.csv')
sectdata <- BestR[is.na(BestR$Ownapps2021all),]
write.csv(sectdata,'./2021/sectdata.csv')

#Use the criteria to check whether it is real mergerhha

sectdata1 <- sectdata %>%
  distinct(CRITERIA,.keep_all = TRUE)
EXTRACTDATA <- table(sectdata$publisher_name20)

raw_EXTR <- as.data.frame(EXTRACTDATA)
raw_EXTR2 <- raw_EXTR[raw_EXTR$Freq>=2,]
raw_EXTR3 <- raw_EXTR[raw_EXTR$Freq==1,]


Mergerfin <- sectdata[sectdata$publisher_name20 %in% raw_EXTR2$Var1,]
Mergerfin$Newapp20 <- Mergerfin$Ownapps20all-Mergerfin$Ownapps20
Mergerfin$Newapp21 <- Mergerfin$Ownapps21all-Mergerfin$Ownapps21
Notsure_nermge <- sectdata[!sectdata$publisher_name20 %in% raw_EXTR2$Var1,]
Notsure_nermge$Newapp20 <- Notsure_nermge$Ownapps20all-Notsure_nermge$Ownapps20
Notsure_nermge$Newapp21 <- Notsure_nermge$Ownapps21all-Notsure_nermge$Ownapps21
write.csv(Mergerfin,'./2021/Mergerfin20.csv')
write.csv(Notsure_nermge,'./2021/Notsure_nermge20.csv')


conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")
dbRemoveTable(conn,"Mergerfin")
dbRemoveTable(conn,"Notsure_nermge")
dbExistsTable(conn,"Notsure_nermge")
dbWriteTable(conn,"Mergerfin",Mergerfin,append=TRUE)
dbWriteTable(conn,"Notsure_nermge",Notsure_nermge,append=TRUE)
dbDisconnect(conn)


conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")

Detapp20 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20200817 A JOIN Mergerfin B ON A.id = B.id20")
Detapp21 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20210816 A JOIN Mergerfin B ON A.id = B.id20")
dbDisconnect(conn)
# SELECT A.* ,B.* from AppMonstaPlayStoreDetails20170814 A JOIN Topapps B ON A.id = B.id



# Detapp17 <- Srawdata1[Srawdata1$id %in% Mergerfin$Id17,]
# Detapp18 <- Srawdata2[Srawdata2$id %in% Mergerfin$Id18,]

colnames(Detapp20) <- paste(colnames(Detapp20),'20',sep='')
colnames(Detapp21) <- paste(colnames(Detapp21),'21',sep='')
colnames(Mergerfin)[3] <- 'id20'
colnames(Mergerfin)[4] <- 'id21'
Mergerfin2 <- merge(Mergerfin,Detapp20,by ='id20',all.x = T)
Mergerfin3 <- merge(Mergerfin2,Detapp21,by='id21',all.x = T)

Mergerfin3 <- Mergerfin3 %>%distinct(id20, .keep_all =TRUE)

Mergerfin3 <- Mergerfin3[order(Mergerfin3$publisher_name20.x,decreasing = T),]

write.csv(Mergerfin3,'./2021/Mergerfin3.csv')

conn <- dbConnect(RSQLite::SQLite(), "E:/haha/appmonsta_app.sqlite")

Detapp20 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20200817 A JOIN Notsure_nermge B ON A.id = B.id20")
Detapp21 <- dbGetQuery(conn, "SELECT A.*  from AppMonstaPlayStoreDetails20210816 A JOIN Notsure_nermge B ON A.id = B.id20")
dbDisconnect(conn)

colnames(Detapp20) <- paste(colnames(Detapp20),'20',sep='')
colnames(Detapp21) <- paste(colnames(Detapp21),'21',sep='')
colnames(Notsure_nermge)[3] <- 'id20'
colnames(Notsure_nermge)[4] <- 'id21'
Notsure_nermge2 <- merge(Notsure_nermge,Detapp20,by='id20',all.x = T)
Notsure_nermge3 <- merge(Notsure_nermge2,Detapp21,by='id21',all.x = T)

Notsure_nermge3 <- Notsure_nermge3 %>%distinct(id20, .keep_all =TRUE)
Notsure_nermge3 <- Notsure_nermge3[order(Notsure_nermge3$publisher_name20.x,decreasing = T),]

write.csv(Notsure_nermge3,'./2021/Notsure_nermge3.csv')


kk <- as.data.frame(table(Notsure_nermge3$publisher_name20.x))

