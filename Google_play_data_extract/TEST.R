rm (list = ls(all=TRUE))
graphics.off()
library(stringr)
script_path <- rstudioapi::getActiveDocumentContext()$path
hash = "(.*)/"
setwd(str_extract(script_path, hash)) 


library('RSQLite')
library('DBI')
conn <- dbConnect(RSQLite::SQLite(), "./data/appmonsta_app.sqlite")
kk <- dbGetQuery(conn, "SELECT A.id,A.publisher_url,A.publisher_id,A.publisher_email,A.publisher_name,
B.id,B.publisher_url,B.publisher_id,B.publisher_email,B.publisher_name 
                 from AppMonstaPlayStoreDetails20150817 A JOIN AppMonstaPlayStoreDetails20160815 B ON 
                 A.id = B.id")


Datafull <- kk
####A is the data from 2015, B is the data from 2016
colnames(Datafull) <- c('A.id','A.publisher_url','A.publisher_id','A.publisher_email','A.publisher_name',
               'B.id','B.publisher_url','B.publisher_id','B.publisher_email','B.publisher_name')


Datafull$A.publisher_id <- tolower(Datafull$A.publisher_id)
Datafull$A.publisher_id <- gsub('\\s','',Datafull$A.publisher_id)

Datafull$B.publisher_id <- tolower(Datafull$B.publisher_id)
Datafull$B.publisher_id <- gsub('\\s','',Datafull$B.publisher_id)

####Maybe check the publisher.id 
Datafull_DIFF.pubid <- Datafull[Datafull$A.publisher_id!=Datafull$B.publisher_id,]
dbDisconnect(conn)
####Maybe check the publisher email is a better way
Datafull_DIFF.emid <- Datafull[Datafull$A.publisher_email!=Datafull$B.publisher_email,]
dbDisconnect(conn)
