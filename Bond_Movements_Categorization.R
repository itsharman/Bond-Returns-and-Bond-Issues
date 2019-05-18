#initialize libraries; RMySQL is essential for proper connection
library(RODBC)
library(odbc)
library(DBI)
library(RMySQL)
library(stringr)
library(dplyr)
library(data.table)

#establish connection
con <- dbConnect(MySQL(), user='jwd47', password='MMcmv5jD', dbname='bond2', host='localhost')
outputfolder  <- ""
comp <- "SOMResearch"

#querydata

ratinginfo <- dbGetQuery(con, "select CUSIP8, RATING_DATE, BofA, Barc, PriorBofA, PriorBarc FROM rate_score_2018 limit 10000")

#find mean of bofa and barc ratings//replace these columns in ratings table with combined score
ratinginfo$meanrating1 = ((ratinginfo$PriorBofA + ratinginfo$PriorBarc) / 2)
ratinginfo$PriorBarc <- NULL
ratinginfo$PriorBofA <- NULL

ratinginfo$meanrating2 = ((ratinginfo$BofA + ratinginfo$Barc) / 2)
ratinginfo$BofA <- NULL
ratinginfo$Barc <- NULL

#remove rows with NA values for meanrating1, as this suggests that this data represents a bond that has just been issued and/or rated 
ratinginfo <- ratinginfo[!is.na(ratinginfo$meanrating1),]

#use mean rating to categorize into 'Investment Grade (IG)', 'Junk (J)', and Distressed '(D)' //delete mean ratings from table

for (i in 1: length(ratinginfo$meanrating1)){
  if (ratinginfo$meanrating1[i] <= 10){
    ratinginfo$RATING1[i] = "IG"
  }
  else if(ratinginfo$meanrating1[i] <= 21)
    ratinginfo$RATING1[i] = "J"
  else ratinginfo$RATING1[i] = "D"
}

ratinginfo$meanrating1 <- NULL

for (i in 1: length(ratinginfo$meanrating2)){
  if (ratinginfo$meanrating2[i] <= 10){
    ratinginfo$RATING2[i] = "IG"
  }
  else if(ratinginfo$meanrating2[i] <= 21)
    ratinginfo$RATING2[i] = "J"
  else ratinginfo$RATING2[i] = "D"
}

ratinginfo$meanrating2 <- NULL

#We want time period >=20020101 to make subsequent sorting more efficient
ratinginfo <- subset(ratinginfo, RATING_DATE >= 20020101)

#remove rows in which RATING1 and RATING2 match (since we want shifts) 
ratinginfo <- ratinginfo[!(ratinginfo$RATING1==ratinginfo$RATING2),]

#load into mysql table;
dbWriteTable(con, "Bond_Ratings_Movements_2002_201803", ratinginfo, overwrite = TRUE, row.names = FALSE)