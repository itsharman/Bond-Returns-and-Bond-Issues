#initialize libraries; RMySQL is essential for proper connection
library(RODBC)
library(odbc)
library(DBI)
library(RMySQL)
library(stringr)
library(dplyr)
library(data.table)

#establish connection
con <- dbConnect(MySQL(), user='hs682', password='dURnM4tW', dbname='bond2', host='localhost')
outputfolder  <- ""
comp <- "SOMResearch"

#querydata
issueinfo <- dbGetQuery(con, "select COMPLETE_CUSIP, OFFERING_DATE, OFFERING_AMT, OFFERING_PRICE, MATURITY, SIC_CODE FROM Issue_Information_2002_201803")

ratinginfo <- dbGetQuery(con, "select CUSIP8, RATING_DATE, BofA, Barc FROM rate_score_2018")

#find mean of bofa and barc ratings//replace these columns in ratings table with combined score
ratinginfo$meanrating = ((ratinginfo$BofA + ratinginfo$Barc) / 2)
ratinginfo$BofA <- NULL
ratinginfo$Barc <- NULL

#use mean rating to categorize into 'Investment Grade (IG)', 'Junk (J)', and Distressed '(D)' //keep meanrating in table

for (i in 1: length(ratinginfo$meanrating)){
  if (ratinginfo$meanrating[i] <= 10){
    ratinginfo$RATING[i] = "IG"
  }
  else if(ratinginfo$meanrating[i] <= 21)
    ratinginfo$RATING[i] = "J"
  else ratinginfo$RATING[i] = "D"
}

#trim down the COMPLETE_CUSIP in the Issue_Information_1980_201803 table to match the CUSIP 8 value in the RATINGs table; rename to CUSIP8

for (i in 1:length(issueinfo$COMPLETE_CUSIP)){
issueinfo$COMPLETE_CUSIP[i] <- substr(as.character(issueinfo$COMPLETE_CUSIP[i]),
                                      start= 1, 
                                      stop= nchar(as.character(issueinfo$COMPLETE_CUSIP[i])) -1)}

colnames(issueinfo)[1] <- "CUSIP8"

#we want data for each month beginning in 2002; remove all issue data that corresponds to issue dates less than 20020101 (rendered obsolete by simply creating the new Issue_Information_2002_201803 table); remove dates/prices with NAs; remove data with offering price not strictly >= 99 and <= 101

issueinfo <- issueinfo[!is.na(issueinfo$OFFERING_DATE),]
issueinfo <- issueinfo[!is.na(issueinfo$OFFERING_PRICE),]
issueinfo <- issueinfo[issueinfo$OFFERING_DATE >= 20020101,]

issueinfonew <- subset(issueinfo, OFFERING_PRICE >= 99)
issueinfonew2 <- subset(issueinfonew, OFFERING_PRICE <= 101)
issueinfo <- issueinfonew2

#process of matching rating data for a firm (cusip8) to a bond issue based on the most-recent RATING placement; adds the RATING data to the corresponding bond issue index (Issue_Information_2002_201803)

for (i in 1: length(issueinfo$CUSIP8)){
    ratingindextable <- which(!is.na(match(ratinginfo$CUSIP8, issueinfo$CUSIP8[i])))
    datetable <- matrix(0, 1, length(ratingindextable))
        for (k in 1: length(datetable)){
        datetable[k] = issueinfo$OFFERING_DATE[i] - ratinginfo$RATING_DATE[ratingindextable[k]]}
    mindateind <- which.min(datetable > 0)
    ratind <- ratingindextable[mindateind]
        if (identical(ratind, integer(0)) == TRUE){
           issueinfo$RATING[i] <- 'NR'}
        else{
           issueinfo$RATING[i] <- ratinginfo$RATING[ratind]}
    ratingindextable <- NULL
    datetable <- NULL
}

dbWriteTable(con, "Issue_Information_2002_201803_Categorized", issueinfo, overwrite = TRUE, row.names = FALSE)