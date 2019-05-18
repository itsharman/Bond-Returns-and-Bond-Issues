#Programmer: Harman Singh

setwd("G:/My Drive/Tobin Research Assistant/OutstandingByDateSIC")
getwd()
install.packages("data.table")
library(lubridate)
library(zoo)
library(dplyr)
library(RMySQL)
library(RODBC)
library(odbc)
library(DBI)
library(stringr)
library(data.table)

#establish connection - ignore; doesn't work
#con <- dbConnect(MySQL(), user='hs682', password='', dbname='bond2', host='localhost')
#outputfolder  <- ""
#comp <- "SOMResearch"

options(scipen = 999) #turns off scientific notation-based outputs
options(scipen = 0) #turns on scientific notation-based outputs

#loading TRACE data
main <- read.table(file="OutstandingByDateSIC_tracedatesyes.txt", header=TRUE)
head(main)
tail(main)
dim(main)

#marketopen contains market trading dates between June 2002 and January 2018
marketopen <- read.table(file="marketopen.txt", header=TRUE)

#limits marketopen to trading dates between June 2002 and December 2017
marketopen_cut <- marketopen[marketopen$'yyyymmdd'<=20171229, ]
dtlist <- data.frame(dates=marketopen_cut$yyyymmdd)

#extracts the last trading day of each month
dtlist_2 <- dtlist %>%
  mutate(date=ymd(dtlist$dates)) %>%
  arrange(date) %>%
  group_by(year=year(date), month=month(date)) %>%
  summarize(
    first_of_month = first(date),
    last_of_month = last(date)
  )

#saves the last trading day of each month
last_trading_days <- data.frame(dates=dtlist_2$last_of_month)

#can ignore this fxn; gives the first day of the month of the inputted date
som <- function(x) {
  y<-as.Date(format(x, "%Y-%m-01"))
  month(y) <- month(y)+1
  y
}

#creates a subset of all the data after June 1, 2002 (when TRACE data begins)
postJune2002 <- main[main$dt>=20020601, ]
head(postJune2002)
tail(postJune2002)

#changing the format of dt for easier parsing in later fxns
postJune2002$dt <- as.character(postJune2002$dt)
postJune2002$dt <- as.Date(postJune2002$dt, "%Y%m%d")

#creates a subset of all the data occurring at the end of the month
end_of_month <- postJune2002[which(postJune2002$dt == last_trading_days$dates), ]
head(end_of_month)
tail(end_of_month)
unique(end_of_month$dt)

#use test variable to test all fxns (other datasets are far too big and slow)
test <- rbind(head(end_of_month), tail(end_of_month))
View(test)

#separating end_of_month into tables defined by credit rating
unrated <- end_of_month[end_of_month$Rating <=0, ]
investment <- end_of_month[end_of_month$Rating>=1 & end_of_month$Rating<=10, ]
junk <- end_of_month[end_of_month$Rating>=11 & end_of_month$Rating<=19, ]
distressed <- end_of_month[end_of_month$Rating>=20, ]
  
unique(unrated$Rating)
unique(investment$Rating)
unique(junk$Rating)
unique(distressed$Rating)

#following two lengths are different, which is beneficial for our hypothesis
length(unique(end_of_month$SIC_CODE)) #424 unique SIC codes in end_of_month
length(unique(postJune2002$SIC_CODE)) #677 unique SIC codes in postJune2002

#takes SIC Code, Date and Table; returns the Barc outstanding shares of given SIC Code
outstanding_Barc <- function(x, y, z) {
  #x: SIC Code
  #y: Date
  #z: Database (ie: unrated, investment, junk, distressed)
  temp <- z[which(z$dt == y), ]
  temp2 <- temp[which(temp$SIC_CODE == x), ]
  output <- sum(temp2$Total_Outstanding_Barc)
  output
}

#takes SIC Code, Date and Table; returns the BofA outstanding shares of given SIC Code
outstanding_BofA <- function(x, y, z) {
  #x: SIC Code
  #y: Date
  #z: Database (ie: unrated, investment, junk, distressed)
  temp <- z[which(z$dt == y), ]
  temp2 <- temp[which(temp$SIC_CODE == x), ]
  output <- sum(temp2$Total_Outstanding_BofA)
  output
}

#takes SIC Code, Date and Table; returns the Barc marketshare % by rating (ie: investment, junk, distressed)
marketshare_Barc <- function(x, y, z) {
  #x: SIC Code
  #y: Date
  #z: Database (ie: unrated, investment, junk, distressed)
  temp <- z[which(z$dt == y), ]
  total_outstanding_per_dt <- sum(temp$Total_Outstanding_Barc)
  output <- outstanding_Barc(x, y, z)/total_outstanding_per_dt
  output
}

#takes SIC Code, Date and Table; returns the BofA marketshare % by rating (ie: investment, junk, distressed)
marketshare_BofA <- function(x, y, z) {
  #x: SIC Code
  #y: Date
  #z: Table (ie: unrated, investment, junk, distressed)
  temp <- z[which(z$dt == y), ]
  total_outstanding_per_dt <- sum(temp$Total_Outstanding_BofA)
  output <- outstanding_BofA(x, y, z)/total_outstanding_per_dt
  output
}

#following section of code creates final_table which will host marketshare % of each category
length(end_of_month$SIC_CODE)
all_SIC_CODES <- length(end_of_month$SIC_CODE)
final_table <- data.frame(matrix(ncol=2, nrow=all_SIC_CODES))
colnames(final_table) <- c('dt', 'SIC_CODE')
View(final_table)
final_table$dt <- end_of_month$dt
final_table$SIC_CODE <- end_of_month$SIC_CODE

#this for-loop fills in final_table with marketshare % of each category (ie: Barc vs BofA & Investment vs Junk vs Distressed)
#NaN means the date doesn't exist in that specific dataset (ie: no distressed transactions on June 28, 2002)
for(i in 1:nrow(final_table)) {
  output <- outstanding_Barc(final_table$SIC_CODE[i], final_table$dt[i], investment)
  print(output)
  final_table$investment_Barc[i] = output
  
  output <- marketshare_Barc(final_table$SIC_CODE[i], final_table$dt[i], investment)
  print(output)
  final_table$percent_investment_Barc[i] = output
  
  output <- outstanding_BofA(final_table$SIC_CODE[i], final_table$dt[i], investment)
  print(output)
  final_table$investment_BofA[i] = output
  
  output <- marketshare_BofA(final_table$SIC_CODE[i], final_table$dt[i], investment)
  print(output)
  final_table$percent_investment_BofA[i] = output
  
  output <- outstanding_Barc(final_table$SIC_CODE[i], final_table$dt[i], junk)
  print(output)
  final_table$junk_Barc[i] = output
  
  output <- marketshare_Barc(final_table$SIC_CODE[i], final_table$dt[i], junk)
  print(output)
  final_table$percent_junk_Barc[i] = output
  
  output <- outstanding_BofA(final_table$SIC_CODE[i], final_table$dt[i], junk)
  print(output)
  final_table$junk_BofA[i] = output
  
  output <- marketshare_BofA(final_table$SIC_CODE[i], final_table$dt[i], junk)
  print(output)
  final_table$percent_junk_BofA[i] = output
  
  output <- outstanding_Barc(final_table$SIC_CODE[i], final_table$dt[i], distressed)
  print(output)
  final_table$distressed_Barc[i] = output
  
  output <- marketshare_Barc(final_table$SIC_CODE[i], final_table$dt[i], distressed)
  print(output)
  final_table$percent_distressed_Barc[i] = output
  
  output <- outstanding_BofA(final_table$SIC_CODE[i], final_table$dt[i], distressed)
  print(output)
  final_table$distressed_BofA[i] = output
  
  output <- marketshare_BofA(final_table$SIC_CODE[i], final_table$dt[i], distressed)
  print(output)
  final_table$percent_distressed_BofA[i] = output
}

#storing final_table in marketshare 
marketshare <- final_table
View(marketshare)

write.csv(marketshare, "marketshare.csv")

#step 2: when there's a change in market share, is it because of a
#rating change, redemption or new issue?

#list of each last trading date from months between June 2002 and December 2017
eom_dates <- unique(end_of_month$dt)

#takes SIC code and rating category; determines how much marketshare changes at end of each month
marketshare_Barc_change <- function(x, y, z, adates) {
  #x: SIC Code
  #y: Date
  #z: Rating Category (investment, junk, distressed)
  #adates: argument to plug in eom_dates
  dloc <- which(adates == y) #returns 1 b/c "2002-06-28" is 1st element of adates
  ndate <- adates[dloc+1]
  mksh_bef <- outstanding_Barc(x, y, z)
  mksh_aft <- outstanding_Barc(x, ndate, z)
  change <- mksh_aft - mksh_bef
  change
}

#takes SIC code and rating category; determines how much marketshare changes at end of each month
marketshare_BofA_change <- function(x, y, z, adates) {
  #x: SIC Code
  #y: Date
  #z: Rating Category (investment, junk, distressed)
  #adates: argument to plug in eom_dates
  dloc <- which(adates == y) #returns 1 b/c "2002-06-28" is 1st element of adates
  ndate <- adates[dloc+1]
  mksh_bef <- outstanding_BofA(x, y, z)
  mksh_aft <- outstanding_BofA(x, ndate, z)
  change <- mksh_aft - mksh_bef
  change
}

#To Do:
#When there's a change in marketshare %, we have to figure out why
#Is it because of a rating change, redemption or new issue?

#Guide:
#Start with an SIC code and a rating category (ie: investment)
#If total outstanding on March 31 is 200 million and on April 30 it's still 200 million, nothing happened. Now go to the next day.
#Suppose on May 31 it's 275 million. That is a change of +75 million.
#Now list the bonds that were in the SIC code and rating class on April 30 and compare that list with May 31.
#If a bond dropped out (it was there on April 30 but not May 31), why? Did its rating change on May 31? If not, did it mature? If not, then see if it was called.
#If a bond was added (it was not there on April 30 but is on May 31), why? Was it just issued? If not, did its rating change?
#Continue this process through the end of the database. Bond information can be found in issue_information_2002_201803 and rate_score_2018 (in bond2 database).

#Please Note:
#Look at issue_information_1980_201803 -> select ISSUE_ID, BOND_TYPE FROM issue_information_1980_201803 LIMIT 50;
#only thing that's corporate bonds is CDEB; we have to filter out everything else - when pulling bonds, only pull CDEB