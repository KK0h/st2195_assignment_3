library(RMySQL)
library(DBI)
library(dplyr)

# ======== create the database ========
if (file.exists("airline2.db")) 
  file.remove("airline2.db")
conn <- dbConnect(RMySQL::MySQL(), dbname='airline2', host='localhost', password='root', user='root')

# ======== write to the database ========
# load in the data from the csv files

hdata_airports <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY"
airports <- read.csv(hdata_airports)
dbWriteTable(conn, "airports", airports, overwrite=TRUE)

hdata_carriers <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q"
carriers <- read.csv(hdata_carriers)
dbWriteTable(conn, "carriers", carriers, overwrite=TRUE)

hdata_planes <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XXSL8A"
planes <- read.csv(hdata_planes)
dbWriteTable(conn, "planes", planes, overwrite=TRUE)

ontime2000 <- read.csv("2000.csv.bz2")
ontime2001 <- read.csv("2001.csv.bz2")
ontime2002 <- read.csv("2002.csv.bz2")
ontime2003 <- read.csv("2003.csv.bz2")
ontime2004 <- read.csv("2004.csv.bz2")
ontime2005 <- read.csv("2005.csv.bz2")
ontime <- rbind(ontime2000, ontime2001, ontime2002, ontime2003, ontime2004, ontime2005)
dbWriteTable(conn, "ontime", ontime, overwrite=TRUE)

dbListTables(conn)

# ======== queries via DBI ========

q1 <- dbGetQuery(conn,
                 "SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
                 FROM planes JOIN ontime USING(tailnum)
                 WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
                 GROUP BY model
                 ORDER BY avg_delay
                 ")
head(q1) #output:
# model avg_delay
# 1   737-2Y5    7.0220
# 2   737-282    8.4336
# 3   737-230   10.4586
# 4   767-3CB   10.7738
# 5  737-282C   11.7658
# 6 767-432ER   13.0491
print(paste0("Model ", q1[1,"model"], " has the lowest associated departure delay")) #output
# "Model 737-2Y5 has the lowest associated departure delay"

q2 <- dbGetQuery(conn, "
                SELECT airports.city AS city, COUNT(*) AS total
                FROM airports JOIN ontime ON ontime.dest = airports.iata
                WHERE ontime.Cancelled = 0
                GROUP BY airports.city
                ORDER BY total DESC
                ")
head(q2) # output:
# city   total
# 1           Chicago 1480177
# 2 Dallas-Fort Worth 1112309
# 3           Atlanta 1058076
# 4       Los Angeles  803770
# 5           Houston  760755
# 6           Phoenix  689156
print(paste0(q2[1, "city"], " has the highest number (", q2[1, "total"], ") of inbound flights excluding cancelled flights")) #output:
# "Chicago has the highest number (1480177) of inbound flights excluding cancelled flights"

q3 <- dbGetQuery(conn, "
                 SELECT carriers.Description AS carrier, COUNT(*) AS total
                 FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
                 WHERE ontime.Cancelled = 1
                 GROUP BY carrier
                 ORDER BY total DESC
                 ")
head(q3) #output:
# carrier
# 1                                                               United Air Lines Inc.
# 2                                                              American Airlines Inc.
# 3                                                                Delta Air Lines Inc.
# 4 US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)
# 5                                                        American Eagle Airlines Inc.
# 6                                                              Southwest Airlines Co.
# total
# 1 88877
# 2 82291
# 3 78159
# 4 70661
# 5 60334
# 6 45903
print(paste0(q3[1, "carrier"], " has the highest number of cancelled flights")) #output:
# "United Air Lines Inc. has the highest number of cancelled flights"

q4 <- dbGetQuery(conn, "
                 SELECT q10.carrier AS carrier, CAST(q10.total/q20.total AS FLOAT) AS ratio
                 FROM (
                 SELECT carriers.Description AS carrier, COUNT(*) AS total
                 FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
                 WHERE ontime.Cancelled = 1
                 GROUP BY carrier
                 ) AS q10 JOIN (
                 SELECT carriers.Description AS carrier, COUNT(*) AS total
                 FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
                 GROUP BY carrier                 
                 ) AS q20 USING(carrier)
                 ORDER BY ratio DESC
                 ")
head(q4) #output:
# carrier
# 1                                                        American Eagle Airlines Inc.
# 2                                                                    Independence Air
# 3                                                               United Air Lines Inc.
# 4                                                                Alaska Airlines Inc.
# 5 US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)
# 6                                                              American Airlines Inc.
# ratio
# 1 0.04568681
# 2 0.03772017
# 3 0.03430421
# 4 0.03257321
# 5 0.02992852
# 6 0.02739875
print(paste0(q4[1,"carrier"], " has the higher number of cancelled flights to total flights")) #output:
# "American Eagle Airlines Inc. has the higher number of cancelled flights to total flights"

# ======== queries via dplyr ========

planes_db <- tbl(conn, "planes")
ontime_db <- tbl(conn, "ontime")
airports_db <- tbl(conn, "airports")
carriers_db <- tbl(conn, "carriers")


q1a <- ontime_db %>%
  rename_all(tolower) %>%
  inner_join(planes_db, by="tailnum", suffix=c(".ontime", "planes")) %>%
  filter(cancelled==0 & diverted==0 & depdelay > 0) %>%
  group_by(model) %>%
  summarize(avg_delay = mean(depdelay, na.rm=TRUE)) %>%
  arrange(avg_delay)
head(q1a) # output:
# A tibble: 6 × 2
# model     avg_delay
# <chr>         <dbl>
# 1 737-2Y5        7.02
# 2 737-282        8.43
# 3 737-230       10.5 
# 4 767-3CB       10.8 
# 5 737-282C      11.8 
# 6 767-432ER     13.0 

q2a <- ontime_db %>%
  inner_join(airports_db, by = c("Dest" = "iata")) %>%
  filter(Cancelled==0) %>%
  group_by(city) %>%
  summarize(total = n()) %>%
  arrange(desc(total))
head(q2a) # output:
# A tibble: 6 × 2
# city                total
# <chr>               <int>
# 1 Chicago           1480177
# 2 Dallas-Fort Worth 1112309
# 3 Atlanta           1058076
# 4 Los Angeles        803770
# 5 Houston            760755
# 6 Phoenix            689156

q3a <- ontime_db %>%
  inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Cancelled==1) %>%
  group_by(Description) %>%
  summarize(total = n()) %>%
  arrange(desc(total))
head(q3a) # output:
# A tibble: 6 × 2
# Description                                                                   total
# <chr>                                                                         <int>
# 1 United Air Lines Inc.                                                         88877
# 2 American Airlines Inc.                                                        82291
# 3 Delta Air Lines Inc.                                                          78159
# 4 US Airways Inc. (Merged with America West 9/05. Reporting for both starting … 70661
# 5 American Eagle Airlines Inc.                                                  60334
# 6 Southwest Airlines Co.                                                        45903

q4a <- ontime_db %>%
  inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarize(ratio = mean(Cancelled, na.rm=TRUE)) %>%
  arrange(desc(ratio))
head(q4a) #output:
# A tibble: 6 × 2
# Description                                                                   ratio
# <chr>                                                                         <dbl>
# 1 American Eagle Airlines Inc.                                                 0.0457
# 2 Independence Air                                                             0.0377
# 3 United Air Lines Inc.                                                        0.0343
# 4 Alaska Airlines Inc.                                                         0.0326
# 5 US Airways Inc. (Merged with America West 9/05. Reporting for both starting… 0.0299
# 6 American Airlines Inc.                                                       0.0274


dbDisconnect(conn)

