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

g2 <- dbGetQuery(conn, "
                SELECT airports.city AS city, COUNT(*) AS total
                FROM airports JOIN ontime ON ontime.dest = airports.iata
                WHERE ontime.Cancelled = 0
                GROUP BY airports.city
                ORDER BY total DESC
                ")
q2 <- g2
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

R version 4.2.1 (2022-06-23 ucrt) -- "Funny-Looking Kid"
Copyright (C) 2022 The R Foundation for Statistical Computing
Platform: x86_64-w64-mingw32/x64 (64-bit)

R is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
Type 'license()' or 'licence()' for distribution details.

R is a collaborative project with many contributors.
Type 'contributors()' for more information and
'citation()' on how to cite R or R packages in publications.

Type 'demo()' for some demos, 'help()' for on-line help, or
'help.start()' for an HTML browser interface to help.
Type 'q()' to quit R.

> installed.packages(RMySQL)
Error in installed.packages(RMySQL) : object 'RMySQL' not found
> install.packages(RMySQL)
Error in install.packages : object 'RMySQL' not found
> install.packages("RMySQL")
WARNING: Rtools is required to build R packages but is not currently installed. Please download and install the appropriate version of Rtools before proceeding:
  
  https://cran.rstudio.com/bin/windows/Rtools/
  Installing package into ‘C:/Users/kathl/AppData/Local/R/win-library/4.2’
(as ‘lib’ is unspecified)
also installing the dependency ‘DBI’


There is a binary version available but the source version is later:
  binary  source needs_compilation
RMySQL 0.10.23 0.10.24              TRUE

Binaries will be installed
trying URL 'https://cran.rstudio.com/bin/windows/contrib/4.2/DBI_1.1.3.zip'
Content type 'application/zip' length 767430 bytes (749 KB)
downloaded 749 KB

trying URL 'https://cran.rstudio.com/bin/windows/contrib/4.2/RMySQL_0.10.23.zip'
Content type 'application/zip' length 471440 bytes (460 KB)
downloaded 460 KB

package ‘DBI’ successfully unpacked and MD5 sums checked
package ‘RMySQL’ successfully unpacked and MD5 sums checked

The downloaded binary packages are in
C:\Users\kathl\AppData\Local\Temp\Rtmpoljhkx\downloaded_packages
> install.packages("DBI")
WARNING: Rtools is required to build R packages but is not currently installed. Please download and install the appropriate version of Rtools before proceeding:
  
  https://cran.rstudio.com/bin/windows/Rtools/
  Installing package into ‘C:/Users/kathl/AppData/Local/R/win-library/4.2’
(as ‘lib’ is unspecified)
trying URL 'https://cran.rstudio.com/bin/windows/contrib/4.2/DBI_1.1.3.zip'
Content type 'application/zip' length 767430 bytes (749 KB)
downloaded 749 KB

package ‘DBI’ successfully unpacked and MD5 sums checked

The downloaded binary packages are in
C:\Users\kathl\AppData\Local\Temp\Rtmpoljhkx\downloaded_packages
> library(RMySQL)
Loading required package: DBI
> library(DBI)
> library(dplyr)

Attaching package: ‘dplyr’

The following objects are masked from ‘package:stats’:
  
  filter, lag

The following objects are masked from ‘package:base’:
  
  intersect, setdiff, setequal, union

> setwd("~/SIM ST2195/Block 3")
> if (file.exists("airline2.db")) 
  +   file.remove("airline2.db")
> conn <- dbConnect(RMySQL::MySQL(), dbname='airline2', host='localhost', password='root', user='root')
> hdata_airports <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY"
> airports <- read.csv(hdata_airports)
> dbWriteTable(conn, "airports", airports, overwrite=TRUE)
Error in .local(conn, statement, ...) : 
  could not run statement: Loading local data is disabled; this must be enabled on both the client and server sides
> hdata_airports <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY"
> airports <- read.csv(hdata_airports)
> dbWriteTable(conn, "airports", airports, overwrite=TRUE)
[1] TRUE
> 
  > hdata_carriers <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q"
> carriers <- read.csv(hdata_carriers)
> dbWriteTable(conn, "carriers", carriers, overwrite=TRUE)
[1] TRUE
> 
  > hdata_planes <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XXSL8A"
> planes <- read.csv(hdata_planes)
> dbWriteTable(conn, "planes", planes, overwrite=TRUE)
[1] TRUE
> dbListTables(conn)
[1] "airports" "carriers" "planes"  
> ontime <- read.csv("ontime.csv")
> dbWriteTable(conn, "ontime", ontime, overwrite=TRUE)
[1] TRUE
> dbListTables(conn)
[1] "airports" "carriers" "ontime"   "planes"  
> q1 <- dbGetQuery(conn,
                   +                  "SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
+                  FROM planes JOIN ontime USING(tailnum)
+                  WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
+                  GROUP BY model
+                  ORDER BY avg_delay
+                  ")
Warning message:
  In .local(conn, statement, ...) :
  Decimal MySQL column 1 imported as numeric
> print(q1)
model avg_delay
1             737-2Y5    7.0220
2             737-282    8.4336
3             737-230   10.4586
4             767-3CB   10.7738
5            737-282C   11.7658
6           767-432ER   13.0491
7             767-324   13.2939
8             767-33A   13.9943
9             757-26D   14.5513
10            737-832   14.9932
11               182A   15.1159
12            777-232   15.2242
13            757-212   15.8758
14            757-232   15.8835
15            767-332   16.0735
16            767-3P6   16.6511
17            EMB-120   17.4394
18          EMB-120ER   17.4833
19           DA 20-A1   17.9888
20            747-451   19.1356
21            757-351   19.1948
22            767-3G5   19.4026
23              MD-88   20.0474
24          767-424ER   20.1430
25            737-990   20.2850
26            757-324   20.9191
27            767-224   20.9941
28           MD-90-30   21.5968
29            737-924   21.6037
30            737-790   21.7520
31            777-224   21.7974
32           A321-211   22.1021
33           A319-114   22.5463
34           A319-132   22.6447
35               G-IV   22.8973
36            737-824   23.1330
37           A320-214   23.1422
38              A109E   23.3652
39            757-231   23.4706
40           A319-112   23.6116
41            737-3B7   23.8836
42            737-3G7   23.9552
43                550   23.9700
44            737-490   24.1853
45            737-33A   24.2089
46            737-3S3   24.2726
47            757-2Q8   24.2779
48            737-4B7   24.3856
49            777-222   24.4540
50            737-301   24.6317
51         PA-32R-300   24.7539
52            737-401   24.8943
53            737-4Q8   24.9714
54            757-224   24.9873
55            737-724   25.0180
56          PA-31-350   25.2649
57            737-3Y0   25.3182
58               182P   25.4285
59           A320-212   25.4630
60              S-50A   25.5008
61               S55A   25.5372
62  VANS AIRCRAFT RV6   25.7500
63            757-222   25.7626
64              T210N   25.8748
65            HST-550   26.0494
66         210-5(205)   26.1761
67           A320-232   26.3414
68               206B   26.5240
69     DC-9-82(MD-82)   26.5866
70            737-4S3   26.6754
71          PA-28-180   26.6806
72              MD 83   26.7927
73           AS 355F1   26.9514
74          FALCON-XP   27.0278
75            767-223   27.0432
76               421C   27.1052
77          KITFOX IV   27.1500
78           A320-211   27.2049
79            737-3TO   27.3250
80          FALCON XP   27.3909
81           A320-231   27.4966
82            747-422   27.5555
83             65-A90   27.5729
84               172M   27.6613
85            757-251   27.7200
86          EXEC 162F   27.7526
87     DC-9-83(MD-83)   27.7825
88                      27.8501
89            757-223   27.8979
90        OTTER DHC-3   27.9129
91                150   27.9309
92            DC-9-31   28.2812
93            757-2G7   28.3879
94               A-1B   28.4981
95            757-2S7   28.5121
96            767-323   28.6080
97            737-524   28.6580
98            DC-9-41   28.7056
99                 60   28.7793
100              690A   28.8013
101           DC-9-32   28.9021
102           DC-9-51   28.9310
103            F85P-1   29.0487
104             T337G   29.1881
105           767-322   29.2031
106          A319-131   29.2408
107         EMB-145LR   29.4736
108              172E   29.5802
109         EMB-135LR   29.6319
110         EMB-145EP   29.6557
111       PA-32RT-300   29.9458
112         EMB-145XR   29.9617
113           757-225   30.1256
114         EMB-135ER   30.3280
115       CL-600-2B19   30.3506
116              1121   30.9342
117              E-90   31.6457
118           737-73A   31.9871
119           737-322   32.2325
120           737-522   32.6918
121           767-2B7   33.9254
122               C90   33.9307
123           EMB-145   34.0000
124            DC-7BF   34.2574
125        ATR 72-212   34.7634
126       CL-600-2C10   35.3914
127           717-200   35.5075
128        ATR-72-212   35.5344
129             S-76A   36.8333
> print(q1[1,"model"])
[1] "737-2Y5"
> print(paste0("Model ", q1[1,"model"], " has the lowest associated departure delay")
        + )
[1] "Model 737-2Y5 has the lowest associated departure delay"
> summarize(q1)
data frame with 0 columns and 1 row
> summarise(q1)
data frame with 0 columns and 1 row
> q1
model avg_delay
1             737-2Y5    7.0220
2             737-282    8.4336
3             737-230   10.4586
4             767-3CB   10.7738
5            737-282C   11.7658
6           767-432ER   13.0491
7             767-324   13.2939
8             767-33A   13.9943
9             757-26D   14.5513
10            737-832   14.9932
11               182A   15.1159
12            777-232   15.2242
13            757-212   15.8758
14            757-232   15.8835
15            767-332   16.0735
16            767-3P6   16.6511
17            EMB-120   17.4394
18          EMB-120ER   17.4833
19           DA 20-A1   17.9888
20            747-451   19.1356
21            757-351   19.1948
22            767-3G5   19.4026
23              MD-88   20.0474
24          767-424ER   20.1430
25            737-990   20.2850
26            757-324   20.9191
27            767-224   20.9941
28           MD-90-30   21.5968
29            737-924   21.6037
30            737-790   21.7520
31            777-224   21.7974
32           A321-211   22.1021
33           A319-114   22.5463
34           A319-132   22.6447
35               G-IV   22.8973
36            737-824   23.1330
37           A320-214   23.1422
38              A109E   23.3652
39            757-231   23.4706
40           A319-112   23.6116
41            737-3B7   23.8836
42            737-3G7   23.9552
43                550   23.9700
44            737-490   24.1853
45            737-33A   24.2089
46            737-3S3   24.2726
47            757-2Q8   24.2779
48            737-4B7   24.3856
49            777-222   24.4540
50            737-301   24.6317
51         PA-32R-300   24.7539
52            737-401   24.8943
53            737-4Q8   24.9714
54            757-224   24.9873
55            737-724   25.0180
56          PA-31-350   25.2649
57            737-3Y0   25.3182
58               182P   25.4285
59           A320-212   25.4630
60              S-50A   25.5008
61               S55A   25.5372
62  VANS AIRCRAFT RV6   25.7500
63            757-222   25.7626
64              T210N   25.8748
65            HST-550   26.0494
66         210-5(205)   26.1761
67           A320-232   26.3414
68               206B   26.5240
69     DC-9-82(MD-82)   26.5866
70            737-4S3   26.6754
71          PA-28-180   26.6806
72              MD 83   26.7927
73           AS 355F1   26.9514
74          FALCON-XP   27.0278
75            767-223   27.0432
76               421C   27.1052
77          KITFOX IV   27.1500
78           A320-211   27.2049
79            737-3TO   27.3250
80          FALCON XP   27.3909
81           A320-231   27.4966
82            747-422   27.5555
83             65-A90   27.5729
84               172M   27.6613
85            757-251   27.7200
86          EXEC 162F   27.7526
87     DC-9-83(MD-83)   27.7825
88                      27.8501
89            757-223   27.8979
90        OTTER DHC-3   27.9129
91                150   27.9309
92            DC-9-31   28.2812
93            757-2G7   28.3879
94               A-1B   28.4981
95            757-2S7   28.5121
96            767-323   28.6080
97            737-524   28.6580
98            DC-9-41   28.7056
99                 60   28.7793
100              690A   28.8013
101           DC-9-32   28.9021
102           DC-9-51   28.9310
103            F85P-1   29.0487
104             T337G   29.1881
105           767-322   29.2031
106          A319-131   29.2408
107         EMB-145LR   29.4736
108              172E   29.5802
109         EMB-135LR   29.6319
110         EMB-145EP   29.6557
111       PA-32RT-300   29.9458
112         EMB-145XR   29.9617
113           757-225   30.1256
114         EMB-135ER   30.3280
115       CL-600-2B19   30.3506
116              1121   30.9342
117              E-90   31.6457
118           737-73A   31.9871
119           737-322   32.2325
120           737-522   32.6918
121           767-2B7   33.9254
122               C90   33.9307
123           EMB-145   34.0000
124            DC-7BF   34.2574
125        ATR 72-212   34.7634
126       CL-600-2C10   35.3914
127           717-200   35.5075
128        ATR-72-212   35.5344
129             S-76A   36.8333
> dim(q1)
[1] 129   2
> head(q1)
model avg_delay
1   737-2Y5    7.0220
2   737-282    8.4336
3   737-230   10.4586
4   767-3CB   10.7738
5  737-282C   11.7658
6 767-432ER   13.0491
> g2 <- dbGetQuery(conn, "
+                 SELECT airports.city AS city, COUNT(*) AS total
+                 FROM airports JOIN ontime ON ontime.dest = airports.iata
+                 WHERE ontime.Cancelled = 0
+                 GROUP BY airports.city
+                 ORDER BY total DESC
+                 ")
> q2 <- g2
> head(q2)
city   total
1           Chicago 1480177
2 Dallas-Fort Worth 1112309
3           Atlanta 1058076
4       Los Angeles  803770
5           Houston  760755
6           Phoenix  689156
> # city   total
  > # 1           Chicago 1480177
  > # 2 Dallas-Fort Worth 1112309
  > # 3           Atlanta 1058076
  > # 4       Los Angeles  803770
  > # 5           Houston  760755
  > # 6           Phoenix  689156
  > print(paste0(q2[1, "city"], " has the highest number (", q2[1, "total"], ") of inbound flights excluding cancelled flights")) #output:
[1] "Chicago has the highest number (1480177) of inbound flights excluding cancelled flights"
> q3 <- dbGetQuery(conn, "
+                  SELECT carriers.Description AS carrier, COUNT(*) AS total
+                  FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
+                  WHERE ontime.Cancelled = 1
+                  GROUP BY carrier
+                  ORDER BY total DESC
+                  ")
> head(q3) #output:
carrier
1                                                               United Air Lines Inc.
2                                                              American Airlines Inc.
3                                                                Delta Air Lines Inc.
4 US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)
5                                                        American Eagle Airlines Inc.
6                                                              Southwest Airlines Co.
total
1 88877
2 82291
3 78159
4 70661
5 60334
6 45903
> # 4 US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)
  > # 5                                                        American Eagle Airlines Inc.
  > # 6                                                              Southwest Airlines Co.
  > # total
  > # 1 88877
  > # 2 82291
  > # 3 78159
  > # 4 70661
  > # 5 60334
  > # 6 45903
  > print(paste0(q3[1, "carrier"], " has the highest number of cancelled flights")) #output:
[1] "United Air Lines Inc. has the highest number of cancelled flights"
> q4 <- dbGetQuery(conn, "
+                  SELECT q10.carrier AS carrier, CAST(q10.total/q20.total AS FLOAT) AS ratio
+                  FROM (
+                  SELECT carriers.Description AS carrier, COUNT(*) AS total
+                  FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
+                  WHERE ontime.Cancelled = 1
+                  GROUP BY carrier
+                  ) AS q10 JOIN (
+                  SELECT carriers.Description AS carrier, COUNT(*) AS total
+                  FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
+                  GROUP BY carrier                 
+                  ) AS q20 USING(carrier)
+                  ORDER BY ratio DESC
+                  ")
> head(q4) #output:
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

