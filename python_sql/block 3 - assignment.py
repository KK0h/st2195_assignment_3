# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 17:04:27 2022

@author: kathl_000
"""

import pandas as pd
import mysql.connector

user,pw, host,db = 'root','root','localhost','airline2'
conn = mysql.connector.connect(user=user, password=pw, host=host, database=db, use_pure=True)
c = conn.cursor()

### query 1

c.execute('''
          SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
                  FROM planes JOIN ontime USING(tailnum)
                  WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
                  GROUP BY model
                  ORDER BY avg_delay
          ''')
print("Model " + c.fetchone()[0] + " has the lowest associated average departure delay")

### query 2

c.execute('''
      SELECT airports.city AS city, COUNT(*) AS total
      FROM airports JOIN ontime ON ontime.dest = airports.iata
      WHERE ontime.Cancelled = 0
      GROUP BY airports.city
      ORDER BY total DESC
          ''')
print(c.fetchone()[0] + " has the highest number of inbound flights (excluding cancelled flights)")

### query 3

c.execute('''
          SELECT carriers.Description AS carrier, COUNT(*) AS total
          FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
          WHERE ontime.Cancelled = 1
          GROUP BY carrier
          ORDER BY total DESC
          ''')
print(c.fetchone()[0] + " has the highest number of cancelled flights")

### query 4 (long version)

# c.execute('''
#        SELECT q10.carrier AS carrier, CAST(q10.total/q20.total AS FLOAT) AS ratio
#        FROM (
#        SELECT carriers.Description AS carrier, COUNT(*) AS total
#        FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
#        WHERE ontime.Cancelled = 1
#        GROUP BY carrier
#        ) AS q10 JOIN (
#        SELECT carriers.Description AS carrier, COUNT(*) AS total
#        FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
#        GROUP BY carrier                 
#        ) AS q20 USING(carrier)
#        ORDER BY ratio DESC
#           ''')
# print(c.fetchone()[0] + " has the highest ratio of cancelled to total flights")

### query 4 (short)

c.execute('''
          SELECT carriers.Description AS carrier, AVG(ontime.Cancelled) AS ratio
          FROM carriers JOIN ONTIME ON ontime.UniqueCarrier = carriers.Code
          WHERE ontime.Cancelled BETWEEN 0 AND 1
          GROUP BY carrier
          ORDER BY ratio DESC
          ''')
print(c.fetchone()[0] + " has the highest ratio of cancelled to total flights")

conn.close()

