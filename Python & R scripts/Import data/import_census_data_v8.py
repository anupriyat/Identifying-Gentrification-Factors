# -*- coding: utf-8 -*-
"""
Created on Tue May 15 18:03:55 2018

@author: jlngu
"""

from census import Census
import us
import psycopg2
import pandas as pd
from psycopg2 import sql

c = Census("your-key-here")

 
allcount = pd.read_csv(r'county_list.csv', dtype={'state_code': object, 'county_code': object} )

conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
cur = conn.cursor()

usa=us.states.mapping('fips', 'abbr')
#testusa = {"02" : "AK"}

for x in usa:
    
    counties=allcount.loc[allcount['state_code'] == x]
    cur.execute("""DROP TABLE IF EXISTS acs_2016_%s;""" % usa[x])
    cur.execute("""CREATE TABLE acs_2016_%s
                (
                B25034_010E FLOAT,
                NAME TEXT,
                BLKGRP TEXT,
                COUNTY TEXT,
                STATE TEXT,
                TRACT TEXT);""" % usa[x]) 
    print ("Table created successfully")
    tbl = 'acs_2016_'+usa[x].lower()
    conn.commit()

    for index, row in counties.iterrows():

        blkdata=c.acs5.state_county_blockgroup(('NAME', 'B25034_010E'), row['state_code'], row['county_code'], Census.ALL)
        cur.executemany(
                sql.SQL("INSERT INTO {} VALUES (%(B25034_010E)s, %(NAME)s, %(block group)s, %(county)s, %(state)s, %(tract)s);")
                .format(sql.Identifier(tbl)), blkdata)
        conn.commit()
    print ("Table populated successfully")
    
conn.close()






