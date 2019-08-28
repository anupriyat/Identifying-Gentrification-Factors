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

def create_dimensional_table(tablename, oldvars, newvars):

conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
cur = conn.cursor()

usa=us.states.mapping('fips', 'abbr')

cur.execute("""DROP TABLE IF EXISTS dimensional_model.demo_age_sex;""")
cur.execute("""CREATE TABLE dimensional_model.demo_age_sex
            (
            total_population int,
            total_male int,
            male_under_5 int,
            male_5_to_9 int,
            male_10_to_14 int,
            male_15_to_19 int,
            male_20_to_24 int,
            male_20_to_24 int,
            male_21 int,
            male_22_to_24 int,
            male_25_to_29 int,
            male_30_to_34 int,
            male_35_to_39 int,
            male_40_to_44 int,
            male_45_to_49 int,
            male_50_to_54 int,
            male_55_to_59 int,
            male_60_to_61 int,
            male_62_to_64 int,
            male_65_to_66 int,
            male_67_to_69 int,
            male_70_to_74 int,
            male_75_to_79 int,
            male_80_to_84 int,
            male_85_plus int,
            total_male int,
            female_under_5 int,
            female_5_to_9 int,
            female_10_to_14 int,
            female_15_to_19 int,
            female_20_to_24 int,
            female_20_to_24 int,
            female_21 int,
            female_22_to_24 int,
            female_25_to_29 int,
            female_30_to_34 int,
            female_35_to_39 int,
            female_40_to_44 int,
            female_45_to_49 int,
            female_50_to_54 int,
            female_55_to_59 int,
            female_60_to_61 int,
            female_62_to_64 int,
            female_65_to_66 int,
            female_67_to_69 int,
            female_70_to_74 int,
            female_75_to_79 int,
            female_80_to_84 int,
            female_85_plus int,
            YEAR int,
            NAME TEXT,
            BLKGRP TEXT,
            COUNTY TEXT,
            STATE TEXT,
            TRACT TEXT);""") 
conn.commit;
print ("Table created successfully")

for y in range(2009,2017):
    for x in usa:

        tbl = 'acs_{}_{}'.format(y,usa[x].lower())
        
INSERT INTO TABLE1 (id, col_1, col_2, col_3)
SELECT id, 'data1', 'data2', 'data3'
FROM TABLE2
WHERE col_a = 'something';
        cur

    


        cur.executemany(
                sql.SQL("INSERT INTO {} \
                        VALUES ( \
  
                        %(tract)s);")
                .format(sql.Identifier(tbl)), blkdata)
        conn.commit()
        print ("Table populated successfully %s %s" % (row['state_code'], row['county_code']))
    
conn.close()






