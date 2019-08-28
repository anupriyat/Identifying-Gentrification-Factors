# -*- coding: utf-8 -*-
"""
Created on Wed May 30 19:26:44 2018

@author: anupriyat
"""

import psycopg2
import pandas as pd
from psycopg2 import sql


conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
cur = conn.cursor()
statement=("CREATE TABLE dimensional_model.chicago_blockgroup_tblu as \
           SELECT a.*, b.Sec_neigh \
           FROM acs_2016_block_groups_tblu a \
           RIGHT JOIN (SELECT DISTINCT geoid, Sec_neigh FROM dimensional_model.\"Chicago_Neighborhood_Multipliers\") b\
           ON a.geoid=b.geoid;")
           
cur.execute(sql.SQL(statement))
conn.commit()
conn.close()

conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
cur = conn.cursor()
statement=("CREATE TABLE dimensional_model.new_york_blockgroup_tblu as \
           SELECT a.*, b.Sec_neigh, b.boroname \
           FROM acs_2016_block_groups_tblu a \
           RIGHT JOIN (SELECT DISTINCT geoid,Sec_neigh,boroname FROM dimensional_model.new_york_neighborhood_multipliers) b\
           ON a.geoid=b.geoid;")
           
cur.execute(sql.SQL(statement))
conn.commit()
conn.close()


