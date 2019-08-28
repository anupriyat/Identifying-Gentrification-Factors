# -*- coding: utf-8 -*-
"""
Created on Mon May 21 16:22:20 2018

@author: jlngu
"""

from census import Census
import us
import psycopg2
import pandas as pd
from psycopg2 import sql



def create_dimensional_table(tablename):
    
    usa=us.states.mapping('fips', 'abbr')
    varlist = pd.read_csv(r'' + tablename + '.csv', dtype={'newvars': object, 'oldvars': object} )
    conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
    cur = conn.cursor()
    
    cur.execute("""DROP TABLE IF EXISTS dimensional_model.%s;""" % tablename)


    conn.commit()
    
    createstatement = ('create table dimensional_model.{} ('+'\n'+'year int,'+'\n'+'state TEXT,'+'\n'+'county TEXT,'+'\n'+'tract TEXT,'+'\n'+'blkgrp TEXT,').format(tablename)
    insertstatement = ('insert into dimensional_model.{} (year, state, county, tract, blkgrp,').format(tablename)
    
    for index, row in varlist.iterrows():   
        createstatement = (createstatement + '\n' + '{} float' + ',').format(row['newvars'].lower())
        insertstatement = (insertstatement + ' {},').format(row['newvars'].lower())
        
    createstatement = createstatement[:-1] + ');'
    insertstatement = insertstatement[:-1] + ')' +'\n'
        
    cur.execute(sql.SQL(createstatement))
    conn.commit()
    
    
    for y in range(2010,2017):
        
        selectstatement = ('\n'+'select '+'\n'+'{} as year,'+'\n'+'state,'+'\n'+'county,'+'\n'+'tract,'+'\n'+'blkgrp,').format(y)
        for index, row in varlist.iterrows():   
            selectstatement = (selectstatement + '\n' + '{},').format(row['oldvars'].lower())

        
        for stat in usa: 
            finalstatement = selectstatement[:-1] + '\n' + ('from acs_{}_{}').format(y,usa[stat].lower())
            finalstatement=insertstatement + finalstatement +';'
            cur.execute(sql.SQL(finalstatement))
            #print(finalstatement)
            
            conn.commit()
        print(tablename + " populated for %s" % y)

    conn.close()


#create_dimensional_table("demo_age_sex") 
#create_dimensional_table("demo_household_income") 
#create_dimensional_table("demo_income_age") 
#create_dimensional_table("demo_race") 
create_dimensional_table("hou_rent") 
create_dimensional_table("hou_unit_characteristics") 
create_dimensional_table("hou_value") 
create_dimensional_table("ten_population_units_structure") 

create_dimensional_table("ten_tenure")
create_dimensional_table("mob_time_to_work")
create_dimensional_table("mob_transportation")

