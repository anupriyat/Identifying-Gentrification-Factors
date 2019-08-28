"""
Created on Mon May 21 16:22:20 2018

@author: anupriyat
"""

from census import Census
import us
import psycopg2
import pandas as pd
from psycopg2 import sql

#varlistt = pd.read_csv(r'' + 'demo_income_age' + '.csv', dtype={'newvars': object, 'oldvars': object} )
#colst=pd.DataFrame(varlistt.columns.values, columns=['colname'])
#print(colst['colname'])

def normalize_data(counttype,tablename):
    
    usa=us.states.mapping('fips', 'abbr')
    varlist = pd.read_csv(r'' + tablename + '.csv', dtype={'newvars': object, 'oldvars': object} )
    conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
    cur = conn.cursor()
    
    cols=pd.DataFrame(varlist.columns.values, columns=['colname'])
    
    cur.execute("""DROP TABLE IF EXISTS dimensional_model.%s;""" % tablename)
    
    conn.commit()
    
    createstatement = ('create table dimensional_model.{} ('+'\n'+'year int,'+'\n'+'state TEXT,'+'\n'+'county TEXT,'+'\n'+'tract TEXT,'+'\n'+'blkgrp TEXT, geoid TEXT,').format(tablename)
    insertstatement = ('insert into dimensional_model.{} (year, state, county, tract, blkgrp, geoid, ').format(tablename)
    
    #if counttype!= 'headcount' and counttype!= 'hhcount' and counttype!= 'unitcount':
    #    return -1
    
    for index,row in cols.iterrows():
        if index>1:
            createstatement = (createstatement + '\n' + '{} TEXT' + ',').format(row['colname'].lower())
            insertstatement = (insertstatement + ' {},').format(row['colname'].lower())
    
    
    
    createstatement = createstatement[:-1] + ', count float);'
    insertstatement = (insertstatement[:-1] + ', {})' +'\n').format(counttype)
    
    cur.execute(sql.SQL(createstatement))
    conn.commit()
    
    
    for y in range(2010,2017):
        
        selectmaster = (' ')
        
        for index, row in varlist.iterrows():  
            
            selectunion= ('\n'+'select {} as year, state, county, tract, blkgrp, state||county||tract||blkgrp,').format(y)
            
            for index,rowc in cols.iterrows():
                if index>1:
                    selectunion=(selectunion +'\'{}\', ').format(row[rowc['colname']])
            
            selectmaster = (selectmaster + selectunion + ' {} from abcdef UNION').format(row['oldvars'].lower())
        selectmaster=selectmaster[:-5]
        #print(selectmaster)
        
        for stat in usa: 
            acsref=('acs_{}_{}').format(y,usa[stat].lower())
            finalstatement = selectmaster.replace('abcdef', acsref)
            finalstatement=insertstatement + finalstatement +';'
            cur.execute(sql.SQL(finalstatement))
            #print(finalstatement)
            conn.commit()
        print(tablename + " populated for %s" % y)
    
    conn.close()



normalize_data("count","demo_age_sex") 
normalize_data("count","demo_household_characteristics") 
normalize_data("count","demo_household_income") 
normalize_data("count","demo_income_age") 
normalize_data("count","demo_occupation") 
#normalize_data("count","hou_status") 
normalize_data("count","demo_race")
