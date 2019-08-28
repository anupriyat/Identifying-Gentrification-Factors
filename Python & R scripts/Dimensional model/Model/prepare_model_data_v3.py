# -*- coding: utf-8 -*-
"""
Created on Sun May 27 19:09:01 2018

@author: anupriyat
"""

import psycopg2
import pandas as pd
from psycopg2 import sql


def generate_rent_gini(renttype):

    renttype='gross'
    
    tablename='gini_rent_%s' % renttype
    
    conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
    cur = conn.cursor()
    statement=("SELECT DISTINCT geoid, 1 FROM dimensional_model.hou_rent_type_bracket")
    cur.execute(sql.SQL(statement))
    geoids=cur.fetchall()
    caller=pd.DataFrame(geoids,columns=['geoid','keyc'])
    caller=caller.set_index('geoid')
    numrows=caller['keyc'].count()
    
    for y in range(2010,2017):

        statement=("SELECT geoid, rent_bracket, sum(count) as grp_ct FROM dimensional_model.hou_rent_type_bracket where year={} and rent_type=\'{}\' group by 1,2").format(y,renttype)
        
        cur.execute(sql.SQL(statement))
        agg_data=cur.fetchall()
        df=pd.DataFrame(agg_data,columns=['geoid','rent_bracket','count'])
        dft=df.pivot(index='geoid', columns='rent_bracket', values='count')
        rowid=('gini_%s' % y) 
        dft[rowid] = 0.0000
        
        for index,row in dft.iterrows():
            
            numsum=0
            densum=0
            total_units=int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']+row['1000 to 1499']+row['1500 plus'])
                
            if total_units != 0:
                
                for i in range(1,int(row['300 less'])):
                    
                    
                    numsum=numsum+(total_units+1-i)*1
                    densum=densum+1
                    
                for i in range(int(row['300 less']),int(row['300 less']+row['300 to 499'])):
                    
                    numsum=numsum+(total_units+1-i)*2
                    densum=densum+2
        
                for i in range(int(row['300 less']+row['300 to 499']),int(row['300 less']+row['300 to 499']+row['500 to 749'])):
                    
                    numsum=numsum+(total_units+1-i)*3
                    densum=densum+3
        
                    
                for i in range(int(row['300 less']+row['300 to 499']+row['500 to 749']),int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999'])):
                    
                    numsum=numsum+(total_units+1-i)*4
                    densum=densum+4
        
                    
                for i in range(int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']),int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']+row['1000 to 1499'])):
                    
                    numsum=numsum+(total_units+1-i)*6
                    densum=densum+6
                    
                for i in range(int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']+row['1000 to 1499']),total_units+1):
                    
                    numsum=numsum+(total_units+1-i)*8
                    densum=densum+8
        
                rent_gini=(1/total_units)*(total_units+1-2*(numsum/densum))
                if rent_gini<0:
                    rent_gini=0
                    
                row[rowid]=rent_gini
              
        caller=caller.join(dft.loc[:, [rowid]])

                
    cur.execute("""DROP TABLE IF EXISTS dimensional_model.%s;""" % tablename)
    conn.commit
    createstatement = ('create table dimensional_model.{} (geoid TEXT, gini_rent_2010 float, gini_rent_2011 float, gini_rent_2012 float, gini_rent_2013 float, gini_rent_2014 float, gini_rent_2015 float, gini_rent_2016 float);').format(tablename)
    cur.execute(sql.SQL(createstatement))
    conn.commit()
    
    insertstatement='INSERT INTO dimensional_model.%s (geoid, gini_rent_2010, gini_rent_2011, gini_rent_2012, gini_rent_2013, gini_rent_2014, gini_rent_2015, gini_rent_2016) VALUES ' % tablename
    ct=0
    
    for index,row in caller.iterrows():
        ct=ct+1
        insertstatement=(insertstatement + '(\'{}\',{},{},{},{},{},{},{}),').format(index,row['gini_2010'],row['gini_2011'],row['gini_2012'],row['gini_2013'],row['gini_2014'],row['gini_2015'],row['gini_2016'])     
        #print(row.index[0])
        if ct % 2000 ==0 or ct==numrows:
            
            insertstatement=insertstatement[:-1]+';'  
            insertstatement=insertstatement.replace('nan', '0')
            cur.execute(sql.SQL(insertstatement))
            conn.commit() 
            print(ct)
            insertstatement='INSERT INTO dimensional_model.%s (geoid, gini_rent_2010, gini_rent_2011, gini_rent_2012, gini_rent_2013, gini_rent_2014, gini_rent_2015, gini_rent_2016) VALUES ' % tablename

    conn.close()


generate_rent_gini('gross')
