# -*- coding: utf-8 -*-
"""
Created on Sun May 27 19:09:01 2018

@author: anupriyat
"""

from census import Census
import us
import psycopg2
import pandas as pd
from psycopg2 import sql
import numpy


def extract_rent_data(renttype, year):

    conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
    cur = conn.cursor()
    
    
    statement=("SELECT geoid, rent_bracket, sum(count) as grp_ct FROM dimensional_model.hou_rent_type_bracket where year={} and rent_type=\'{}\' group by 1,2").format(year,renttype)
    
    cur.execute(sql.SQL(statement))
    agg_data=cur.fetchall()
    df=pd.DataFrame(agg_data,columns=['geoid','rent_bracket','count'])
    dft=df.pivot(index='geoid', columns='rent_bracket', values='count')    
    dft['gini'] = 0.0000
    #df=pd.DataFrame(agg_data)
    return dft


test=extract_rent_data('gross',2016)
 



#testdf=dft.head(100)

for index,row in test.iterrows():
    
    
    
    numsum=0
    densum=0
    total_units=int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']+row['1000 to 1499']+row['1500 plus'])
    
    #print('***************************************')
    #print(index)
    #print('total units: %s' % total_units)
    #print('300 less: %s' % row['300 less'])
    #print('500 less: %s' % row['300 to 499'])
    #print('750 less: %s' % row['500 to 749'])
    #print('1000 less: %s' % row['750 to 999'])
    #print('1500 less: %s' % row['1000 to 1499'])
    #print('1500 plus: %s' % row['1500 plus'])
    

    if total_units != 0:
        
        for i in range(1,int(row['300 less'])):
            
            #print('executed 300less')
            
            numsum=numsum+(total_units+1-i)*1
            densum=densum+1
            #print('iter 300: %s' % (total_units+1-i))
            
        for i in range(int(row['300 less']),int(row['300 less']+row['300 to 499'])):
            
            numsum=numsum+(total_units+1-i)*2
            densum=densum+2
            #print('iter 500: %s' % (total_units+1-i))

        for i in range(int(row['300 less']+row['300 to 499']),int(row['300 less']+row['300 to 499']+row['500 to 749'])):
            
            numsum=numsum+(total_units+1-i)*3
            densum=densum+3
            #print('iter 750: %s' % (total_units+1-i))

            
        for i in range(int(row['300 less']+row['300 to 499']+row['500 to 749']),int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999'])):
            
            numsum=numsum+(total_units+1-i)*4
            densum=densum+4
            #print('iter 1000: %s' % (total_units+1-i))

            
        for i in range(int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']),int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']+row['1000 to 1499'])):
            
            numsum=numsum+(total_units+1-i)*6
            densum=densum+6
            #print('iter 1500: %s' % (total_units+1-i))
            
        for i in range(int(row['300 less']+row['300 to 499']+row['500 to 749']+row['750 to 999']+row['1000 to 1499']),total_units+1):
            
            numsum=numsum+(total_units+1-i)*8
            densum=densum+8
            #print('iter 1500p: %s' % (total_units+1-i))

        rent_gini=(1/total_units)*(total_units+1-2*(numsum/densum))
        if rent_gini<0:
            rent_gini=0
        row['gini']=rent_gini
        
        #print('numsum: %s' % numsum)
        #print('densum: %s' % densum)
        #print(rent_gini)
    
    
        
        

