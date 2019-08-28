# -*- coding: utf-8 -*-
"""
Created on Sun May 27 19:09:01 2018

@author: anupriyat
"""

import psycopg2
import pandas as pd
from psycopg2 import sql


def generate_rent_gini(renttype):

    
    tablename='yr_gini_income'
    
    conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
    cur = conn.cursor()
    statement=("SELECT DISTINCT geoid, 1 FROM dimensional_model.hou_rent_type_bracket")
    cur.execute(sql.SQL(statement))
    geoids=cur.fetchall()
    caller=pd.DataFrame(geoids,columns=['geoid','keyc'])
    caller=caller.set_index('geoid')
    numrows=caller['keyc'].count()
    cur.execute("""DROP TABLE IF EXISTS dimensional_model.%s;""" % tablename)
    conn.commit()
    createstatement = ('create table dimensional_model.{} (year int, geoid TEXT, gini_income float);').format(tablename)
    cur.execute(sql.SQL(createstatement))
    conn.commit()

    for y in range(2015,2016):

        statement=("SELECT geoid, income, sum(count) as grp_ct FROM dimensional_model.demo_income_age where year={} group by 1,2").format(y)
        
        cur.execute(sql.SQL(statement))
        agg_data=cur.fetchall()
        df=pd.DataFrame(agg_data,columns=['geoid','rent_bracket','count'])
        dft=df.pivot(index='geoid', columns='rent_bracket', values='count')
        rowid=('gini_%s' % y)
        dft[rowid] = 0.0000
        ctd=0
        oldpct=-1
        for index,row in dft.iterrows():
            
            numsum=0
            densum=0
            ctd=ctd+1
            pct=ctd/numrows*100
    
            if int(pct)>oldpct:
                print('progress %s ' % pct)
            oldpct=pct
            
            cumulative_incomes=pd.DataFrame(columns=["cumul", "cumminus1","mult"], data=[
                [row['10k less'],1, 5],
                [row['10k less']+row['income 10k to 15k'], row['10k less'], 10],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k'], row['10k less']+row['income 10k to 15k'], 15],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k'], 20],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k'], 25],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k'], 30],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k'], 35],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k'], 40],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k'], 45],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k'], 50],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k'], 60],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k'], 75],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k'], 100],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k']+row['income 125k to 150k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k'], 125],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k']+row['income 125k to 150k']+row['income 150k to 200k'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k']+row['income 125k to 150k'], 150],
                [row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k']+row['income 125k to 150k']+row['income 150k to 200k']+row['income 200k plus'], row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k']+row['income 125k to 150k']+row['income 150k to 200k'], 300]
                ])  

            total_pop=int(row['10k less']+row['income 10k to 15k']+row['income 15k to 20k']+row['income 20k to 25k']+row['income 25k to 30k']+row['income 30k to 35k']+row['income 35k to 40k']+row['income 40k to 45k']+row['income 45k to 50k']+row['income 50k to 60k']+row['income 60k to 75k']+row['income 75k to 100k']+row['income 100k to 125k']+row['income 125k to 150k']+row['income 150k to 200k']+row['income 200k plus'])
                
            #print('ping2 %s' % ctd)

            if total_pop != 0:
                
                for idx, rowc in cumulative_incomes.iterrows():
                    
                    for i in range(int(rowc['cumminus1']),int(rowc['cumul'])):
                        
                        numsum=numsum+(total_pop+1-i)*rowc["mult"]
                        densum=densum+rowc["mult"]
                    
                        #print('ping3 %s' % ctd)
        
                rent_gini=(1/total_pop)*(total_pop+1-2*(numsum/densum))
                if rent_gini<0:
                    rent_gini=0
                    
                row[rowid]=rent_gini
              
        caller=caller.join(dft.loc[:, [rowid]])

                
    
        insertstatement='INSERT INTO dimensional_model.%s (year, geoid, gini_income) VALUES ' % tablename
        ct=0
    
        for index,row in dft.iterrows():
            ct=ct+1
            insertstatement=(insertstatement + '({},\'{}\',{}),').format(y,index,row[rowid])     
            #print(row.index[0])
            if ct % 2000 ==0 or ct==numrows:
        
                insertstatement=insertstatement[:-1]+';'  
                insertstatement=insertstatement.replace('nan', '0')
                cur.execute(sql.SQL(insertstatement))
                conn.commit() 
                print(ct)
                insertstatement='INSERT INTO dimensional_model.%s (year, geoid, gini_income) VALUES ' % tablename

    conn.close()


generate_rent_gini('gross')
