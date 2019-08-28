# -*- coding: utf-8 -*-
"""
Created on Wed May 30 17:25:51 2018

@author: anupriyat
"""

# -*- coding: utf-8 -*-
"""
Created on Sun May 27 19:09:01 2018

@author: jlngu
"""

import psycopg2
import pandas as pd
from psycopg2 import sql


def generate_bg_valuation(valuetype):

    valuetype='actual'
    tablename='yr_median_hu_value_%s' % valuetype
    
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
    createstatement = ('create table dimensional_model.{} (year int, geoid TEXT, median_value float);').format(tablename)
    cur.execute(sql.SQL(createstatement))
    conn.commit()

    for y in range(2010,2017):

        statement=("SELECT geoid, value_type, sum(count) as grp_ct FROM dimensional_model.hou_value where year={} and valuation_type=\'{}\' group by 1,2").format(y,valuetype)
        
        cur.execute(sql.SQL(statement))
        agg_data=cur.fetchall()
        df=pd.DataFrame(agg_data,columns=['geoid','rent_bracket','count'])
        dft=df.pivot(index='geoid', columns='rent_bracket', values='count')
        rowid=('value_%s' % y) 
        dft[rowid] = 0.0000
        
        for index,row in dft.iterrows():

            
            cumulative_incomes=pd.DataFrame(columns=["cumul", "cumminus1","mult"], data=[
                [row['10k less'],1, 5],
                [row['10k less']+row['10k to 15k'], row['10k less'], 12.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k'], row['10k less']+row['10k to 15k'], 17.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k'], row['10k less']+row['10k to 15k']+row['15k to 20k'], 22.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k'], 27.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k'], 32.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k'], 37.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k'], 45],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k'], 55],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k'], 65],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k'], 75],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k'], 85],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k'], 95],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k'], 112.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k'], 137.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k'], 162.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k'], 187.5],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k'], 225],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k'], 275],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k'], 350],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k'], 450],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k']+row['500k to 750k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k'], 625],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k']+row['500k to 750k']+row['750k to 1000k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k']+row['500k to 750k'], 875],
                [row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k']+row['500k to 750k']+row['750k to 1000k']+row['1000k to 1500k'], row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k']+row['500k to 750k']+row['750k to 1000k'], 1250]
                ])  

            total_pop=int(row['10k less']+row['10k to 15k']+row['15k to 20k']+row['20k to 25k']+row['25k to 30k']+row['30k to 35k']+row['35k to 40k']+row['40k to 50k']+row['50k to 60k']+row['60k to 70k']+row['70k to 80k']+row['80k to 90k']+row['90k to 100k']+row['100k to 125k']+row['125k to 150k']+row['150k to 175k']+row['175k to 200k']+row['200k to 250k']+row['250k to 300k']+row['300k to 400k']+row['400k to 500k']+row['500k to 750k']+row['750k to 1000k']+row['1000k to 1500k'])
                
            if total_pop != 0:
                
                for idx, rowc in cumulative_incomes.iterrows():
    
                        
                    if (total_pop/2)<int(rowc['cumul']) and (total_pop/2)>int(rowc['cumminus1']):
                        
                        row[rowid]=rowc['mult']
                        break
                                
                
    
        insertstatement='INSERT INTO dimensional_model.%s (year, geoid, median_value) VALUES ' % tablename
        ct=0

        for index,row in dft.iterrows():
            ct=ct+1
            insertstatement=(insertstatement + '({},\'{}\',{}),').format(y,index,row[rowid])     
            #print(row.index[0])
            if ct % 5000 ==0 or ct==numrows:
        
                insertstatement=insertstatement[:-1]+';'  
                insertstatement=insertstatement.replace('nan', '0')
                cur.execute(sql.SQL(insertstatement))
                conn.commit() 
                print(ct)
                insertstatement='INSERT INTO dimensional_model.%s (year, geoid, median_value) VALUES ' % tablename

    conn.close()


generate_bg_valuation('asked')
generate_bg_valuation('actual')




conn = psycopg2.connect(database="postgres", user = "postgres", password = "SelectALL2017", host = "104.197.103.221", port = "5432")
cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS dimensional_model.yr_median_hu_value_split;""" )
statement=("CREATE TABLE yr_median_hu_value_split as\
           SELECT a.year, a.geoid, a.median_value as asked_median_value, b.median_value as actual_median_value, b.median_value-a.median_value as valuation_offset\
           FROM dimensionalmodel.yr_median_hu_value_asked a\
           LEFT JOIN dimensionalmodel.yr_median_hu_value_actual b\
           ON a.year=b.year and a.geoid=b.geoid;")
           
cur.execute(sql.SQL(statement))
conn.commit()
conn.close()
