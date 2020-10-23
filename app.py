import pandas as pd
import numpy as np
from flask import Flask,json, request, jsonify
import mysql.connector
from datetime import datetime
from flask_cors import CORS
app= Flask(__name__)
CORS(app)
@app.route('/<district>')
def shool_dash_map_table(district):
    db = mysql.connector.connect(
    host="34.214.24.229",
    user="IE-tech",
    passwd="IE-tech@2O2O",
    database="compassMay")
    qr="""select * from
(SELECT sm.ID,um.USER_ID,sm.name as school_name,um.email_id,count(atd.user_id) as practice_count,um.district_name,count(ll.last_logged_in)
   from user_master um left join user_profile up on up.USER_ID=um.USER_ID
   left join school_master sm on sm.id=up.SCHOOL_ID
   left join audio_track_detail atd on um.user_id=atd.user_id
   left join login_logs ll on um.user_id=ll.user_id
   where um.user_name not like '%TEST%' and um.IS_DISABLED != 'Y' and um.IS_BLOCKED != 'Y' and sm.name not like '%blocked%'
   and um.INCOMPLETE_SIGNUP != 'Y' and um.district_name like '%"""+district+"""%' and sm.name != " "
   group by um.user_id) x
   left join 
   (SELECT um.USER_ID as id1,count(atd.user_id) as practice_count12
   from user_master um left join user_profile up on up.USER_ID=um.USER_ID
   left join school_master sm on sm.id=up.SCHOOL_ID
   left join audio_track_detail atd on um.user_id=atd.user_id
   left join login_logs ll on um.user_id=ll.user_id
   where um.user_name not like '%TEST%' and um.IS_DISABLED != 'Y' and um.IS_BLOCKED != 'Y' and sm.name not like '%blocked%' and date(atd.MODIFIED_DATE) > '2019-07-31'
   and um.INCOMPLETE_SIGNUP != 'Y' and um.district_name like '%"""+district+"""%' and sm.name != " "
   group by um.user_id) y
   
   on x.USER_ID=y.id1"""
    df=pd.read_sql(qr, con=db)
    df['practice_count12'].fillna(0, inplace = True)
    df['practice_count12'] = df['practice_count12'].apply(np.int64)
    df['district_name'] = df['district_name'].str.capitalize() 
    dfdd=df[['district_name','practice_count12']]
    dfdd1=dfdd.groupby(['district_name'])['practice_count12'].sum().reset_index()
    links0 = dfdd1.rename(columns={'district_name' : 'name', 'practice_count12' : 'Practice Count'}).to_dict('r')
    df1=df[['email_id','practice_count12','count(ll.last_logged_in)']]
    df1.rename(columns = {'count(ll.last_logged_in)':'login'}, inplace = True) 
    df1.loc[(df1['practice_count12'] > 50) , 'hex'] = '#006400' #Power
    df1.loc[(df1['practice_count12'] > 6) & (df1['practice_count12'] <= 50), 'hex'] = '#00a651'  #ACTIVE
    df1.loc[(df1['practice_count12'] > 0) & (df1['practice_count12'] <= 6), 'hex'] = '#fff44f'  #PASSIVE
    df1.loc[(df1['practice_count12'] == 0) & (df1['practice_count12'] == 0), 'hex'] = '#ff8300' #DROMANT
    df2=df1[['email_id','hex']]
    links = df2.rename(columns={'email_id' : 'name', 'hex' : 'hex'}).to_dict('r')
    dfdatas=df[['school_name','practice_count12','ID']]
    dfdata2=dfdatas.groupby(['ID','school_name'])['practice_count12'].sum().reset_index()
    dfdata3=dfdata2[['school_name','practice_count12']]
    links1 = dfdata3.rename(columns={'school_name' : 'name', 'practice_count12' : 'Practice Count'}).to_dict('r')
    dfdatae=df[['email_id','practice_count12']]
    links2 = dfdatae.rename(columns={'email_id' : 'name', 'practice_count12' : 'Practice Count'}).to_dict('r')
    links0.extend(links1)
    links0.extend(links2)
    dfst=df[['school_name','email_id']]
    links3 = dfst.rename(columns={'school_name' : 'source', 'email_id' : 'target'}).to_dict('r')
    df4=df[['district_name','school_name','ID']]
    df5 = df4.drop_duplicates(subset='ID', keep="first")
    df6=df5[['district_name','school_name']]
    links4 = df6.rename(columns={'district_name' : 'source', 'school_name' : 'target'}).to_dict('r')
    results = []
    for n in links3:
        for m in links4:
            if n['source']==m['target']:
                results.append(m)
            elif m not in results:
                results.append(n)
          
    res = [] 
    for i in results: 
        if i not in res: 
            res.append(i) 
    temp={"nodes":links0,"links":res,"attributes":links}
    return(json.dumps(temp))
if __name__== "__main__":
     app.run()
