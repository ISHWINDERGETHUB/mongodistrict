import pymongo 
import urllib 
import pandas as pd
from datetime import datetime
import numpy as np
from datetime import timedelta
import time
import datetime
from pandas import DataFrame
from pymongo import MongoClient
from bson.objectid import ObjectId
from flask import Flask,json, request, jsonify
from flask_cors import CORS
app= Flask(__name__)
CORS(app)
@app.route('/mongo/<district>')   
def mongo_spider(district):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+district+"")},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
     {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","ID":"$schoolId._id","school_name":"$schoolId.NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}
            
    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
    collection1=db.login_logs.aggregate([{"$match":{"USER_ID._id":{
                        "$in":user_list
                            
                    }    ,"USER_ID.schoolId":{"$exists":1}}},
    {"$group":{"_id":"$USER_ID._id",
            "count":{"$sum":1},
            }},
    {"$project":{"_id":0,"USER_ID":"$_id","count(last_logged_in)":"$count"}}
            ])
    df2= DataFrame(list(collection1)).fillna(0)
    collection2 = db.audio_track_master.aggregate([
    {"$match":{"USER_ID._id":{
                        "$in":user_list
                            
                    }    ,"USER_ID.schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'MODIFIED_DATE':{"$gt":datetime.datetime(2019,7,31)}},
    {'USER_ID.schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_ID.USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_ID.USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$group":{"_id":{"USER_ID":"$USER_ID._id"},
            "NEW":{"$addToSet":"$USER_ID._id"},
            "count":{"$sum":1},
            "USER_NAME": { "$first": "$USER_ID.USER_NAME" }
            }},
        {"$project":{"_id":0,"USER_ID":"$_id.USER_ID","practice_count12":"$count"}}
            
    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df2, on='USER_ID',how='left').fillna(0)
    final1=pd.merge(final, df3, on='USER_ID',how='left').fillna(0)
    df=final1[["ID","USER_ID","school_name","email_id","district_name","count(last_logged_in)","practice_count12"]]
    df['practice_count12'].fillna(0, inplace = True)
    df['practice_count12'] = df['practice_count12'].apply(np.int64)
    df['district_name'] = df['district_name'].str.capitalize() 
    dfdd=df[['district_name','practice_count12']]
    dfdd1=dfdd.groupby(['district_name'])['practice_count12'].sum().reset_index()
    links0 = dfdd1.rename(columns={'district_name' : 'name', 'practice_count12' : 'Practice Count'}).to_dict('r')
    df1=df[['email_id','practice_count12','count(last_logged_in)']]
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
            if m['target']==n['source']:
                results.append(m)
    res_list = [i for n, i in enumerate(results) if i not in results[n + 1:]] 

    for n in links3:
        for m in res_list:
            if m['target']==n['source']:
                res_list.append(n)
    temp={"nodes":links0,"links":res_list,"attributes":links}

    return(json.dumps(temp))

@app.route('/card/<district>')
def card(district):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
         {"DISTRICT_ID._id":ObjectId(""+district+"")},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
     {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","ID":"$schoolId._id","school_name":"$schoolId.NAME",
                 "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}         
    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
    collection1=db.login_logs.aggregate([{"$match":{"USER_ID._id":{
                        "$in":user_list

                    }    ,"USER_ID.schoolId":{"$exists":1}}},
    {"$group":{"_id":"$USER_ID._id",
             "count":{"$sum":1},
              }},
       {"$project":{"_id":0,"USER_ID":"$_id","count(last_logged_in)":"$count"}}
              ])
    df2= DataFrame(list(collection1)).fillna(0)
    collection2 = db.audio_track_master.aggregate([
    {"$match":{"USER_ID._id":{
                        "$in":user_list

                    }    ,"USER_ID.schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'MODIFIED_DATE':{"$gt":datetime.datetime(2019,7,31)}},
    {'USER_ID.schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_ID.USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_ID.USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$group":{"_id":{"USER_ID":"$USER_ID._id"},
             "NEW":{"$addToSet":"$USER_ID._id"},
             "count":{"$sum":1},
              "USER_NAME": { "$first": "$USER_ID.USER_NAME" }
              }},
        {"$project":{"_id":0,"USER_ID":"$_id.USER_ID","practice_count12":"$count"}}         
    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df2, on='USER_ID',how='left').fillna(0)
    final1=pd.merge(final, df3, on='USER_ID',how='left').fillna(0)
    df=final1[["ID","USER_ID","school_name","email_id","district_name","count(last_logged_in)","practice_count12"]]
    Dormant=df[df["practice_count12"]==0].count()["practice_count12"]
    Passive=df[(df['practice_count12'] > 0) & (df['practice_count12'] <= 6)].count()["practice_count12"]
    Active=df[(df['practice_count12'] > 6) & (df['practice_count12'] <= 50)].count()["practice_count12"]
    Power=df[(df['practice_count12'] > 50)].count()["practice_count12"]
    User_count=Active+Passive+Dormant
    school_count=df['school_name'].nunique()
    Onboarding0 = df.groupby('ID')['USER_ID'].nunique().to_frame(name = 'user_count').reset_index()
    Onboarding=Onboarding0[Onboarding0["user_count"]==1].count()["user_count"]
    Engaged01=df[(df['practice_count12'] > 0)]
    Engaged0 = Engaged01.groupby('ID')['practice_count12'].count().reset_index()
    Engaged1=pd.merge(Engaged0, Onboarding0, on='ID',how='left')
    Engaged1['percentage']=Engaged1['practice_count12']*100/Engaged1['user_count']
    Engaged2=Engaged1[(Engaged1['user_count'] > 1)]
    Engaged=Engaged2[(Engaged2['percentage'] > 20)].count()["percentage"]
    Intervention=school_count-(Engaged+Onboarding)
    temp={'dormant':int(Dormant) ,'passive':int(Passive) ,'active':int(Active), 'power':int(Power) ,'usercount':int(User_count),'onboarding':int(Onboarding),'engaged':int(Engaged),'intervention':int(Intervention),'totalschool':int(school_count)}
    return(json.dumps(temp))

@app.route('/schoolname/<n>')
def school_name(n):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    df = DataFrame(list(db.user_master.aggregate([
     {"$match":
            {'$and': [
                 {'ROLE_ID._id' :{'$eq':ObjectId(""+n+"")}},
                {"IS_DISABLED":{"$ne":"Y"}},
                  {"IS_BLOCKED":{"$ne":"Y"}},
                 {"INCOMPLETE_SIGNUP":{"$ne":"Y"}},
                 {"schoolId.NAME":{"$ne":""}},
                {"schoolId.NAME":{"$not":{"$regex":"None",'$options':'i'}}},
                { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                           {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                             {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}}]}},

    { "$group":{ 
           "_id":"$schoolId._id",
        "NAME":{"$first":"$schoolId.NAME"}
    }}

        ,
        {"$project":{"_id":0,"NAME":1}}
     ]))).fillna("NO SCHOOL NAME")
    return json.dumps(df["NAME"].values.tolist())



@app.route('/360dashcount/<district>')   
def tech_dash_count(district):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('I#L@teST^m0NGO_2o20!')
    client = MongoClient("mongodb://%s:%s@34.214.24.229:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+district+"")},
             {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
    school_list=df1["ID"].tolist()
    school_count = df1.groupby('school_name')["school_name"].nunique()
    user_count=df1.groupby('USER_ID')["USER_ID"].nunique()
    collection3 = db.audio_track_master.aggregate(
    [{'$match':{'$and':[{'USER_ID.schoolId._id':
        {"$in":school_list}},
    ]}},
    {"$match":
        {"$and":[
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'USER_ID.schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_ID.USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_ID.USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}},
    {'$group':{'_id':"null",

        'MINDFUL_MINUTES':{'$sum':{'$round':
            [{'$divide':[{'$subtract':['$CURSOR_END','$cursorStart']}, 60]},2]}},



        }},  {"$project":{"_id":0,"MINDFUL_MINUTES":1}}
    ])
    df3= DataFrame(list(collection3)).fillna(0)
    mindful_minutes=int(df3["MINDFUL_MINUTES"])
    collection4 = db.user_master.aggregate([
        {'$match':{'$and':[{'schoolId._id':
        {"$in":school_list}},
                           {'CREATED_DATE':{"$gt":datetime.datetime(2020,3,17)}},
    ]}},
         {"$match":
    {"$or":[{"ROLE_ID.ROLE_NAME":{"$regex":"present",'$options':'i'}
             }]}},
    {"$match":
        {"$and":[ 
        {'IS_DISABLED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","CREATED_DATE":1}}

    ])
    df4= DataFrame(list(collection4)).fillna(0)
    parent_count=df4["USER_ID"].count()
    temp={"school_count":int(school_count.count()),"user_count":int(user_count.count()),"mindful_minutes":int(mindful_minutes),"parent_count":int(parent_count)}
    return(json.dumps(temp))

if __name__== "__main__":
     app.run()
