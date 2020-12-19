
import urllib 
import pandas as pd
from datetime import datetime
import numpy as np
from datetime import timedelta
import time
import datetime
from pandas import DataFrame
import plotly.express as px
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
            {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
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
            {"schoolId._id":{"$in":db.school_master.distinct( "_id", { "IS_PORTAL": "Y" } )}},
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
    if df1.empty:
        temp={"school_count":0,"user_count":0,"MINDFUL_MINUTES":0,"parent_count":0}
        return(json.dumps(temp))
    else:
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

@app.route('/practice/distable/<m>/dormant')
def table_fkj(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+m+"")},
             {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
             {'EMAIL_ID':{"$not":{"$regex":'1gen','$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","CREATED_DATE":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$CREATED_DATE"} },"ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
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
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","practice_date":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$MODIFIED_DATE"} }}}

    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df3, on='USER_ID',how='left').fillna(0)
    final1=final[final["practice_date"]== 0 ]
    export=final1[["USER_NAME","email_id","school_name","CREATED_DATE","practice_date"]].values.tolist()
    temp={"data":export}
    return(json.dumps(temp))


@app.route('/practice/distable/<m>/lsy')
def table_fhj(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+m+"")},
             {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","CREATED_DATE":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$CREATED_DATE"} },"ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
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
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","practice_date":{ "$dateToString": {"format":  "%Y-%m-%d %H:%M:%S", "date": "$MODIFIED_DATE"} }}}

    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df3, on='USER_ID',how='left').fillna(0)
    final1=final[final["practice_date"]!= 0 ]
    df6=final1.groupby(['USER_ID'])['practice_date'].max().reset_index()
    df6=df6[df6["practice_date"]<"2020-08-01"]
    user_detail=pd.merge(df6, final1, on='USER_ID',how='left').fillna(0)
    export=user_detail[["USER_NAME","email_id","school_name","CREATED_DATE","practice_date_y"]].values.tolist()
    temp={"data":export}
    return(json.dumps(temp))


@app.route('/user/distable/<m>/lsy')
def table_fio(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+m+"")},
             {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","CREATED_DATE":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$CREATED_DATE"} },"ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
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
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","practice_date":{ "$dateToString": {"format":  "%Y-%m-%d %H:%M:%S", "date": "$MODIFIED_DATE"} }}}

    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df3, on='USER_ID',how='left').fillna(0)
    final1=final[final["practice_date"]!= 0 ]
    df6=final1.groupby(['USER_ID'])['practice_date'].max().reset_index()
    df6=df6[df6["practice_date"]<"2020-08-01"]
    user_detail=pd.merge(df6, final1, on='USER_ID',how='left').fillna(0)
    export=user_detail[["USER_ID","USER_NAME","email_id","school_name","CREATED_DATE","practice_date_x","practice_date_y"]]
    export1=export.groupby(["USER_ID",'USER_NAME','email_id','school_name','CREATED_DATE','practice_date_x']).size().reset_index(name='Practice count')
    user_ids=export1["USER_ID"].tolist()
    collection3 =db.subscription_master.aggregate([
    {"$match":{"USER_ID._id":{
                        "$in":user_ids

                    }    ,"USER_ID.schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'USER_ID.schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_ID.USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_ID.USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","renewable_date":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$SUBSCRIPTION_EXPIRE_DATE"} }}}

    ])
    df4= DataFrame(list(collection3)).fillna(0)
    final_report=pd.merge(export1, df4, on='USER_ID',how='left').fillna(0)
    export_final=final_report[["USER_NAME","email_id","school_name","CREATED_DATE","practice_date_x","Practice count","renewable_date"]].values.tolist()
    temp={"data":export_final}
    return(json.dumps(temp))

@app.route('/practice/distable/<m>/csy')
def table_fxs(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+m+"")},
             {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","CREATED_DATE":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$CREATED_DATE"} },"ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
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
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","practice_date":{ "$dateToString": {"format":  "%Y-%m-%d %H:%M:%S", "date": "$MODIFIED_DATE"} }}}

    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df3, on='USER_ID',how='left').fillna(0)
    final1=final[final["practice_date"]!= 0 ]
    df6=final1.groupby(['USER_ID'])['practice_date'].max().reset_index()
    df6=df6[df6["practice_date"]>"2020-07-31"]
    user_detail=pd.merge(df6, final1, on='USER_ID',how='left').fillna(0)
    export=user_detail[["USER_NAME","email_id","school_name","CREATED_DATE","practice_date_y"]].values.tolist()
    temp={"data":export}
    return(json.dumps(temp))



@app.route('/user/distable/<m>/csy')
def table_fkho(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+m+"")},
             {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
        {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}}, 
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"USER_ID":"$_id","CREATED_DATE":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$CREATED_DATE"} },"ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])
    df1= DataFrame(list(collection)).fillna(0)
    user_list=df1["USER_ID"].tolist()
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
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","practice_date":{ "$dateToString": {"format":  "%Y-%m-%d %H:%M:%S", "date": "$MODIFIED_DATE"} }}}

    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df1, df3, on='USER_ID',how='left').fillna(0)
    final1=final[final["practice_date"]!= 0 ]
    df6=final1.groupby(['USER_ID'])['practice_date'].max().reset_index()
    df6=df6[df6["practice_date"]>"2020-07-31"]
    user_detail=pd.merge(df6, final1, on='USER_ID',how='left').fillna(0)
    export=user_detail[["USER_ID","USER_NAME","email_id","school_name","CREATED_DATE",'practice_date_x',"practice_date_y"]]
    export1=export.groupby(["USER_ID",'USER_NAME','email_id','school_name','CREATED_DATE','practice_date_x']).size().reset_index(name='Practice count')
    user_ids=export1["USER_ID"].tolist()
    collection3 =db.subscription_master.aggregate([
    {"$match":{"USER_ID._id":{
                    "$in":user_ids

                }    ,"USER_ID.schoolId":{"$exists":1}}},
    {"$match":
    {"$and":[
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'USER_ID.schoolId.NAME':{"$not":{"$regex":'Blocked', '$options':'i'}}}]}},
    {"$match":
    {"$and":[{'USER_ID.USER_NAME':{"$not":{"$regex":"Test",'$options':'i'}}},
    {'USER_ID.USER_NAME':{"$not":{"$regex":'1gen','$options':'i'}}}]}}
    ,
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","renewable_date":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$SUBSCRIPTION_EXPIRE_DATE"} }}}

    ])
    df4= DataFrame(list(collection3)).fillna(0)
    final_report=pd.merge(export1, df4, on='USER_ID',how='left').fillna(0)
    export_final=final_report[["USER_NAME","email_id","school_name","CREATED_DATE","practice_date_x","Practice count","renewable_date"]].values.tolist()
    temp={"data":export_final}
    return(json.dumps(temp))


@app.route('/practice/distable/<m>/parents')
def table_fks(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master.aggregate([
    {"$match":{"schoolId":{"$exists":1}}},
    {"$match":
        {"$and":[
        {"DISTRICT_ID._id":ObjectId(""+m+"")},
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
    school_list=df1["ID"].tolist()
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
    {"$project":{"USER_ID":"$_id","CREATED_DATE":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$CREATED_DATE"} },"ID":"$schoolId._id","school_name":"$schoolId.NAME","USER_NAME":"$USER_NAME",
                "email_id":"$EMAIL_ID","district_name":"$DISTRICT_ID.DISTRICT_NAME"}}

    ])

    df4= DataFrame(list(collection4)).fillna(0)
    user_list=df4["USER_ID"].tolist()
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
    {"$project":{"_id":0,"USER_ID":"$USER_ID._id","practice_date":{ "$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$MODIFIED_DATE"} }}}

    ])
    df3= DataFrame(list(collection2)).fillna(0)
    final=pd.merge(df4, df3, on='USER_ID',how='left').fillna(0)
    export=final[["USER_NAME","email_id","school_name","CREATED_DATE","practice_date"]].values.tolist()
    temp={"data":export}
    return(json.dumps(temp))

@app.route('/all/distable/<m>/all')
def table_fkjhs(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master
    query=[{'$match':{'$and':[{
    "DISTRICT_ID._id": {
    "$in":[ObjectId(""+m+"")]
    }   
    },
    { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    #   {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
    #   {'DEVICE_USED':{"$regex":'webapp','$options':'i'}},
    #   {'schoolId.NAME':{'$not':{"$regex":'Blocked','$options':'i'}}},
    {'schoolId.BLOCKED_BY_CAP':{'$exists':0}},
    {'IS_ADMIN':'Y'}
    ]
    }},
    {"$project":{"_id":0,
    'UMUSER_ID':'$_id',"USER_NAME":'$USER_NAME',
    "UMEMAIL":'$EMAIL_ID',  
    "CREATED_DATE":'$CREATED_DATE',
    "UMSCHOOLID":'$schoolId._id',
                 "UMSCHOOLNAME":'$schoolId.NAME',
                 "CITY":'$schoolId.CITY',
                 "STATE":'$schoolId.STATE',
                 "COUNTRY":'$schoolId.COUNTRY',
                }},
    ]
    merge1=list(collection.aggregate(query))
    overallum=pd.DataFrame(merge1)
    #     
    overallum["CREATED_DATE"]=overallum["CREATED_DATE"].dt.strftime('%d %b %Y')
    email=list(overallum["UMUSER_ID"])
    schoolid=list(overallum["UMSCHOOLID"])
    #     overallum.to_csv("lifetimecheck.csv")

    ################################sub_master################################

    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    # db.subscription_master.ensureIndex("USER_ID._id", 1) 
    collection = db.subscription_master
    qr=[
    {"$match":{"$and":[{'USER_ID._id':{"$in":email}},
    #                        {'PLAN_ID.PLAN_NAME':"Cloud"},


                      ]}},
    {"$project":{"_id":0,
    'SMUSER_ID':'$USER_ID._id',
    "SMEMAIL":'$USER_ID.EMAIL_ID',
    "PLANID":"$PLAN_ID.PLAN_NAME",
    "comment":"$COMMENT_BY_DS_TEAM",
    "RENEWAL_DATE":"$SUBSCRIPTION_EXPIRE_DATE",
    }},]
    merge=list(collection.aggregate(qr))
    overall=pd.DataFrame(merge)
    overall["RENEWAL_DATE"]=overall["RENEWAL_DATE"].dt.strftime('%d %b %Y')
    mergeddf=pd.merge(overallum, overall, how='left', left_on='UMUSER_ID', right_on='SMUSER_ID')
    db=client.compass
    collection = db.audio_track_master
    qra=[
    {"$match":{'$and':[{'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
    {'USER_ID.schoolId._id':{'$in':schoolid}},
    ]}},
    {'$group':{'_id':'$USER_ID.schoolId._id', 
    'atdLastpractice':{'$max':'$MODIFIED_DATE'},
    'atdPracticecount':{'$sum':1},
    'atdTotal_Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}}]
    merge11=list(collection.aggregate(qra))
    atd=pd.DataFrame(merge11)
    atd["atdLastpractice"]=atd["atdLastpractice"].dt.strftime('%d %b %Y')
    finalmerge=pd.merge(mergeddf, atd, how='left', left_on='UMSCHOOLID', right_on='_id')
    finalmerge['atdLastpractice'].fillna("NO PRACTICE", inplace=True)
    finalmerge['atdPracticecount'].fillna(0, inplace=True)
    finalmerge.fillna("NO INFO AVAILABLE", inplace=True)
    finaldata=finalmerge[["UMSCHOOLID","UMUSER_ID","UMSCHOOLNAME","STATE","CITY","USER_NAME","UMEMAIL","CREATED_DATE","atdLastpractice","RENEWAL_DATE","atdPracticecount"]]
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('int')
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('str')
    finaldata["UMSCHOOLID"] = finaldata['UMSCHOOLID'].astype('str')
    finaldata["UMUSER_ID"] = finaldata['UMUSER_ID'].astype('str')
    schoolcount = len(set(schoolid))
    temp={"data":finaldata.values.tolist()}
    return(json.dumps(temp))

@app.route('/all/distable/<m>/all/scount')
def table_fkhgjhs(m):
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master
    query=[{'$match':{'$and':[{
    "DISTRICT_ID._id": {
    "$in":[ObjectId(""+m+"")]
    }   
    },
    { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    #   {'ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
    #   {'DEVICE_USED':{"$regex":'webapp','$options':'i'}},
    #   {'schoolId.NAME':{'$not':{"$regex":'Blocked','$options':'i'}}},
    {'schoolId.BLOCKED_BY_CAP':{'$exists':0}},
    {'IS_ADMIN':'Y'}
    ]
    }},
    {"$project":{"_id":0,
    'UMUSER_ID':'$_id',"USER_NAME":'$USER_NAME',
    "UMEMAIL":'$EMAIL_ID',  
    "CREATED_DATE":'$CREATED_DATE',
    "UMSCHOOLID":'$schoolId._id',
                 "UMSCHOOLNAME":'$schoolId.NAME',
                 "CITY":'$schoolId.CITY',
                 "STATE":'$schoolId.STATE',
                 "COUNTRY":'$schoolId.COUNTRY',
                }},
    ]
    merge1=list(collection.aggregate(query))
    overallum=pd.DataFrame(merge1)
    #     
    overallum["CREATED_DATE"]=overallum["CREATED_DATE"].dt.strftime('%d %b %Y')
    email=list(overallum["UMUSER_ID"])
    schoolid=list(overallum["UMSCHOOLID"])
    #     overallum.to_csv("lifetimecheck.csv")

    ################################sub_master################################

    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    # db.subscription_master.ensureIndex("USER_ID._id", 1) 
    collection = db.subscription_master
    qr=[
    {"$match":{"$and":[{'USER_ID._id':{"$in":email}},
    #                        {'PLAN_ID.PLAN_NAME':"Cloud"},


                      ]}},
    {"$project":{"_id":0,
    'SMUSER_ID':'$USER_ID._id',
    "SMEMAIL":'$USER_ID.EMAIL_ID',
    "PLANID":"$PLAN_ID.PLAN_NAME",
    "comment":"$COMMENT_BY_DS_TEAM",
    "RENEWAL_DATE":"$SUBSCRIPTION_EXPIRE_DATE",
    }},]
    merge=list(collection.aggregate(qr))
    overall=pd.DataFrame(merge)
    overall["RENEWAL_DATE"]=overall["RENEWAL_DATE"].dt.strftime('%d %b %Y')
    mergeddf=pd.merge(overallum, overall, how='left', left_on='UMUSER_ID', right_on='SMUSER_ID')
    db=client.compass
    collection = db.audio_track_master
    qra=[
    {"$match":{'$and':[{'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
    {'USER_ID.schoolId._id':{'$in':schoolid}},
    ]}},
    {'$group':{'_id':'$USER_ID.schoolId._id', 
    'atdLastpractice':{'$max':'$MODIFIED_DATE'},
    'atdPracticecount':{'$sum':1},
    'atdTotal_Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}}]
    merge11=list(collection.aggregate(qra))
    atd=pd.DataFrame(merge11)
    atd["atdLastpractice"]=atd["atdLastpractice"].dt.strftime('%d %b %Y')
    finalmerge=pd.merge(mergeddf, atd, how='left', left_on='UMSCHOOLID', right_on='_id')
    finalmerge['atdLastpractice'].fillna("NO PRACTICE", inplace=True)
    finalmerge['atdPracticecount'].fillna(0, inplace=True)
    finalmerge.fillna("NO INFO AVAILABLE", inplace=True)
    finaldata=finalmerge[["UMSCHOOLID","UMUSER_ID","UMSCHOOLNAME","STATE","CITY","USER_NAME","UMEMAIL","CREATED_DATE","atdLastpractice","RENEWAL_DATE","atdPracticecount"]]
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('int')
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('str')
    schoolcount = len(set(schoolid))
    temp={"data":schoolcount}
    return(json.dumps(temp))

@app.route('/schoolsearch/<name>')
def school_search_mongo1(name):
    name1=name.replace("%20"," ")
    print(name1,"hola")
    from bson.regex import Regex
    from pymongo import MongoClient
    from flask import Flask,json
    import urllib 
    import pandas as pd
    mongo_uri = "mongodb://admin:" + urllib.parse.quote("I#L@teST^m0NGO_2o20!") + "@34.214.24.229:27017/"
    client = MongoClient(mongo_uri)
    # client = MongoClient("mongodb://host:port/")
    database = client["compass"]
    collection = database["user_master"]
    # Created with Studio 3T, the IDE for MongoDB - https://studio3t.com
    query = {}
    query["schoolId.NAME"] = name1
    #     query["EMAIL_ID"] = Regex(u".*amorgan@methacton\\.org.*", "i")
    query["USER_NAME"] = {
        u"$not": Regex(u".*TEST.*", "i")
    }
    query["IS_BLOCKED"] = {
        u"$ne": u"Y"
    }
    query["IS_DISABLED"] = {
        u"$ne": u"Y"
    }
    query["INCOMPLETE_SIGNUP"] = {
        u"$ne": u"Y"
    }
    query["DEVICE_USED"] = Regex(u".*webapp.*", "i")
    projection = {}
    projection["USER_ID.USER_ID"] = 1.0
    projection["EMAIL_ID"] = 1.0
    projection["CREATED_DATE"] = 1.0
    projection["USER_NAME"] = 1.0
    projection["IS_ADMIN"] = 1.0
    projection["schoolId.ADDRESS"] = 1.0
    projection["schoolId.CITY"] = 1.0
    projection["schoolId.STATE"] = 1.0
    projection["schoolId.COUNTRY"] = 1.0
    projection["schoolId.NAME"] = 1.0
    cursor = collection.find(query, projection = projection)
    dfum=(list(cursor))
    dfum=pd.json_normalize(dfum, max_level=1)
    schoolname=dfum["schoolId.NAME"][0]
    country=dfum["schoolId.COUNTRY"][0]
    city=dfum["schoolId.CITY"][0]
    state=dfum["schoolId.STATE"][0]
    address=dfum["schoolId.ADDRESS"][0]
    admin1=dfum[dfum['IS_ADMIN']=='Y']
    admin2=admin1['USER_NAME']
    admin3=list(admin2)
    admin=admin3[0]
    adminemail1=admin1['EMAIL_ID']
    admine=list(adminemail1)
    # adminemail=[dfum['EMAIL_ID'][dfum['IS_ADMIN']=='Y']][0]
    adminemail=admine[0]
    print(adminemail)
    email=list(dfum['EMAIL_ID'])
    print(email)
    totaluser=len(email)
    collection = database["audio_track_master"]
#     Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/
    pipeline = [
        {
            u"$match": {
                u"USER_ID.EMAIL_ID": {
                    u"$in": email
                }
            }
        }, 
        {
            u"$group": {
                u"_id": {
                    u"USER_ID\u1390_id": u"$USER_ID._id"
                },
                u"MAX(MODIFIED_DATE)": {
                    u"$max": u"$MODIFIED_DATE"
                },
                u"COUNT(USER_ID\u1390_id)": {
                    u"$sum": 1
                }
            }
        }, 
        {
            u"$project": {
                u"USER_ID._id": u"$_id.USER_ID\u1390_id",
                u"MAX(MODIFIED_DATE)": u"$MAX(MODIFIED_DATE)",
                u"COUNT(USER_ID\u1390_id)": u"$COUNT(USER_ID\u1390_id)",
                u"_id": 0
            }
        }
    ]
    cursor = collection.aggregate(
        pipeline, 
        allowDiskUse = True
    )
    dfatd=list(cursor)
    dfatd=pd.json_normalize(dfatd, max_level=1)
    print(dfatd)
    collection = database["subscription_master"]
    # Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/
    pipeline = [
        {
            u"$match": {
                u"USER_ID.EMAIL_ID": {
                    u"$in": email
                }
            }
        }, 
        {
            u"$group": {
                u"_id": {
                    u"USER_ID\u1390_id": u"$USER_ID._id"
                },
                u"MAX(SUBSCRIPTION_EXPIRE_DATE)": {
                    u"$max": u"$SUBSCRIPTION_EXPIRE_DATE"
                }
            }
        }, 
        {
            u"$project": {
                u"MAX(SUBSCRIPTION_EXPIRE_DATE)": u"$MAX(SUBSCRIPTION_EXPIRE_DATE)",
                u"USER_ID._id": u"$_id.USER_ID\u1390_id",
                u"_id": 0
            }
        }
    ]
    cursor = collection.aggregate(
        pipeline, 
        allowDiskUse = True
    )
    dfsbm=list(cursor)
    dfsbm=pd.json_normalize(dfsbm, max_level=1)
    print(dfatd,"atd")
    
    try:
        dffinal=pd.merge(dfum,dfatd,left_on='_id',right_on='USER_ID._id',how='left',suffixes=('_',''))
        dffinalnew=pd.merge(dffinal,dfsbm,left_on='_id',right_on='USER_ID._id',how='left',suffixes=('_',''))
    except:
        dfum['MAX(MODIFIED_DATE)']='NO PRACTICE'
        dfum['COUNT(USER_ID᎐_id)']=0
        dffinal=dfum
        dffinalnew=pd.merge(dffinal,dfsbm,left_on='_id',right_on='USER_ID._id',how='left',suffixes=('_',''))
        
        
        
#     schoolname=dfum["schoolId.NAME"][0]
    country=dfum["schoolId.COUNTRY"][0]
    city=dfum["schoolId.CITY"][0]
    state=dfum["schoolId.STATE"][0]
    address=dfum["schoolId.ADDRESS"][0]
#     admin=[dfum['USER_NAME'][dfum['IS_ADMIN']=='Y']][0]
#     admin=admin[0]
#     adminemail=[dfum['EMAIL_ID'][dfum['IS_ADMIN']=='Y']][0]
#     adminemail=adminemail[0]
    email=list(dfum['EMAIL_ID'])
    totaluser=len(email)
    dffinalnew['MAX(MODIFIED_DATE)'].fillna("NO PRACTICE", inplace=True)
    dffinalnew['MAX(SUBSCRIPTION_EXPIRE_DATE)'].fillna(" ", inplace=True)
    dffinalnew['COUNT(USER_ID᎐_id)'].fillna(0, inplace=True)
    pracsum=sum(list(dffinalnew['COUNT(USER_ID᎐_id)']))
    dffinalnew.fillna(value=pd.np.nan, inplace=True)
    MAX=[]
    for i in dffinalnew['MAX(MODIFIED_DATE)']:
        if  i != 'NO PRACTICE' :
            MAX.append(i.strftime("%d %b %Y "))
        else:
            MAX.append("NO PRACTICE")
    SUBSCRIPTION_EXPIRE_DATE=[]
    for i in dffinalnew['MAX(SUBSCRIPTION_EXPIRE_DATE)']:
        if  i != ' ' :
            SUBSCRIPTION_EXPIRE_DATE.append(i.strftime("%d %b %Y "))
        else:
            SUBSCRIPTION_EXPIRE_DATE.append(" ")        
    CREATED_DATE=[]
    for i in dffinalnew['CREATED_DATE']:
        if  i != ' ' :
            CREATED_DATE.append(i.strftime("%d %b %Y "))
        else: 
            CREATED_DATE.append(" ")
    data=[]
    for T,k,l,m,o,p in zip(dffinalnew['USER_NAME'].tolist(),dffinalnew['EMAIL_ID'].tolist(),CREATED_DATE,MAX,SUBSCRIPTION_EXPIRE_DATE,dffinalnew['COUNT(USER_ID᎐_id)'].tolist()):
        #print(p,q,r)
        data.append([T,k,l,m,o,p])
    temp={"data":data,"school_practice_count":str(pracsum),"school_name":name,"country":country,"state":state,"city":city,"address":address,"admin_name":admin,"admin_email":adminemail,"user_count":totaluser}
#     ,"school_practice_count":str(card_detail['school_practice_count1'][0])
#     temp={"data":data}
    return json.dumps(temp)


@app.route('/portal_api/<inputid>')
def portal_api(inputid):    
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.user_master
    from bson.objectid import ObjectId
    query=[{'$match':{'$and':[{
    "DISTRICT_ID._id":ObjectId(""+inputid+"")   
    },
    { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    {'IS_PORTAL':'Y'},
   {'schoolId.NAME':{'$not':{"$regex":'Blocked','$options':'i'}}},
    {'schoolId.BLOCKED_BY_CAP':{'$exists':0}},
    ]
    }},
    {"$project":{"_id":0,
    "UMSCHOOLID":'$schoolId._id',}},
    ]
    merge11=list(collection.aggregate(query))
    overallum11=pd.DataFrame(merge11)
    lifetimelist=list(set(overallum11["UMSCHOOLID"]))
    total_school=len(lifetimelist)
    collection = db.user_master
    query=[{'$match':{'$and':[{
    "schoolId._id": {
    "$in":lifetimelist
    }   
    },
    { 'USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    {'schoolId.BLOCKED_BY_CAP':{'$exists':0}},
    ]
    }},
    {"$project":{"_id":0,
    'ROLE':'$ROLE_ID.ROLE_NAME',
    'UMUSER_ID':'$_id',"USER_NAME":'$USER_NAME',
    "UMSCHOOLID":'$schoolId._id',
                 "UMSCHOOLNAME":'$schoolId.NAME',
                }},
    ]
    merge1=list(collection.aggregate(query))
    overallum=pd.DataFrame(merge1)
    email=list(overallum["UMUSER_ID"])
    schoolid=list(overallum["UMSCHOOLID"])
    ################################sub_master################################
    collection = db.subscription_master
    qr=[
    {"$match":{"$and":[{'USER_ID._id':{"$in":email}},]}},
    {"$project":{"_id":0,
    'SMUSER_ID':'$USER_ID._id',
    "RENEWAL_DATE":"$SUBSCRIPTION_EXPIRE_DATE",
    }},]
    merge=list(collection.aggregate(qr))
    overall=pd.DataFrame(merge)
    mergeddf=pd.merge(overallum, overall, how='left', left_on='UMUSER_ID', right_on='SMUSER_ID')
    db=client.compass
    collection = db.audio_track_master
    qra=[
    {"$match":{'$and':[{'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
    {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
    {'USER_ID.USER_NAME':{'$not':{'$regex':'1gen', '$options':'i'}}},
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}}, {'USER_ID.schoolId.NAME':{'$not':{'$regex':'test', '$options':'i'}}},
    {'USER_ID.schoolId._id':{'$in':schoolid}},
    ]}},
    {'$group':{'_id':'$USER_ID.schoolId._id', 
    'atdLastpractice':{'$max':'$MODIFIED_DATE'},
    'atdPracticecount':{'$sum':1},
    'atdTotal_Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}}]
    merge110=list(collection.aggregate(qra))
    atd=pd.DataFrame(merge110)
    mmm=str(round(sum(atd["atdTotal_Mindful_Minutes"])))
    finalmerge=pd.merge(mergeddf, atd, how='left', left_on='UMSCHOOLID', right_on='_id')
    finaldata=finalmerge[["UMSCHOOLID","UMSCHOOLNAME","UMUSER_ID","ROLE","atdLastpractice","RENEWAL_DATE","atdPracticecount"]]
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].fillna(0)
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('int')
    finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('str')
    usercount=0
    try:
        usercount=len(finaldata[finaldata["ROLE"]=='user'])
    except:
        pass
    familycount=0
    try:
        familycount=len(finaldata[finaldata["ROLE"]=='PRESENT'])
    except:
        pass
    finaldata=finaldata[finaldata["ROLE"]=='user']
    xxx=finaldata[["UMSCHOOLID","UMSCHOOLNAME","RENEWAL_DATE"]]
    xxx['year'] = pd.DatetimeIndex(xxx['RENEWAL_DATE']).year
#     print(xxx)
    xxx['is_paid'] = np.where(xxx['year']>2020,"Y", "N")
    sorted_df = xxx.sort_values(by=['is_paid'], ascending=False)
    yyy=sorted_df[["UMSCHOOLID","UMSCHOOLNAME","is_paid"]]
    cvb=yyy.drop_duplicates(subset="UMSCHOOLID", keep='first', inplace=False)
    totschnew=len(cvb[cvb["is_paid"]=="Y"])
#     print(totschnew,"totalschool")
    data2=[]
    cvb = cvb.sort_values(by=['UMSCHOOLNAME'], ascending=True)
    cvb.reset_index(inplace = True)
    cvb["UMSCHOOLID"] = cvb["UMSCHOOLID"].astype('str')
    for i in range(len(cvb)):
        data2.append({"school_id":cvb["UMSCHOOLID"][i],"school_name":cvb["UMSCHOOLNAME"][i],"is_paid":cvb["is_paid"][i]})
    finaldata={"data":data2,"total_school":totschnew,"user_count":usercount,"family_count":familycount,"mindful_minutes":mmm}
    return json.dumps(finaldata)

@app.route('/bubble')
def bubblee():
    df2=pd.read_csv("bubbledata124.csv")
    fig = px.scatter(df2.query("YEAR==2007"), x="USER ENGAGEMENT", y="FAMILY ENGAGEMENT",
            size="OVERALL PRACTICE", color="SCHOOL COUNT",title="DATA IN CSY",
                     hover_name="DISTRICT NAME", log_x=True, size_max=60, width=1300, height=600)
    fig.update_layout(
    title={'font': {
    'family': 'Montserrat',
    'size': 18,
    'color': '#000000'
    },
    'text': "DATA IN CSY",
    'y':0.9,
    'x':0.5,
    'xanchor': 'center',
    'yanchor': 'top'})

    # convert it to JSON
    fig_json = fig.to_json()
    return (fig_json)

@app.route('/test_portal_api/<inputid>')
def test_portal_api(inputid):  
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('test!_2o20')
    client = MongoClient("mongodb://%s:%s@52.37.152.224:27017/" % (username, password))
    db=client.compass
    collection = db.user_master
    from bson.objectid import ObjectId
    query=[{'$match':{'$and':[{
    "DISTRICT_ID._id":ObjectId(""+inputid+"")    
    },
    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},
    {'IS_PORTAL':'Y'},

    ]
    }},
    {"$project":{"_id":0,
    "UMSCHOOLID":'$schoolId._id',}},
    ]
    merge11=list(collection.aggregate(query))
    overallum11=pd.DataFrame(merge11)
    lifetimelist=list(set(overallum11["UMSCHOOLID"]))
    total_school=len(lifetimelist)
    collection = db.user_master
    query=[{'$match':{'$and':[{
    "schoolId._id": {
    "$in":lifetimelist
    }   
    },

    {'INCOMPLETE_SIGNUP':{"$ne":'Y'}},
    {'IS_DISABLED':{"$ne":'Y'}},
    {'IS_BLOCKED':{"$ne":'Y'}},

    ]
    }},
    {"$project":{"_id":0,
    'ROLE':'$ROLE_ID.ROLE_NAME',
    'UMUSER_ID':'$_id',"USER_NAME":'$USER_NAME',
    "UMSCHOOLID":'$schoolId._id',
                 "UMSCHOOLNAME":'$schoolId.NAME',
                }},
    ]
    merge1=list(collection.aggregate(query))
    overallum=pd.DataFrame(merge1)
    email=list(overallum["UMUSER_ID"])
    schoolid=list(overallum["UMSCHOOLID"])
    ################################sub_master################################
    collection = db.subscription_master
    qr=[
    {"$match":{"$and":[{'USER_ID._id':{"$in":email}},]}},
    {"$project":{"_id":0,
    'SMUSER_ID':'$USER_ID._id',
    "RENEWAL_DATE":"$SUBSCRIPTION_EXPIRE_DATE",
    }},]
    merge=list(collection.aggregate(qr))
    overall=pd.DataFrame(merge)
    mergeddf=pd.merge(overallum, overall, how='left', left_on='UMUSER_ID', right_on='SMUSER_ID')
    db=client.compass
    collection = db.audio_track_master
    qra=[
    {"$match":{'$and':[
    {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}}, 
    {'USER_ID.IS_BLOCKED':{"$ne":'Y'}}, 
    {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
    {'USER_ID.schoolId._id':{'$in':schoolid}},
    ]}},
    {'$group':{'_id':'$USER_ID.schoolId._id', 
    'atdLastpractice':{'$max':'$MODIFIED_DATE'},
    'atdPracticecount':{'$sum':1},
    'atdTotal_Mindful_Minutes':{"$sum":{"$round":[{"$divide":[{"$subtract":['$CURSOR_END','$cursorStart']}, 60]},2]}}}}]
    merge110=list(collection.aggregate(qra))
    atd=pd.DataFrame(merge110)
    if atd.empty:
        atd=0
        mmm=str(atd)
        finalmerge=mergeddf
        finaldata=finalmerge[["UMSCHOOLID","UMSCHOOLNAME","UMUSER_ID","ROLE","RENEWAL_DATE"]]
        usercount=0
        try:
            usercount=len(finaldata[finaldata["ROLE"]=='user'])
        except:
            pass
        familycount=0
        try:
            familycount=len(finaldata[finaldata["ROLE"]=='PRESENT'])
        except:
            pass
        finaldata=finaldata[finaldata["ROLE"]=='user']
        xxx=finaldata[["UMSCHOOLID","UMSCHOOLNAME","RENEWAL_DATE"]]
        xxx['year'] = pd.DatetimeIndex(xxx['RENEWAL_DATE']).year
        #     print(xxx)
        xxx['is_paid'] = np.where(xxx['year']>2020,"Y", "N")
        sorted_df = xxx.sort_values(by=['is_paid'], ascending=False)
        yyy=sorted_df[["UMSCHOOLID","UMSCHOOLNAME","is_paid"]]
        cvb=yyy.drop_duplicates(subset="UMSCHOOLID", keep='first', inplace=False)
        totschnew=len(cvb[cvb["is_paid"]=="Y"])
        #     print(totschnew,"totalschool")
        data2=[]
        cvb = cvb.sort_values(by=['UMSCHOOLNAME'], ascending=True)
        cvb.reset_index(inplace = True)
        cvb["UMSCHOOLID"] = cvb["UMSCHOOLID"].astype('str')
        for i in range(len(cvb)):
            data2.append({"school_id":cvb["UMSCHOOLID"][i],"school_name":cvb["UMSCHOOLNAME"][i],"is_paid":cvb["is_paid"][i]})
        finaldata={"data":data2,"total_school":totschnew,"user_count":usercount,"family_count":familycount,"mindful_minutes":mmm}
    else:
        mmm=str(round(sum(atd["atdTotal_Mindful_Minutes"])))
        finalmerge=pd.merge(mergeddf, atd, how='left', left_on='UMSCHOOLID', right_on='_id')
        finaldata=finalmerge[["UMSCHOOLID","UMSCHOOLNAME","UMUSER_ID","ROLE","atdLastpractice","RENEWAL_DATE","atdPracticecount"]]
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].fillna(0)
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('int')
        finaldata["atdPracticecount"] = finaldata['atdPracticecount'].astype('str')
        usercount=0
        try:
            usercount=len(finaldata[finaldata["ROLE"]=='user'])
        except:
            pass
        familycount=0
        try:
            familycount=len(finaldata[finaldata["ROLE"]=='PRESENT'])
        except:
            pass
        finaldata=finaldata[finaldata["ROLE"]=='user']
        xxx=finaldata[["UMSCHOOLID","UMSCHOOLNAME","RENEWAL_DATE"]]
        xxx['year'] = pd.DatetimeIndex(xxx['RENEWAL_DATE']).year
        #     print(xxx)
        xxx['is_paid'] = np.where(xxx['year']>2020,"Y", "N")
        sorted_df = xxx.sort_values(by=['is_paid'], ascending=False)
        yyy=sorted_df[["UMSCHOOLID","UMSCHOOLNAME","is_paid"]]
        cvb=yyy.drop_duplicates(subset="UMSCHOOLID", keep='first', inplace=False)
        totschnew=len(cvb[cvb["is_paid"]=="Y"])
        #     print(totschnew,"totalschool")
        data2=[]
        cvb = cvb.sort_values(by=['UMSCHOOLNAME'], ascending=True)
        cvb.reset_index(inplace = True)
        cvb["UMSCHOOLID"] = cvb["UMSCHOOLID"].astype('str')
        for i in range(len(cvb)):
            data2.append({"school_id":cvb["UMSCHOOLID"][i],"school_name":cvb["UMSCHOOLNAME"][i],"is_paid":cvb["is_paid"][i]})
        finaldata={"data":data2,"total_school":totschnew,"user_count":usercount,"family_count":familycount,"mindful_minutes":mmm}
    return json.dumps(finaldata)

@app.route('/rtusercount')
def realtimeusercount():
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.audio_track_master
    query4=[{"$match":{
             '$and':[{ 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                       {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                         {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
              {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
              {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
              {'USER_ID.IS_BLOCKED':{"$ne":'Y'}},
              {'USER_ID.ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
              {'USER_ID.DEVICE_USED':{"$regex":'webapp','$options':'i'}},
              {'USER_ID.schoolId.NAME':{'$not':{"$regex":'Blocked','$options':'i'}}},
              {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':0}},
              {'MODIFIED_DATE': {'$gte': datetime.datetime.utcnow()-datetime.timedelta(seconds=60)}}
              ]}},
           {'$group':
           {'_id':'$USER_ID._id',
               'State':{'$first':'$USER_ID.schoolId.STATE'},
               'Country':{'$first':'$USER_ID.schoolId.COUNTRY'}
               }}
           ]
    realtime=list(collection.aggregate(query4))
    realtimeuserpractising=pd.DataFrame(realtime)
    temp={'userpracticing':len(realtimeuserpractising)}
    return json.dumps(temp)

@app.route('/rtmapcount')
def realtimemaprcount():
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('A_dM!n|#!_2o20')
    client = MongoClient("mongodb://%s:%s@44.234.88.150:27017/" % (username, password))
    db=client.compass
    collection = db.audio_track_master
    query4=[{"$match":{
             '$and':[{ 'USER_ID.USER_NAME':{"$not":{"$regex":"test",'$options':'i'}}},
                       {'USER_ID.EMAIL_ID':{"$not":{"$regex":"test",'$options':'i'}}},
                         {'USER_ID.EMAIL_ID':{"$not":{"$regex":"1gen",'$options':'i'}}},
              {'USER_ID.INCOMPLETE_SIGNUP':{"$ne":'Y'}},
              {'USER_ID.IS_DISABLED':{"$ne":'Y'}},
              {'USER_ID.IS_BLOCKED':{"$ne":'Y'}},
              {'USER_ID.ROLE_ID._id':{'$ne':ObjectId("5f155b8a3b6800007900da2b")}},
              {'USER_ID.DEVICE_USED':{"$regex":'webapp','$options':'i'}},
              {'USER_ID.schoolId.NAME':{'$not':{"$regex":'Blocked','$options':'i'}}},
              {'USER_ID.schoolId.BLOCKED_BY_CAP':{'$exists':0}},
              {'MODIFIED_DATE': {'$gte': datetime.datetime.utcnow()-datetime.timedelta(seconds=60)}}
              ]}},
           {'$group':
           {'_id':'$USER_ID._id',
               'State':{'$first':'$USER_ID.schoolId.STATE'},
               'Country':{'$first':'$USER_ID.schoolId.COUNTRY'}
               }}
           ]
    realtime=list(collection.aggregate(query4))
    realtimeuserpractising=pd.DataFrame(realtime)
    if realtimeuserpractising.empty:
        df = pd.DataFrame(columns=['State', 'STATE_SHOT','text','_id'])
        for i in range(1):
            df.loc[i] = ['none'] +['none'] +['NO USER PRACTICING RIGHT NOW']+ [0]
            df1=df[["State","_id","STATE_SHOT",'text']]
            links0 =df1.rename(columns={'STATE_SHOT' : 'code', '_id' : 'value','State':'name','text':'text'}).to_dict('r')
    else:
        us_state_shot = {
            'Alabama': 'AL',
            'Alaska': 'AK',
            'American Samoa': 'AS',
            'Arizona': 'AZ',
            'Arkansas': 'AR',
            'California': 'CA',
            'Colorado': 'CO',
            'Connecticut': 'CT',
            'Delaware': 'DE',
            'District of Columbia': 'DC',
            'Florida': 'FL',
            'Georgia': 'GA',
            'Guam': 'GU',
            'Hawaii': 'HI',
            'Idaho': 'ID',
            'Illinois': 'IL',
            'Indiana': 'IN',
            'Iowa': 'IA',
            'Kansas': 'KS',
            'Kentucky': 'KY',
            'Louisiana': 'LA',
            'Maine': 'ME',
            'Maryland': 'MD',
            'Massachusetts': 'MA',
            'Michigan': 'MI',
            'Minnesota': 'MN',
            'Mississippi': 'MS',
            'Missouri': 'MO',
            'Montana': 'MT',
            'Nebraska': 'NE',
            'Nevada': 'NV',
            'New Hampshire': 'NH',
            'New Jersey': 'NJ',
            'New Mexico': 'NM',
            'New York': 'NY',
            'North Carolina': 'NC',
            'North Dakota': 'ND',
            'Northern Mariana Islands':'MP',
            'Ohio': 'OH',
            'Oklahoma': 'OK',
            'Oregon': 'OR',
            'Pennsylvania': 'PA',
            'Puerto Rico': 'PR',
            'Rhode Island': 'RI',
            'South Carolina': 'SC',
            'South Dakota': 'SD',
            'Tennessee': 'TN',
            'Texas': 'TX',
            'Utah': 'UT',
            'Vermont': 'VT',
            'Virgin Islands': 'VI',
            'Virginia': 'VA',
            'Washington': 'WA',
            'West Virginia': 'WV',
            'Wisconsin': 'WI',
            'Wyoming': 'WY'
        }
        realtimeuserpractising["STATE_SHOT"] = realtimeuserpractising["State"].map(us_state_shot) 
        df1=realtimeuserpractising.groupby(["State","STATE_SHOT"]).count().reset_index()
        df2=df1[["State","_id","STATE_SHOT"]]
        links0 =df2.rename(columns={'STATE_SHOT' : 'code', '_id' : 'value','State':'name'}).to_dict('r')
    return json.dumps(links0)


if __name__== "__main__":
     app.run()
