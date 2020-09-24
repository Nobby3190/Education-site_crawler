#!/usr/bin/env python
# coding: utf-8

# In[7]:


#!/usr/bin/env python
# coding: utf-8
from monitor import monitor
import sys
import traceback
import os
from selenium import webdriver
import time
import requests
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime 
import sqlalchemy
import numpy as np
#log變數
#-------------------------------------------------------------
monitor = monitor() #引入監控程式
daytime = monitor.daytime() #時間
absFilePath = os.path.abspath('') #路徑（切換成.py使用__file__）
path, filename = os.path.split(absFilePath) #路徑
confini = "config.ini" # config名稱（config須自行建立）
#=============================================================

#計時開始
#-------------------------------------------------------------
timeS = monitor.timing()
#=============================================================

#conf變數
#-------------------------------------------------------------
errlogfilename, logfilename, serialnumfile ,codenum , to_mail , gmail_user, gmail_password , Subject , logdb ,logtable , loguser, logpw , logip , setlogfile ,token= monitor.conf(confini)
#=============================================================

#主程式
#-------------------------------------------------------------
# your function

def wash():
    try:
        engine = create_engine(f'mysql+pymysql://{loguser}:{logpw}@{logip}:3306/{logdb}?charset=utf8')
        sql = "SELECT * FROM wash;"
        df = pd.read_sql(sql,engine)
        df["hours"] = df.hours.astype("float32")
        df['price'] = df['price'].map(lambda name: name.replace(",",""))
        df['price'] = df['price'].astype('int')

# ----------------------------------------------------------------------

        df['web'] ='私塾堂'
        df['tech'] = np.nan
        df['lan'] = np.nan
        df['bz'] = np.nan
        df['others'] = False
        df['type'] = 'online'
        df['all_city'] = np.nan
        df['taipei_dist'] = np.nan
        df['address'] = np.nan
        df['weekday'] = False
        df['weekends'] = False
        df['start_time'] = np.nan
        df['end_time'] = np.nan
        df = df[['web','title','hours','price','today','tech','lan','bz','others','type','all_city','taipei_dist','address','weekday','weekends','start_time','end_time']]

        for i in range(len(df)):
            if 'Autodesk 3ds Max 基礎課程 第1回 | 共2回' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MKT'
            elif 'Vizible基礎課程' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MKT'
            elif 'Autodesk 3ds Max 基礎課程 第2回 | 共2回' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MKT'
            elif 'Autodesk 3DS MAX 2021 Features Highlight' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MKT'
            elif '影片剪輯課程' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MKT'
            elif 'HyperPDM 管理者設定課程(產品開發管理)' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MGMT'
            elif 'Autodesk Vault 基礎課程' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MGMT'
            elif 'Forge【開發技巧專題】- 模型聚合和多模型管理' in df['title'].values[i]:
                df.loc[i,'bz'] = 'MGMT'
            else:
                df.loc[i,'others'] = True

        return df,engine
    except Exception as e:
        print(str(e))

def type_wash(df,engine):
    try:
        df.to_sql('web2_mission_completed',engine,index=False,if_exists = 'replace',
                 dtype = {
                     "web": sqlalchemy.types.Text(),
                     "title" : sqlalchemy.types.Text(),
                     "hours" : sqlalchemy.types.Float(precision=1),
                     "price" : sqlalchemy.types.INTEGER(),
                     "today" : sqlalchemy.types.Text(),
                     "tech" : sqlalchemy.types.Text(),
                     "lan" : sqlalchemy.types.Text(),
                     "bz" : sqlalchemy.types.Text(),
                     "others" : sqlalchemy.types.Boolean
                 }
                 )
    except Exception as e:
        print(str(e))

#=============================================================

#先設定錯誤備註為空值
description = ''

#主要執行區
#-------------------------------------------------------------
try:
    #logfile分隔線
    #--------------------------
    monitor.filewriteS(setlogfile)
    #--------------------------
    processnum = 1
    #function1
    df,engine = wash()
    description = '執行完第一步驟時錯誤'
    logtext = '一般log紀錄  第一步'
    monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
#     processnum = 2
#     #function2
#     item_list = main(urls_list)
#     description = '執行完第二步驟時錯誤'
#     logtext = '一般log紀錄  第二步'
#     monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
    processnum = 3
    #function3
    type_wash(df,engine)
    description = '執行完第三步驟時錯誤'
    logtext = '一般log紀錄  第三步'
    monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
    #.........
    state = "success"
    errnum = 0
except Exception as e:  
    state = "fail"
    errnum = 1
    #文件寫入點1 寫入錯誤
    monitor.toErrFile(errlogfilename , daytime, filename, e ,processnum,description)
#=============================================================

# 比對  應放入資料庫筆數 / 已入資料庫筆數
# note:此處因為沒有真實資料先使用log資料，上線後請改成爬蟲或清洗資料
#------------------------------------------------------------- 
try:
    #請放入此次執行應進SQL行數（改成df行數）
    sSQL = len(df)
    #請select出此次執行已進入SQL行數(改成已輸入的DATA數量)
    sql = "select count(*) as cou from web2_mission_completed;"
    #SQL query 請更改 ip account password databases query
    db, cursor ,accounts = monitor.pymysqlcon(logip, loguser, logpw, logdb ,sql)
    aSQL = accounts[0]['cou']
except:
    sSQL = 0
    aSQL = 99
#=============================================================  

#執行次數紀錄
#-------------------------------------------------------------   
monitor.serialnum(serialnumfile,codenum,errnum) 
#=============================================================  

#花費時間
#-------------------------------------------------------------   
timeE = monitor.timing() 
timeSP = timeE -timeS
#=============================================================   

#文件寫入點2 寫入log檔
#-------------------------------------------------------------   
monitor.toFile(logfilename , daytime, timeS, timeE ,filename ,state ,sSQL ,aSQL )
#=============================================================   

# log to SQL
#-------------------------------------------------------------

missSQL = sSQL - aSQL  #漏掉資料量
try:
    #建立 SQL 語法 insert & createtable
    sql , sqlcreate = monitor.logSQL(logtable, daytime,timeSP,filename,state,sSQL,aSQL ,missSQL ,logdb)
    #log to SQL 
    #  1.create table
    monitor.pymysqlcon(logip, loguser, logpw, logdb ,sqlcreate)
    #  2.insert log
    monitor.pymysqlcon(logip, loguser, logpw, logdb ,sql)
    logerrnum =0
except:
    logerrnum = 1
#=============================================================  

# line Send error message
#-------------------------------------------------------------   
if errnum == 1:        
    # 修改為你要傳送的訊息內容
    message = str(errlogfilename) +"\n"+ str(daytime) +"\n"+ str(filename) +"\n"+ str(processnum) +"\n"+ str(description)
    # 修改為你的權杖內容
    monitor.lineNotifyMessage(token, message)
    
if logerrnum == 1:        
    # 修改為你要傳送的訊息內容
    message = str(errlogfilename) +"\n"+ str(daytime) +"\n"+ str(filename) +"\n"+ 'log to SQL error'
    # 修改為你的權杖內容
    monitor.lineNotifyMessage(token, message)
#=============================================================  

# 一、自定義log 建議
# --- START crawing at 2020-05-13 11:42:20.386133 ---
# ---
# Finished crawing [ spark ] at 2020-05-13 11:44:05.156622
# [Success] Check Point 1 : CorpNo. 71 = JobNo. 71
# [Success] Check Point 2 : CorpNo. and JobNo. (71/71) = TotalJobs 71 and NO Exceptions
# [Success] Check Point 3 : CorpNo. or JobNo. (71/71) = InsertedJobs 71 
# ---

# 二、line傳送錯誤訊息
# 去 https://notify-bot.line.me/zh_TW/ 個人頁面設定 tocken


# In[8]:


df


# In[ ]:




