#!/usr/bin/env python
# coding: utf-8

# In[1]:


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

def read_data():
    try:
        engine = create_engine(f'mysql+pymysql://allen:allen0319@192.168.56.150:3306/education?charset=utf8')
        sql = "SELECT * FROM web1_test;"
        df = pd.read_sql(sql,engine)
        df["hours"] = df.hours.astype("float32")
        df['price'] = df['price'].map(lambda name: name.replace(",",""))
        df['price'] = df['price'].astype('int')
        return df
    except Exception as e:
        print(str(e))
        
def wash(df):
    try:
        df['web'] = 'GoGoCourse購購課'
        df['tech'] = np.nan
        df['lan'] = np.nan
        df['bz'] = np.nan
        df['others'] = True
        df['type'] = 'offline'
        df['all_city'] = np.nan
        df['taipei_dist'] = np.nan
        df['address'] = np.nan
        df['weekday'] = False
        df['weekends'] = False
        df['start_time'] = np.nan
        df['end_time'] = np.nan
        df = df[['web','title','hours','price','time','location','today','tech','lan','bz','others','type','all_city','taipei_dist','address','weekday','weekends','start_time','end_time']]

        # ----------tech---------------------------

        for i in range(len(df)):
            if 'Python' in df['title'].values[i]:
                df.loc[i,'tech'] = 'Python'
                df.loc[i,'others'] = False
            elif 'Java' in df['title'].values[i]:
                df.loc[i,'tech'] = 'Java'
                df.loc[i,'others'] = False
            elif '網頁開發' in df['title'].values[i]:
                df.loc[i,'tech'] = 'WebDesign'
                df.loc[i,'others'] = False
            elif 'Linux' in df['title'].values[i]:
                df.loc[i,'tech'] = 'Linux'
                df.loc[i,'others'] = False
            elif 'C#' in df['title'].values[i]:
                df.loc[i,'tech'] = 'C#'
                df.loc[i,'others'] = False
            elif 'R' in df['title'].values[i]:
                df.loc[i,'tech'] = 'R'
                df.loc[i,'others'] = False
            elif 'C++' in df['title'].values[i]:
                df.loc[i,'tech'] = 'C++'
                df.loc[i,'others'] = False
            elif '資料庫' in df['title'].values[i]:
                df.loc[i,'tech'] = 'Database'
                df.loc[i,'others'] = False
            elif '網路工程' in df['title'].values[i]:
                df.loc[i,'tech'] = 'Network'
                df.loc[i,'others'] = False
            else:
                df.loc[i,'tech'] = np.nan

        # -------lan-----------------------

        for j in range(len(df)):
            if '英語' in df['title'].values[j]:
                df.loc[j,'lan'] = 'ENG'
                df.loc[j,'others'] = False
            else:
                df.loc[j,'lan'] = np.nan

        # -------bz-------------------

        for l in range(len(df)):
            if '商務' in df['title'].values[l]:
                df.loc[l,'bz'] = 'MGMT'
                df.loc[l,'others'] = False
            elif '管理' in df['title'].values[l]:
                df.loc[l,'bz'] = 'MGMT'
                df.loc[l,'others'] = False
            elif '證照' in df['title'].values[l]:
                df.loc[l,'bz'] = 'LIS'
                df.loc[l,'others'] = False
            elif '財務' in df['title'].values[l]:
                df.loc[l,'bz'] = 'FIN'
                df.loc[i,'others'] = False
            elif '會計' in df['title'].values[l]:
                df.loc[l,'bz'] = 'FIN'
                df.loc[l,'others'] = False
            elif '理財' in df['title'].values[l]:
                df.loc[l,'bz'] = 'FIN'
                df.loc[i,'others'] = False
            elif 'ETF' in df['title'].values[l]:
                df.loc[l,'bz'] = 'FIN'
                df.loc[l,'others'] = False
            elif '外匯' in df['title'].values[l]:
                df.loc[l,'bz'] = 'FIN'
                df.loc[l,'others'] = False
            elif '股市投資' in df['title'].values[l]:
                df.loc[l,'bz'] = 'FIN'
                df.loc[l,'others'] = False
            else:
                df.loc[l,'bz'] = np.nan


        # -------location、address-------------------

        for k in range(len(df)):
        #   taipei
            if '大安中心' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市大安區信義路四段1號3樓'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大安區'
            elif '華岡' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市大安區信義路四段1號3樓'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大安區'
            elif '福華' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市大安區新生南路三段三十號'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大安區'
            elif 'investU線上社大 台北教室' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市信義區松山路421號16樓之2'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '信義區'
            elif '為你而讀' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市大安區和平東路一段75巷2-1號'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大安區'
            elif '臺北市青少年發展處' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市仁愛路一段17號'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '中正區'
            elif '華岡' and '福華' in df['location'].values[k]:
                df.loc[k,'all_city'] = 'taipei'
            elif '身障資源中心' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市中山區長安西路5巷2號'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '中山區'
            elif '台北市萬華區艋舺大道140號' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市萬華區艋舺大道140號'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '萬華區'
            elif '台灣文創訓練中心' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市中山區松江路131號'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '中山區'
            elif '台北市大同區' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市大同區迪化街一段32巷14號2樓'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大同區'
            elif '台北市大安區106永康街75巷8號5樓之2' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市大安區106永康街75巷8號5樓之2'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大安區'
            elif '台北市大安區' in df['location'].values[k]:
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '大安區'
            elif '創世紀國際不動產管理顧問有限公司' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市信義區信義路四段415號8樓之5'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '信義區'
            elif '台北市中山區長安東路二段92號2樓' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市中山區長安東路二段92號2樓'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '中山區'
            elif '台北市南京東路四段2號1-10號店鋪' in df['location'].values[k]:
                df.loc[k,'address'] = '台北市南京東路四段2號1-10號店鋪'
                df.loc[k,'all_city'] = 'taipei'
                df.loc[k,'taipei_dist'] = '松山區'
            elif '淡江大學台北校區' in df['location'].values[k] and '台灣文創訓練中心' in df['location'].values[k]:
                df.loc[k,'all_city'] = 'taipei'
        #   taichung
            elif '台中新創館' in df['location'].values[k]:
                df.loc[k,'address'] = '台中市西區台灣大道2段2號3F-3'
                df.loc[k,'all_city'] = 'taichung'
            elif '台中東海大學' in df['location'].values[k]:
                df.loc[k,'address'] = '臺中市臺灣大道四段1727號管理大樓 東海大學第二教學區'
                df.loc[k,'all_city'] = 'taichung'
        #   kaohsiung
            elif '高雄信義館' in df['location'].values[k]:
                df.loc[k,'address'] = '高雄市苓雅區中正二路175號13樓之3'
                df.loc[k,'all_city'] = 'kaohsiung'
            elif '' in df['location'].values[k]:
                df.loc[k,'address'] = np.nan
                df.loc[k,'all_city'] = np.nan
            elif '高雄市三民區博愛一路366號14樓' in df['location'].values[k]:
                df.loc[k,'address'] = '高雄市三民區博愛一路366號14樓'
                df.loc[k,'all_city'] = 'kaoshiung'
            else:
                df['location'].values[k] = df.loc[k,'address']

        # # # -----weekdays------------------

        for n in range(len(df)):
            if '六' in df['time'].values[n] or '日' in df['time'].values[n]:
                df.loc[n,'weekends'] = True
            elif '一' in df['time'].values[n] or '二' in df['time'].values[n] or '三' in df['time'].values[n] or '四' in df['time'].values[n] or '五' in df['time'].values[n]:
                df.loc[n,'weekday'] = True
            else:
                df.loc[n,'weekends'] = False
                df.loc[n,'weekday'] = False
        data = df.drop(['time','location'], axis = 1)
        return data
    except Exception as e:
        print(str(e))
def putintoSql(data):
    engine = create_engine('mysql+pymysql://allen:allen0319@192.168.56.150:3306/education?charset=utf8')
    data.to_sql('web1_test',engine,index=False,if_exists='replace')
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
    description = '執行完第一步驟時錯誤'
    logtext = '一般log紀錄  第一步'
    items = read_data()
    monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
    processnum = 2
    #function2
    description = '執行完第二步驟時錯誤'
    logtext = '一般log紀錄  第二步'
    washed_items = wash(items)
    monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
    processnum = 3
    #function3
    description = '執行完第三步驟時錯誤'
    logtext = '一般log紀錄  第三步'
    putintoSql(washed_items)
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
    sSQL = len(item_list)
    #請select出此次執行已進入SQL行數(改成已輸入的DATA數量)
    sql = "select count(*) as cou from web1_test;"
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


# In[ ]:




