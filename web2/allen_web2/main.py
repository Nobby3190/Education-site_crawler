#!/usr/bin/env python
# coding: utf-8

# In[2]:


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
def url():
    try:
        pages_list = [30,31,32,33,34,35,36,37,38,39,53,46,47,51]
        urls_list = []
        for x in pages_list:
            education_url = f'https://www.sishutang.com.tw/course-list/Autodesk_%E8%A3%BD%E9%80%A0%E6%A5%AD2D%E7%B3%BB%E5%88%97%E8%AA%B2%E7%A8%8B/{x}'
            page = requests.get(education_url)
            soup = bs(page.text,'lxml')
            for i in range(1,30):
                href = soup.select(f"#course > section > div.content-wrap > div.content-detail > div > div.course > div > div:nth-child({i}) > div > div.text > a")
                for url in href:
                    urls = url["href"]
                    urls_list.append(urls)
    except Error as e:
        print(str(e))
    finally:
        return urls_list
def main(urls_list):
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        driver = webdriver.Chrome(chrome_options = options, executable_path = "/Users/t3019ein/Desktop/chromedriver")
        courses = url()
        item_list = []
    #   課程主頁的網址
        for j in range(len(courses)):
            print(j)
            page = driver.get(courses[j])
            items_a = driver.find_elements_by_xpath("//div[@class='info-head head-skin01']/h1[@class='info-title']")
            driver.implicitly_wait(3)
            dic={}
    #       課程名稱
            for a in items_a:
                dic["title"] = a.text.strip().replace("\n","")
                items_b = driver.find_elements_by_xpath("//div[@class='course-lector']/div[@class='hour']/span")
                driver.implicitly_wait(3)
    #       上課時數
            for b in items_b:
                dic["hours"] = b.text.strip().replace("(小時)","")
                items_c = driver.find_elements_by_xpath("//div[@class='course-lector']/div[@class='lector']/span")
                driver.implicitly_wait(3)
    #       上課老師
            for c in items_c:
                dic["teacher"] = c.text.strip().replace("/","")
            items_d = driver.find_elements_by_xpath("//div[@class='recommend']/div[@class='price']")
            driver.implicitly_wait(3)
    #       課程價格
            for d in items_d:
                dic["price"] = d.text.strip().replace("NT$","").replace("月","").replace("|","").replace("/","").replace(",","")
            today = datetime.today().strftime("%Y%m%d")
    #       爬蟲時間
            dic["today"] = today
            item_list.append(dic)
            time.sleep(3)
    except Error as e:
        print(str(e))
    finally:
        driver.quit()
        return item_list
def putintoSql(item_list):
    df = pd.DataFrame(item_list)
    engine = create_engine(f'mysql+pymysql://{loguser}:{logpw}@{logip}:3306/{logdb}?charset=utf8')
    df.to_sql('wash0723',engine,index=False)

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
    urls_list = url()
    description = '執行完第一步驟時錯誤'
    logtext = '一般log紀錄  第一步'
    monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
    processnum = 2
    #function2
    item_list = main(urls_list)
    description = '執行完第二步驟時錯誤'
    logtext = '一般log紀錄  第二步'
    monitor.filewrite(setlogfile,logtext,processnum)
    #--------------------------
    processnum = 3
    #function3
    putintoSql(item_list)
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
    sSQL = len(item_list)
    #請select出此次執行已進入SQL行數(改成已輸入的DATA數量)
    sql = "select count(*) as cou from wash0723;"
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





# In[ ]:




