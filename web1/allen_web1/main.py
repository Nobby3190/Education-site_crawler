#!/usr/bin/env python
# coding: utf-8

# In[4]:


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
def UrlAndCrawl():
    try:
        page_list = ["career","education","living","health","senior","pets","language","design","creative-industry","technology","business","investment-finance"]
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        driver = webdriver.Chrome(chrome_options = options, executable_path = "/Users/t3019ein/Desktop/chromedriver")
        driver.implicitly_wait(3)
        urls_list = []
        for x in page_list:
            for i in range(1,15):
                raw_page = driver.get(f"https://ggc.hkxf.com.tw/category/{x}/{i}")
                time.sleep(2)
                for j in range(1,11):
                    urls = driver.find_elements_by_xpath(f"//*[@id='CourseContainer']/div[{j}]/div[2]/a")
                    for k in urls:
                        href = k.get_attribute("href")
                        urls_list.append(href)

        item_list = []
        for z in urls_list:
            dic = {}
            driver.get(z)
            title = driver.find_elements_by_xpath("//div[@class='pr-2 pl-2'][2]/h2[@class='mt-3 mb-2 font-weight-bold']")
            driver.implicitly_wait(3)
        #           課程標題
            for a in title:
                dic["title"] = a.text.strip().replace("⭐","").replace("🏠","").replace("《","").replace("》","").replace("【","").replace("】","").replace("—","").replace("~","").replace("│","").replace("®","")
            during = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[1]/span[@class='course-info-details text-gray-darkgray']")
            driver.implicitly_wait(3)
        #           課程時程
            for c in during:
                dic["during"] = c.text.strip()
            hours = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[3]/span[@class='course-info-details text-gray-darkgray']")
            driver.implicitly_wait(3)
        #           課程時數
            for d in hours:
                dic["hours"] = d.text.strip().replace("hr","")
            location = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[5]/a/span[@class='course-info-details']")
            driver.implicitly_wait(3)
        # #           課程地點
            for e in location:
                dic["location"] = e.text.strip()
            price = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[6]/span[@class='course-info-details f-10 text-gray-darkgray']")
            driver.implicitly_wait(3)
        # #           課程價格
            for f in price:
                dic["price"] = f.text.strip().replace("NTD$","")
            coursetime = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[2]/span[@class='course-info-details align-middle text-gray-darkgray']")
            driver.implicitly_wait(3)
#             課程時間
            for g in coursetime:
                dic['time'] = g.text.strip()
            weekday = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[1]/span[@class='course-info-details text-gray-darkgray']")
            driver.implicitly_wait(3)
#             課程是禮拜幾
            for h in weekday:
                dic['weekday'] = h.text.strip()
            address = driver.find_elements_by_xpath("//ul[@class='course-info-list mt-20 mt-4 ml-0 pl-0']/li[5]/a/span[@class='course-info-details']")
            for v in address:
                dic['address'] = v.text.strip()
            today = datetime.today().strftime("%Y%m%d")
        # #           爬蟲時間
            dic["today"] = today
            item_list.append(dic)
    #         time.sleep(3)
    except NameError as e:
        print(str(e))
    finally:
        return item_list
        driver.quit()
def putintoSql(item_list):
#   寫進mysql
    df = pd.DataFrame(item_list)
    engine = create_engine('mysql+pymysql://allen:allen0319@192.168.56.150:3306/education?charset=utf8')
    df.to_sql('web1_test',engine,index=False,if_exists='replace')
    
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
    items = UrlAndCrawl()
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
    description = '執行完第三步驟時錯誤'
    logtext = '一般log紀錄  第三步'
    putintoSql(items)
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




