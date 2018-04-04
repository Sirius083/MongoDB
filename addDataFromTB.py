# -*- coding: utf-8 -*-
"""
Created on Tue Jan 09 16:25:00 2018

@author: Sirius

py3 版本 学校意外断电断网，数据缺失
1. 手动在TB上下载缺失的合约到TB_DATA_DIR中，更改数据库名字
2. 运行此程序
3. dump 数据库中的数据，restore到原数据库中

"""
#----------------------------------------------------------------------
# 读TB的csv文件       
import os
import re
import sys
import json
import pymongo
import pandas as pd
from datetime import datetime

from FindTradingDay import *
from TradeBase import readTradeDayList,changeZSNameMinusyear


TB_DATA_DIR = 'C:\\Users\\Sirius\\Desktop\\mongoDB\\20180405'
MIN_DB_NAME = 'VnTrader_1MIN_0404_night'              # 分钟数据库名称

MONGODB_BIN_PATH = 'D:\\MongoDB\\bin'
TRADINGDAY_START = '20180101'
TRADINGDAY_END = '20180501'
replaceStartDay = '20180401' # 为了减少处理dataframe的运算量
TRADINGDAY_PATH = 'C:\\Users\\Sirius\\Desktop\\TL_tradingDay.xlsx'


holidaylist = ['20171009','20180102'] # 节假日没有夜盘的日期

startTime = '20180404 00:06:00'
endTime = '20180404 02:35:00'
startTime = datetime.strptime(startTime, "%Y%m%d %H:%M:%S")
endTime = datetime.strptime(endTime, "%Y%m%d %H:%M:%S")

# 将文件名中的（）去掉
for root,dirs,files in os.walk(TB_DATA_DIR):
    for onefile in files:
        path = os.path.join(TB_DATA_DIR, onefile)
        if '(' in path:
            contract = onefile.split('(')[0]
            contract = changeZSNameMinusyear(contract)
            newname = TB_DATA_DIR + '\\' + contract + '.csv'
            os.rename(path, newname)


CONTRACT_LIST = os.listdir(TB_DATA_DIR)
CONTRACT_LIST = [con.split('.')[0] for con in CONTRACT_LIST]

tradingDayList = readTradeDayList(TRADINGDAY_PATH,TRADINGDAY_START,TRADINGDAY_END)
# path = 'C:\\Users\\Sirius\\Desktop\\mongoDB\\MissingData20180403\\au1808.csv'

def datetime_process(d):
    '''将 timestamp 转化为 datetime'''
    for k, v in d.items():
        if k == 'datetime' or k == 'localTime':
            if d[k]:
               d[k] = datetime.utcfromtimestamp(v/1000)
    return d 


for root,dirs,files in os.walk(TB_DATA_DIR):
    for onefile in files:
        contract = onefile.split('.')[0]
        if contract in CONTRACT_LIST:
            print ('--------------------------------------------')
            dbClient = pymongo.MongoClient('localhost', 27017)
            coll = dbClient[MIN_DB_NAME][contract]
            path = os.path.join(TB_DATA_DIR, onefile)
            df = pd.read_csv(path, encoding = 'utf-8', names = ['datetime','open','high','low','close','volume','openInterest'])
            if len(df) == 0:
                continue
            #------------------------------------------------------------------------------
            # 给df增加ActionDay和TradingDay
            ActionDay = [t[:10].replace('/','') for t in df['datetime']]
            df['ActionDay'] = ActionDay
            df = df[df['ActionDay'] >= replaceStartDay] # 为了加速计算
            df = df.reset_index(drop=True)
            
            timeList = [datetime.strptime(t, "%Y/%m/%d %H:%M") for t in df['datetime']] # u'2018/01/09 11:21'
            TradingDay = [findTradingDay(t,tradingDayList) for t in timeList]
            df['TradingDay'] = TradingDay
            df['datetime'] = timeList
            df = df.reset_index(drop=True)
            
            # 找到给定时间段内的数据
            mask = (df['datetime'] > startTime) & (df['datetime'] <= endTime)
            data = df.loc[mask]
                        
            data = data.sort_values(by = 'datetime', ascending = True)# 按照时间排序
            data = data.reset_index(drop=True)
            records = []
            for index, row in data.iterrows(): 
                row = row.T.to_json()
                records.append(json.loads(row,object_hook = datetime_process))
            if len(records) == 0:
                print('无合约数据%s'%contract)
                continue
            coll.insert(records)
            print ('合约%s已更新完毕，插入%d条' % (contract, len(records)))

# mongorestore -d VnTrader_1MIN_ALL_1106 dumpSC0403/VnTrader_1MIN_ALL_0403
# mongodump --db VnTrader_1MIN_0403_night  --out min_0403_night
# mongodump --db VnTrader_1MIN_0404_night  --out min_0404_night               