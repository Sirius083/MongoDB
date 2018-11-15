# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 14:24:05 2018

@author: Sirius

每天收盘收清理tick数据
20180418以后的数据
假设每天收到数据的tradingDay计算正确
将不同数据库的tick合并
有datatime不同但是其余条全部相同的
datetime相同的按照acvolume排序
acvolume相同的按照localTime排序
不同数据源(mc,gf,jlz)的不用合并
删除完异常数据再合并
"""

# 20180810: gf 数据库换名称
# 传入311的数据
# 20171107：数据清洗

import re
import os
import json
import time
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime
from checkInTradeTime import getTradeTime,checkInTradeTime
import more_itertools as mit
from TradeBase import readTradeDayList

# 对 vnpy 收到的郑商所数据，CF801--> CF1801
# 包括期权数据
def changeZSNameAddyear(contract):
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY','AP']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] # 合约名称
    if symbol in ZS:
       if len(contract) > 6:
          contract = re.match(r"([a-zA-Z]+)([0-9]+)([a-zA-Z]+)([0-9]+)", contract).groups()[:2]
          contract = contract[0] + contract[1]
       y3 = str(datetime.now().year)[-2] # 当前year的第三位
       contract = contract[:-3] + y3 + contract[-3:]
    return contract

'''
def datetime_process(d): 
    # 将 timestamp 转化为 datetime
    for k, v in d.items():
        if k == 'datetime' or k == 'localTime':
            d[k] = datetime.utcfromtimestamp(v/1000)
    return d
'''

def datetime_process(d): 
    # 将 timestamp 转化为 datetime
    for k, v in d.items():
        if k == 'datetime':
            d[k] = datetime.utcfromtimestamp(v/1000)
        if k == 'localTime':
           if d[k]:
               d[k] = datetime.utcfromtimestamp(v/1000)
           else:
               d[k] = None
    return d



def get_contractName(dbClient_jlz, dbClient_gf, dbClient_mc, tradingDay):
    # 返回给定交易日所有未过期合约的名字
    yearMonth = tradingDay[2:-2]
    futureName = [c for c in allcoll_names if len(c) <= 6]
    futureName = [c for c in futureName if changeZSNameAddyear(c)[-4:] >= yearMonth]
    optionZS = [c for c in allcoll_names if len(c) > 6 and '-' not in c] # ZS 期权合约
    optionDS = [c for c in allcoll_names if '-' in c]                    # DS 期权合约
    optionZS = [c for c in optionZS if changeZSNameAddyear(c)[2:] >= yearMonth]
    optionDS = [c for c in optionDS if c.split('-')[0][-4:] >= yearMonth]
    optionName = optionZS + optionDS

    return futureName,optionName

#=======================================================================================
# jlz 数据库
host_jlz = '58.206.97.70'
port_jlz = 28001
min_jlz = 'jlz_1MIN'

# gf 数据库
host_gf = '58.206.97.70'
port_gf = 28001
min_gf = 'gf_1MIN_0810'

# mc 数据库
host_mc = '58.206.97.70'
port_mc = 28001
min_mc = 'mc_1MIN'
#==============================================================================
def processMinDay(contract,tradingDay,dbName):
    # 重新认证，以防连接超时
    dbClient_jlz = pymongo.MongoClient(host_jlz, port_jlz)[min_jlz]
    dbClient_jlz.authenticate("jlz", "jlz_607")

    dbClient_gf = pymongo.MongoClient(host_gf, port_gf)[min_gf]
    dbClient_gf.authenticate("gf", "gf_607")
    
    dbClient_mc = pymongo.MongoClient(host_mc, port_mc)[min_mc]
    dbClient_mc.authenticate("mc", "mc_607")
    
    coll_jlz = dbClient_jlz[contract]
    cursor = coll_jlz.find(flt).sort('datetime',pymongo.ASCENDING)
    d1 = pd.DataFrame(list(cursor))
     
    coll_gf = dbClient_gf[contract]
    cursor = coll_gf.find(flt).sort('datetime',pymongo.ASCENDING)
    d2 = pd.DataFrame(list(cursor))
    
    coll_mc = dbClient_mc[contract]
    cursor = coll_mc.find(flt).sort('datetime',pymongo.ASCENDING)
    d3 = pd.DataFrame(list(cursor))
    
   
    if len(d1) == max(len(d1), len(d2), len(d3)):
       data = d1
    
    elif len(d2) == max(len(d1), len(d2), len(d3)):
       data = d2
    
    elif len(d3) == max(len(d1), len(d2), len(d3)):
       data = d3
       
    if len(data) == 0:
       print('contract %s, len 0'%contract)
       return

    # 价格异常
    data = data[(data['close'] > 0) & (data['close']< priceMax)
            & (data['open'] > 0) & (data['open']< priceMax)
            & (data['high'] > 0) & (data['high']< priceMax)
            & (data['low'] > 0) & (data['low']< priceMax)]
    
    if len(data) == 0:
       print('contract %s, len 0'%contract)
       return
    
    # 非交易时间段
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0]
    contractTime = getTradeTime(symbol)
    inTradeTime = [checkInTradeTime(contractTime,t) for t in data.datetime] 
    data = data[inTradeTime] 
    data = data.sort_values(by = 'datetime', ascending = True)
    data = data.reset_index(drop = True)
    del data['_id']
    
    if len(data) == 0:
       return
    
    data = data.sort_values(by = 'datetime', ascending = True)
    data = data.reset_index(drop = True)
    
    volume = np.diff(np.array(data.acvolume))
    volume = np.insert(volume, 0, data.acvolume.iloc[0])
    data.volume = volume
    
    # drop if nan in column 'datetime'
    data = data.dropna(subset=['datetime'])
    
    # 
    
    # 将数据传入新的数据库中，将新数据传入本地
    dbClient_new = pymongo.MongoClient('localhost', 27017)
    collnew = dbClient_new[dbName][contract]
    records = []
    for index, row in data.iterrows(): 
        row = row.T.to_json()
        records.append(json.loads(row,object_hook = datetime_process))
    collnew.insert(records)
    
    with open('result_min.txt', 'a') as resultfile:
         resultfile.write("min processing tradingDay %s, contract %s done\n" %(tradingDay, contract))

    print('tradingDay %s, process %s, total len %s'% (tradingDay, contract, len(data)))


#====================================================================================
# 运行程序
# 找到所有列表出现合约的名字
dbClient_jlz = pymongo.MongoClient(host_jlz, port_jlz)[min_jlz]
dbClient_jlz.authenticate("jlz", "jlz_607")
dbClient_gf = pymongo.MongoClient(host_gf, port_gf)[min_gf]
dbClient_gf.authenticate("gf", "gf_607")
dbClient_mc = pymongo.MongoClient(host_mc, port_mc)[min_mc]
dbClient_mc.authenticate("mc", "mc_607")

allcoll_names_jlz = dbClient_jlz.collection_names()
allcoll_names_gf = dbClient_gf.collection_names()
allcoll_names_mc = dbClient_mc.collection_names()
allcoll_names = allcoll_names_jlz + allcoll_names_gf + allcoll_names_mc
allcoll_names = sorted(set(allcoll_names))

tradingDayPath = 'C:\\Users\\Sirius\\Desktop\\Dataprocess\\TL_tradingDay.xlsx'
tradeDayList = readTradeDayList(tradingDayPath,'20100101','20181231') 
sday = tradeDayList.index('20180810')
# eday = tradeDayList.index('20180810')
tradingDayList = tradeDayList[sday:]
#===========================================================
# 进行数据处理
min_new = 'vnpy_min_clean_20171107'
option_new = 'vnpy_min_clean_option_20171107'
priceMax = 1e7
start_time = time.time()

tradingDay_ind = tradingDayList.index('20181024')
tradingDayList = tradingDayList[tradingDay_ind+1:]

for tradingDay in tradingDayList:
    # 找到未过期合约的名字
    yearMonth = tradingDay[2:-2]
            
    futureName = [c for c in allcoll_names if len(c) <= 6]
    futureName = [c for c in futureName if changeZSNameAddyear(c)[-4:] >= yearMonth]
    
    optionZS = [c for c in allcoll_names if len(c) > 6 and '-' not in c] # ZS 期权合约
    optionDS = [c for c in allcoll_names if '-' in c]                    # DS 期权合约
    
    optionZS = [c for c in optionZS if changeZSNameAddyear(c)[2:] >= yearMonth]
    optionDS = [c for c in optionDS if c.split('-')[0][-4:] >= yearMonth]
    optionName = optionZS + optionDS
   
    flt={'TradingDay':tradingDay}
    for contract in  futureName:
        processMinDay(contract,tradingDay,min_new)
    
    for contract in  optionName: 
        processMinDay(contract,tradingDay,option_new)

print ('插入合约完毕，插入数据库耗时：%s' % (time.time() - start_time))   

'''
# 从某一天没处理完的开始处理
tradingDay = '20181024'
yearMonth = tradingDay[2:-2]
        
futureName = [c for c in allcoll_names if len(c) <= 6]
futureName = [c for c in futureName if changeZSNameAddyear(c)[-4:] >= yearMonth]

optionZS = [c for c in allcoll_names if len(c) > 6 and '-' not in c] # ZS 期权合约
optionDS = [c for c in allcoll_names if '-' in c]                    # DS 期权合约

optionZS = [c for c in optionZS if changeZSNameAddyear(c)[2:] >= yearMonth]
optionDS = [c for c in optionDS if c.split('-')[0][-4:] >= yearMonth]
optionName = optionZS + optionDS

flt={'TradingDay':tradingDay}

ind_tmp = futureName.index('TF1906')
futureName = futureName[ind_tmp+1:]

for contract in  futureName:
    processMinDay(contract,tradingDay,min_new)

for contract in  optionName: 
    processMinDay(contract,tradingDay,option_new)
'''
