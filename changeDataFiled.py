# -*- coding: utf-8 -*-
"""
Created on Thu Mar 08 09:09:58 2018

@author: Sirius
"""

# ZS所20180307晚上的tradingDay计算错误的，需要进行批量修改
import pymongo
import re
from datetime import datetime
import pandas as pd
dbClient = pymongo.MongoClient('localhost', 27017)
LOCAL_MIN_DB = 'VnTrader_1MIN_ALL_1106'
LOCAL_TICK_DB = 'VnTrader_Tick_ALL_1106'

# 判断合约是否属于郑商所
def getContractList(contract):
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0]
    if symbol in ZS:
        return True
    else:
        return False
        
CONTRACT_LIST = dbClient[LOCAL_MIN_DB].collection_names()
CONTRACT_LIST = [c for c in CONTRACT_LIST if getContractList(c)]
CONTRACT_LIST = sorted(CONTRACT_LIST)

# 给定时间内的数据
flt = {'datetime':{'$gte':datetime(2018,3,7,20,58,0),'$lte':datetime(2018,3,7,23,59,0)}}


db_min = dbClient[LOCAL_MIN_DB]
for coll in CONTRACT_LIST:
    coll = db_min[coll]
    coll.update_many(flt, {'$set':{'TradingDay':'20180308'}}) # set: 替换为指定的值
    
db_tick = dbClient[LOCAL_TICK_DB]
for coll in CONTRACT_LIST:
    coll = db_tick[coll]
    coll.update_many(flt, {'$set':{'TradingDay':'20180308'}})
