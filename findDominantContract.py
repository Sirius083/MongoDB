# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 19:17:14 2018

@author: Sirius

从数据库中读取，找到主力合约(按照今天的平均成交量，选择最大的一个)
"""

import re
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta

# 对 vnpy 收到的郑商所数据，CF801--> CF1801
def changeZSNameAddyear(contract):
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY','AP']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] # 合约名称
    if symbol in ZS:
        y3 = str(datetime.now().year)[-2] # 当前year的第三位
        contract = contract[:-3] + y3 + contract[-3:]
    return contract

today = datetime.strftime(datetime.now().date(), "%Y%m%d")

dbClient = pymongo.MongoClient('localhost', 27017)
db = dbClient['VnTrader_1MIN_ALL_1106']
CONTRACT_LIST = db.collection_names()
CONTRACT_LIST = [c for c in CONTRACT_LIST if len(c) <=6 ]       # 不要期权
CONTRACT_LIST = [changeZSNameAddyear(c) for c in CONTRACT_LIST] # 郑商所改名字
CONTRACT_LIST = [changeZSNameAddyear(c) for c in CONTRACT_LIST if int(c[-4:]) > int(today[2:-2])] # 未交割合约

mydict = {}  # 存储合约名和对应最新交易量
for contract in CONTRACT_LIST:
    coll = db[contract]
    flt = {'TradingDay':{'$gte':today}}
    livedata = coll.find(flt).sort('datetime',pymongo.ASCENDING)
    df = pd.DataFrame(list(livedata))
    if len(df) == 0:
        continue
    df = df[['volume']]
    mydict[contract] = np.mean(df.values) # 平均分钟成交量
    
SYMBOLS = [re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] for contract in CONTRACT_LIST]
SYMBOLS = list(set(SYMBOLS))

dcontract = {}
for key,item in mydict.items():
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", key).groups()[0]
    if symbol in dcontract.keys():
       if item > dcontract[symbol][1]:
           dcontract[symbol] = [key, item]
    else:
        dcontract[symbol] = [key, item]


dominent = {}
for key,item in dcontract.items():
    dominent[key] = item[0] 

# 打印主力合约
for key,item in dominent.items():
    print(key,item)
    
    