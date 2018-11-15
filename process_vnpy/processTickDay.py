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
计算买卖比

# 传一周的期货数据，需要10+个小时

20180526: 首先删除重复数据，然后比较。用数据量最大的那个
20180722： 处理TradingDay, 从20180528-20180713
"""

import re
import os
import json
import time
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime
from checkInTradeTimeTick import getTradeTime,checkInTradeTime
import more_itertools as mit


#==============================================================================
#                            计算用到的函数
#==============================================================================
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

def datetime_process(d): 
    # 将 timestamp 转化为 datetime
    for k, v in d.items():
        if k == 'datetime' or k == 'localTime':
            d[k] = datetime.utcfromtimestamp(v/1000)
    return d

def ticktest(df_new):
    # 计算买卖比(LEE)
    side = [np.nan]
    for i in range(1,len(df_new)):
        while  i-1 != 0 and df_new.ix[i,'lastPrice'] == df_new.ix[i-1,'lastPrice']:
            i = i - 1
        else: # while 条件不满足时候执行
            if i-1 == 0: # 如果循环到开头
               side.append(np.nan) # 循环到第一个，仍无法判断方向，赋值为nan
            else:        # 如果目前条件可以判断
                if df_new.ix[i,'lastPrice'] > df_new.ix[i-1,'lastPrice']:
                    side.append('B')
                elif df_new.ix[i,'lastPrice'] < df_new.ix[i-1,'lastPrice']:
                    side.append('S')
    return side

def processTickDay(contract,dbName):
    # 重新认证，以防连接超时
    print('process ', tradingDay, ' ', contract)

    dbClient_jlz = pymongo.MongoClient(host_jlz, port_jlz)[tick_jlz]
    dbClient_jlz.authenticate("jlz", "jlz_607")
    
    dbClient_gf = pymongo.MongoClient(host_gf, port_gf)[tick_gf]
    dbClient_gf.authenticate("gf", "gf_607")
    
    dbClient_mc = pymongo.MongoClient(host_mc, port_mc)[tick_mc]
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
       return
    
    # 价格异常
    data = data[(data['lastPrice'] > 0) & (data['lastPrice']< priceMax)
            & (data['askPrice1'] > 0) & (data['askPrice1']< priceMax)
            & (data['bidPrice1'] > 0) & (data['bidPrice1']< priceMax)]
    
    if len(data) == 0:
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
    
    # 删除重复数据库
    # 买一卖一价格，买一卖一量，最新价只要有不同的就不算重复数据
    alltime = data['datetime']
    dup = np.where(np.diff(alltime) == np.timedelta64(0,'ns'))[0].tolist() # 与前一条时间相同的
    dup = np.array(dup) + 1
    dup = [list(group) for group in mit.consecutive_groups(dup)]
    dup = [[l[0]-1] + l for l in dup]  # 将有datetime
    
    for dupind in dup:
        # 1. 按照acvolume 排序 2. acvolume 相同的按照 localTime 排序
        tmp = data[['askVolume1','bidVolume1','askPrice1','bidPrice1']].loc[dupind,:].duplicated()
        ind = tmp[tmp == True].index
        if len(ind) != 0:
           data = data.drop(ind)
    
    data = data.sort_values(by = ['datetime', 'acvolume', 'localTime'], ascending = True)
    data = data.reset_index(drop = True)
    
    volume = np.diff(np.array(data.acvolume))
    volume = np.insert(volume, 0, data.acvolume.iloc[0])
    data.volume = volume
    
    data['side'] = ticktest(data)

    # 将数据传入新的数据库中
    dbnew = dbClient_new[dbName]
    # dbnew.authenticate("jlz", "jlz_607")
    collnew = dbnew[contract]
    
    records = []
    for index, row in data.iterrows(): 
        row = row.T.to_json()
        records.append(json.loads(row,object_hook = datetime_process))
    collnew.insert(records)
    print('process tradingDay %s, %s, total len %s'% (tradingDay, contract, len(data)))

    
#==============================================================================
#                           常量定义
#============================================================================== 
# tradingDay = '20180418'
# flt={'TradingDay':tradingDay}
priceMax = 1e7

# 清理好的数据库存放的位置
dbClient_new = pymongo.MongoClient('localhost', 27017)
tick_new =    'VnTrader_Tick_Future_0528_0713'
option_new =  'VnTrader_Tick_Option_0528_0713'

# jlz 数据库
host_jlz = '58.206.97.70'
port_jlz = 28001
tick_jlz = 'jlz_tick'

# gf 数据库
host_gf = '58.206.97.70'
port_gf = 28001
tick_gf = 'gf_tick_0810' # gf数据库20180810以后的数据传入gf_tick_0810和gf_1MIN_0810

# mc 数据库
host_mc = '58.206.97.70'
port_mc = 28001
tick_mc = 'mc_tick'

dbClient_jlz = pymongo.MongoClient(host_jlz, port_jlz)[tick_jlz]
dbClient_jlz.authenticate("jlz", "jlz_607")

dbClient_gf = pymongo.MongoClient(host_gf, port_gf)[tick_gf]
dbClient_gf.authenticate("gf", "gf_607")

dbClient_mc = pymongo.MongoClient(host_mc, port_mc)[tick_mc]
dbClient_mc.authenticate("mc", "mc_607")
#==============================================================================
# 找到所有数据库的名字
allcoll_names_jlz = dbClient_jlz.collection_names()
allcoll_names_gf = dbClient_gf.collection_names()
allcoll_names_mc = dbClient_mc.collection_names()

allcoll_names = allcoll_names_jlz + allcoll_names_gf + allcoll_names_mc
allcoll_names = sorted(set(allcoll_names))


#==============================================================================
# for tradingDay in ['20180528','20180529','20180530','20180531','20180601']:
# for tradingDay in ['20180604','20180605','20180606','20180607','20180608']:
# for tradingDay in ['20180611','20180612','20180613','20180614','20180615']:
# for tradingDay in ['20180619','20180620','20180621','20180622']:
# for tradingDay in ['20180625','20180626','20180627','20180628','20180629']:
# for tradingDay in ['20180702','20180703','20180704','20180705','20180706']:    
# for tradingDay in ['20180709','20180710','20180711','20180712','20180713']:
# for tradingDay in ['20180716','20180717','20180718','20180719','20180720']:
# for tradingDay in ['20180723','20180724','20180725','20180726','20180727']:
# for tradingDay in ['20180730','20180731','20180801','20180802','20180803']:
# for tradingDay in ['20180806','20180807','20180808','20180809','20180810']:  
# for tradingDay in ['20180813','20180814','20180815','20180816','20180817']:
# for tradingDay in ['20180820','20180821','20180822','20180823','20180824']:
# for tradingDay in ['20180827','20180828','20180829','20180830','20180831']:
# for tradingDay in ['20180903','20180904','20180905','20180906','20180907']:
# for tradingDay in ['20180910','20180911','20180912','20180913','20180914']:
# for tradingDay in ['20180917','20180918','20180919','20180920','20180921']:
# for tradingDay in ['20180925','20180926','20180927','20180928']:
# for tradingDay in ['20181008','20181009','20181010','20181011','20181012']:
# for tradingDay in ['20181015','20181016','20181017','20181018','20181019']:
# for tradingDay in ['20181022','20181023','20181024']:
for tradingDay in ['20181025','20181026','20181029','20181030','20181031','20181101','20181102']:
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
        processTickDay(contract,tick_new)
    
    for contract in  optionName: 
        processTickDay(contract,option_new)
  
# print ('插入合约完毕，插入数据库耗时：%s' % (time.time() - start_time))

'''
tradingDay = '20181024'
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

ind_tmp = futureName.index('TS1812')
futureName = futureName[ind_tmp:]

for contract in  futureName:
    processTickDay(contract,tick_new)
for contract in  optionName: 
    processTickDay(contract,option_new)
'''

'''
# 进程意外中断，查找到哪一个合约
tradingDay = '20180529'
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

# 首先检车期货合约
dbnew = dbClient_new['VnTrader_Tick_Future_0528']
# 检查最后一个期货合约
for contract in futureName:
    coll = dbnew[contract]
    cursor = coll.find(flt).sort('datetime',pymongo.ASCENDING)
    d = pd.DataFrame(list(cursor))
    print(contract, len(d))

# 再检查期权合约
dbnew = dbClient_new['VnTrader_Tick_Option_0528']
for contract in optionName:
    coll = dbnew[contract]
    cursor = coll.find(flt).sort('datetime',pymongo.ASCENDING)
    d = pd.DataFrame(list(cursor))
    print(contract, len(d))

'''    
'''
# 比较两个 dataframe 是否相等
# 20180418 ag1807 len(jlz) = 6010 len(gf) = 6032
data1 # jlz 
data2 # gf
data = data1.append(data2)
data = data.sort_values(by = 'datetime', ascending = True)
data = data.reset_index(drop = True)
ne = (data1 != data2).any(1)
merged = d1.merge(d2, indicator=True, how='outer')
merged[merged['_merge'] == 'right_only']
tmp = data1.sort_index().sort_index(axis=1) == data2.sort_index().sort_index(axis=1)
'''

# 删除某天的合约
# coll = db_min[coll]
# coll.remove({'':{'$gte':datetime(2018,5,4,20,59)}})
