# -*- coding: utf-8 -*-
"""
Created on Tue Feb 06 09:11:09 2018

@author: Sirius
# 20180210: 高菲分钟数据库VnTrader_1MIN_ALL_0129补充 20180209:21:19:00 到 20180209 21:56:00数据
"""
# 本地数据库，指定合约，数据库的收到的最后分钟
import re
import os
# os.chdir('C:\\Users\\Sirius\\Desktop\\mongoDB\\dump from another host')
import sys
import pymongo
import pandas as pd
from datetime import datetime
from ALLSETTING import *


# 判断合约是否属于郑商所
def isinSC(contract):
    # DS = ['cs', 'pp', 'bb', 'fb', 'jd', 'a', 'c', 'b', 'jm', 'i', 'j', 'm', 'l', 'p', 'v', 'y']
    # SC = ['zn','ag','al','au','cu','ru','pb','rb','fu','ni','bu','wr','hc','sn']
    ZJ = ['T','TF','IC','IF','IH','BTCCNY','ZECCNY']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0]
    if symbol in ZJ:
        return True
    else:
        return False
#------------------------------------------------------------------------------
# 需要修改的变量
HOST_MIN_DB = 'VnTrader_1MIN_ALL_1106'
HOST_TICK_DB = 'VnTrader_Tick_ALL_0324'
HOST_NAME = ' --host 192.168.1.107:27017'
# HOST_NAME = ' '
#------------------------------------------------------------------------------
# 如果之前存在这个文件 dumpcommond_min.txt, 删除
try:
    os.remove(MONGODB_BIN_PATH + '\\dumpcommond_min.txt')
    os.remove(MONGODB_BIN_PATH + '\\dumpcommond_tick.txt')
except OSError:
    pass
#------------------------------------------------------------------------------ 
dbClient = pymongo.MongoClient('192.168.1.107', 27017)
CONTRACT_LIST = dbClient['VnTrader_1MIN_ALL_1106'].collection_names()
CONTRACT_LIST = [c for c in CONTRACT_LIST if isinSC(c)]   # 用高菲数据库的上期所合约名称
CONTRACT_LIST = [c for c in CONTRACT_LIST if len(c) <= 6] # 去除期权数据

for contract in CONTRACT_LIST:
    dump_start = pd.Series()
    dump_start['datetime'] = datetime.strptime('2018-03-29 12:00:00', "%Y-%m-%d %H:%M:%S") # dump 开始时间
    ds_time = dump_start.T.to_json().split(':')[1][:-1]
    
    dump_end = pd.Series()
    dump_end['datetime'] = datetime.strptime('2018-03-29 15:15:00', "%Y-%m-%d %H:%M:%S")   # dump 结束时间
    de_time = dump_end.T.to_json().split(':')[1][:-1]
    
    
    # 生成分数dump文件
    command_min = 'mongodump' + str(HOST_NAME) + ' --db ' + HOST_MIN_DB + ' --collection ' + contract + ' --out min_remote ' + \
          ' --query "{ datetime: {$gte: new Date(' + ds_time + '), $lt: new Date(' + de_time + ')}}"'
    with open(MONGODB_BIN_PATH + '\\dumpcommond_min.txt','a') as myfile:
         myfile.write(command_min + '\n')
         
    
    # 生成tick的dump文件
    command_tick = 'mongodump' + str(HOST_NAME) + ' --db ' + HOST_TICK_DB + ' --collection ' + contract + ' --out tick_remote ' + \
          ' --query "{ datetime: {$gte: new Date(' + ds_time + '), $lt: new Date(' + de_time + ')}}"'
    with open(MONGODB_BIN_PATH + '\\dumpcommond_tick.txt','a') as myfile:
         myfile.write(command_tick + '\n')
         
'''
#------------------------------------------------------------------------------
# 删除数据库指定时间之后收到的数据
#!!!!!!!! 这一部分千万不要随便执行，检查是否需要删数据库数据
dbClient = pymongo.MongoClient('localhost', 27017)
LOCAL_MIN_DB = 'VnTrader_1MIN_ALL_1106'
LOCAL_TICK_DB = 'VnTrader_Tick_ALL_1106'
db_min = dbClient[LOCAL_MIN_DB]
for coll in CONTRACT_LIST:
    coll = db_min[coll]
    coll.remove({'datetime':{'$gte':datetime(2018,2,9,21,19)}})
     
db_tick = dbClient[LOCAL_TICK_DB]
for coll in CONTRACT_LIST:
    coll = db_tick[coll]
    coll.remove({'datetime':{'$gte':datetime(2018,2,5,21,19)}})
'''
#------------------------------------------------------------------------------
# 从 dump 的文件恢复到本地数据库中
# mongorestore -d VnTrader_1MIN_ALL_1106 min_remote/VnTrader_1MIN_ALL_1106
# mongorestore -d VnTrader_Tick_ALL_1106 tick_remote/VnTrader_Tick_ALL_0324

# -----------------------------------------------------------------------------
# 将新收到的数据导入原来的数据库
'''
mongodump --db VnTrader_1MIN_ALL_0206 --out min_new
mongodump --db VnTrader_Tick_ALL_0206 --out tick_new
'''
#------------------------------------------------------------------------------
# 删除tick_remote 和 min_remote文件夹，否则下次导入忘记删除