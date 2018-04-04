# -*- coding: utf-8 -*-
"""
Created on Fri Mar 02 15:41:34 2018

@author: Sirius

# 每周将这周收到的数据 restore 到 311
# 需要修改的：MONGODB_BIN_PATH，HOST_MIN_DB，HOST_TICK_DB
# line40, line45 起始截止时间
# dump的数据在 min_remote 和 tick_remote中
# restore 的命令的最后两行
"""

# 本地数据库，指定合约，数据库的收到的最后分钟
import re
import os
import sys
import pymongo
import pandas as pd
from datetime import datetime
from ALLSETTING import *

MONGODB_BIN_PATH = 'D:\\MongoDB\\bin'
#------------------------------------------------------------------------------
# 需要修改的变量
HOST_MIN_DB = 'VnTrader_1MIN_ALL_1106'
HOST_TICK_DB = 'VnTrader_Tick_ALL_1106'

#------------------------------------------------------------------------------
# 如果之前存在这个文件 dumpcommond_min.txt, 删除
try:
    os.remove(MONGODB_BIN_PATH + '\\dumpcommond_min.txt')
    os.remove(MONGODB_BIN_PATH + '\\dumpcommond_tick.txt')
except OSError:
    pass
#------------------------------------------------------------------------------ 
dbClient = pymongo.MongoClient('localhost', 27017)
CONTRACT_LIST = dbClient[HOST_MIN_DB].collection_names()

for contract in CONTRACT_LIST:
    dump_start = pd.Series()
    dump_start['datetime'] = datetime.strptime('2018-03-23 21:00:00', "%Y-%m-%d %H:%M:%S") # dump 开始时间
    ds_time = dump_start.T.to_json().split(':')[1][:-1]
    
    dump_end = pd.Series()
    dump_end['datetime'] = datetime.strptime('2018-03-30 15:20:00', "%Y-%m-%d %H:%M:%S")   # dump 结束时间
    de_time = dump_end.T.to_json().split(':')[1][:-1]
    
    
    # 生成分数dump文件
    command_min = 'mongodump' + ' --db ' + HOST_MIN_DB + ' --collection ' + contract + ' --out min_remote ' + \
          ' --query "{ datetime: {$gte: new Date(' + ds_time + '), $lt: new Date(' + de_time + ')}}"'
    with open(MONGODB_BIN_PATH + '\\dumpcommond_min.txt','a') as myfile:
         myfile.write(command_min + '\n')
         
    
    # 生成tick的dump文件
    command_tick = 'mongodump' + ' --db ' + HOST_TICK_DB + ' --collection ' + contract + ' --out tick_remote ' + \
          ' --query "{ datetime: {$gte: new Date(' + ds_time + '), $lt: new Date(' + de_time + ')}}"'
    with open(MONGODB_BIN_PATH + '\\dumpcommond_tick.txt','a') as myfile:
         myfile.write(command_tick + '\n')
         
#------------------------------------------------------------------------------
# 将要数据restore备份到311终端
# mongorestore /host 58.206.97.70:28001 /u jlz /p jlz_607 /authenticationDatabase jlz_1MIN /db jlz_1MIN min_remote/VnTrader_1MIN_ALL_1106
# mongorestore /host 58.206.97.70:28001 /u jlz /p jlz_607 /authenticationDatabase jlz_tick /db jlz_tick tick_remote/VnTrader_Tick_ALL_1106