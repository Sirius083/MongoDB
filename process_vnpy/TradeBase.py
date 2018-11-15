# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 15:11:18 2017

@author: Molly
# 交易时间表 

# 2017期货交易时间一览表
# http://baijiahao.baidu.com/s?id=1562101987009149&wfr=spider&for=pc
debug:
增加了节假日日期列表
"""
#==============================================================================
#不同品种的交易时间表

import re
import pandas as pd
import numpy as np
import datetime
import os
import re
import collections

# tradeDayFile_path: 存储 TL_tradingDay 的 xlsx的表格
# startDay:          开始日期（包括），字符串
# endDay:            结束日期（包括），字符串
def readTradeDayList(tradeDayFile_path,startDay,endDay):
    path =  tradeDayFile_path # 存储tradingDay的list 1993-2018年的交易日列表
    df = pd.read_excel(path) # 用read_excel不用read_csv
    # 读取交易日
    openclose = np.array(df.iloc[:,2]) # 是否开盘（1开盘）
    tradingDayInd = np.where(openclose == 1)[0]
    tradingDayStr = np.array(df.iloc[:,0][tradingDayInd])
    
    tradingDay = [datetime.datetime.strptime(d, "%Y-%m-%d") for d in tradingDayStr]
    startDay = datetime.datetime.strptime(startDay, "%Y%m%d")
    endDay = datetime.datetime.strptime(endDay, "%Y%m%d")
    
    # data from TL tradingDay
    tradeDayList = [t for t in tradingDay if startDay <= t <= endDay]
    tradeDayList = sorted(tradeDayList) # 读入日期倒叙
    # print 'len(tradingDay)',len(tradeDayList)
    tradeDayList = [t.strftime("%Y%m%d") for t in tradeDayList] # datetime 转化为 string
    # 这里需要将其进行倒叙排序
    return tradeDayList

# 在holidayList中的日期，本该有夜盘，但是由于法定假期的前一个夜盘不开市，故会缺失一部分数据
# import进来后，成为全局变量
holidayList = ['20171009','20180102','20180222','20180409']

# 检查合约是否在tradeTime列表中
def checkSymbolInList(contract):
    SC = ['zn','ag','al','au','cu','ru','pb','rb','fu','ni','bu','wr','hc','sn','sc']
    DS = ['cs', 'pp', 'bb', 'fb', 'jd', 'a', 'c', 'b', 'jm', 'i', 'j', 'm', 'l', 'p', 'v', 'y']
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY','AP']
    ZJ = ['T','TF','TS','IC','IF','IH','BTCCNY','ZECCNY'] # 中金所，国债，与股指期货
    allsymbol = SC + DS + ZS + ZJ
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] # 合约名称
    if symbol in allsymbol:
        return True
    else:
        return False

# print   checkSymbolInList('rb1801')
# print   checkSymbolInList('jlz1801') 

# 通过合约名称，判断是否需要改变大小写
# symbol : 当前的合约名称
# return : bool值，True: 更改; False:不更改
def upperLower(contract):
    SC = ['zn','ag','al','au','cu','ru','pb','rb','fu','ni','bu','wr','hc','sn','sc']
    DS = ['cs', 'pp', 'bb', 'fb', 'jd', 'a', 'c', 'b', 'jm', 'i', 'j', 'm', 'l', 'p', 'v', 'y']
    # ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY']
    # ZJ = ['T','TF','IC','IF','IH','BTCCNY','ZECCNY'] # 中金所，国债，与股指期货
    chgList = SC + DS
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] # 合约名称
    if symbol.lower() in chgList:
        return True
    else:
        return False
# upperLower('RB1801')
# upperLower('CF801')

# 对 vnpy 收到的郑商所数据，CF801--> CF1801
def changeZSNameAddyear(contract):
    import datetime
    import re
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY','AP']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] # 合约名称
    if symbol in ZS:
        y3 = str(datetime.datetime.now().year)[-2] # 当前year的第三位
        contract = contract[:-3] + y3 + contract[-3:]
    return contract
    

def changeZSNameMinusyear(contract):
    import datetime
    import re
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY','AP']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0] # 合约名称
    if symbol in ZS:
        contract = contract[:-4] + contract[-3:]
    return contract

# print changeZSNameAddyear('rb1801')
# print changeZSNameAddyear('CF801')
# print changeZSNameMinusyear('rb1801')
# print changeZSNameMinusyear('CF1801') 
        
# 判断合约是否属于郑商所
def isinZS(contract):
    ZS = ['CF','RS','OI','RM','SR','PM','WH','RI','MA','FG','ZC','JR','LR','SM','SF','TA','CY','AP']
    symbol = re.match(r"([a-zA-Z]+)([0-9]*)", contract).groups()[0]
    if symbol in ZS:
        return True
    else:
        return False

def TradeTimeFuncTick(symbol):
   tradeTickTimeType = {'stock':[('09:30','11:30'),('13:00','15:00')], 
                        'debt':[('09:15','11:30'),('13:00','15:15')],
                        'day':[('09:00','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '23:00':[('21:00','23:00'),('09:00','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '23:30':[('21:00','23:30'),('09:00','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '01:00':[('21:00','01:00'),('09:00','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '02:30':[('21:00','02:30'),('09:00','10:15'),('10:30','11:30'),('13:30','15:00')]}
   symbolType = {'stock':['IF','IH','IC'],
                 'debt':['T','TF','TS'],
                 'day':["JR","LR","PM","RI","RS","SF","SM","WH","c","cs","jd","l","v","pp",'wr','fu','fb','bb','AP'],
                 '23:00':['rb','ru','hc','bu','hc'],
                 '23:30':['CF','FG','TA','SR','ZC','MA','OI','RM','CY','TC','j','jm','m','y','a','b','p','i'],
                 '01:00':['al','ni','cu','pb','zn','sn'],
                 '02:30':['ag','au','sc']} 
   name = re.match(r"([a-zA-Z]+)([0-9]*)", symbol).groups()[0] # 品种
   for key,values in symbolType.items():     
       if name in values:
          st = key # symbol type
          break
   TradeTime = tradeTickTimeType[st]
   return TradeTime

def TradeTimeFunc(symbol):
   import re
   tradeMinTimeType = {'stock':[('09:30','11:29'),('13:00','14:59')], 
                     'debt':[('09:15','11:29'),('13:00','15:14')],
                     'day':[('09:00','10:14'),('10:30','11:29'),('13:30','14:59')],
                     '23:00':[('21:00','22:59'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')],
                     '23:30':[('21:00','23:29'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')],
                     '01:00':[('21:00','00:59'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')],
                     '02:30':[('21:00','02:29'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')]}
   symbolType = {'stock':['IF','IH','IC'],
                 'debt':['T','TF','TS'],
                 'day':["JR","LR","PM","RI","RS","SF","SM","WH","c","cs","jd","l","v","pp",'wr','fu','fb','bb','AP'],
                 '23:00':['rb','ru','hc','bu','hc'],
                 '23:30':['CF','FG','TA','SR','ZC','MA','OI','RM','CY','TC','j','jm','m','y','a','b','p','i'],
                 '01:00':['al','ni','cu','pb','zn','sn'],
                 '02:30':['ag','au','sc']} 
   name = re.match(r"([a-zA-Z]+)([0-9]*)", symbol).groups()[0] # 品种
   for key,values in symbolType.items():     
       if name in values:
          st = key # symbol type
          break
   TradeTime = tradeMinTimeType[st]
   return TradeTime
   

# tick保留集合竞价时间
# 如果symbol不在里面，print错误信息
def TradeTimeTickAuction(symbol):
    import os
    tradeTickTimeType = {'stock':[('09:29','11:30'),('13:00','15:00')], 
                        'debt':[('09:14','11:30'),('13:00','15:15')],
                        'day':[('08:59','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '23:00':[('20:59','23:00'),('08:59','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '23:30':[('20:59','23:30'),('08:59','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '01:00':[('20:59','01:00'),('08:59','10:15'),('10:30','11:30'),('13:30','15:00')],
                        '02:30':[('20:59','02:30'),('08:59','10:15'),('10:30','11:30'),('13:30','15:00')]}
    symbolType = {'stock':['IF','IH','IC'],
                 'debt':['T','TF','TS'],
                 'day':["JR","LR","PM","RI","RS","SF","SM","WH","c","cs","jd","l","v","pp",'wr','fu','fb','bb','AP'],
                 '23:00':['rb','ru','hc','bu','hc'],
                 '23:30':['CF','FG','TA','SR','ZC','MA','OI','RM','CY','TC','j','jm','m','y','a','b','p','i'],
                 '01:00':['al','ni','cu','pb','zn','sn'],
                 '02:30':['ag','au','sc']} 
    name = re.match(r"([a-zA-Z]+)([0-9]*)", symbol).groups()[0] # 品种
    for key,values in symbolType.items():     
        if name in values:
           st = key
           break
    # else: # 在正常结束时会执行
    #    print u'%s不在 TradeTimeTickAuction 列表中，当前文件是%s'%(symbol,os.getcwd())
    #   return None
    TradeTime = tradeTickTimeType[st]
    return TradeTime
    
    
def TradeMinGapTime(symbol):
   import re
   from collections import OrderedDict 
   tradeMinTimeType = {'stock':OrderedDict([('night',[]),('day',[('09:30', '11:29'),('13:00', '14:59')])]),
                       'debt':OrderedDict([('night',[]),('day',[('09:15', '11:29'),('13:00', '15:14')])]),
                       'day':OrderedDict([('night',[]),('day',[('09:00', '10:14'),('10:30', '11:29'),('13:30', '14:59')])]),
                       'apple':OrderedDict([('night',[]),('day',[('09:00', '11:29'),('13:30', '14:59')])]),
                       '23:00':OrderedDict([('night',[('21:00','22:59')]),('day',[('09:00', '10:14'),('10:30', '11:29'),('13:30', '14:59')])]),
                       '23:30':OrderedDict([('night',[('21:00','23:29')]),('day',[('09:00', '10:14'),('10:30', '11:29'),('13:30', '14:59')])]),
                       '01:00':OrderedDict([('night',[('21:00','23:59'),('00:00','00:59')]),('day',[('09:00', '10:14'),('10:30', '11:29'),('13:30', '14:59')])]),
                       '02:30':OrderedDict([('night',[('21:00','23:59'),('00:00','02:29')]),('day',[('09:00', '10:14'),('10:30', '11:29'),('13:30', '14:59')])])
                      }  
   symbolType = {'stock':['IF','IH','IC'],
                 'debt':['T','TF','TS'],
                 'day':["JR","LR","PM","RI","RS","SF","SM","WH","c","cs","jd","l","v","pp",'wr','fu','fb','bb','AP'],
                 'apple':['AP'],
                 '23:00':['rb','ru','hc','bu','hc'],
                 '23:30':['CF','FG','TA','SR','ZC','MA','OI','RM','CY','TC','j','jm','m','y','a','b','p','i'],
                 '01:00':['al','ni','cu','pb','zn','sn'],
                 '02:30':['ag','au','sc']} 
   name = re.match(r"([a-zA-Z]+)([0-9]*)", symbol).groups()[0] # 品种
   for key,values in symbolType.items():     
       if name in values:
          st = key # symbol type
          break
   TradeTime = tradeMinTimeType[st]
   return TradeTime
   

def tick_Num(curTime,tradeTime):    #给定当前时间（ps:当前时间在交易时间），计算到当前交易日结束还有多长时间，返回理论上的tick数
   import datetime
   tradeDatetime = []
   for i in range(len(tradeTime)):
      tradeDatetime.append([datetime.datetime.strptime(d,"%H:%M") for d in tradeTime[i]])
   if len(tradeTime) == 4:
      if datetime.datetime.strptime(tradeTime[0][1],"%H:%M").hour<8:  #夜盘隔夜
         tradeDatetime[0][0] = tradeDatetime[0][0]+datetime.timedelta(days = -1)
      else:  #有夜盘但是不隔夜
         tradeDatetime[0][0] = tradeDatetime[0][0]+datetime.timedelta(days = -1)
         tradeDatetime[0][1] = tradeDatetime[0][1]+datetime.timedelta(days = -1)
      if curTime.hour > 20:
         cTime = curTime.replace(year = 1899,month = 12,day = 31,second = 0,microsecond = 0) 
      else:
         cTime = curTime.replace(year = 1900,month = 1,day = 1,second = 0,microsecond = 0)
      for i in range(len(tradeTime)):
         if cTime >= tradeDatetime[i][0] and  cTime <= tradeDatetime[i][1]:
            cmin = (tradeDatetime[i][1] - cTime).days*1440 + (tradeDatetime[i][1] - cTime).seconds/60
            for j in range(i+1,len(tradeTime)):
               cmin = cmin + (tradeDatetime[j][1] - tradeDatetime[j][0]).seconds/60
            break
   else:  #没有夜盘
      cTime = curTime.replace(year = 1900,month = 1,day = 1,second = 0,microsecond = 0)
      for i in range(len(tradeTime)):
         if cTime >= tradeDatetime[i][0] and  cTime <= tradeDatetime[i][1]:
            cmin =  (tradeDatetime[i][1] - cTime).seconds/60
            for j in range(i+1,len(tradeTime)):
               cmin = cmin + (tradeDatetime[j][1] - tradeDatetime[j][0]).seconds/60
            break
   ticknum = cmin*120
   return ticknum
   
#------------------------------------------------------------------------------
def Nmin(symbol):  # 计算每个品种或者合约每个交易日的分钟数
  TradeTime = TradeTimeFunc(symbol)
  if TradeTime == [('09:30','11:29'),('13:00','14:59')]:
    Nmin = 240
  elif TradeTime == [('09:15','11:29'),('13:00','15:14')]:
    Nmin = 270
  elif TradeTime == [('09:00','10:14'),('10:30','11:29'),('13:30','14:59')]:
    Nmin = 225
  elif TradeTime == [('21:00','22:59'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')]:
    Nmin = 345
  elif TradeTime == [('21:00','23:29'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')]:
    Nmin = 375
  elif TradeTime == [('21:00','00:59'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')]:
    Nmin = 465
  elif TradeTime == [('21:00','02:29'),('09:00','10:14'),('10:30','11:29'),('13:30','14:59')]:
    Nmin = 555
  return Nmin

      
if __name__ == '__main__':                
   import datetime            
   tradetime = [('09:00','10:15'),('10:30','11:30'),('13:30','15:00')]
   #tradetime = [('21:00','23:00'),('9:00','10:15'),('10:30','11:30'),('13:30','15:00')]
   #tradetime = [('21:00','23:30'),('9:00','10:15'),('10:30','11:30'),('13:30','15:00')]
   #tradetime = [('21:00','1:00'),('9:00','10:15'),('10:30','11:30'),('13:30','15:00')]     
   #tradetime = [('21:00','2:30'),('9:00','10:15'),('10:30','11:30'),('13:30','15:00')]
   curTime = datetime.datetime.strptime('20170912 10:30','%Y%m%d %H:%M')
   ticknum = tick_Num(curTime,tradetime)
   # print ticknum
