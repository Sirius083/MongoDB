# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 18:44:08 2018

@author: Sirius
"""

# 国内法定节假日(含周末)
holidays = ['20181230','20190101',\
'20190204','20190205','20190206','20190207','20190208','20190209','20190210',\
'20190405','20190406','20190407',\
'20190501',\
'20190607','20190608','20190609',\
'20190913','20190914','20190915',\
'20191001','20191002','20191003','20191004','20191005','20191006','20191007']


# 2019年所有日子: '20190101'
from dateutil import rrule
from datetime import datetime

a = '20190101'
b = '20191231'
datelist = []
for dt in rrule.rrule(rrule.DAILY,
                      dtstart=datetime.strptime(a, '%Y%m%d'),
                      until=datetime.strptime(b, '%Y%m%d')):
    datelist = datelist + [dt.strftime('%Y%m%d')]
    

# 2019所有日子去掉节假日
days = list(set(datelist) - set(holidays))

# 去掉周六周天
alldays = []
for day in days:
    date = datetime.strptime(day, '%Y%m%d').weekday()
    if date not in [5,6]:
        alldays = alldays + [day]
        
alldays = sorted(alldays)

'''
# 将结果写入文件
txt_path = r'C:\Users\Sirius\Desktop\TL_tradingDay_2019.txt'
with open(txt_path, 'w') as f:
    for item in alldays:
        f.write("%s\n" % item)
'''

        
        
        
