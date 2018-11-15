# -*- coding: utf-8 -*-
'''
判断当前的bar在不在交易时间里
# 20180417 15:00 等结束交易时间删除
'''

def checkInTradeTime(contractTime, now):
	'''
	判断当前时间在不在交易时间段内
	'''
	nowTime = str(now.time())
	for i in range(len(contractTime)):
		if nowTime >= contractTime[i][0] and nowTime < contractTime[i][1]:
			return True
	return False


def getTradeTime(contract):
	'''
	获取每个合约的交易时间列表
	'''
	if contract in ['rb','ru','hc','bu']: #23:00
		contractTime = [["09:00:00","10:14:00"],["10:30:00","11:29:00"],["13:30:00","14:59:00"],["21:00:00","22:59:00"]]

	elif contract in ['ag','au','sc']: #2:30
		contractTime = [["09:00:00","10:14:00"],["10:30:00","11:29:00"],["13:30:00","14:59:00"],["21:00:00","24:00:00"],["00:00:00","02:29:00"]]

	elif contract in ['al','ni','cu','pb','zn','sn']: #1:00
		contractTime = [["09:00:00","10:14:00"],["10:30:00","11:29:00"],["13:30:00","14:59:00"],["21:00:00","24:00:00"],["00:00:00","00:59:00"]]

	elif contract in ['wr','fu','c','cs','jd','bb','fb','l','v','pp','WH','PM','RI','LR','RS','SF','SM','AP','JR']: #只商品期货日盘
		contractTime = [["09:00:00","10:14:00"],["10:30:00","11:29:00"],["13:30:00","14:59:00"]]

	elif contract in ['SR','CF','CY','ZC','FG','TA','MA','OI','RM','m','y','a','b','p','j','jm','i']:#23:30
		contractTime = [["09:00:00","10:14:00"],["10:30:00","11:29:00"],["13:30:00","14:59:00"],["21:00:00","23:29:00"]]

	elif contract in ['IF','IC','IH']:
		contractTime = [["09:30:00","11:29:00"],["13:00:00","14:59:00"]]

	elif contract in ['TF','T','TS']: #
		contractTime = [["09:15:00","11:29:00"],["13:00:00","15:14:00"]]

	return contractTime

'''
if __name__ == '__main__':
    import datetime
    contractTime = getTradeTime('cs')
    time = datetime.datetime.now()+datetime.timedelta(hours=5)
    print time
    print checkInTradeTime(contractTime,time)
'''
