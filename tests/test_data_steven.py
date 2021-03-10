# coding=utf-8
# 

import unittest2
from repo_data.constants import Constants
from repo_data.data import initializeDatastore, clearRepoData, getRepo \
									, getRepoTransactionHistory \
									, getUserTranIdsFromRepoName
from repo_data.repo_datastore import saveRepoMasterFileToDB, saveRepoTradeFileToDB \
									, saveRepoRerateFileToDB
from steven_utils.utility import mergeDict
from toolz.functoolz import compose
from functools import partial
from os.path import join, dirname, abspath


getCurrentDir = lambda: dirname(abspath(__file__))


class TestDatastore(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestDatastore, self).__init__(*args, **kwargs)
		initializeDatastore('uat')



	def testAddRepoMaster(self):
		clearRepoData()
		inputFile = join(getCurrentDir(), 'samples', 'RepoMaster_20210218_20210218121101.xml')
		self.assertEqual(2, saveRepoMasterFileToDB(inputFile))
		self.assertEqual(0, saveRepoMasterFileToDB(inputFile)) # duplicate
		


	def testAddRepoTransaction(self):
		"""
		repo master is in testAddRepoMaster()

		Add 2 trades, cancel 1
		"""
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_20210218_20210218121202.xml')
		self.assertEqual(3, saveRepoTradeFileToDB(inputFile))
		
		data = getRepo()	# open or closed repo
		self.assertEqual(1, len(data))
		repoName, userTranId = data[0]['RepoName'], data[0]['TransactionId']
		self.assertEqual([userTranId], getUserTranIdsFromRepoName(repoName))

		data = getRepo(status='canceled')
		self.assertEqual(1, len(data))
		self.assertEqual('310952', data[0]['TransactionId'])

		# add the file again, only the cancel record is saved.
		self.assertEqual(1, saveRepoTradeFileToDB(inputFile))

		history = getRepoTransactionHistory('310955')
		self.assertEqual(1, len(history))
		self.assertEqual('open', history[0]['Action'])
		self.assertEqual(0.65, history[0]['InterestRate'])

		# open, cancel, cancel
		history = getRepoTransactionHistory('310952')
		self.assertEqual(3, len(history))
		self.assertEqual('open', history[0]['Action'])
		self.assertEqual(0.75, history[0]['InterestRate'])
		self.assertEqual('2021-01-25', history[0]['Date'])

		self.assertEqual('cancel', history[1]['Action'])
		self.assertEqual('cancel', history[2]['Action'])



	def testAll(self):
		"""
		Add 7 repo positions, 3 of type OPEN, 4 of type fixed term.
		Then close 1, cancel 1, both of type OPEN.
		"""
		clearRepoData()
		self.assertEqual(7, saveRepoMasterFileToDB(
			join(getCurrentDir(), 'samples', 'RepoMaster_20210305_20210305154653.xml')))

		self.assertEqual(9, saveRepoTradeFileToDB(
			join(getCurrentDir(), 'samples', 'RepoTrade_20210305_20210305155621.xml')))

		data = getRepo()
		self.assertEqual(6, len(data))	# 6 not canceled
		self.verifyHKDRepoPosition(data)
		self.verifyUSDRepoPosition(data)
		self.verifyClosedRepoPosition(data)

		# there is one repo canceled
		data = getRepo(status='canceled')
		self.assertEqual(1, len(data))
		self.verifyCanceledRepoPosition(data)

		# Add 2 rerate actions to a repo position 316444
		self.assertEqual(2, saveRepoRerateFileToDB(
			join(getCurrentDir(), 'samples', 'Repo_ReRate_20210305_20210305174826.xml')))

		# add the same file again, 2 more rerate actions added
		self.assertEqual(2, saveRepoRerateFileToDB(
			join(getCurrentDir(), 'samples', 'Repo_ReRate_20210305_20210305174826.xml')))
	
		self.assertEqual(4, len(list(filter( lambda el: el['Action'] == 'rerate'
										   , getRepoTransactionHistory('316444')))))



	def verifyHKDRepoPosition(self, data):
		L = list(filter(lambda p: p['Currency'] == 'HKD', data))
		self.assertEqual(2, len(L))	# one open, one close

		L = list(filter(lambda p: p['Status'] == 'open', L))
		self.assertEqual(1, len(L))
		
		position = L[0]
		self.assertEqual('316444', position['TransactionId'])
		self.assertEqual('RR', position['Type'])
		self.assertEqual('TEST_R', position['Portfolio'])
		self.assertEqual('BOCHK', position['Custodian'])
		self.assertEqual('ISIN', position['CollateralIDType'])
		self.assertEqual('HK0000163607', position['CollateralID'])
		self.assertEqual('2021-01-04', position['TradeDate'])
		self.assertEqual('2021-01-08', position['SettleDate'])
		self.assertEqual(True, position['IsOpenRepo'])
		self.assertEqual('', position['MaturityDate'])
		self.assertEqual(150000, position['Quantity'])
		self.assertEqual('HKD', position['Currency'])
		self.assertAlmostEqual(76.590277, position['Price'], 6)
		self.assertEqual(100000, position['CollateralValue'])
		self.assertEqual('MMRPEA24TS', position['RepoName'])
		self.assertEqual('BB', position['Broker'])
		self.assertEqual(0, position['Haircut'])
		self.assertEqual('open', position['Status'])
		self.assertEqual('ACT/360', position['DayCount'])



	def verifyUSDRepoPosition(self, data):
		L = list(filter(lambda p: p['Currency'] == 'USD', data))
		self.assertEqual(4, len(L))

		L = list(filter(lambda p: p['MaturityDate'] == '2021-04-05', data))
		self.assertEqual(1, len(L))
		
		position = L[0]
		self.assertEqual('316456', position['TransactionId'])
		self.assertEqual('RR', position['Type'])
		self.assertEqual('TEST_R', position['Portfolio'])
		self.assertEqual('BOCHK', position['Custodian'])
		self.assertEqual('ISIN', position['CollateralIDType'])
		self.assertEqual('XS2178949561', position['CollateralID'])
		self.assertEqual('2021-02-01', position['TradeDate'])
		self.assertEqual('2021-02-03', position['SettleDate'])
		self.assertEqual(False, position['IsOpenRepo'])
		self.assertEqual('2021-04-05', position['MaturityDate'])
		self.assertEqual(200000, position['Quantity'])
		self.assertEqual('USD', position['Currency'])
		self.assertAlmostEqual(92.76, position['Price'], 6)
		self.assertEqual(150000, position['CollateralValue'])
		self.assertEqual('MMRPEB2418', position['RepoName'])
		self.assertEqual('HSBC-REPO', position['Broker'])
		self.assertEqual(0, position['Haircut'])
		self.assertEqual('open', position['Status'])
		self.assertEqual('ACT/365', position['DayCount'])



	def verifyClosedRepoPosition(self, data):
		L = list(filter(lambda p: p['Status'] == 'closed', data))
		self.assertEqual(1, len(L))
		position = L[0]

		self.assertEqual('316447', position['TransactionId'])
		self.assertEqual('RR', position['Type'])
		self.assertEqual('TEST_R', position['Portfolio'])
		self.assertEqual('BOCHK', position['Custodian'])
		self.assertEqual('ISIN', position['CollateralIDType'])
		self.assertEqual('HK0000163607', position['CollateralID'])
		self.assertEqual('2021-01-04', position['TradeDate'])
		self.assertEqual('2021-01-06', position['SettleDate'])
		self.assertEqual(True, position['IsOpenRepo'])
		self.assertEqual('', position['MaturityDate'])
		self.assertEqual(200000, position['Quantity'])
		self.assertEqual('HKD', position['Currency'])
		self.assertAlmostEqual(86.42707494, position['Price'], 6)
		self.assertEqual(150000, position['CollateralValue'])
		self.assertEqual('MMRPED25DS', position['RepoName'])
		self.assertEqual('BB', position['Broker'])
		self.assertEqual(0, position['Haircut'])
		self.assertEqual('closed', position['Status'])	# closed explicitly
		self.assertEqual('ACT/360', position['DayCount'])



	def verifyCanceledRepoPosition(self, data):
		position = data[0]

		self.assertEqual('316450', position['TransactionId'])
		self.assertEqual('RR', position['Type'])
		self.assertEqual('TEST_R', position['Portfolio'])
		self.assertEqual('BOCHK', position['Custodian'])
		self.assertEqual('ISIN', position['CollateralIDType'])
		self.assertEqual('HK0000163607', position['CollateralID'])
		self.assertEqual('2021-01-04', position['TradeDate'])
		self.assertEqual('2021-01-07', position['SettleDate'])
		self.assertEqual(True, position['IsOpenRepo'])
		self.assertEqual('', position['MaturityDate'])
		self.assertEqual(250000, position['Quantity'])
		self.assertEqual('HKD', position['Currency'])
		self.assertAlmostEqual(92.29298952, position['Price'], 6)
		self.assertEqual(200000, position['CollateralValue'])
		self.assertEqual('MMRPE425FC', position['RepoName'])
		self.assertEqual('BB', position['Broker'])
		self.assertEqual(0, position['Haircut'])
		self.assertEqual('canceled', position['Status'])
		self.assertEqual('ACT/360', position['DayCount'])