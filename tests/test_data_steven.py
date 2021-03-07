# coding=utf-8
# 

import unittest2
from repo_data.constants import Constants
from repo_data.data import initializeDatastore, clearRepoData, getRepo
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

		Add 2 trades, then cancel 1
		"""
		inputFile = join(getCurrentDir(), 'samples', 'RepoTrade_20210218_20210218121202.xml')
		self.assertEqual(3, saveRepoTradeFileToDB(inputFile))
		self.assertEqual(1, len(getRepo('2001-01-01')))
		self.assertEqual(1, len(getRepo('2001-01-01', status='canceled')))



	def testAddRepoTransaction2(self):
		"""
		Add 1 trade, then close it.
		"""
		clearRepoData()
		self.assertEqual(1, saveRepoMasterFileToDB(
			join(getCurrentDir(), 'samples', 'RepoMaster_20210216.xml')))

		self.assertEqual(2, saveRepoTradeFileToDB(
			join(getCurrentDir(), 'samples', 'RepoTrade_20210216.xml')))

		self.assertEqual(0, len(getRepo('2001-01-01')))
		self.assertEqual(1, len(getRepo('2001-01-01', status='closed')))



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

		# all repos that are still open as of 2021-02-04
		data = getRepo('2021-02-04')
		self.assertEqual(5, len(data))
		self.verifyHKDRepoPosition(data)
		self.verifyUSDRepoPosition(data)

		# there is one matured on 03-05, 4 still open as of 03-05
		self.assertEqual(4, len(getRepo('2021-03-05')))

		# there is one manually closed repo
		self.assertEqual(1, len(getRepo('2001-01-01', status='closed')))

		# one 2021-03-05, there is one manually closed repo and one 
		# matured repo
		data = getRepo('2021-03-05', status='closed')
		self.assertEqual(2, len(data))
		self.verifyClosedRepoPosition(data)

		# there is one repo canceled
		data = getRepo('2001-03-05', status='canceled')
		self.assertEqual(1, len(data))
		self.verifyCanceledRepoPosition(data)



	def testAll2(self):
		"""
		Add 2 rerate actions to a repo position
		"""
		self.assertEqual(2, saveRepoRerateFileToDB(
			join(getCurrentDir(), 'samples', 'Repo_ReRate_20210305_20210305174826.xml')))
		


		

	def verifyHKDRepoPosition(self, data):
		L = list(filter(lambda p: p['Currency'] == 'HKD', data))
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
		L = list(filter(lambda p: p['MaturityDate'] != '', data))
		self.assertEqual(1, len(L))
		position = L[0]

		self.assertEqual('316472', position['TransactionId'])
		self.assertEqual('RR', position['Type'])
		self.assertEqual('TEST_R', position['Portfolio'])
		self.assertEqual('BOCHK', position['Custodian'])
		self.assertEqual('ISIN', position['CollateralIDType'])
		self.assertEqual('US00131MAJ27', position['CollateralID'])
		self.assertEqual('2021-02-25', position['TradeDate'])
		self.assertEqual('2021-02-26', position['SettleDate'])
		self.assertEqual(False, position['IsOpenRepo'])
		self.assertEqual('2021-03-05', position['MaturityDate'])
		self.assertEqual(300000, position['Quantity'])
		self.assertEqual('USD', position['Currency'])
		self.assertAlmostEqual(91.28946759, position['Price'], 6)
		self.assertEqual(250000, position['CollateralValue'])
		self.assertEqual('MMRPE825GK', position['RepoName'])
		self.assertEqual('BNP-REPO', position['Broker'])
		self.assertEqual(0, position['Haircut'])
		self.assertEqual('open', position['Status'])	# closed due to maturity
		self.assertEqual('ACT/365', position['DayCount'])

		L = list(filter(lambda p: p['MaturityDate'] == '', data))
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