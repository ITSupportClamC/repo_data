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

		# all open repos, use a very early date to avoid any maturity
		data = getRepo('2001-01-01')
		self.assertEqual(5, len(data))
		self.verifyHKDRepoPosition(data)

		# there is one matured on 03-05
		self.assertEqual(4, len(getRepo('2021-03-05')))

		# there is one manually closed repo
		self.assertEqual(1, len(getRepo('2001-01-01', status='closed')))

		# one 2021-03-05, there is one manually closed repo and one 
		# matured repo
		self.assertEqual(2, len(getRepo('2021-03-05', status='closed')))

		# there is one repo canceled
		self.assertEqual(1, len(getRepo('2001-03-05', status='canceled')))



	def verifyHKDRepoPosition(self, data):
		position = list(filter(lambda p: p['Currency'] == 'HKD', data))[0]
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