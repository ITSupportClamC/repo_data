# coding=utf-8
# 

import unittest2
from repo_data.constants import Constants
from repo_data.data import initializeDatastore, clearRepoData, getRepo
from repo_data.repo_datastore import saveRepoMasterFileToDB, saveRepoTradeFileToDB
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
