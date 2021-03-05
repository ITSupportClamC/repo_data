# coding=utf-8
#
# Read information from repo XML files and save them to datastore.
# 
# Since this modules uses the datastore functions such as addRepoMaster,
# make sure set the database mode (production or test) before calling 
# any of them.
# 

from aim_xml.repo_data import getRawDataFromXML, getRepoTradeFromFile \
							, getRepoRerateFromFile
from repo_data.data import addRepoMaster, addRepoTransaction \
						, cancelRepoTransaction, closeRepoTransaction \
						, rerateRepoTransaction, initializeDatastore \
						, clearRepoData, getRepo
from repo_data.utils.error_handling import RepoMasterAlreadyExistError \
						, RepoTransactionAlreadyExistError
from toolz.functoolz import compose
from functools import partial
import logging
logger = logging.getLogger(__name__)



"""
	[List] keys to be kept, [Dictionary] d
		=> [Dictionary] d
"""
keepKeys = lambda keys, d: \
	{k: d[k] for k in set(keys).intersection(set(d))}



def saveRepoMasterFileToDB(file):
	"""
	[String] repo master file (without Geneva header) 
		=> [Int] no. of master entries saved into datastore
	"""
	logger.debug('saveRepoMasterFileToDB(): {0}'.format(file))
	def addRepo(masterInfo):
		"""
		[Dictionary] repo master info => [Int] result
		return 1 if the master info is added to DB, 0 otherwise
		"""
		try:
			logger.debug('saveRepoMasterFileToDB(): add repo master code {0}'.format(
							masterInfo.get('Code', '')))
			addRepoMaster(keepKeys(
				['Code', 'BifurcationCurrency', 'AccrualDaysPerMonth', 'AccrualDaysPerYear']
			  , masterInfo))
			return 1

		except RepoMasterAlreadyExistError: 
			logger.warning('saveRepoMasterFileToDB(): repo master code {0} already exists'.format(
							masterInfo.get('Code', '')))
			return 0

		else:
			logger.exception('saveRepoMasterFileToDB()')
			return 0


	return \
	compose(
		sum
	  , partial(map, addRepo)
	  , getRawDataFromXML
	)(file)



def saveRepoTradeFileToDB(file):
	"""
	[String] repo trade file (without Geneva header) 
		=> [Int] no. of trades saved into datastore
	"""
	logger.debug('saveRepoTradeFileToDB(): {0}'.format(file))

	isRepoOpenTrade = lambda t: \
		t['TransactionType'] in ('ReverseRepo_InsertUpdate', 'Repo_InsertUpdate') \
		and 'LoanAmount' in t

	isRepoCloseTrade = lambda t: \
		t['TransactionType'] in ('ReverseRepo_InsertUpdate', 'Repo_InsertUpdate') \
		and not 'LoanAmount' in t

	isRepoCancelTrade = lambda t: \
		t['TransactionType'] in ('ReverseRepo_Delete', 'Repo_Delete')

	def addRepo(trade):
		try:
			logger.debug('saveRepoTradeFileToDB(): add repo transaction id {0}'.format(
						trade.get('UserTranId1', '')))

			if isRepoOpenTrade(trade):
				addRepoTransaction(keepKeys( 
				  [ 'TransactionType', 'UserTranId1', 'Portfolio'
				  , 'LocationAccount', 'Investment', 'EventDate'
				  , 'SettleDate', 'OpenEnded', 'ActualSettleDate'
				  , 'Quantity', 'CounterInvestment', 'Price'
				  , 'NetCounterAmount', 'RepoName', 'Coupon'
				  , 'LoanAmount', 'Broker'
				  ]
				, trade))
				return 1

			elif isRepoCloseTrade(trade):
				closeRepoTransaction(keepKeys(['UserTranId1', 'ActualSettleDate'], trade))
				return 1

			elif isRepoCancelTrade(trade):
				cancelRepoTransaction(keepKeys(['UserTranId1'], trade))
				return 1

			else:
				logger.error('saveRepoTradeFileToDB(): invalid trade type'.format(
							trade.get('TransactionType', '')))
				return 0

		except RepoTransactionAlreadyExistError:
			logger.warning('saveRepoTradeFileToDB(): repo transaction {0} already exists'.format(
							trade.get('UserTranId1', '')))
			return 0

		except:
			logger.exception('saveRepoTradeFileToDB():')
			return 0
	# end of addRepo

	return \
	compose(
		sum
	  , partial(map, addRepo)
	  , getRepoTradeFromFile
	)(file)



def saveRepoRerateFileToDB(file):
	"""
	[String] repo rerate file (without Geneva header) 
		=> [Int] no. of rerate actions saved into datastore
	"""
	logger.debug('saveRepoRerateFileToDB(): {0}'.format(file))

	def rerateEntry(el):
		"""
		[Dictionary] el => [Dictionary] rerate entry
		"""
		if el['Loan'].startswith('UserTranId1='):
			return { 'UserTranId1': el['Loan'][12:]
				   , 'RateDate': el['RateTable']['RateDate']
				   , 'Rate': el['RateTable']['Rate']
				   }
		else:
			logger.error('saveRepoRerateFileToDB(): failed to find UserTranId1: {0}'.format(
						el['Loan']))
			raise ValueError


	def addRepo(el):
		try:
			logger.debug('saveRepoRerateFileToDB(): add repo rerate id {0}'.format(
						el['UserTranId1']))
			print(el)
			rerateRepoTransaction(el)
			return 1

		except:
			logger.exception('saveRepoRerateFileToDB()')
			return 0


	return \
	compose(
		sum
	  , partial(map, addRepo)
	  , partial(map, rerateEntry)
	  , getRepoRerateFromFile
	)(file)