# coding=utf-8
# 
import logging
logger = logging.getLogger(__name__)



def initializeDatastore(mode):
	"""
	[String] mode ('production' means produciton mode, test mode otherwise)

	side effect: initialize the underlying data store in test or production
	mode.
	"""
	pass



def clearRepoData():
	"""
	Clears all the data in the repo master, repo transaction, and repo
	transaction history.

	Throws NoDataClearingInProuctionMode if the underlying datastore is
	in production mode.
	"""
	pass



def getRepo( date, status='open', portfolio='all', custodian='all', repoName='all'
		   , broker='all', hasHairCut='all'):
	"""
	[String] date (yyyy-mm-dd)
		=> [Iterable] repo transactions
	"""
	return []



def addRepoMaster(master):
	"""
	[Dictionary] master

	Side effect: add the repo master to datastore

	Throws: RepoMasterAlreadyExist
	"""
	pass



def addRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: add the repo transaction to datastore

	Throws: RepoTransactionAlreadyExist
			RepoMasterNotExist
	"""
	pass



def closeRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: close the repo transaction in datastore

	Throws: RepoTransactionNotExist
			CloseCanceledRepoTransaction
	"""
	pass



def cancelRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: cancel the repo transaction in datastore

	Throws: RepoTransactionNotExist
	"""
	pass