# coding=utf-8
# 
import logging
from repo_data.constants import Constants
from repo_data.controllers import AppController

#controller supposed to be stateless so it is safe to have a singleton , one for all users 
controller = AppController()

def initializeDatastore(mode):
	"""
	[String] mode ('production' means produciton mode, test mode otherwise)

	side effect: initialize the underlying data store in test or production
	mode.
	"""
	return controller.initializeDatastore(mode)
	

def clearRepoData():
	"""
	Clears all the data in the repo master, repo transaction, and repo
	transaction history.

	Throws NoDataClearingInProuctionMode if the underlying datastore is
	in production mode.
	"""
	return controller.clearRepoData()



def getRepo( date, status='open', portfolio='all', custodian='all', repoName='all'
		   , broker='all', hasHairCut='all'):
	"""
	[String] date (yyyy-mm-dd) => [Iterable] repo transactions
	"""
	return controller.getRepo(date, status, portfolio, custodian, repoName, broker, hasHairCut)



def getRepoTransactionHistory(userTranId):
	"""
	[String] userTranId	=> [Iterable] ([Dictionary] transaction)
	"""
	return[]



def getUserTranIdsFromRepoName(repoName):
	"""
	[String] repo name => [Iterable] ([String] user tran id)
	"""
	return []



def addRepoMaster(master):
	"""
	[Dictionary] master

	Side effect: add the repo master to datastore

	Throws: RepoMasterAlreadyExistError
	"""
	return controller.addRepoMaster(master)



def addRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: add the repo transaction to datastore

	Throws: RepoTransactionAlreadyExistError
			RepoMasterNotExistError
	"""
	return controller.addRepoTransaction(transaction)



def closeRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: close the repo transaction in datastore

	Throws: RepoTransactionNotExistError
			CloseCanceledRepoTransactionError
	"""
	return controller.closeRepoTransaction(transaction)



def cancelRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: cancel the repo transaction in datastore

	Throws: RepoTransactionNotExistError
	"""
	return controller.cancelRepoTransaction(transaction)



def rerateRepoTransaction(transaction):
	"""
	[Dictionary] transaction

	Side effect: update the interest rate of the repo transaction in datastore

	Throws: RepoTransactionNotExistError
	"""
	return controller.rerateRepoTransaction(transaction)
