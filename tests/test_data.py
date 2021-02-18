# # coding=utf-8
# # 

import logging
import logging.config
from datetime import datetime
from os.path import abspath, dirname, join

import unittest2
from repo_data.constants import Constants
from repo_data.data import (addRepoMaster, 
							addRepoTransaction,
                            cancelRepoTransaction, 
							clearRepoData,
                            closeRepoTransaction, 
							getRepo, 
							initializeDatastore,
							rerateRepoTransaction)
from repo_data.models.repo_master import RepoMaster
from repo_data.models.repo_transaction import RepoTransaction
from repo_data.models.repo_transaction_history import RepoTransactionHistory
from repo_data.utils.database import DBConn
from repo_data.utils.error_handling import (CloseCanceledRepoTransactionError,
                                            NoDataClearingInProuctionModeError,
                                            RepoMasterAlreadyExistError,
                                            RepoTransactionAlreadyExistError,
                                            RepoTransactionNotExistError,
											InvalidRepoTransactionTypeError)
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

getCurrentDirectory = lambda : \
	dirname(abspath(__file__))

class TestRepoDataAPI(unittest2.TestCase):

	unittest_dbmode = Constants.DBMODE_UAT

	def __init__(self, *args, **kwargs):
		super(TestRepoDataAPI, self).__init__(*args, **kwargs)

	def setUp(self):
		#-- when fileConfig is being called, it will remove all the loggers created
		#-- when importing AppControllers.py when importing file repo_data
		#-- added disable_existing_loggers=False to prevent this
		logging.config.fileConfig( join(getCurrentDirectory(), "..", "logging_config.ini"),
									defaults={'date':datetime.now().date().strftime('%Y-%m-%d')},
									disable_existing_loggers=False
								)
		initializeDatastore("uat")
		clearRepoData()

	def testInitializeDatastore(self):
		#-- test run without error throw
		self.assertEqual(0, initializeDatastore("production"))
		#self.assertEqual(0, initializeDatastore("test"))
		self.assertEqual(0, initializeDatastore("uat"))

	def testClearRepoData(self):
		#-- test if NoDataClearingInProuctionModeError raise under production mode
		initializeDatastore("production")
		with self.assertRaises(NoDataClearingInProuctionModeError):
			clearRepoData()
		initializeDatastore("uat")
		clearRepoData()
		session = sessionmaker(bind=DBConn.get_db(self.unittest_dbmode))()
		self.assertEqual(0, session.query(func.count(RepoMaster.id)).scalar())
		self.assertEqual(0, session.query(func.count(RepoTransaction.id)).scalar())
		self.assertEqual(0, session.query(func.count(RepoTransactionHistory.id)).scalar())

	def testAddRepoMaster(self):
		#-- 1. normal creation
		master = self._get_test_repo_master()
		self.assertEqual(addRepoMaster(master), 0)
		#-- 2. test invalid AccrualDaysPerMonth
		master = self._get_test_repo_master()
		master["AccrualDaysPerMonth"] = "ACT"
		with self.assertRaises(ValueError):
			addRepoMaster(master)
		#-- 3. test invalid AccrualDaysPerMonth
		master = self._get_test_repo_master()
		master["AccrualDaysPerYear"] = "ACT"
		with self.assertRaises(ValueError):
			addRepoMaster(master)
		#-- 4. test duplicated insertion. Shall raise RepoMasterAlreadyExistError
		master = self._get_test_repo_master()
		with self.assertRaises(RepoMasterAlreadyExistError):
			addRepoMaster(master)
			
	def testAddRepoTransaction(self):
		master = self._get_test_repo_master()
		addRepoMaster(master)
		#-- 1. this invalid transaction shall raise input validation error (valueError)
		#-- 1.1. incorrect EventDate
		transaction = self._get_test_transaction()
		transaction["EventDate"] = "2018-08-32"
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.2. incorrect SettleDate
		transaction = self._get_test_transaction()
		transaction["SettleDate"] = "2018-08-32"
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.3. incorrect ActualSettleDate
		transaction = self._get_test_transaction()
		transaction["ActualSettleDate"] = "2018-08-32 00:00:00"
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.4. incorrect ActualSettleDate with non CALC value
		transaction = self._get_test_transaction()
		transaction["ActualSettleDate"] = "CALCC"
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.5. incorrect Investment
		transaction = self._get_test_transaction()
		transaction["Investment"] = "ISIN="
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.6. incorrect Investment 2
		transaction = self._get_test_transaction()
		transaction["Investment"] = "=XS1234567890"
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.7. incorrect Investment 3
		transaction = self._get_test_transaction()
		transaction["Investment"] = "XS1234567890"
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 1.8. incorrect transactionType
		transaction = self._get_test_transaction()
		transaction["TransactionType"] = "abc12345"
		with self.assertRaises(InvalidRepoTransactionTypeError):
			addRepoTransaction(transaction)
		#-- 1.9. incorrect OpenEnded
		transaction = self._get_test_transaction()
		transaction["OpenEnded"] = ""
		with self.assertRaises(ValueError):
			addRepoTransaction(transaction)
		#-- 2. good transactions
		#-- 2.1. good transaction 1 - no OpenEnded
		transaction = self._get_test_transaction()
		self.assertEqual(addRepoTransaction(transaction), 0)
		session = sessionmaker(bind=DBConn.get_db(self.unittest_dbmode))()
		transaction_result = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['UserTranId1']) \
								.first()
		self.assertEqual(transaction['UserTranId1'], transaction_result.transaction_id)
		session.commit()
		#-- 2.2. good transaction 2 - Maturity Date will be ""
		#-- condition: is_open_repo is true, maturity_date is CALC
		transaction = self._get_test_transaction()
		transaction["UserTranId1"] = "test100002"
		transaction["OpenEnded"] = "CALC"
		transaction["ActualSettleDate"] = "CALC"
		self.assertEqual(addRepoTransaction(transaction), 0)
		transaction_result = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['UserTranId1']) \
								.first()
		self.assertEqual("", transaction_result.maturity_date)
		session.commit()
		#-- 2.3. good transaction 3 - Maturity Date will be value of ActualSettleDate
		#-- condition: is_open_repo is true, maturity_date is not CALC
		transaction = self._get_test_transaction()
		transaction["UserTranId1"] = "test100003"
		transaction["OpenEnded"] = "CALC"
		self.assertEqual(addRepoTransaction(transaction), 0)
		transaction_result = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['UserTranId1']) \
								.first()
		self.assertEqual("2020-03-10", transaction_result.maturity_date)
		session.commit()
		#-- 2.4. good transaction 4 - Maturity Date will be ERROR
		#-- condition: is_open_repo is false, maturity_date is CALC
		transaction = self._get_test_transaction()
		transaction["UserTranId1"] = "test100004"
		transaction["ActualSettleDate"] = "CALC"
		self.assertEqual(addRepoTransaction(transaction), 0)
		transaction_result = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['UserTranId1']) \
								.first()
		self.assertEqual("ERROR", transaction_result.maturity_date)
		session.commit()
		#-- 2.5. good transaction 5 - Maturity Date will be value of ActualSettleDate
		#-- condition: is_open_repo is false, maturity_date is not CALC
		transaction = self._get_test_transaction()
		transaction["UserTranId1"] = "test100005"
		self.assertEqual(addRepoTransaction(transaction), 0)
		transaction_result = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['UserTranId1']) \
								.first()
		self.assertEqual("2020-03-10", transaction_result.maturity_date)
		session.commit()
		#-- 2.6. good transaction 6 - Maturity Date will be value of ActualSettleDate
		transaction = self._get_test_transaction()
		transaction["UserTranId1"] = "test100006"
		transaction["Portfolio"] = "中文基金1"
		transaction["LocationAccount"] = "中國銀行"
		self.assertEqual(addRepoTransaction(transaction), 0)
		transaction_result = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['UserTranId1']) \
								.first()
		self.assertEqual("中文基金1", transaction_result.portfolio)
		self.assertEqual("中國銀行", transaction_result.custodian)
		session.close()
		#-- 3. duplicated transaction - shall raise RepoMasterAlreadyExistError
		transaction = self._get_test_transaction()
		with self.assertRaises(RepoTransactionAlreadyExistError):
			addRepoTransaction(transaction)

	def testCancelRepoTransaction(self):
		pass

	def testCloseRepoTransaction(self):
		pass

	def testRerateRepoTransaction(self):
		pass

	def testGetRepo(self):
		#-- preprocess: add 2 repo master and 6 transaction
		master1 = self._get_test_repo_master()
		addRepoMaster(master1)
		master2 = {
			"Code" : "MMRPE420BS-2",
			"BifurcationCurrency" : "USD",
			"AccrualDaysPerMonth" : "Actual",
			"AccrualDaysPerYear" : "180"
		}
		addRepoMaster(master2)
		transaction1 = {
			"TransactionType" : "ReverseRepo_InsertUpdate",
			"UserTranId1" : "300734",
			"Portfolio" : "12734",
			"LocationAccount" : "BOCHK",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2020-12-31T00:00:00",
			"SettleDate" : "2020-12-31T00:00:00",
			"OpenEnded" : "CALC",
			"ActualSettleDate" : "CALC",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BNP-REPO"
		}
		addRepoTransaction(transaction1)
		transaction2 = {
			"TransactionType" : "ReverseRepo_InsertUpdate",
			"UserTranId1" : "300735",
			"Portfolio" : "12734",
			"LocationAccount" : "BOCHK",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2020-12-31T00:00:00",
			"SettleDate" : "2020-12-31T00:00:00",
			"OpenEnded" : "CALC",
			"ActualSettleDate" : "2020-03-08T00:00:00",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BOC-REPO"
		}
		addRepoTransaction(transaction2)
		transaction3 = {
			"TransactionType" : "Repo_InsertUpdate",
			"UserTranId1" : "300736",
			"Portfolio" : "12736",
			"LocationAccount" : "BOCHK",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2020-12-31T00:00:00",
			"SettleDate" : "2020-12-31T00:00:00",
			"ActualSettleDate" : "CALC",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS-2",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BNP-REPO"
		}
		addRepoTransaction(transaction3)
		transaction4 = {
			"TransactionType" : "Repo_InsertUpdate",
			"UserTranId1" : "300737",
			"Portfolio" : "中文",
			"LocationAccount" : "中國銀行",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2020-12-31T00:00:00",
			"SettleDate" : "2020-12-31T00:00:00",
			"ActualSettleDate" : "2020-03-08T00:00:00",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS-2",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BOC-REPO"
		}
		addRepoTransaction(transaction4)
		transaction5 = {
			"TransactionType" : "Repo_InsertUpdate",
			"UserTranId1" : "300738",
			"Portfolio" : "12734",
			"LocationAccount" : "BOCHK",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2020-12-31T00:00:00",
			"SettleDate" : "2020-12-31T00:00:00",
			"ActualSettleDate" : "2020-03-10T00:00:00",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS-2",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BOC-REPO"
		}
		addRepoTransaction(transaction5)
		#-- preprocess: create 1 canceled and 1 closed transaction
		cancel_transaction = {
			"UserTranId1" : "300738"
		}
		cancelRepoTransaction(cancel_transaction)
		transaction6 = {
			"TransactionType" : "Repo_InsertUpdate",
			"UserTranId1" : "300739",
			"Portfolio" : "12734",
			"LocationAccount" : "BOCHK",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2020-12-31T00:00:00",
			"SettleDate" : "2020-12-31T00:00:00",
			"ActualSettleDate" : "2020-03-10T00:00:00",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS-2",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BOC-REPO"
		}
		addRepoTransaction(transaction6)
		close_transaction = {
			"UserTranId1" : "300739",
			"ActualSettleDate" : "2018-08-31T00:00:00"
		}
		closeRepoTransaction(close_transaction)
		#-- 1. invalid value
		#-- 1.1 wrong date
		with self.assertRaises(ValueError):
			getRepo(
				date='2020-12-32',
				status='open',
				portfolio='all',
				custodian='all',
				repoName='all',
		   		broker='all',
				hasHairCut='all'
				)
		#-- 1.2 wrong hasHairCut
		with self.assertRaises(ValueError):
			getRepo(
				date='2020-12-32',
				hasHairCut='0'
				)
		#-- 2. normal query status
		#-- 2.1 status: open (default)
		res = getRepo(
				date='2020-03-09'
			)
		with DBConn.get_db(self.unittest_dbmode).connect() as con:
			count = con.execute("""
								SELECT count(transaction_id)  
								FROM repo_transactions
								where status='open' 
								and (
									is_open_repo=True
									or maturity_date > '2020-03-09'
									)
								""").scalar()
			con.close()
		self.assertEqual(count, len(res))
		#-- 2.2 status: closed
		res = getRepo(
				date='2020-03-09',
				status='closed',
				portfolio='all',
				custodian='all',
				repoName='all',
				broker='all',
				hasHairCut='all'
				)
		with DBConn.get_db(self.unittest_dbmode).connect() as con:
			count = con.execute("""
								SELECT count(transaction_id) 
								FROM repo_transactions
								where status = 'closed' 
								or (
									is_open_repo = 0
									and maturity_date <= '2020-03-09'
									and status != 'canceled'
									);
								""").scalar()
			con.close()
		self.assertEqual(count, len(res))
		#-- 2.3 status: cancel
		res = getRepo(
				date='2020-03-09',
				status='canceled',
				portfolio='all',
				custodian='all',
				repoName='all',
				broker='all',
				hasHairCut='all'
				)
		#-- 2.4 status: all
		res = getRepo(
				date='2020-03-09',
				status='all'
				)
		with DBConn.get_db(self.unittest_dbmode).connect() as con:
			count = con.execute("""
								SELECT count(transaction_id)  
								FROM repo_transactions
								""").scalar()
			con.close()
		self.assertEqual(count, len(res))
		#-- 3. normal query portfolio
		#-- 3.1 porfolio: english
		res = getRepo(
				date='2020-03-09',
				portfolio='12734'
				)
		with DBConn.get_db(self.unittest_dbmode).connect() as con:
			count = con.execute("""
								SELECT count(transaction_id)  
								FROM repo_transactions
								where status='open' 
								and (
									is_open_repo=True
									or maturity_date > '2020-03-09'
									)
								and portfolio='12734'
								""").scalar()
			con.close()
		self.assertEqual(count, len(res))
		#-- 3.2 porfolio: 中文
		res = getRepo(
				date='2020-03-09',
				status='all',
				portfolio='中文'
				)
		self.assertEqual(1, len(res))
		#-- 4. normal query portfolio
		#-- 4.1 custodian: 中國銀行
		res = getRepo(
				date='2020-03-09',
				status='all',
				custodian='中國銀行'
				)
		self.assertEqual(1, len(res))
		#-- 4.2 custodian: english
		res = getRepo(
				date='2020-03-09',
				custodian='BOCHK'
				)
		self.assertEqual(5, len(res))
		#-- 4. normal query repoName
		#-- 5.1 repoName: normal value (search status default input 'open')
		res = getRepo(
				date='2020-03-09',
				repoName='MMRPE420BS-2'
				)
		self.assertEqual(3, len(res))
		#-- 5.2 repoName: empty result
		res = getRepo(
				date='2020-03-09',
				repoName='--10--'
				)
		self.assertEqual(0, len(res))
		#-- 6. normal query repoName
		#-- 6.1 repoName: normal value
		res = getRepo(
				date='2020-03-09',
				broker='BOC-REPO'
				)
		self.assertEqual(3, len(res))
		#-- 6.2 repoName: normal and status cancel
		res = getRepo(
				date='2020-03-09',
				status='closed',
				broker='BOC-REPO'
				)
		with DBConn.get_db(self.unittest_dbmode).connect() as con:
			count = con.execute("""
								SELECT count(transaction_id)
								FROM repo_transactions
								where status = 'closed' 
								or (
									is_open_repo = 0
									and maturity_date <= '2020-03-09'
									and status != 'canceled'
									)
								and broker='BOC-REPO'
								""").scalar()
			con.close()
		self.assertEqual(count, len(res))
		#-- 6.3 repoName: empty result
		res = getRepo(
				date='2020-03-09',
				broker='--REPO--'
				)
		self.assertEqual(0, len(res))
		#-- 7. normal query repoName
		#-- 7.1 hasHairCut: true
		res = getRepo(
				date='2020-03-09',
				hasHairCut='True'
				)
		self.assertEqual(0, len(res))
		#-- 7.2 hasHairCut: false and status close
		res = getRepo(
				date='2020-03-09',
				status='closed',
				hasHairCut='False'
				)
		with DBConn.get_db(self.unittest_dbmode).connect() as con:
			count = con.execute("""
								SELECT count(transaction_id)
								FROM repo_transactions
								where status = 'closed' 
								or (
									is_open_repo = 0
									and maturity_date <= '2020-03-09'
									and status != 'canceled'
									)
								and haircut = 0
								""").scalar()
			con.close()
		self.assertEqual(count, len(res))

	def _get_test_transaction(self):
		transaction = {
			"TransactionType" : "Repo_InsertUpdate",
			"UserTranId1" : "300734",
			"Portfolio" : "12734",
			"LocationAccount" : "BOCHK",
			"Investment" : "Isin=XS1234567890",
			"EventDate" : "2018-08-27T00:00:00",
			"SettleDate" : "2018-08-27T00:00:00",
			"ActualSettleDate" : "2020-03-10T00:00:00",
			"Quantity" : "300000",
			"CounterInvestment" : "USD",
			"Price" : "95.23",
			"NetCounterAmount" : "1818234",
			"RepoName" : "MMRPE420BS",
			"Coupon" : "0.95",
			"LoanAmount" : "1818234",
			"Broker" : "BNP-REPO"
		}
		return transaction

	def _get_test_repo_master(self):
		master = {
			"Code" : "MMRPE420BS",
			"BifurcationCurrency" : "USD",
			"AccrualDaysPerMonth" : "Actual",
			"AccrualDaysPerYear" : "360"
		}
		return master
