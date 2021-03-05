# coding=utf-8
# 
import logging
from repo_data.constants import Constants
from repo_data.utils.error_handling import (NoDataClearingInProuctionModeError,
											InvalidRepoTransactionTypeError,
											DataStoreNotYetInitializeError)
from repo_data.utils.database import DBConn
from repo_data.utils.validator import AppValidatorFactory
from repo_data.services.repo_master_services import RepoMasterServices
from repo_data.services.repo_transaction_services import RepoTransactionServices
from repo_data.services.repo_transaction_history_services import RepoTransactionHistoryServices
from cerberus import SchemaError

#-- serivce and DB connection shall be stateless so it is safe to have a singleton
#db = DBConn.get_db()
#repo_master_services = RepoMasterServices(db)
#repo_transaction_services = RepoTransactionServices(db)
#repo_transaction_history_services = RepoTransactionHistoryServices(db)

class AppController:

	logger = None
	dbmode = None
	repo_master_services = None
	repo_transaction_services = None
	repo_transaction_history_services = None

	def __init__(self):
		self.logger = logging.getLogger(__name__)
		self.dbmode = None

	def initializeDatastore(self, mode):
		if (mode == "production"):
			self.dbmode = Constants.DBMODE_PRODUCTION
			self.logger.info("Change datastore mode to DBMODE_PRODUCTION")
		elif (mode == "uat"):
			self.dbmode = Constants.DBMODE_UAT
			self.logger.info("Change datastore mode to DBMODE_UAT")
		else:
			self.dbmode = Constants.DBMODE_TEST
			self.logger.info("Change datastore mode to DBMODE_TEST")
		db = DBConn.get_db(self.dbmode)
		self.repo_master_services = RepoMasterServices(db)
		self.repo_transaction_services = RepoTransactionServices(db)
		self.repo_transaction_history_services = RepoTransactionHistoryServices(db)
		return 0

	def clearRepoData(self):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		print(self.dbmode)
		if self.dbmode == Constants.DBMODE_PRODUCTION:
			error_message = "clearRepoData can only run under DBMODE_TEST mode"
			self.logger.warn(error_message)
			raise NoDataClearingInProuctionModeError(error_message)
		else:
			self.logger.warn("clear data in repo_transaction_history table")
			self.repo_transaction_history_services.delete_all()
			self.logger.warn("clear data in repo_transaction table")
			self.repo_transaction_services.delete_all()
			self.logger.debug("clear data in repo_master")			
			self.repo_master_services.delete_all()
			return 0
			#repo_transaction_history.clear()

	def addRepoMaster(self, master):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		v = AppValidatorFactory().get_validator("addRepoMaster")
		if not v.validate(master):
			message = "Input validation error. Details: " + str(v.errors)
			self.logger.error(message)
			raise ValueError(message)
		#-- data parsing
		accrualDaysPerMonth = master["AccrualDaysPerMonth"]
		if master["AccrualDaysPerMonth"].lower() == "actual":
			accrualDaysPerMonth = "ACT"
		accrualDaysPerYear = master["AccrualDaysPerYear"]
		if master["AccrualDaysPerYear"].lower() == "actual":
			accrualDaysPerYear= "ACT"
		#-- create data model
		data_master = {
			"code" : master["Code"],
			"currency" : master["BifurcationCurrency"],
			"date_count" : accrualDaysPerMonth + 
							"/" + 
							accrualDaysPerYear
		}
		self.repo_master_services.create(data_master)
		return 0

	def addRepoTransaction(self, transaction):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		v = AppValidatorFactory().get_validator("addRepoTransaction")
		#-- validate input fields
		if not v.validate(transaction):
			message = "Input validation error. Details: " + str(v.errors)
			self.logger.error(message)
			raise ValueError(message)
		#-- data parsing
		#-- get transaction_type
		transaction_type = ""
		if transaction["TransactionType"] == "Repo_InsertUpdate":
			transaction_type = "RP"
		elif transaction["TransactionType"] == "ReverseRepo_InsertUpdate":
			transaction_type = "RR"
		else:
			message = "Ipput validation error. TransactionType must be either Repo_InsertUpdate or ReverseRepo_InsertUpdate"
			self.logger.error(message)
			raise InvalidRepoTransactionTypeError(message)
		#-- get collaterial type and id
		investment_split = transaction["Investment"].split("=", 1)
		collateral_id_type = investment_split[0]
		collateral_id = investment_split[1]
		is_open_repo = 0
		#-- is_open_repo
		if "OpenEnded" in transaction and transaction["OpenEnded"].lower() == "calc":
			is_open_repo = 1
		#-- maturity_date
		maturity_date = ""
		if transaction["ActualSettleDate"].lower() == "calc":
			if is_open_repo:
				maturity_date = ""
			else:
				maturity_date = "ERROR"
		else:
			maturity_date = transaction["ActualSettleDate"][0:10]
		#-- create data model
		data_transaction = {
			"transaction_id" : transaction["UserTranId1"],
			"transaction_type" : transaction_type,
			"portfolio" : transaction["Portfolio"],
			"custodian" : transaction["LocationAccount"],
			"collateral_id_type" : collateral_id_type,
			"collateral_id" : collateral_id,
			"collateral_global_id" : "",
			"trade_date" :  transaction["EventDate"][0:10],
			"settle_date" : transaction["SettleDate"][0:10],
			"is_open_repo" : is_open_repo,
			"maturity_date" : maturity_date,
			"quantity" :  float(transaction["Quantity"]),
			"currency" :  transaction["CounterInvestment"],
			"price" : float(transaction["Price"]),
			"collateral_value" : float(transaction["NetCounterAmount"]),
			"repo_code" : transaction["RepoName"],
			"interest_rate" : float(transaction["Coupon"]),
			"loan_amount" : float(transaction["LoanAmount"]),
			"broker" : transaction["Broker"],
			"haircut" : 0,
			"status" : Constants.REPO_TRANS_HISTORY_ACTION_OPEN
		}
		#-- add to repo_transaction and repo_transaction_history
		self.repo_transaction_services.create(data_transaction)
		return 0

	def cancelRepoTransaction(self, transaction):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		v = AppValidatorFactory().get_validator("cancelRepoTransaction")
		#-- validate input fields
		if not v.validate(transaction):
			message = "Input validation error. Details: " + str(v.errors)
			self.logger.error(message)
			raise ValueError(message)
		#-- create data model
		data_transaction = {
			"transaction_id" : transaction["UserTranId1"]
		}
		self.repo_transaction_services.cancel(data_transaction)
		return 0

	def closeRepoTransaction(self, transaction):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		v = AppValidatorFactory().get_validator("closeRepoTransaction")
		#-- validate input fields
		if not v.validate(transaction):
			message = "Input validation error. Details: " + str(v.errors)
			self.logger.error(message)
			raise ValueError(message)
		#-- data parsing
		#-- maturity_date
		maturity_date = transaction["ActualSettleDate"][0:10]
		#-- create data model
		data_transaction = {
			"transaction_id" : transaction["UserTranId1"],
			"maturity_date" : maturity_date
		}
		self.repo_transaction_services.close(data_transaction)
		return 0

	def rerateRepoTransaction(self, transaction):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		v = AppValidatorFactory().get_validator("rerateRepoTransaction")
		#-- validate input fields
		if not v.validate(transaction):
			message = "Input validation error. Details: " + str(v.errors)
			self.logger.error(message)
			raise ValueError(message)
		#-- data parsing
		#-- rate_date
		rate_date = transaction["RateTable"]["RateDate"][0:10]
		#-- create data model
		data_transaction = {
			"transaction_id" : transaction["UserTranId1"],
			"interest_rate" : transaction["RateTable"]["Rate"],
			"rate_date" : rate_date
		}
		self.repo_transaction_services.rerate(data_transaction)
		return 0
	
	def getRepo( self, date, status='open', portfolio='all', custodian='all', repo_code='all'
		   , broker='all', has_hair_cut='all'):
		if self.dbmode is None:
			raise DataStoreNotYetInitializeError("Plase call initializeDatastore to initialize datastore")
		params = {
			"date" : date,
			"status" : status,
			"portfolio" : portfolio,
			"custodian" : custodian,
			"repo_code" : repo_code,
			"broker" : broker,
			"has_hair_cut" : has_hair_cut
		}
		v = AppValidatorFactory().get_validator("getRepo")
		#-- validate input fields
		if not v.validate(params):
			message = "Input validation error. Details: " + str(v.errors)
			self.logger.error(message)
			raise ValueError(message)
		transactions = self.repo_transaction_services.query(params)
		return transactions