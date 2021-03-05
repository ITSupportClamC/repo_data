# coding=utf-8
# 
import logging
from datetime import datetime
from repo_data.constants import Constants
from repo_data.models.repo_master import RepoMaster
from repo_data.models.repo_transaction import RepoTransaction
from repo_data.models.repo_transaction_history import RepoTransactionHistory
from sqlalchemy import and_, or_
from sqlalchemy.orm import sessionmaker
from repo_data.utils.error_handling import (RepoTransactionAlreadyExistError,
											RepoTransactionNotExistError,
											CloseCanceledRepoTransactionError,
											RepoMasterNotExistError)

class RepoTransactionServices:

	def __init__(self, db):
		self.logger = logging.getLogger(__name__)
		self.db = db

	def delete_all(self):
		try:
			session = sessionmaker(bind=self.db)()
			session.query(RepoTransaction).delete()
			session.commit()
		except Exception as e:
			self.logger.error("Failed to delete all records in RepoTransaction")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def create(self, transaction):
		try:
			session = sessionmaker(bind=self.db)()
			#-- throw exception if transaction_id already exist
			has_record = bool(session.query(RepoTransaction) \
									.filter_by(transaction_id=transaction['transaction_id']) \
									.first())
			if has_record:
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							". record already exists"
				self.logger.warn(message)
				raise RepoTransactionAlreadyExistError(message)
			#-- throw exception if transaction_id already exist
			has_repo_master_record = bool(session.query(RepoMaster) \
									.filter_by(code=transaction['repo_code']) \
									.first())
			if not has_repo_master_record:
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							". The transaction's repo_code " + \
							transaction['repo_code'] + " not exists"
				self.logger.warn(message)
				raise RepoMasterNotExistError(message)
			repo_transaction = RepoTransaction(**transaction)
			#-- create transaction
			session.add(repo_transaction)
			#-- add transaction history
			transaction_history = {
				"transaction_id" : transaction["transaction_id"],
				"action" : Constants.REPO_TRANS_HISTORY_ACTION_OPEN,
				"date" : transaction["settle_date"],
				"interest_rate" : transaction["interest_rate"]
			}
			repo_transaction_history = RepoTransactionHistory(**transaction_history)
			session.add(repo_transaction_history)
			#-- commit the transaction after both transaction and transaction_history added
			session.commit()
			self.logger.info("Record " +  transaction['transaction_id'] + " added successfully")
		except RepoTransactionAlreadyExistError:
			#-- avoid RepoTransactionAlreadyExistError being captured by Exception
			raise
		except RepoMasterNotExistError:
			#-- avoid RepoMasterNotExistError being captured by Exception
			raise
		except Exception as e:
			self.logger.error("Failed to add transaction")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def cancel(self, transaction):
		try:
			session = sessionmaker(bind=self.db)()
			repo_trans_to_cxl = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['transaction_id']) \
								.first()
			#-- throw error if transaction not exists
			if not bool(repo_trans_to_cxl):
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							" not exists"
				self.logger.warn(message)
				raise RepoTransactionNotExistError(message)
			#-- update transaction by updating the status to cancel
			repo_trans_to_cxl.status = Constants.REPO_TRANS_STATUS_CANCEL
			#-- add a record to the transaction history
			transaction_history = {
				"transaction_id" : repo_trans_to_cxl.transaction_id,
				"action" : Constants.REPO_TRANS_HISTORY_ACTION_CANCEL,
				"date" : datetime.today().strftime('%Y-%m-%d'),
				"interest_rate" : 0
			}
			repo_transaction_history = RepoTransactionHistory(**transaction_history)
			session.add(repo_transaction_history)
			session.commit()
			self.logger.info("Record " +  repo_trans_to_cxl.transaction_id + " updated successfully")
		except RepoTransactionNotExistError:
			#-- avoid RepoTransactionNotExistError being captured by Exception
			raise
		except Exception as e:
			self.logger.error("Failed to update transaction")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def close(self, transaction):
		try:
			session = sessionmaker(bind=self.db)()
			repo_trans_to_cls = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['transaction_id']) \
								.first()
			#-- throw error if transaction not exists
			if not bool(repo_trans_to_cls):
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							" not exists"
				self.logger.warn(message)
				raise RepoTransactionNotExistError(message)
			#-- throw error if transaction has been closed or canceled
			if repo_trans_to_cls.status == Constants.REPO_TRANS_STATUS_CANCEL or \
					repo_trans_to_cls.status == Constants.REPO_TRANS_STATUS_CLOSE:
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							". is either closed or canceled"
				self.logger.warn(message)
				raise CloseCanceledRepoTransactionError(message)
			#-- update transaction by updating the status to cancel
			repo_trans_to_cls.status = Constants.REPO_TRANS_STATUS_CLOSE
			#-- add a record to the transaction history
			transaction_history = {
				"transaction_id" : repo_trans_to_cls.transaction_id,
				"action" : Constants.REPO_TRANS_HISTORY_ACTION_CLOSE,
				"date" : transaction["maturity_date"],
				"interest_rate" : 0
			}
			repo_transaction_history = RepoTransactionHistory(**transaction_history)
			session.add(repo_transaction_history)
			session.commit()
			self.logger.info("Record " +  repo_trans_to_cls.transaction_id + " updated successfully")
		except RepoTransactionNotExistError:
			#-- avoid RepoTransactionNotExistError being captured by Exception
			raise
		except CloseCanceledRepoTransactionError:
			#-- avoid CloseCanceledRepoTransactionError being captured by Exception
			raise
		except Exception as e:
			self.logger.error("Failed to update transaction")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def rerate(self, transaction):
		try:
			session = sessionmaker(bind=self.db)()
			repo_trans_to_rerate = session.query(RepoTransaction) \
								.filter_by(transaction_id=transaction['transaction_id']) \
								.first()
			#-- throw error if transaction not exists
			if not bool(repo_trans_to_rerate):
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							" not exists"
				self.logger.warn(message)
				raise RepoTransactionNotExistError(message)
			#-- throw error if transaction has been closed or canceled
			if repo_trans_to_rerate.status == Constants.REPO_TRANS_STATUS_CANCEL or \
					repo_trans_to_rerate.status == Constants.REPO_TRANS_STATUS_CLOSE:
				message = "transaction_id: " + \
							transaction['transaction_id'] + \
							". is either closed or canceled"
				self.logger.warn(message)
				raise CloseCanceledRepoTransactionError(message)
			#-- update transaction by updating the status to cancel
			repo_trans_to_rerate.interest_rate = transaction["interest_rate"]
			#-- add a record to the transaction history
			transaction_history = {
				"transaction_id" : repo_trans_to_rerate.transaction_id,
				"action" : Constants.REPO_TRANS_HISTORY_ACTION_RERATE,
				"date" : transaction["rate_date"],
				"interest_rate" : repo_trans_to_rerate.interest_rate
			}
			repo_transaction_history = RepoTransactionHistory(**transaction_history)
			session.add(repo_transaction_history)
			session.commit()
			self.logger.info("Record " +  repo_trans_to_rerate.transaction_id + " updated successfully")
		except RepoTransactionNotExistError:
			#-- avoid RepoTransactionNotExistError being captured by Exception
			raise
		except CloseCanceledRepoTransactionError:
			#-- avoid CloseCanceledRepoTransactionError being captured by Exception
			raise
		except Exception as e:
			self.logger.error("Failed to update transaction")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def query(self, params):
		try:
			session = sessionmaker(bind=self.db)()
			conditions = []
			if not params["status"] is None and not params["status"] is "all":
				if str(params["status"]).lower() == Constants.REPO_TRANS_STATUS_OPEN:
					date_to_compare = datetime.strptime(params["date"], '%Y-%m-%d')
					conditions.append(
						and_(
								(RepoTransaction.status == params["status"]),
								(or_(
									RepoTransaction.is_open_repo == 1,
									RepoTransaction.maturity_date > date_to_compare
									)
								)
							)
					)
				elif str(params["status"]).lower() == Constants.REPO_TRANS_STATUS_CLOSE:
					date_to_compare = datetime.strptime(params["date"], '%Y-%m-%d')
					conditions.append(
						or_(
								(RepoTransaction.status == params["status"]),
								(and_(
									RepoTransaction.is_open_repo == 0,
									RepoTransaction.maturity_date <= date_to_compare,
									RepoTransaction.status != Constants.REPO_TRANS_STATUS_CANCEL
									)
								)
							) 
					)
				elif str(params["status"]).lower() == Constants.REPO_TRANS_STATUS_CANCEL:
					conditions.append(RepoTransaction.status == params["status"])
				else:
					self.logger.warn("Unknown query status: " + str(params["status"]))
			if not params["portfolio"] is None and not params["portfolio"] is "all":
				conditions.append(RepoTransaction.portfolio == params["portfolio"])
			if not params["custodian"] is None and not params["custodian"] is "all":
				conditions.append(RepoTransaction.custodian == params["custodian"])
			if not params["repo_code"] is None and not params["repo_code"] is "all":
				conditions.append(RepoTransaction.repo_code == params["repo_code"])
			if not params["broker"] is None and not params["broker"] is "all":
				conditions.append(RepoTransaction.broker == params["broker"])
			if not params["has_hair_cut"] is None and not params["has_hair_cut"] is "all":
				if str(params["has_hair_cut"]).lower() == "true":
					conditions.append(RepoTransaction.haircut != 0)
				else:
					conditions.append(RepoTransaction.haircut == 0)
			#self.logger.debug(str(conditions))
			transactions = session.query(
					RepoTransaction.transaction_id.label("TransactionId"), \
					RepoTransaction.transaction_type.label("Type"), \
					RepoTransaction.portfolio.label("Portfolio"), \
					RepoTransaction.custodian.label("Custodian"), \
					RepoTransaction.collateral_id_type.label("TransactionId"), \
					RepoTransaction.collateral_id.label("CollateralIDType"), \
					RepoTransaction.collateral_global_id.label("CollateralID"), \
					RepoTransaction.trade_date.label("TradeDate"), \
					RepoTransaction.settle_date.label("SettleDate"), \
					RepoTransaction.is_open_repo.label("IsOpenRepo"), \
					RepoTransaction.maturity_date.label("MaturityDate"), \
					RepoTransaction.quantity.label("Quantity"), \
					RepoTransaction.currency.label("Currency"), \
					RepoTransaction.price.label("PriceFrom"), \
					RepoTransaction.collateral_value.label("CollateralValue"), \
					RepoTransaction.repo_code.label("RepoName"), \
					RepoTransaction.interest_rate.label("InterestRate"), \
					RepoTransaction.loan_amount.label("LoanAmount"), \
					RepoTransaction.broker.label("Broker"), \
					RepoTransaction.haircut.label("Haircut"), \
					RepoTransaction.status.label("Status"),
					RepoMaster.date_count.label("DayCount")) \
				.filter(RepoTransaction.repo_code == RepoMaster.code) \
				.filter(and_(*conditions))
			#self.logger.debug("Print the generated SQL:")
			#self.logger.debug(transactions)
			#-- return as list of dictionary
			def model2dict(row):
				d = {}
				for column in row.keys():
					if column == "TradeDate" or \
							column == "SettleDate":
						d[column] = str(getattr(row, column))[0:10]
					elif column == "Quantity" or \
							column == "Quantity" or \
							column == "PriceFrom" or \
							column == "CollateralValue" or \
							column == "InterestRate" or \
							column == "LoanAmount" or \
							column == "Haircut":
						d[column] = float(getattr(row, column))
					else:
						d[column] = str(getattr(row, column))
				return d
			transactions_d = [model2dict(t) for t in transactions]
			#self.logger.error("Print the list of dictionary output:")
			#self.logger.debug(transactions_d)
			return transactions_d
		except Exception as e:
			self.logger.error("Failed to exceution the query:")
			self.logger.error(conditions)
			self.logger.error("Error message:")
			self.logger.error(e)
			raise
		finally:
			session.close()