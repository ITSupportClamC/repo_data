# coding=utf-8
# 
import logging
from repo_data.constants import Constants
from repo_data.models.repo_transaction_history import RepoTransactionHistory
from sqlalchemy.orm import sessionmaker

class RepoTransactionHistoryServices:

	def __init__(self, db):
		self.logger = logging.getLogger(__name__)
		self.db = db
		
	def delete_all(self):
		try:
			session = sessionmaker(bind=self.db)()
			session.query(RepoTransactionHistory).delete()
			session.commit()
		except Exception as e:
			self.logger.error("Failed to delete all records in RepoTransactionHistory")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def query(self, params):
		try:
			session = sessionmaker(bind=self.db)()
			transaction_histories = session.query(
					RepoTransactionHistory.transaction_id.label("TransactionId"), \
					RepoTransactionHistory.action.label("Action"), \
					RepoTransactionHistory.date.label("Date"), \
					RepoTransactionHistory.interest_rate.label("InterestRate"), \
					RepoTransactionHistory.created_at.label("TimeStamp")) \
				.filter(RepoTransactionHistory.transaction_id == params['transaction_id']) \
				.order_by(RepoTransactionHistory.created_at)
			#self.logger.debug("Print the generated SQL:")
			#self.logger.debug(transaction_histories)
			#-- return as list of dictionary
			def model2dict(row):
				d = {}
				for column in row.keys():
					if column == "Date":
						d[column] = str(getattr(row, column))[0:10]
					elif column == "InterestRate":
						d[column] = float(getattr(row, column))
					else:
						d[column] = str(getattr(row, column))
				return d
			transaction_histories_d = [model2dict(t) for t in transaction_histories]
			#self.logger.error("Print the list of dictionary output:")
			#self.logger.debug(transaction_histories_d)
			return transaction_histories_d
		except Exception as e:
			self.logger.error("Error message:")
			self.logger.error(e)
			raise
		finally:
			session.close()