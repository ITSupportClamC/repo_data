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