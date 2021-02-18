# coding=utf-8
# 
import logging
from repo_data.utils.error_handling import RepoMasterAlreadyExistError
from repo_data.constants import Constants
from repo_data.models.repo_master import RepoMaster
from sqlalchemy.orm import sessionmaker

class RepoMasterServices:

	def __init__(self, db):
		self.logger = logging.getLogger(__name__)
		self.db = db

	def delete_all(self):
		try:
			session = sessionmaker(bind=self.db)()
			session.query(RepoMaster).delete()
			session.commit()
		except Exception as e:
			self.logger.error("Failed to delete all records in RepoMaster")
			self.logger.error(e)
			raise
		finally:
			session.close()

	def create(self, master):
		try:
			session = sessionmaker(bind=self.db)()
			has_record = bool(session.query(RepoMaster).filter_by(code=master['code']).first())
			if has_record:
				message = "Record " + master['code'] + " already exists"
				self.logger.warn(message)
				raise RepoMasterAlreadyExistError(message)
			else:
				repo_master = RepoMaster(**master)
				session.add(repo_master)
				session.commit()
				self.logger.info("Record " + master['code'] + " added successfully")
		except RepoMasterAlreadyExistError:
			#-- avoid RepoMasterAlreadyExistError being captured by Exception
			raise
		except Exception as e:
			self.logger.error("Failed to add repo_master")
			self.logger.error(e)
			raise
		finally:
			session.close()