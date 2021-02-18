# from repo_data.models.base_model import BaseModel, ModelException
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base(name='BaseModel')

class RepoMaster(BaseModel):
	__tablename__ = "repo_masters"
	id = Column(Integer, primary_key=True)
	code = Column(String(100))
	currency = Column(String(5))
	date_count = Column(String(100))
	created_at = Column(DateTime)
	updated_at = Column(DateTime)

	def __init__(self, code, currency, date_count):
		self.code = code
		self.currency = currency
		self.date_count = date_count

	def __repr__(self):
		res = {}
		columns = [m.key for m in model.__table__.columns]
		for column in columns:
			res[column] = getattr(model, column)
		return res
