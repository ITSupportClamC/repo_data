from sqlalchemy import Column, Integer, String, DateTime, Numeric
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base(name='BaseModel')

class RepoTransactionHistory(BaseModel):
	__tablename__ = "repo_transaction_history"
	id = Column(Integer, primary_key=True)
	transaction_id = Column(String(20))
	action = Column(String(10))
	date = Column(DateTime)
	interest_rate = Column(Numeric(asdecimal=False))
	created_at = Column(DateTime)
	updated_at = Column(DateTime)
	created_by = Column(Integer)
	updated_by = Column(Integer)

	def __init__(self, \
				transaction_id, \
				action, \
				date, \
				interest_rate):
		self.transaction_id = transaction_id
		self.action = action
		self.date = date
		self.interest_rate = interest_rate

	def __repr__(self):
		res = {}
		columns = [m.key for m in model.__table__.columns]
		for column in columns:
			res[column] = getattr(model, column)
		return res
