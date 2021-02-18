from sqlalchemy import Column, Integer, String, DateTime, Numeric
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base(name='BaseModel')

class RepoTransaction(BaseModel):
	__tablename__ = "repo_transactions"
	id = Column(Integer, primary_key=True)
	transaction_id = Column(String(20))
	transaction_type = Column(String(100))
	portfolio = Column(String(100))
	custodian = Column(String(100))
	collateral_id_type = Column(String(100))
	collateral_id = Column(String(100))
	collateral_global_id = Column(String(100))
	trade_date = Column(DateTime)
	settle_date = Column(DateTime)
	is_open_repo = Column(Integer)
	maturity_date = Column(String(100))
	quantity = Column(Numeric(asdecimal=False))
	currency = Column(String(5))
	price = Column(Numeric(asdecimal=False))
	collateral_value = Column(Numeric(asdecimal=False))
	repo_code = Column(String(100))
	interest_rate = Column(Numeric(asdecimal=False))
	loan_amount = Column(Numeric(asdecimal=False))
	broker = Column(String(100))
	haircut = Column(Numeric(asdecimal=False))
	status = Column(String(10))
	created_at = Column(DateTime)
	updated_at = Column(DateTime)
	created_by = Column(Integer)
	updated_by = Column(Integer)

	def __init__(self, \
				transaction_id, \
				transaction_type, \
				portfolio, \
				custodian, \
				collateral_id_type, \
				collateral_id, \
				collateral_global_id, \
				trade_date, \
				settle_date, \
				is_open_repo, \
				maturity_date, \
				quantity, \
				currency, \
				price, \
				collateral_value, \
				repo_code, \
				interest_rate, \
				loan_amount, \
				broker, \
				haircut, \
				status):
		self.transaction_id = transaction_id
		self.transaction_type = transaction_type
		self.portfolio = portfolio
		self.custodian = custodian
		self.collateral_id_type = collateral_id_type
		self.collateral_id = collateral_id
		self.collateral_global_id = collateral_global_id
		self.trade_date = trade_date
		self.settle_date = settle_date
		self.is_open_repo = is_open_repo
		self.maturity_date = maturity_date
		self.quantity = quantity
		self.currency =currency
		self.price = price
		self.collateral_value = collateral_value
		self.repo_code = repo_code
		self.interest_rate = interest_rate
		self.loan_amount = loan_amount
		self.broker = broker
		self.haircut = haircut
		self.status = status

	def __repr__(self):
		res = {}
		columns = [m.key for m in model.__table__.columns]
		for column in columns:
			res[column] = getattr(model, column)
		return res
