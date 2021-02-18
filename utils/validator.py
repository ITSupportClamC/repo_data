# coding=utf-8
# 
from cerberus import Validator
from cerberus.errors import BasicErrorHandler
from datetime import datetime
from repo_data.utils.error_handling import InvalidRepoTransactionTypeError
import yaml
import re

class AppValidatorFactory:

	def get_validator(self, method_name):
		if method_name == "addRepoMaster":
			return self._get_add_repo_master_validator()
		elif method_name == "addRepoTransaction":
			return self._get_add_transaction_validator()
		elif method_name == "cancelRepoTransaction":
			return self._get_cancel_transaction_validator()
		elif method_name == "closeRepoTransaction":
			return self._get_close_transaction_validator()
		elif method_name == "rerateRepoTransaction":
			return self._get_rerate_transaction_validator()
		elif method_name == "getRepo":
			return self._get_repo_validator()
		else:
			raise Exception("No validator defined for method_name: " + \
								method_name + \
								". Please check and add back")

	def _get_add_repo_master_validator(self):
		#-- note: yaml need to use space not tab for indentation
		schema_text = '''
Code:
  required: true
  type: string
  maxlength: 100
BifurcationCurrency:
  required: true
  type: string
  maxlength: 5
AccrualDaysPerMonth:
  required: true
  check_with: actual_or_number_format
AccrualDaysPerYear:
  required: true
  check_with: actual_or_number_format
'''
		schema = yaml.load(schema_text, Loader=yaml.FullLoader)
		return AppValidator(schema)

	def _get_add_transaction_validator(self):
		schema_text = '''
TransactionType:
  required: true
  type: string
  maxlength: 100
UserTranId1:
  required: true
  type: string
  maxlength: 20
Portfolio:
  required: true
  type: string
  maxlength: 100
LocationAccount:
  required: true
  type: string
  maxlength: 100
Investment:
  required: true
  check_with: investment_format
  maxlength: 100
EventDate:
  required: true
  check_with: iso8601_date_format
SettleDate:
  required: true
  check_with: iso8601_date_format
OpenEnded:
  required: false
  check_with: openended_format
ActualSettleDate:
  required: true
  check_with: iso8601_date_or_calc_format
Quantity:
  required: true
  check_with: float_format
CounterInvestment:
  required: true
  type: string
  maxlength: 5
Price:
  required: true
  check_with: float_format
NetCounterAmount:
  required: true
  check_with: float_format
RepoName:
  required: true
  type: string
  maxlength: 100
Coupon:
  required: true
  check_with: float_format
LoanAmount:
  required: true
  check_with: float_format
Broker:
  required: true
  type: string
  maxlength: 100
'''
		schema = yaml.load(schema_text, Loader=yaml.FullLoader)
		return AppValidator(schema)


	def _get_cancel_transaction_validator(self):
		schema_text = '''
UserTranId1:
  required: true
  type: string
  maxlength: 20
'''
		schema = yaml.load(schema_text, Loader=yaml.FullLoader)
		return AppValidator(schema)

	def _get_close_transaction_validator(self):
		schema_text = '''
UserTranId1:
  required: true
  type: string
  maxlength: 20
ActualSettleDate:
  required: true
  check_with: iso8601_date_format
'''
		schema = yaml.load(schema_text, Loader=yaml.FullLoader)
		return AppValidator(schema)

	def _get_rerate_transaction_validator(self):
		schema_text = '''
UserTranId1:
  required: true
  type: string
  maxlength: 20
RateTable:
  required: true
  type: dict
  empty: false
  schema:
    Rate:
      required: true
      check_with: float_format
    RateDate:
      required: true
      check_with: iso8601_date_format
'''
		schema = yaml.load(schema_text, Loader=yaml.FullLoader)
		return AppValidator(schema)

	def _get_repo_validator(self):
		schema_text = '''
date:
  required: true
  check_with: yyyy_mm_dd_date_format
status:
  type: string
  allowed: ['all', 'open', 'closed', 'canceled']
portfolio:
  type: string
  maxlength: 100
custodian:
  type: string
  maxlength: 100
repo_code:
  type: string
  maxlength: 100
broker:
  type: string
  maxlength: 100
has_hair_cut:
  type: string
  allowed: ['all', 'True', 'False', 'true', 'false']
'''
		schema = yaml.load(schema_text, Loader=yaml.FullLoader)
		return AppValidator(schema)

class AppValidator(Validator):
	
	iso8601_date_format_regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'

	def _check_with_iso8601_date_format(self, field, value):
		match_iso8601 = re.compile(self.iso8601_date_format_regex).match
		if not match_iso8601(value):
			self._error(field, "date format must be in yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	
	def _check_with_iso8601_date_or_calc_format(self, field, value):
		if not str(value).lower() == "calc":
			match_iso8601 = re.compile(self.iso8601_date_format_regex).match
			if not match_iso8601(value):
				self._error(field, "must be either CALC or in date format yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

	def _check_with_maturity_date_format(self, field, value):
		if str(self.document.get('is_open_repo', "false")).lower() == "true":
			#-- if is_open_repo is true, maturity_date shall be empty
			if str(value.lower()) is not "":
				self._error(field, "maturity_date shall be empty when is_open_repo is true")
		else:
			#-- test the format by substring first 10 chars and see if fit format yyyy-mm-dd			
			try:
				datetime.strptime(value, "%Y-%m-%d")
			except ValueError:
				self._error(field, "the first 10 chars of the date must be in yyyy-mm-dd")

	def _check_with_yyyy_mm_dd_date_format(self, field, value):
		try:
			datetime.strptime(value, "%Y-%m-%d")
		except ValueError:
			self._error(field, "date format must be in yyyy-mm-dd")

	def _check_with_actual_or_number_format(self, field, value):
		if not value.lower() == "actual":
			try:
				val = int(value)
			except ValueError:
				self._error(field, "must be either 'Actual' or an integer")

	def _check_with_float_format(self, field, value):
		try:
			val = float(value)
		except ValueError:
			self._error(field, "must be of number type")

	def _check_with_investment_format(self, field, value):
		#-- return error if the string does not contains "="
		#-- or after spliting, either splited string are empty
		#-- not use regex to avoid failing to check non-english chars
		if not "=" in value:
			self._error(field, "must be in format of 'Collateral ID TYPE=Collateral ID'")
		else:
			res = value.split("=", 1)
			if len(res) < 2:
				self._error(field, "must be in format of 'Collateral ID TYPE=Collateral ID'")
			else:
				if res[0] == "" or res[1] == "":
					self._error(field, "must be in format of 'Collateral ID TYPE=Collateral ID'")

	def _check_with_openended_format(self, field, value):
		if value.lower() != "calc":
			self._error(field, "must contains string value CALC")
