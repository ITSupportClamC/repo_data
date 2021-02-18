# coding=utf-8
# 
from os.path import abspath, dirname, join

import configparser
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from repo_data.constants import Constants

getCurrentDirectory = lambda : \
	dirname(abspath(__file__))

class DBConn:

	#-- return a db engine based on the given mode
	@staticmethod
	def get_db(mode):
		if mode != Constants.DBMODE_TEST and \
				mode != Constants.DBMODE_UAT and \
				mode != Constants.DBMODE_PRODUCTION:
			message = "Unkown database mode: " + str(mode)
			raise Exception(message)
		config = configparser.ConfigParser()
		config.read( join(getCurrentDirectory(), "..", "database_config.ini") )
		#-- default is test database
		config_section = 'Database Test'
		if (mode == Constants.DBMODE_UAT):
			config_section = 'Database UAT'
		if (mode == Constants.DBMODE_PRODUCTION):
			config_section = 'Database Production'
		username = config.get(config_section, 'username')
		password = config.get(config_section, 'password')
		host = config.get(config_section, 'host')
		port = config.get(config_section, 'port')
		dbname = config.get(config_section, 'dbname')
		#-- convert port to a string
		conn_string = "mysql+mysqlconnector://" + username + ":" + password + "@" + host + ":" + str(port) + "/" + dbname
		engine = create_engine(conn_string)
		return engine
