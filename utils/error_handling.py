# coding=utf-8
# 
class NoDataClearingInProuctionModeError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class RepoMasterAlreadyExistError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class RepoMasterNotExistError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class RepoTransactionAlreadyExistError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class RepoTransactionNotExistError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class CloseCanceledRepoTransactionError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class InvalidRepoTransactionTypeError(Exception):
	def __init__(self, msg):
		super().__init__(msg)

class DataStoreNotYetInitializeError(Exception):
	def __init__(self, msg):
		super().__init__(msg)