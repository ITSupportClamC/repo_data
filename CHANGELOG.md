# Changelog

## v1.0.0

### Changed

- Initial Release
- Include initializeDatastore, clearRepoData, getRepo, addRepoMaster, addRepoTransaction
- Notes:
  - The field repo_transaction.maturity does not store as datetime in mysql as it need to store empty string and words ERROR
  - Has tested adding transaction with chinese character in Portfolio and LocationAccount
  - All API will close the sqlalchemy session after each call automatically to avoid db connection leak. So no need to explicitly call a function to close DB connection
  - Database creation SQL script added to folder `sql`

## v1.1.0

### Changed

- Added closeRepoTransaction, cancelRepoTransaction, rerateRepoTransaction and their test cases

## v1.2.0

### Changed

- Added getRepoTransactionHistory, getUserTranIdsFromRepoName and their test cases
- Adjsut the search parameter logics and update the test cases. Changes as below:
  - Get rid of the parameter "date"
  - Change logics of parameter "status" based on the updated requirments
