# Changelog

## v1.0.0

### Changed

- Initial Release
- Include initializeDatastore, clearRepoData, getRepo, addRepoMaster, addRepoTransaction
- Notes:
  1. The field repo_transaction.maturity does not store as datetime in mysql as it need to store empty string and words ERROR
  2. Has tested adding tranasction with chinese character in Portfolio and LocationAccount
  3. The program will handle transaction begin and commit for atomic operation of adding and update tranasction and transaction_history. After some testing, table does not include adding foreign key as it will cause foreign key constraint error when trying commit new transactin and transaction history together. And it might cause program and performance issue if need to lift and add back the contraint every time adding transaction.
  4. All API will close the sqlalchemy session after each call automatically to avoid db connection leak
  5. Database creation SQL added to folder `sql`
