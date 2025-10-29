#pragma once
#include "duckdb.hpp"
#include "storage/onelake_transaction.hpp"
#include <unordered_map>

namespace duckdb {

class OneLakeCatalog;

class OneLakeTransactionManager : public TransactionManager {
public:
    OneLakeTransactionManager(AttachedDatabase &db, OneLakeCatalog &catalog);
    ~OneLakeTransactionManager() override;

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;

    void Checkpoint(ClientContext &context, bool force = false) override;

private:
    OneLakeCatalog &onelake_catalog;
    mutex transaction_lock;
    unordered_map<Transaction *, unique_ptr<OneLakeTransaction>> transactions;
};

} // namespace duckdb