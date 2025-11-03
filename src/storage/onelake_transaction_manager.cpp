#include "storage/onelake_transaction_manager.hpp"
#include "storage/onelake_transaction.hpp"
#include "storage/onelake_catalog.hpp"

namespace duckdb {

OneLakeTransactionManager::OneLakeTransactionManager(AttachedDatabase &db, OneLakeCatalog &catalog)
    : TransactionManager(db), onelake_catalog(catalog) {
}

OneLakeTransactionManager::~OneLakeTransactionManager() = default;

Transaction &OneLakeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<OneLakeTransaction>(*this, context, onelake_catalog);
	auto transaction_ptr = transaction.get();
	lock_guard<mutex> l(transaction_lock);
	transactions.emplace(transaction_ptr, std::move(transaction));
	return *transaction_ptr;
}

ErrorData OneLakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto it = transactions.find(&transaction);
	if (it != transactions.end()) {
		transactions.erase(it);
	}
	return ErrorData();
}

void OneLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto it = transactions.find(&transaction);
	if (it != transactions.end()) {
		transactions.erase(it);
	}
}

void OneLakeTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// OneLake doesn't need checkpointing as it's not a local storage system
}

} // namespace duckdb
