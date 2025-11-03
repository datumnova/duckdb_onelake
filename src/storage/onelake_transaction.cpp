#include "storage/onelake_transaction.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_transaction_manager.hpp"

namespace duckdb {

OneLakeTransaction::OneLakeTransaction(OneLakeTransactionManager &manager, ClientContext &context,
                                       OneLakeCatalog &catalog)
    : Transaction(manager, context), onelake_catalog(catalog) {
}

OneLakeTransaction &OneLakeTransaction::Get(ClientContext &context, Catalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog.GetAttached());
	if (!transaction.IsReadOnly()) {
		throw InvalidInputException("OneLake transactions must be read-only");
	}
	return transaction.Cast<OneLakeTransaction>();
}

} // namespace duckdb
