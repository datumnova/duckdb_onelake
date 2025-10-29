#pragma once
#include "duckdb.hpp"

namespace duckdb {

class OneLakeCatalog;
class OneLakeTransactionManager;

class OneLakeTransaction : public Transaction {
public:
    OneLakeTransaction(OneLakeTransactionManager &manager, ClientContext &context, OneLakeCatalog &catalog);

    static OneLakeTransaction &Get(ClientContext &context, Catalog &catalog);

private:
    OneLakeCatalog &onelake_catalog;
};

} // namespace duckdb