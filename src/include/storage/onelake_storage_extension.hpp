#pragma once
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class OneLakeStorageExtension : public StorageExtension {
public:
    OneLakeStorageExtension() {
        attach = OneLakeStorageExtension::AttachInternal;
        create_transaction_manager = OneLakeStorageExtension::CreateTransactionManager;
    }

    static unique_ptr<Catalog> AttachInternal(optional_ptr<StorageExtensionInfo> storage_info,
                                            ClientContext &context, AttachedDatabase &db,
                                            const string &name, AttachInfo &info,
                                            AttachOptions &attach_options);
    
    static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                   AttachedDatabase &db, Catalog &catalog);

    string GetName() const { return "onelake"; }
};

} // namespace duckdb