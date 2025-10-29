#include "storage/onelake_storage_extension.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_transaction_manager.hpp"
#include "onelake_credentials.hpp"
#include "onelake_secret.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

unique_ptr<Catalog> OneLakeStorageExtension::AttachInternal(optional_ptr<StorageExtensionInfo> storage_info,
                                                          ClientContext &context, AttachedDatabase &db,
                                                          const string &name, AttachInfo &info,
                                                          AttachOptions &attach_options) {
    auto &secret_manager = SecretManager::Get(context);

    // Determine workspace ID from the attach path; fall back to the name if needed
    string workspace_identifier = info.path.empty() ? name : info.path;
    string workspace_id;
    if (workspace_identifier.empty()) {
        throw InvalidInputException("OneLake workspace ID must be provided in ATTACH statement");
    }

    // Normalize identifiers so the catalog only stores the GUID while the secret lookup keeps the scheme prefix
    const string scope_prefix = "onelake://";
    if (StringUtil::StartsWith(workspace_identifier, scope_prefix)) {
        workspace_id = workspace_identifier.substr(scope_prefix.size());
    } else {
        workspace_id = workspace_identifier;
    }
    if (workspace_id.empty()) {
        throw InvalidInputException("OneLake workspace ID must not be empty");
    }

    // Extract optional default lakehouse preference if provided via ATTACH options
    string default_lakehouse;
    for (auto &option : info.options) {
        if (StringUtil::CIEquals(option.first, "default_lakehouse") && !option.second.IsNull()) {
            if (option.second.type().id() == LogicalTypeId::VARCHAR) {
                default_lakehouse = StringValue::Get(option.second);
            } else {
                default_lakehouse = option.second.ToString();
            }
        }
    }

    // Try to get the secret for OneLake credentials
    auto catalog_transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    // Scope secrets on the specific workspace to allow standard DuckDB secret resolution
    const auto secret_path = "onelake://" + workspace_id;
    auto secret_match = secret_manager.LookupSecret(catalog_transaction, secret_path, "onelake");
    if (!secret_match.HasMatch()) {
        secret_match = secret_manager.LookupSecret(catalog_transaction, "onelake://", "onelake");
    }
    if (!secret_match.HasMatch()) {
        throw InvalidInputException("No OneLake secret found. Create a secret with: CREATE SECRET (TYPE ONELAKE, ...)");
    }

    auto &secret = secret_match.GetSecret();
    OneLakeCredentials credentials;

    // Extract credentials from secret
    auto kv_secret = dynamic_cast<const KeyValueSecret*>(&secret);
    if (!kv_secret) {
        throw InvalidInputException("OneLake secret must be a key-value secret");
    }

    Value tenant_id, client_id, client_secret;
    if (!kv_secret->TryGetValue("tenant_id", tenant_id) || 
        !kv_secret->TryGetValue("client_id", client_id) ||
        !kv_secret->TryGetValue("client_secret", client_secret)) {
        throw InvalidInputException("OneLake secret must contain tenant_id, client_id, and client_secret");
    }

    credentials.tenant_id = tenant_id.ToString();
    credentials.client_id = client_id.ToString();
    credentials.client_secret = client_secret.ToString();

    if (!credentials.IsValid()) {
        throw InvalidInputException("Invalid OneLake credentials provided");
    }

    // Create the OneLake catalog
    return make_uniq<OneLakeCatalog>(db, workspace_id, name, credentials, std::move(default_lakehouse));
}

unique_ptr<TransactionManager> OneLakeStorageExtension::CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, 
                                                                               AttachedDatabase &db,
                                                                               Catalog &catalog) {
    auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
    return make_uniq<OneLakeTransactionManager>(db, onelake_catalog);
}

} // namespace duckdb