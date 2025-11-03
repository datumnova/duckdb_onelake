#include "storage/onelake_http_util.hpp"

#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "onelake_api.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include <unordered_set>
#include <cstdlib>

namespace duckdb {

namespace {

void SetEnvIfUnset(const char *name, const string &value) {
	if (value.empty()) {
		return;
	}
	const char *existing = std::getenv(name);
	if (existing && existing[0] != '\0') {
		return;
	}
#ifdef _WIN32
	_putenv_s(name, value.c_str());
#else
	setenv(name, value.c_str(), 1);
#endif
}

void EnsureAzureClientCredentialEnv(const OneLakeCredentials &credentials) {
	SetEnvIfUnset("AZURE_CLIENT_ID", credentials.client_id);
	SetEnvIfUnset("AZURE_TENANT_ID", credentials.tenant_id);
	SetEnvIfUnset("AZURE_CLIENT_SECRET", credentials.client_secret);
}

} // namespace

void EnsureHttpBearerSecret(ClientContext &context, OneLakeCatalog &catalog, const OneLakeSchemaEntry *schema_entry) {
	// Delta kernel defers to Azure default credentials; seed env vars so our service principal is used.
	EnsureAzureClientCredentialEnv(catalog.GetCredentials());

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	auto &credentials = catalog.GetCredentials();
	auto token = OneLakeAPI::GetAccessToken(credentials, OneLakeTokenAudience::OneLakeDfs);

	string workspace_id = catalog.GetWorkspaceId();
	duckdb::vector<string> scopes;
	std::unordered_set<string> seen;
	auto add_scope = [&](const string &scope) {
		if (scope.empty()) {
			return;
		}
		if (!seen.insert(scope).second) {
			return;
		}
		scopes.push_back(scope);
	};

	string base_https = "https://onelake.dfs.fabric.microsoft.com/" + workspace_id;
	string base_abfss = "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com";
	add_scope(base_https);
	add_scope(base_https + "/");
	add_scope(base_abfss);
	add_scope(base_abfss + "/");

	auto add_lakehouse_scopes = [&](const string &identifier) {
		if (identifier.empty()) {
			return;
		}
		add_scope(base_https + "/" + identifier);
		add_scope(base_https + "/" + identifier + "/");
		add_scope(base_https + "/" + identifier + ".Lakehouse");
		add_scope(base_https + "/" + identifier + ".Lakehouse/");
		add_scope(base_abfss + "/" + identifier);
		add_scope(base_abfss + "/" + identifier + "/");
		add_scope(base_abfss + "/" + identifier + ".Lakehouse");
		add_scope(base_abfss + "/" + identifier + ".Lakehouse/");
		add_scope(base_https + "/" + identifier + "/Tables");
		add_scope(base_abfss + "/" + identifier + "/Tables");
	};

	if (schema_entry && schema_entry->schema_data) {
		const auto &lakehouse_id = schema_entry->schema_data->id;
		const auto &lakehouse_name = schema_entry->schema_data->name;

		add_lakehouse_scopes(lakehouse_id);
		add_lakehouse_scopes(lakehouse_name);
	}

	add_scope(base_https + "/Tables");
	add_scope(base_abfss + "/Tables");

	auto secret = make_uniq<KeyValueSecret>(scopes, "http", "config", workspace_id);
	secret->secret_map["bearer_token"] = Value(token);
	secret->redact_keys = {"bearer_token"};

	const BaseSecret *base_ptr = secret.release();
	unique_ptr<const BaseSecret> entry(base_ptr);
	secret_manager.RegisterSecret(transaction, std::move(entry), OnCreateConflict::REPLACE_ON_CONFLICT,
	                              SecretPersistType::TEMPORARY, SecretManager::TEMPORARY_STORAGE_NAME);
}

} // namespace duckdb
