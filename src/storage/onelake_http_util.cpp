#include "storage/onelake_http_util.hpp"

#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "onelake_api.hpp"
#include "onelake_logging.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
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
	if (credentials.provider != OneLakeCredentialProvider::ServicePrincipal) {
		return;
	}
	SetEnvIfUnset("AZURE_CLIENT_ID", credentials.client_id);
	SetEnvIfUnset("AZURE_TENANT_ID", credentials.tenant_id);
	SetEnvIfUnset("AZURE_CLIENT_SECRET", credentials.client_secret);
}

} // namespace

void EnsureHttpBearerSecret(ClientContext &context, OneLakeCatalog &catalog, const OneLakeSchemaEntry *schema_entry) {
	// Delta kernel defers to Azure default credentials; seed env vars so our service principal is used.
	EnsureAzureClientCredentialEnv(catalog.GetCredentials());
	ONELAKE_LOG_DEBUG(&context, "[delta] Ensuring HTTP bearer secret for workspace=%s",
	                  catalog.GetWorkspaceId().c_str());

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	auto &credentials = catalog.GetCredentials();
	auto token = OneLakeAPI::GetAccessToken(&context, credentials, OneLakeTokenAudience::OneLakeDfs);
	ONELAKE_LOG_DEBUG(&context, "[delta] Retrieved DFS token (length=%llu)", static_cast<long long>(token.size()));

	string workspace_id = catalog.GetWorkspaceId();
	duckdb::vector<string> dfs_scopes;
	duckdb::vector<string> blob_scopes;
	std::unordered_set<string> dfs_seen;
	std::unordered_set<string> blob_seen;
	auto push_scope = [&](duckdb::vector<string> &target, std::unordered_set<string> &target_seen,
	                      const string &scope) {
		if (scope.empty()) {
			return;
		}
		if (!target_seen.insert(scope).second) {
			return;
		}
		target.push_back(scope);
	};
	auto add_scope = [&](const string &scope) {
		if (scope.empty()) {
			return;
		}
		auto lowered = StringUtil::Lower(scope);
		if (lowered.find("onelake.blob.") != string::npos) {
			push_scope(blob_scopes, blob_seen, scope);
		} else {
			push_scope(dfs_scopes, dfs_seen, scope);
		}
	};

	string base_https_dfs = "https://onelake.dfs.fabric.microsoft.com/" + workspace_id;
	string base_https_blob = "https://onelake.blob.fabric.microsoft.com/" + workspace_id;
	string base_abfss = "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com";
	add_scope(base_https_dfs);
	add_scope(base_https_dfs + "/");
	add_scope(base_https_blob);
	add_scope(base_https_blob + "/");
	add_scope(base_abfss);
	add_scope(base_abfss + "/");

	auto add_lakehouse_scopes = [&](const string &identifier) {
		if (identifier.empty()) {
			return;
		}
		add_scope(base_https_dfs + "/" + identifier);
		add_scope(base_https_dfs + "/" + identifier + "/");
		add_scope(base_https_dfs + "/" + identifier + ".Lakehouse");
		add_scope(base_https_dfs + "/" + identifier + ".Lakehouse/");
		add_scope(base_https_blob + "/" + identifier);
		add_scope(base_https_blob + "/" + identifier + "/");
		add_scope(base_https_blob + "/" + identifier + ".Lakehouse");
		add_scope(base_https_blob + "/" + identifier + ".Lakehouse/");
		add_scope(base_abfss + "/" + identifier);
		add_scope(base_abfss + "/" + identifier + "/");
		add_scope(base_abfss + "/" + identifier + ".Lakehouse");
		add_scope(base_abfss + "/" + identifier + ".Lakehouse/");
		add_scope(base_https_dfs + "/" + identifier + "/Tables");
		add_scope(base_https_blob + "/" + identifier + "/Tables");
		add_scope(base_abfss + "/" + identifier + "/Tables");
	};

	if (schema_entry && schema_entry->schema_data) {
		const auto &lakehouse_id = schema_entry->schema_data->id;
		const auto &lakehouse_name = schema_entry->schema_data->name;

		add_lakehouse_scopes(lakehouse_id);
		add_lakehouse_scopes(lakehouse_name);
	}

	add_scope(base_https_dfs + "/Tables");
	add_scope(base_https_blob + "/Tables");
	add_scope(base_abfss + "/Tables");

	auto register_secret = [&](const duckdb::vector<string> &scope_list, OneLakeTokenAudience audience,
	                           const string &suffix) {
		if (scope_list.empty()) {
			return;
		}
		auto token_value = OneLakeAPI::GetAccessToken(&context, credentials, audience);
		ONELAKE_LOG_DEBUG(&context, "[delta] Registering %s HTTP bearer secret with %llu scope entries",
		                  suffix.empty() ? "dfs" : suffix.c_str(), static_cast<long long>(scope_list.size()));
		auto secret = make_uniq<KeyValueSecret>(scope_list, "http", "config", workspace_id + suffix);
		secret->secret_map["bearer_token"] = Value(token_value);
		secret->redact_keys = {"bearer_token"};

		const BaseSecret *base_ptr = secret.release();
		unique_ptr<const BaseSecret> entry(base_ptr);
		secret_manager.RegisterSecret(transaction, std::move(entry), OnCreateConflict::REPLACE_ON_CONFLICT,
		                              SecretPersistType::TEMPORARY, SecretManager::TEMPORARY_STORAGE_NAME);
	};

	register_secret(dfs_scopes, OneLakeTokenAudience::OneLakeDfs, "");
	register_secret(blob_scopes, OneLakeTokenAudience::OneLakeBlob, "_blob");
	ONELAKE_LOG_INFO(&context, "[delta] HTTP bearer secrets prepared for workspace=%s", workspace_id.c_str());
}

} // namespace duckdb
