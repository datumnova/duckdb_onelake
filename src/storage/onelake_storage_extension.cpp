#include "storage/onelake_storage_extension.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_transaction_manager.hpp"
#include "onelake_credentials.hpp"
#include "onelake_api.hpp"
#include "onelake_secret.hpp"
#include "onelake_logging.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include <vector>

namespace duckdb {

namespace {

struct ParsedAttachTarget {
	string workspace_token;
	string lakehouse_token;
	bool has_lakehouse = false;
};

static bool LooksLikeGuid(const string &value) {
	if (value.size() != 36) {
		return false;
	}
	for (idx_t i = 0; i < value.size(); i++) {
		char ch = value[i];
		switch (i) {
		case 8:
		case 13:
		case 18:
		case 23:
			if (ch != '-') {
				return false;
			}
			break;
		default:
			if (!StringUtil::CharacterIsHex(ch)) {
				return false;
			}
		}
	}
	return true;
}

static string NormalizeComparisonToken(const string &value) {
	string lower = StringUtil::Lower(value);
	string normalized;
	normalized.reserve(lower.size());
	for (char ch : lower) {
		if (StringUtil::CharacterIsAlphaNumeric(ch)) {
			normalized.push_back(ch);
		}
	}
	return normalized;
}

static string NormalizeLakehouseToken(string token) {
	StringUtil::Trim(token);
	if (token.empty()) {
		throw InvalidInputException("Lakehouse name in ATTACH path must not be empty");
	}
	const string suffix = ".lakehouse";
	if (token.size() >= suffix.size()) {
		auto tail = token.substr(token.size() - suffix.size());
		if (StringUtil::CIEquals(tail, suffix)) {
			token = token.substr(0, token.size() - suffix.size());
			StringUtil::Trim(token);
			if (token.empty()) {
				throw InvalidInputException(
				    "Lakehouse name in ATTACH path cannot consist solely of the '.Lakehouse' suffix");
			}
		}
	}
	return token;
}

static ParsedAttachTarget ParseAttachTarget(const string &raw_identifier) {
	if (raw_identifier.empty()) {
		throw InvalidInputException("OneLake ATTACH path must be in format 'workspace-name/lakehouse-name.Lakehouse'");
	}
	string working = raw_identifier;
	StringUtil::Trim(working);
	if (working.empty()) {
		throw InvalidInputException("OneLake ATTACH path must be in format 'workspace-name/lakehouse-name.Lakehouse'");
	}

	// Find the required slash separator
	auto slash_pos = working.find('/');
	if (slash_pos == string::npos) {
		throw InvalidInputException("OneLake ATTACH path must be in format 'workspace-name/lakehouse-name.Lakehouse'");
	}

	string workspace_part = working.substr(0, slash_pos);
	string lakehouse_part = working.substr(slash_pos + 1);

	// Check for additional path segments (not allowed)
	if (lakehouse_part.find('/') != string::npos) {
		throw InvalidInputException("OneLake ATTACH path must be in format 'workspace-name/lakehouse-name.Lakehouse'");
	}

	StringUtil::Trim(workspace_part);
	StringUtil::Trim(lakehouse_part);

	if (workspace_part.empty() || lakehouse_part.empty()) {
		throw InvalidInputException("OneLake ATTACH path must be in format 'workspace-name/lakehouse-name.Lakehouse'");
	}

	ParsedAttachTarget result;
	result.workspace_token = workspace_part;
	result.lakehouse_token = NormalizeLakehouseToken(lakehouse_part);
	result.has_lakehouse = true;

	return result;
}

static OneLakeCredentials ExtractCredentialsFromSecret(const BaseSecret &secret) {
	const auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&secret);
	if (!kv_secret) {
		throw InvalidInputException("OneLake secret must be a key-value secret");
	}

	OneLakeCredentials credentials;
	string provider = "service_principal";
	Value provider_value;
	if (kv_secret->TryGetValue("provider", provider_value) && !provider_value.IsNull()) {
		provider = StringUtil::Lower(provider_value.ToString());
	}

	if (provider == "credential_chain") {
		credentials.provider = OneLakeCredentialProvider::CredentialChain;
		Value chain_value;
		if (!kv_secret->TryGetValue("chain", chain_value) || chain_value.IsNull()) {
			throw InvalidInputException("OneLake credential_chain secret must contain chain");
		}
		string chain_str = chain_value.ToString();
		credentials.credential_chain = NormalizeOneLakeCredentialChain(chain_str);
		if (credentials.credential_chain.empty()) {
			throw InvalidInputException("OneLake credential_chain secret must contain chain");
		}
		Value env_var_value;
		if (kv_secret->TryGetValue("env_token_variable", env_var_value) && !env_var_value.IsNull()) {
			auto env_var = env_var_value.ToString();
			StringUtil::Trim(env_var);
			if (!env_var.empty()) {
				credentials.env_storage_variable = env_var;
				credentials.env_fabric_variable = env_var;
			}
		}
		Value env_fabric_value;
		if (kv_secret->TryGetValue("env_fabric_token_variable", env_fabric_value) && !env_fabric_value.IsNull()) {
			auto env_var = env_fabric_value.ToString();
			StringUtil::Trim(env_var);
			if (!env_var.empty()) {
				credentials.env_fabric_variable = env_var;
			}
		}
		Value env_storage_value;
		if (kv_secret->TryGetValue("env_storage_token_variable", env_storage_value) && !env_storage_value.IsNull()) {
			auto env_var = env_storage_value.ToString();
			StringUtil::Trim(env_var);
			if (!env_var.empty()) {
				credentials.env_storage_variable = env_var;
			}
		}
		if (credentials.env_fabric_variable.empty()) {
			credentials.env_fabric_variable = ONELAKE_DEFAULT_ENV_FABRIC_TOKEN_VARIABLE;
		}
		if (credentials.env_storage_variable.empty()) {
			credentials.env_storage_variable = ONELAKE_DEFAULT_ENV_STORAGE_TOKEN_VARIABLE;
		}
	} else if (provider == "service_principal") {
		credentials.provider = OneLakeCredentialProvider::ServicePrincipal;
		Value tenant_id, client_id, client_secret;
		if (!kv_secret->TryGetValue("tenant_id", tenant_id) || !kv_secret->TryGetValue("client_id", client_id) ||
		    !kv_secret->TryGetValue("client_secret", client_secret)) {
			throw InvalidInputException("OneLake secret must contain tenant_id, client_id, and client_secret");
		}
		credentials.tenant_id = tenant_id.ToString();
		credentials.client_id = client_id.ToString();
		credentials.client_secret = client_secret.ToString();
	} else {
		throw InvalidInputException("Unsupported OneLake secret provider: %s", provider);
	}

	if (!credentials.IsValid()) {
		throw InvalidInputException("Invalid OneLake credentials provided");
	}

	return credentials;
}

static OneLakeWorkspace ResolveWorkspace(ClientContext &context, OneLakeCredentials &credentials,
                                         const string &workspace_token) {
	string token = workspace_token;
	StringUtil::Trim(token);
	if (token.empty()) {
		throw InvalidInputException("OneLake workspace identifier must not be empty");
	}

	if (LooksLikeGuid(token)) {
		OneLakeWorkspace workspace;
		workspace.id = token;
		workspace.name = token;
		workspace.display_name = token;
		return workspace;
	}

	auto workspaces = OneLakeAPI::GetWorkspaces(context, credentials);
	if (workspaces.empty()) {
		throw InvalidInputException("Workspace '%s' not found. No accessible workspaces were returned", token);
	}

	auto normalized_token = NormalizeComparisonToken(token);
	vector<string> available;
	available.reserve(workspaces.size());

	for (auto &workspace : workspaces) {
		const string &label = workspace.display_name.empty() ? (workspace.name.empty() ? workspace.id : workspace.name)
		                                                     : workspace.display_name;
		available.push_back(label);

		if (!workspace.id.empty() && StringUtil::CIEquals(workspace.id, token)) {
			return workspace;
		}
		if (!workspace.display_name.empty() && StringUtil::CIEquals(workspace.display_name, token)) {
			return workspace;
		}
		if (!workspace.name.empty() && StringUtil::CIEquals(workspace.name, token)) {
			return workspace;
		}
		string normalized_candidate =
		    NormalizeComparisonToken(workspace.display_name.empty() ? workspace.name : workspace.display_name);
		if (!normalized_candidate.empty() && normalized_candidate == normalized_token) {
			return workspace;
		}
	}

	string available_str;
	if (available.empty()) {
		available_str = "<none>";
	} else {
		available_str = StringUtil::Join(available, ", ");
	}
	throw InvalidInputException("Workspace '%s' not found. Available workspaces: %s", token, available_str);
}

} // namespace

unique_ptr<Catalog> OneLakeStorageExtension::AttachInternal(optional_ptr<StorageExtensionInfo> storage_info,
                                                            ClientContext &context, AttachedDatabase &db,
                                                            const string &name, AttachInfo &info,
                                                            AttachOptions &attach_options) {
	auto &secret_manager = SecretManager::Get(context);

	// Parse workspace and lakehouse from ATTACH target (required format: workspace/lakehouse.Lakehouse)
	string attach_target = info.path.empty() ? name : info.path;
	auto parsed_target = ParseAttachTarget(attach_target);
	string workspace_token = parsed_target.workspace_token;
	string default_lakehouse = parsed_target.lakehouse_token;
	ONELAKE_LOG_INFO(&context, "[attach] Resolving workspace='%s' lakehouse='%s'", workspace_token.c_str(),
	                 default_lakehouse.c_str());

	// Check for unsupported options
	for (auto &option : info.options) {
		if (StringUtil::CIEquals(option.first, "default_lakehouse")) {
			throw InvalidInputException("DEFAULT_LAKEHOUSE option is not supported. Specify lakehouse in ATTACH path: "
			                            "'workspace-name/lakehouse-name.Lakehouse'");
		}
	}

	// Try to auto-create secrets from environment variables on every attach
	auto env_secret_status = TryAutoCreateSecretsFromEnv(context);

	// Resolve credentials using the most specific secret available
	auto catalog_transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	const auto secret_candidate_path = "onelake://" + workspace_token;
	auto secret_match = secret_manager.LookupSecret(catalog_transaction, secret_candidate_path, "onelake");
	if (!secret_match.HasMatch()) {
		secret_match = secret_manager.LookupSecret(catalog_transaction, "onelake://", "onelake");
	}
	if (!secret_match.HasMatch()) {
		if (env_secret_status.onelake_missing_token) {
			const string variable_name = env_secret_status.onelake_variable.empty()
			                                 ? string(ONELAKE_DEFAULT_ENV_FABRIC_TOKEN_VARIABLE)
			                                 : env_secret_status.onelake_variable;
			throw InvalidInputException(
			    "No OneLake secret found because environment variable '%s' is unset or empty. "
			    "Export a Fabric token via 'export %s=...' (or SET VARIABLE) or create a secret manually with: "
			    "CREATE SECRET (TYPE ONELAKE, ...)",
			    variable_name.c_str(), variable_name.c_str());
		}
		throw InvalidInputException("No OneLake secret found. Create a secret with: CREATE SECRET (TYPE ONELAKE, ...)");
	}

	OneLakeCredentials credentials = ExtractCredentialsFromSecret(secret_match.GetSecret());
	OneLakeWorkspace resolved_workspace = ResolveWorkspace(context, credentials, workspace_token);
	string workspace_id = resolved_workspace.id;
	ONELAKE_LOG_INFO(&context, "[attach] Workspace resolved to id=%s", workspace_id.c_str());

	if (!StringUtil::CIEquals(workspace_id, workspace_token)) {
		const auto refined_secret_path = "onelake://" + workspace_id;
		auto refined_match = secret_manager.LookupSecret(catalog_transaction, refined_secret_path, "onelake");
		if (refined_match.HasMatch() && refined_match.GetSecret().GetName() != secret_match.GetSecret().GetName()) {
			credentials = ExtractCredentialsFromSecret(refined_match.GetSecret());
			secret_match = refined_match;
		}
	}

	return make_uniq<OneLakeCatalog>(db, workspace_id, name, std::move(credentials), std::move(default_lakehouse));
}

unique_ptr<TransactionManager>
OneLakeStorageExtension::CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                                  Catalog &catalog) {
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
	return make_uniq<OneLakeTransactionManager>(db, onelake_catalog);
}

} // namespace duckdb
