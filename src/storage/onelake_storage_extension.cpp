#include "storage/onelake_storage_extension.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_transaction_manager.hpp"
#include "onelake_credentials.hpp"
#include "onelake_api.hpp"
#include "onelake_secret.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include <iostream>
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
		throw InvalidInputException("OneLake workspace identifier must be provided in ATTACH statement");
	}
	string working = raw_identifier;
	StringUtil::Trim(working);
	if (working.empty()) {
		throw InvalidInputException("OneLake workspace identifier must not be empty");
	}

	const string scope_prefix = "onelake://";
	if (StringUtil::StartsWith(working, scope_prefix)) {
		working = working.substr(scope_prefix.size());
		StringUtil::Trim(working);
		if (working.empty()) {
			throw InvalidInputException("OneLake workspace identifier must not be empty");
		}
	}

	ParsedAttachTarget result;
	auto slash_pos = working.find('/');
	string workspace_part = working;
	string remainder;
	if (slash_pos != string::npos) {
		workspace_part = working.substr(0, slash_pos);
		remainder = working.substr(slash_pos + 1);
	}

	StringUtil::Trim(workspace_part);
	if (workspace_part.empty()) {
		throw InvalidInputException("OneLake workspace identifier must not be empty");
	}
	result.workspace_token = workspace_part;

	if (!remainder.empty()) {
		// Only consider the first path segment after the workspace token
		auto second_slash = remainder.find('/');
		if (second_slash != string::npos) {
			remainder = remainder.substr(0, second_slash);
		}
		StringUtil::Trim(remainder);
		if (!remainder.empty()) {
			result.lakehouse_token = NormalizeLakehouseToken(remainder);
			result.has_lakehouse = true;
		}
	}

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
		StringUtil::Trim(chain_str);
		credentials.credential_chain = StringUtil::Lower(chain_str);
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

	// Determine workspace token and optional lakehouse from ATTACH target
	string attach_target = info.path.empty() ? name : info.path;
	auto parsed_target = ParseAttachTarget(attach_target);
	string workspace_token = parsed_target.workspace_token;
	string default_lakehouse;
	bool default_from_path = false;
	if (parsed_target.has_lakehouse) {
		default_lakehouse = parsed_target.lakehouse_token;
		default_from_path = true;
	}

	for (auto &option : info.options) {
		if (!StringUtil::CIEquals(option.first, "default_lakehouse") || option.second.IsNull()) {
			continue;
		}
		string option_value;
		if (option.second.type().id() == LogicalTypeId::VARCHAR) {
			option_value = StringValue::Get(option.second);
		} else {
			option_value = option.second.ToString();
		}
		StringUtil::Trim(option_value);
		if (default_from_path && !StringUtil::CIEquals(default_lakehouse, option_value)) {
			throw InvalidInputException(
			    "Default lakehouse specified both in ATTACH target and options with conflicting values");
		}
		default_lakehouse = option_value;
		default_from_path = true;
	}

	// Resolve credentials using the most specific secret available
	auto catalog_transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	const auto secret_candidate_path = "onelake://" + workspace_token;
	auto secret_match = secret_manager.LookupSecret(catalog_transaction, secret_candidate_path, "onelake");
	if (!secret_match.HasMatch()) {
		secret_match = secret_manager.LookupSecret(catalog_transaction, "onelake://", "onelake");
	}
	if (!secret_match.HasMatch()) {
		throw InvalidInputException("No OneLake secret found. Create a secret with: CREATE SECRET (TYPE ONELAKE, ...)");
	}

	OneLakeCredentials credentials = ExtractCredentialsFromSecret(secret_match.GetSecret());
	OneLakeWorkspace resolved_workspace = ResolveWorkspace(context, credentials, workspace_token);
	string workspace_id = resolved_workspace.id;

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
