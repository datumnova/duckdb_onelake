#include "onelake_secret.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

static unique_ptr<BaseSecret> CreateOneLakeSecretFunction(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("onelake://");
	}

	// Create key-value secret with OneLake credentials
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.name, input.storage_type);

	string provider = input.provider.empty() ? "config" : StringUtil::Lower(input.provider);
	string auth_mode;
	string chain_value;
	bool chain_explicit = false;

	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "tenant_id") {
			secret->secret_map["tenant_id"] = named_param.second;
		} else if (lower_name == "client_id") {
			secret->secret_map["client_id"] = named_param.second;
		} else if (lower_name == "client_secret") {
			secret->secret_map["client_secret"] = named_param.second;
		} else if (lower_name == "chain") {
			chain_value = StringValue::Get(named_param.second);
			secret->secret_map["chain"] = named_param.second;
			chain_explicit = true;
		} else {
			throw InvalidInputException("Unknown OneLake secret parameter: %s", named_param.first);
		}
	}

	if (provider.empty() || provider == "config" || provider == "service_principal") {
		auth_mode = "service_principal";
		secret->secret_map["provider"] = Value(auth_mode);
		if (secret->secret_map.find("tenant_id") == secret->secret_map.end()) {
			throw InvalidInputException("OneLake service_principal secret requires 'tenant_id' parameter");
		}
		if (secret->secret_map.find("client_id") == secret->secret_map.end()) {
			throw InvalidInputException("OneLake service_principal secret requires 'client_id' parameter");
		}
		if (secret->secret_map.find("client_secret") == secret->secret_map.end()) {
			throw InvalidInputException("OneLake service_principal secret requires 'client_secret' parameter");
		}
	} else if (provider == "credential_chain") {
		auth_mode = "credential_chain";
		secret->secret_map["provider"] = Value(auth_mode);
		if (!chain_explicit) {
			throw InvalidInputException("OneLake credential_chain secret requires 'chain' parameter");
		}

		auto lowered_chain = StringUtil::Lower(chain_value);
		auto chain_parts = StringUtil::Split(lowered_chain, ';');
		bool has_cli = false;
		for (auto &part : chain_parts) {
			StringUtil::Trim(part);
			std::string trimmed = part;
			if (trimmed == "cli") {
				has_cli = true;
				break;
			}
		}
		if (!has_cli) {
			throw InvalidInputException("OneLake credential_chain secret 'chain' must include 'cli'");
		}
	} else {
		throw InvalidInputException("Unsupported OneLake secret provider: %s", provider);
	}

	return std::move(secret);
}

static void SetOneLakeSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["tenant_id"] = LogicalType::VARCHAR;
	function.named_parameters["client_id"] = LogicalType::VARCHAR;
	function.named_parameters["client_secret"] = LogicalType::VARCHAR;
	function.named_parameters["chain"] = LogicalType::VARCHAR;
}

void RegisterOneLakeSecret(ExtensionLoader &loader) {
	// Register the OneLake secret type
	SecretType secret_type;
	secret_type.name = "onelake";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	// Register the secret creation function
	CreateSecretFunction onelake_secret_function = {"onelake", "config", CreateOneLakeSecretFunction};
	SetOneLakeSecretParameters(onelake_secret_function);
	loader.RegisterFunction(onelake_secret_function);

	CreateSecretFunction onelake_chain_function = {"onelake", "credential_chain", CreateOneLakeSecretFunction};
	SetOneLakeSecretParameters(onelake_chain_function);
	loader.RegisterFunction(onelake_chain_function);
}

} // namespace duckdb
