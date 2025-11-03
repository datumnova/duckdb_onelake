#include "onelake_secret.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static unique_ptr<BaseSecret> CreateOneLakeSecretFunction(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("onelake://");
	}

	// Create key-value secret with OneLake credentials
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.name, input.storage_type);

	// Required parameters
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "tenant_id") {
			secret->secret_map["tenant_id"] = named_param.second;
		} else if (lower_name == "client_id") {
			secret->secret_map["client_id"] = named_param.second;
		} else if (lower_name == "client_secret") {
			secret->secret_map["client_secret"] = named_param.second;
		} else {
			throw InvalidInputException("Unknown OneLake secret parameter: %s", named_param.first);
		}
	}

	// Validate required parameters
	if (secret->secret_map.find("tenant_id") == secret->secret_map.end()) {
		throw InvalidInputException("OneLake secret requires 'tenant_id' parameter");
	}
	if (secret->secret_map.find("client_id") == secret->secret_map.end()) {
		throw InvalidInputException("OneLake secret requires 'client_id' parameter");
	}
	if (secret->secret_map.find("client_secret") == secret->secret_map.end()) {
		throw InvalidInputException("OneLake secret requires 'client_secret' parameter");
	}

	return std::move(secret);
}

static void SetOneLakeSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["tenant_id"] = LogicalType::VARCHAR;
	function.named_parameters["client_id"] = LogicalType::VARCHAR;
	function.named_parameters["client_secret"] = LogicalType::VARCHAR;
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
}

} // namespace duckdb
