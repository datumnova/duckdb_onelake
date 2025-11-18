#include "onelake_secret.hpp"
#include "onelake_credentials.hpp"
#include "onelake_logging.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/config.hpp"
#include <cstdlib>
#include <exception>
#include <mutex>

namespace duckdb {

namespace {

string GetOptionValueOrDefault(ClientContext &context, const string &option_name, const string &fallback) {
	auto &config = DBConfig::GetConfig(context);
	std::lock_guard<std::mutex> lock(config.config_lock);
	auto entry = config.options.set_variables.find(option_name);
	if (entry != config.options.set_variables.end() && !entry->second.IsNull()) {
		auto value = entry->second.ToString();
		StringUtil::Trim(value);
		if (!value.empty()) {
			return value;
		}
	}
	return fallback;
}

struct EnvTokenVariableConfig {
	string fabric;
	string storage;
	string blob;
};

string TryGetSetVariableValue(ClientContext &context, const string &variable_name) {
	auto &config = DBConfig::GetConfig(context);
	std::lock_guard<std::mutex> lock(config.config_lock);
	auto entry = config.options.set_variables.find(variable_name);
	if (entry != config.options.set_variables.end() && !entry->second.IsNull()) {
		auto value = entry->second.ToString();
		StringUtil::Trim(value);
		return value;
	}
	return string();
}

EnvTokenVariableConfig GetEnvTokenVariableConfig(ClientContext &context) {
	EnvTokenVariableConfig config;
	config.fabric =
	    GetOptionValueOrDefault(context, ONELAKE_ENV_FABRIC_TOKEN_OPTION, ONELAKE_DEFAULT_ENV_FABRIC_TOKEN_VARIABLE);
	config.storage =
	    GetOptionValueOrDefault(context, ONELAKE_ENV_STORAGE_TOKEN_OPTION, ONELAKE_DEFAULT_ENV_STORAGE_TOKEN_VARIABLE);
	config.blob =
	    GetOptionValueOrDefault(context, ONELAKE_ENV_BLOB_TOKEN_OPTION, ONELAKE_DEFAULT_ENV_BLOB_TOKEN_VARIABLE);
	return config;
}

void TryAutoCreateAzureEnvSecret(ClientContext &context, const CreateSecretInput &input,
                                 const EnvTokenVariableConfig &env_config) {
	const string variable_name =
	    env_config.storage.empty() ? string(ONELAKE_DEFAULT_ENV_STORAGE_TOKEN_VARIABLE) : env_config.storage;
	auto token_value = ResolveTokenFromContextOrEnv(&context, variable_name);
	if (token_value.empty()) {
		ONELAKE_LOG_INFO(&context, "[secrets] Skipping Azure env secret auto-creation because '%s' is unset or empty",
		                 variable_name.c_str());
		return;
	}

	CreateSecretInput azure_input;
	azure_input.type = "azure";
	azure_input.provider = "access_token";
	azure_input.storage_type = input.storage_type;
	azure_input.name = "env_secret";
	azure_input.options["access_token"] = Value(token_value);
	azure_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	azure_input.persist_type = input.persist_type;

	try {
		SecretManager::Get(context).CreateSecret(context, azure_input);
		ONELAKE_LOG_INFO(&context, "[secrets] Auto-created Azure access token secret '%s' using variable '%s'",
		                 azure_input.name.c_str(), variable_name.c_str());
	} catch (const Exception &ex) {
		ONELAKE_LOG_WARN(&context, "[secrets] Failed to auto-create Azure env secret: %s", ex.what());
	} catch (const std::exception &ex) {
		ONELAKE_LOG_WARN(&context, "[secrets] Failed to auto-create Azure env secret: %s", ex.what());
	}
}

} // namespace

string ResolveTokenFromContextOrEnv(ClientContext *context, const string &variable_name) {
	auto trimmed_name = variable_name;
	StringUtil::Trim(trimmed_name);
	if (trimmed_name.empty()) {
		return string();
	}
	if (context) {
		auto value = TryGetSetVariableValue(*context, trimmed_name);
		if (!value.empty()) {
			return value;
		}
	}
	const char *env_value = std::getenv(trimmed_name.c_str());
	if (!env_value) {
		return string();
	}
	string token = env_value;
	StringUtil::Trim(token);
	return token;
}

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

		auto chain_parts = ParseOneLakeCredentialChain(chain_value);
		if (chain_parts.empty()) {
			throw InvalidInputException("OneLake credential_chain secret requires a non-empty 'chain' value");
		}
		for (auto &part : chain_parts) {
			if (part != "cli" && part != "env") {
				throw InvalidInputException("Unknown OneLake credential chain step '%s'. Supported values: cli, env",
				                            part);
			}
		}
		secret->secret_map["chain"] = Value(NormalizeOneLakeCredentialChain(chain_value));
		auto env_config = GetEnvTokenVariableConfig(context);
		secret->secret_map["env_fabric_token_variable"] = Value(env_config.fabric);
		secret->secret_map["env_storage_token_variable"] = Value(env_config.storage);
		secret->secret_map["env_blob_token_variable"] = Value(env_config.blob);
		if (chain_parts.size() == 1 && chain_parts[0] == "env") {
			TryAutoCreateAzureEnvSecret(context, input, env_config);
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
