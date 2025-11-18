#pragma once
#include "duckdb.hpp"

namespace duckdb {

inline constexpr const char *ONELAKE_ENV_FABRIC_TOKEN_OPTION = "onelake_env_fabric_token_variable";
inline constexpr const char *ONELAKE_ENV_STORAGE_TOKEN_OPTION = "onelake_env_storage_token_variable";
inline constexpr const char *ONELAKE_DEFAULT_ENV_FABRIC_TOKEN_VARIABLE = "FABRIC_API_TOKEN";
inline constexpr const char *ONELAKE_DEFAULT_ENV_STORAGE_TOKEN_VARIABLE = "AZURE_STORAGE_TOKEN";

struct EnvSecretAttemptResult {
	bool onelake_created = false;
	bool onelake_missing_token = false;
	string onelake_variable;
	bool azure_created = false;
	bool azure_missing_token = false;
	string azure_variable;
};

void RegisterOneLakeSecret(ExtensionLoader &loader);

EnvSecretAttemptResult TryAutoCreateSecretsFromEnv(ClientContext &context);

string ResolveTokenFromContextOrEnv(ClientContext *context, const string &variable_name);

} // namespace duckdb
