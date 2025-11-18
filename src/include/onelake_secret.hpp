#pragma once
#include "duckdb.hpp"

namespace duckdb {

inline constexpr const char *ONELAKE_ENV_FABRIC_TOKEN_OPTION = "onelake_env_fabric_token_variable";
inline constexpr const char *ONELAKE_ENV_STORAGE_TOKEN_OPTION = "onelake_env_storage_token_variable";
inline constexpr const char *ONELAKE_ENV_BLOB_TOKEN_OPTION = "onelake_env_blob_token_variable";

inline constexpr const char *ONELAKE_DEFAULT_ENV_FABRIC_TOKEN_VARIABLE = "FABRIC_API_TOKEN";
inline constexpr const char *ONELAKE_DEFAULT_ENV_STORAGE_TOKEN_VARIABLE = "AZURE_STORAGE_TOKEN";
inline constexpr const char *ONELAKE_DEFAULT_ENV_BLOB_TOKEN_VARIABLE = "ONELAKE_BLOB_TOKEN";

void RegisterOneLakeSecret(ExtensionLoader &loader);

string ResolveTokenFromContextOrEnv(ClientContext *context, const string &variable_name);

} // namespace duckdb
