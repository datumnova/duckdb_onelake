#include "onelake_extension.hpp"
#include "storage/onelake_storage_extension.hpp"
#include "onelake_secret.hpp"
#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
#include "onelake_parser_extension.hpp"
#endif
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	RegisterOneLakeSecret(loader);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	if (config.extension_parameters.find(ONELAKE_ENV_FABRIC_TOKEN_OPTION) == config.extension_parameters.end()) {
		config.AddExtensionOption(ONELAKE_ENV_FABRIC_TOKEN_OPTION,
		                          "Environment variable name that stores the Fabric API access token",
		                          LogicalType::VARCHAR, Value(ONELAKE_DEFAULT_ENV_FABRIC_TOKEN_VARIABLE));
	}
	if (config.extension_parameters.find(ONELAKE_ENV_STORAGE_TOKEN_OPTION) == config.extension_parameters.end()) {
		config.AddExtensionOption(ONELAKE_ENV_STORAGE_TOKEN_OPTION,
		                          "Environment variable name that stores the OneLake storage access token",
		                          LogicalType::VARCHAR, Value(ONELAKE_DEFAULT_ENV_STORAGE_TOKEN_VARIABLE));
	}
	config.storage_extensions["onelake"] = make_uniq<OneLakeStorageExtension>();
#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
	config.parser_extensions.push_back(CreateOneLakeParserExtension());
#endif

	ExtensionHelper::TryAutoLoadExtension(loader.GetDatabaseInstance(), "httpfs");
	ExtensionHelper::TryAutoLoadExtension(loader.GetDatabaseInstance(), "delta");
}

void OnelakeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string OnelakeExtension::Name() {
	return "onelake";
}

std::string OnelakeExtension::Version() const {
#ifdef EXT_VERSION_ONELAKE
	return EXT_VERSION_ONELAKE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(onelake, loader) {
	duckdb::LoadInternal(loader);
}
}
