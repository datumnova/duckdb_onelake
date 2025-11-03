#include "onelake_extension.hpp"
#include "storage/onelake_storage_extension.hpp"
#include "onelake_secret.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    // Register OneLake secret type
    RegisterOneLakeSecret(loader);
    
    // Register storage extension for catalog functionality
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    config.storage_extensions["onelake"] = make_uniq<OneLakeStorageExtension>();

    // Try to auto-load dependencies used for Delta access
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