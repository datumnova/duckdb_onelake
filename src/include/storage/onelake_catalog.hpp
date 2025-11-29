#pragma once
#include "duckdb/catalog/catalog.hpp"
#include "storage/onelake_schema_set.hpp"
#include "onelake_credentials.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

class OneLakeSchemaEntry;

/// @brief Represents a OneLake workspace as a DuckDB Catalog.
///
/// This class maps a Microsoft Fabric Workspace to a DuckDB Catalog (Database).
/// It manages schemas (which map to Lakehouses) and handles authentication and connection details.
class OneLakeCatalog : public Catalog {
public:
	/// @brief Constructs a new OneLakeCatalog.
	/// @param db_p The attached database instance.
	/// @param workspace_id The ID of the OneLake workspace.
	/// @param catalog_name The name of the catalog in DuckDB.
	/// @param credentials The credentials for accessing OneLake.
	/// @param default_schema The default schema (lakehouse) to use.
	explicit OneLakeCatalog(AttachedDatabase &db_p, const string &workspace_id, const string &catalog_name,
	                        OneLakeCredentials credentials, string default_schema = string());
	~OneLakeCatalog() override;

	string workspace_id;
	AccessMode access_mode;
	OneLakeCredentials credentials;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "onelake";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	void ClearCache();

	// Getters for OneLake-specific properties

	/// @brief Gets the OneLake Workspace ID.
	const string &GetWorkspaceId() const {
		return workspace_id;
	}

	/// @brief Gets the credentials used by this catalog.
	OneLakeCredentials &GetCredentials() {
		return credentials;
	}
	const OneLakeCredentials &GetCredentials() const {
		return credentials;
	}
	bool HasUserConfiguredDefault() const {
		return user_configured_default;
	}
	const string &GetConfiguredDefaultPreference() const {
		return configured_default_preference;
	}
	bool HasDefaultSchema() const;
	void SetDefaultSchema(const string &schema_name);
	string GetDefaultSchema() const override;

	/// @brief Sets the hash of the last registered secret to optimize updates.
	void SetLastSecretHash(size_t hash) {
		last_secret_hash = hash;
	}

	/// @brief Gets the hash of the last registered secret.
	size_t GetLastSecretHash() const {
		return last_secret_hash;
	}

private:
	OneLakeSchemaSet schemas;
	string default_schema;
	string configured_default_preference;
	bool user_configured_default;
	size_t last_secret_hash = 0;
};

} // namespace duckdb
