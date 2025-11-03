#pragma once
#include "duckdb/catalog/catalog.hpp"
#include "storage/onelake_schema_set.hpp"
#include "onelake_credentials.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

class OneLakeSchemaEntry;

class OneLakeCatalog : public Catalog {
public:
	explicit OneLakeCatalog(AttachedDatabase &db_p, const string &workspace_id, const string &catalog_name,
	                        OneLakeCredentials credentials, string default_schema = string());
	~OneLakeCatalog();

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
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	void ClearCache();

	// Getters for OneLake-specific properties
	const string &GetWorkspaceId() const {
		return workspace_id;
	}
	OneLakeCredentials &GetCredentials() {
		return credentials;
	}
	const OneLakeCredentials &GetCredentials() const {
		return credentials;
	}
	const string &GetConfiguredDefaultSchema() const {
		return default_schema;
	}
	bool HasDefaultSchema() const;
	void SetDefaultSchema(const string &schema_name);
	string GetDefaultSchema() const override;

private:
	OneLakeSchemaSet schemas;
	string default_schema;
};

} // namespace duckdb
