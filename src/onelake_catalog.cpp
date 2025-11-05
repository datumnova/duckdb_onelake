#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

OneLakeCatalog::OneLakeCatalog(AttachedDatabase &db_p, const string &workspace_id, const string &catalog_name,
                               OneLakeCredentials credentials, string default_schema_p)
    : Catalog(db_p), workspace_id(workspace_id), access_mode(AccessMode::AUTOMATIC),
      credentials(std::move(credentials)), schemas(*this), default_schema(std::move(default_schema_p)),
      configured_default_preference(default_schema), user_configured_default(!default_schema.empty()) {
}

OneLakeCatalog::~OneLakeCatalog() = default;

void OneLakeCatalog::Initialize(bool load_builtin) {
	// OneLake catalogs don't need built-in schema initialization
}

optional_ptr<CatalogEntry> OneLakeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("OneLake catalog does not support CREATE SCHEMA");
}

void OneLakeCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void OneLakeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<OneLakeSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> OneLakeCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA) {
		if (default_schema.empty() && transaction.HasContext()) {
			auto &context = transaction.GetContext();
			schemas.Scan(context, [&](CatalogEntry &schema) {
				if (default_schema.empty()) {
					default_schema = schema.name;
				}
			});
		}
		if (default_schema.empty()) {
			if (if_not_found == OnEntryNotFound::RETURN_NULL) {
				return nullptr;
			}
			throw InvalidInputException(
			    "Attempting to fetch the default schema - but no default lakehouse was provided in the connection "
			    "string");
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Lakehouse with name \"%s\" not found", schema_lookup.GetEntryName());
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool OneLakeCatalog::InMemory() {
	return false;
}

string OneLakeCatalog::GetDBPath() {
	return workspace_id;
}

DatabaseSize OneLakeCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	// OneLake doesn't provide size information through standard APIs
	return size;
}

void OneLakeCatalog::ClearCache() {
	schemas.ClearEntries();
}

PhysicalOperator &OneLakeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("OneLake catalog does not support CREATE TABLE AS");
}

PhysicalOperator &OneLakeCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("OneLake catalog does not support INSERT operations");
}

PhysicalOperator &OneLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	throw NotImplementedException("OneLake catalog does not support DELETE operations");
}

PhysicalOperator &OneLakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
	throw NotImplementedException("OneLake catalog does not support UPDATE operations");
}

unique_ptr<LogicalOperator> OneLakeCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                            TableCatalogEntry &table,
                                                            unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("OneLake catalog does not support CREATE INDEX operations");
}

bool OneLakeCatalog::HasDefaultSchema() const {
	return !default_schema.empty();
}

void OneLakeCatalog::SetDefaultSchema(const string &schema_name) {
	default_schema = schema_name;
}

string OneLakeCatalog::GetDefaultSchema() const {
	if (default_schema.empty()) {
		return Catalog::GetDefaultSchema();
	}
	return default_schema;
}

} // namespace duckdb
