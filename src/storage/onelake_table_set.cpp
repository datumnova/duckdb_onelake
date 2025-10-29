#include "onelake_api.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_table_set.hpp"
#include "storage/onelake_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

OneLakeTableSet::OneLakeTableSet(OneLakeSchemaEntry &schema) : OneLakeInSchemaSet(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, const OneLakeTable &table_info) {
    // For now, create a generic string column - in practice you'd get the actual schema
    // from OneLake table metadata or Delta table schema
    return ColumnDefinition("data", LogicalType::VARCHAR);
}

void OneLakeTableSet::LoadEntries(ClientContext &context) {
    auto &transaction = OneLakeTransaction::Get(context, catalog);
    auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
    
    // Get the lakehouse ID from the schema data
    if (!schema.schema_data) {
        throw InternalException("Schema data not available for OneLake schema");
    }
    
    auto lakehouse_id = schema.schema_data->id;
    
    // Get tables from the OneLake API
    auto tables = OneLakeAPI::GetTables(context, onelake_catalog.GetWorkspaceId(), lakehouse_id, onelake_catalog.GetCredentials());
    
    for (auto &table : tables) {
        CreateTableInfo info;
        // For now, create a simple schema - in practice you'd get the actual schema from Delta metadata
        info.columns.AddColumn(ColumnDefinition("data", LogicalType::VARCHAR));
        
        info.table = table.name;
        auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, info);
        table_entry->table_data = make_uniq<OneLakeTable>(table);
        
        CreateEntry(std::move(table_entry));
    }
}

optional_ptr<CatalogEntry> OneLakeTableSet::RefreshTable(ClientContext &context, const string &table_name) {
    auto table_info = GetTableInfo(context, table_name);
    auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, *table_info);
    auto table_ptr = table_entry.get();
    CreateEntry(std::move(table_entry));
    return table_ptr;
}

unique_ptr<OneLakeTableInfo> OneLakeTableSet::GetTableInfo(ClientContext &context, const string &table_name) {
    auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();

    if (!schema.schema_data) {
        throw InternalException("Schema data not available for OneLake schema");
    }

    auto lakehouse_id = schema.schema_data->id;

    // Get detailed table info from OneLake API
    auto table_info_api = OneLakeAPI::GetTableInfo(context, onelake_catalog.GetWorkspaceId(), lakehouse_id, table_name, onelake_catalog.GetCredentials());

    auto table_info = make_uniq<OneLakeTableInfo>();
    table_info->create_info->table = table_name;
    table_info->name = table_info_api.name;
    table_info->format = table_info_api.format;
    table_info->location = table_info_api.location;
    table_info->partition_columns = table_info_api.partition_columns;

    // For now, create a simple schema - in practice you'd parse the Delta table schema
    table_info->create_info->columns.AddColumn(ColumnDefinition("data", LogicalType::VARCHAR));

    return table_info;
}

optional_ptr<CatalogEntry> OneLakeTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
    throw NotImplementedException("OneLake table creation not supported - tables are managed through Fabric");
}

void OneLakeTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
    throw NotImplementedException("OneLake table renaming not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
    throw NotImplementedException("OneLake column renaming not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
    throw NotImplementedException("OneLake add column not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
    throw NotImplementedException("OneLake remove column not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
    switch (alter.alter_table_type) {
    case AlterTableType::RENAME_TABLE:
        AlterTable(context, alter.Cast<RenameTableInfo>());
        break;
    case AlterTableType::RENAME_COLUMN:
        AlterTable(context, alter.Cast<RenameColumnInfo>());
        break;
    case AlterTableType::ADD_COLUMN:
        AlterTable(context, alter.Cast<AddColumnInfo>());
        break;
    case AlterTableType::REMOVE_COLUMN:
        AlterTable(context, alter.Cast<RemoveColumnInfo>());
        break;
    default:
        throw NotImplementedException("OneLake alter table operation not supported");
    }
}

} // namespace duckdb