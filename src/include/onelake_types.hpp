#pragma once
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

struct OneLakeLakehouse {
    string id;
    string name;
    string display_name;
    string description;
};

struct OneLakeTable {
    string name;
    string type; // "Table" or "View"
    string format; // "Delta", "Parquet", etc.
    string location;
};

struct OneLakeTableInfo {
    OneLakeTableInfo() {
        create_info = make_uniq<CreateTableInfo>();
    }

    OneLakeTableInfo(const string &schema_name, const string &table_name) {
        create_info = make_uniq<CreateTableInfo>(string(), schema_name, table_name);
    }

    OneLakeTableInfo(const SchemaCatalogEntry &schema, const string &table_name) {
        create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table_name);
    }

    const string &GetTableName() const {
        return create_info->table;
    }

    string name;
    string format;
    string location;
    vector<string> partition_columns;
    unique_ptr<CreateTableInfo> create_info;
};

} // namespace duckdb