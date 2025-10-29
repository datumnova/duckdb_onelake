#pragma once
#include "storage/onelake_catalog_set.hpp"
#include "storage/onelake_table_entry.hpp"

namespace duckdb {

struct CreateTableInfo;
class OneLakeSchemaEntry;

class OneLakeTableSet : public OneLakeInSchemaSet {
public:
    explicit OneLakeTableSet(OneLakeSchemaEntry &schema);

public:
    optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

    unique_ptr<OneLakeTableInfo> GetTableInfo(ClientContext &context, const string &table_name);
    optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

    void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
    void LoadEntries(ClientContext &context) override;

    void AlterTable(ClientContext &context, RenameTableInfo &info);
    void AlterTable(ClientContext &context, RenameColumnInfo &info);
    void AlterTable(ClientContext &context, AddColumnInfo &info);
    void AlterTable(ClientContext &context, RemoveColumnInfo &info);
};

} // namespace duckdb