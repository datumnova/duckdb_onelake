#pragma once
#include "onelake_types.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class OneLakeTableEntry : public TableCatalogEntry {
public:
    OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
    OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, OneLakeTableInfo &info);

    unique_ptr<OneLakeTable> table_data;

public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;
    void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                              ClientContext &context) override;
};

} // namespace duckdb