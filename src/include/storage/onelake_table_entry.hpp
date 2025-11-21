#pragma once
#include "onelake_types.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types.hpp"
#include <mutex>

#include <vector>

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

	void SetPartitionColumns(vector<string> columns);
	const vector<string> &GetPartitionColumns() const {
		return partition_columns;
	}
	void SetCreateMetadata(unique_ptr<OneLakeCreateTableMetadata> metadata);
	OneLakeCreateTableMetadata *GetCreateMetadata();
	const OneLakeCreateTableMetadata *GetCreateMetadata() const;

	string GetCachedResolvedPath() const;
	void RememberResolvedPath(const string &path);

private:
	void UpdateColumnDefinitions(const vector<string> &names, const vector<LogicalType> &types);

private:
	mutable std::mutex bind_lock;
	vector<string> partition_columns;
	string resolved_path;
	unique_ptr<OneLakeCreateTableMetadata> create_metadata;
};

} // namespace duckdb
