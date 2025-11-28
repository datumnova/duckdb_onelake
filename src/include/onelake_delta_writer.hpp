#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/column_definition.hpp"

#include <vector>

namespace duckdb {
class ClientContext;
class DataChunk;

struct OneLakeDeltaWriteRequest {
	string table_uri;
	string token_json;
	string options_json;
	vector<string> column_names;

	// Phase 1: Write mode configuration
	string write_mode = "append"; // append|overwrite|error_if_exists|ignore
	string schema_mode;           // empty|merge|overwrite
	string replace_where;         // SQL predicate for conditional replace

	// Phase 1: Advanced options
	bool safe_cast = false;
	idx_t target_file_size = 0; // 0 = use default
	idx_t write_batch_size = 0; // 0 = use default
};

struct OneLakeDeltaDeleteMetrics {
	idx_t rows_deleted = 0;
	idx_t files_removed = 0;
	idx_t files_added = 0;
};

struct OneLakeDeltaUpdateMetrics {
	idx_t rows_updated = 0;
	idx_t files_removed = 0;
	idx_t files_added = 0;
};

class OneLakeDeltaWriter {
public:
	//! Converts the provided chunk into Arrow IPC and forwards it to the Rust delta writer.
	static void Append(ClientContext &context, DataChunk &chunk, const OneLakeDeltaWriteRequest &request);

	//! Creates a new Delta table in OneLake storage with the specified schema.
	static void CreateTable(ClientContext &context, const string &table_uri, const vector<ColumnDefinition> &columns,
	                        const string &token_json, const string &options_json);

	//! Drops (deletes) a Delta table from OneLake storage.
	//! WARNING: This is a destructive operation!
	static void DropTable(ClientContext &context, const string &table_uri, const string &token_json,
	                      const string &options_json);

	//! Deletes rows from a Delta table matching the specified predicate.
	//! Returns metrics about the operation (rows deleted, files affected).
	//! WARNING: This is a destructive operation!
	static OneLakeDeltaDeleteMetrics Delete(ClientContext &context, const string &table_uri, const string &predicate,
	                                        const string &token_json);

	//! Updates rows in a Delta table matching the specified predicate.
	//! Returns metrics about the operation (rows updated, files affected).
	//! WARNING: This is a destructive operation!
	static OneLakeDeltaUpdateMetrics Update(ClientContext &context, const string &table_uri, const string &predicate,
	                                        const string &updates_json, const string &token_json);
};

} // namespace duckdb
