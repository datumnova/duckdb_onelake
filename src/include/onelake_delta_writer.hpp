#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"

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
	string write_mode = "append";  // append|overwrite|error_if_exists|ignore
	string schema_mode;            // empty|merge|overwrite
	string replace_where;          // SQL predicate for conditional replace
	
	// Phase 1: Advanced options
	bool safe_cast = false;
	idx_t target_file_size = 0;    // 0 = use default
	idx_t write_batch_size = 0;    // 0 = use default
};

class OneLakeDeltaWriter {
public:
	//! Converts the provided chunk into Arrow IPC and forwards it to the Rust delta writer.
	static void Append(ClientContext &context, DataChunk &chunk, const OneLakeDeltaWriteRequest &request);
};

} // namespace duckdb
