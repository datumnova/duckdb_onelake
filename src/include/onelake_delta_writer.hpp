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
};

class OneLakeDeltaWriter {
public:
	//! Converts the provided chunk into Arrow IPC and forwards it to the Rust delta writer.
	static void Append(ClientContext &context, DataChunk &chunk, const OneLakeDeltaWriteRequest &request);
};

} // namespace duckdb
