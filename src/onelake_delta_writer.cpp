#include "onelake_delta_writer.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/common/arrow/arrow.hpp"

#include "onelake_delta_writer_c_api.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <unordered_map>

namespace duckdb {

namespace {

class SingleBatchStreamHolder {
public:
	SingleBatchStreamHolder(ClientContext &context, DataChunk &chunk, const vector<string> &column_names)
	    : types(chunk.GetTypes()), properties(), batch_returned(false) {
		properties.client_context = &context;
		if (!column_names.empty() && column_names.size() == types.size()) {
			names = column_names;
		} else {
			names.reserve(types.size());
			for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
				names.push_back("col" + std::to_string(col_idx));
			}
		}

		unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_type_cast;
		ArrowConverter::ToArrowArray(chunk, &array, properties, extension_type_cast);

		stream.private_data = this;
		stream.get_schema = GetSchema;
		stream.get_next = GetNext;
		stream.release = Release;
		stream.get_last_error = GetLastError;
	}

	SingleBatchStreamHolder(const SingleBatchStreamHolder &) = delete;
	SingleBatchStreamHolder &operator=(const SingleBatchStreamHolder &) = delete;

	~SingleBatchStreamHolder() {
		if (array.release) {
			array.release(&array);
		}
	}

	ArrowArrayStream *GetStream() {
		return &stream;
	}

	void Close() {
		if (stream.release) {
			stream.release(&stream);
		}
	}

private:
	static int GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *schema) {
		if (!stream || !schema || !stream->private_data) {
			return EINVAL;
		}
		auto &holder = *reinterpret_cast<SingleBatchStreamHolder *>(stream->private_data);
		auto schema_options = holder.properties;
		schema_options.client_context = holder.properties.client_context;
		try {
			ArrowConverter::ToArrowSchema(schema, holder.types, holder.names, schema_options);
		} catch (const Exception &ex) {
			holder.last_error = ex.what();
			return EIO;
		}
		return 0;
	}

	static int GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
		if (!stream || !out || !stream->private_data) {
			return EINVAL;
		}
		auto &holder = *reinterpret_cast<SingleBatchStreamHolder *>(stream->private_data);
		if (holder.batch_returned) {
			std::memset(out, 0, sizeof(*out));
			return 0;
		}
		*out = holder.array;
		holder.array.release = nullptr;
		holder.batch_returned = true;
		return 0;
	}

	static const char *GetLastError(struct ArrowArrayStream *stream) {
		if (!stream || !stream->private_data) {
			return nullptr;
		}
		auto &holder = *reinterpret_cast<SingleBatchStreamHolder *>(stream->private_data);
		return holder.last_error.empty() ? nullptr : holder.last_error.c_str();
	}

	static void Release(struct ArrowArrayStream *stream) {
		if (!stream || !stream->private_data) {
			return;
		}
		auto &holder = *reinterpret_cast<SingleBatchStreamHolder *>(stream->private_data);
		if (holder.array.release) {
			holder.array.release(&holder.array);
		}
		stream->private_data = nullptr;
		stream->get_schema = nullptr;
		stream->get_next = nullptr;
		stream->get_last_error = nullptr;
		stream->release = nullptr;
	}

private:
	ArrowArrayStream stream {};
	ArrowArray array {};
	vector<LogicalType> types;
	vector<string> names;
	ClientProperties properties;
	bool batch_returned;
	string last_error;
};

} // namespace

void OneLakeDeltaWriter::Append(ClientContext &context, DataChunk &chunk, const OneLakeDeltaWriteRequest &request) {
	if (chunk.size() == 0) {
		return;
	}

	SingleBatchStreamHolder stream_holder(context, chunk, request.column_names);
	std::array<char, 1024> error_buffer {};

	const char *table_uri = request.table_uri.c_str();
	const char *token_json = request.token_json.empty() ? "" : request.token_json.c_str();
	const char *options_json = request.options_json.empty() ? "" : request.options_json.c_str();

	const auto status = ol_delta_append(stream_holder.GetStream(), table_uri, token_json, options_json,
	                                    error_buffer.data(), error_buffer.size());
	stream_holder.Close();

	if (status != static_cast<int>(OlDeltaStatus::Ok)) {
		string error_msg = error_buffer[0] ? string(error_buffer.data()) : "Unknown delta writer error";
		throw IOException("Delta writer failed: %s", error_msg.c_str());
	}
}

void OneLakeDeltaWriter::CreateTable(ClientContext &context, const string &table_uri,
                                     const vector<ColumnDefinition> &columns, const string &token_json,
                                     const string &options_json) {
	// Convert DuckDB column definitions to Arrow schema JSON
	// We'll use ArrowConverter to get proper Arrow type mappings
	vector<LogicalType> types;
	vector<string> names;
	types.reserve(columns.size());
	names.reserve(columns.size());

	for (const auto &col : columns) {
		types.push_back(col.Type());
		names.push_back(col.GetName());
	}

	// Create a minimal Arrow schema using ArrowConverter
	ArrowSchema arrow_schema;
	auto client_props = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_props);

	// Convert ArrowSchema to JSON - serialize to match Arrow's Field format
	string schema_json = "{\"fields\":[";
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0)
			schema_json += ",";

		const auto &col = columns[i];
		const auto &type = col.Type();

		schema_json += "{\"name\":\"" + col.GetName() + "\",";

		// Map DuckDB types to Arrow DataType enum strings
		schema_json += "\"data_type\":";
		switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			schema_json += "\"Boolean\"";
			break;
		case LogicalTypeId::TINYINT:
			schema_json += "\"Int8\"";
			break;
		case LogicalTypeId::SMALLINT:
			schema_json += "\"Int16\"";
			break;
		case LogicalTypeId::INTEGER:
			schema_json += "\"Int32\"";
			break;
		case LogicalTypeId::BIGINT:
			schema_json += "\"Int64\"";
			break;
		case LogicalTypeId::FLOAT:
			schema_json += "\"Float32\"";
			break;
		case LogicalTypeId::DOUBLE:
			schema_json += "\"Float64\"";
			break;
		case LogicalTypeId::VARCHAR:
			schema_json += "\"Utf8\"";
			break;
		case LogicalTypeId::DATE:
			schema_json += "\"Date32\"";
			break;
		case LogicalTypeId::TIMESTAMP:
			schema_json += "{\"Timestamp\":[\"Microsecond\",null]}";
			break;
		case LogicalTypeId::DECIMAL: {
			auto width = DecimalType::GetWidth(type);
			auto scale = DecimalType::GetScale(type);
			schema_json += "{\"Decimal\":[" + to_string(width) + "," + to_string(scale) + ",128]}";
			break;
		}
		default:
			// Fallback to string for unsupported types
			schema_json += "\"Utf8\"";
			break;
		}

		schema_json += ",\"nullable\":true";
		schema_json += ",\"dict_id\":0";
		schema_json += ",\"dict_is_ordered\":false";
		schema_json += ",\"metadata\":{}";
		schema_json += "}";
	}
	schema_json += "],\"metadata\":{}}";

	// Clean up the Arrow schema
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}

	std::array<char, 1024> error_buffer {};

	const char *table_uri_cstr = table_uri.c_str();
	const char *schema_json_cstr = schema_json.c_str();
	const char *token_json_cstr = token_json.empty() ? "" : token_json.c_str();
	const char *options_json_cstr = options_json.empty() ? "" : options_json.c_str();

	const auto status = ol_delta_create_table(table_uri_cstr, schema_json_cstr, token_json_cstr, options_json_cstr,
	                                          error_buffer.data(), error_buffer.size());

	if (status != static_cast<int>(OlDeltaStatus::Ok)) {
		string error_msg = error_buffer[0] ? string(error_buffer.data()) : "Unknown table creation error";
		throw IOException("Delta table creation failed: %s", error_msg.c_str());
	}
}

void OneLakeDeltaWriter::DropTable(ClientContext &context, const string &table_uri, const string &token_json,
                                   const string &options_json) {
	std::array<char, 4096> error_buffer {};

	const char *table_uri_cstr = table_uri.c_str();
	const char *token_json_cstr = token_json.empty() ? "" : token_json.c_str();
	const char *options_json_cstr = options_json.empty() ? "{\"deleteData\":true}" : options_json.c_str();

	const auto status = ol_delta_drop_table(table_uri_cstr, token_json_cstr, options_json_cstr, error_buffer.data(),
	                                        error_buffer.size());

	if (status != static_cast<int>(OlDeltaStatus::Ok)) {
		string error_msg = error_buffer[0] ? string(error_buffer.data()) : "Unknown drop table error";
		throw IOException("Delta table drop failed: %s", error_msg.c_str());
	}
}

OneLakeDeltaDeleteMetrics OneLakeDeltaWriter::Delete(ClientContext &context, const string &table_uri,
                                                     const string &predicate, const string &token_json) {
	std::array<char, 4096> error_buffer {};

	uint64_t rows_deleted = 0;
	uint64_t files_removed = 0;
	uint64_t files_added = 0;

	const char *table_uri_cstr = table_uri.c_str();
	const char *predicate_cstr = predicate.c_str();
	const char *token_json_cstr = token_json.empty() ? "" : token_json.c_str();

	const auto status = ol_delta_delete(table_uri_cstr, predicate_cstr, token_json_cstr, error_buffer.data(),
	                                    error_buffer.size(), &rows_deleted, &files_removed, &files_added);

	if (status != static_cast<int>(OlDeltaStatus::Ok)) {
		string error_msg = error_buffer[0] ? string(error_buffer.data()) : "Unknown delete error";
		throw IOException("Delta DELETE failed: %s", error_msg.c_str());
	}

	OneLakeDeltaDeleteMetrics metrics;
	metrics.rows_deleted = static_cast<idx_t>(rows_deleted);
	metrics.files_removed = static_cast<idx_t>(files_removed);
	metrics.files_added = static_cast<idx_t>(files_added);

	return metrics;
}

OneLakeDeltaUpdateMetrics OneLakeDeltaWriter::Update(ClientContext &context, const string &table_uri,
                                                     const string &predicate, const string &updates_json,
                                                     const string &token_json) {
	std::array<char, 4096> error_buffer {};

	uint64_t rows_updated = 0;
	uint64_t files_removed = 0;
	uint64_t files_added = 0;

	const char *table_uri_cstr = table_uri.c_str();
	const char *predicate_cstr = predicate.c_str();
	const char *updates_json_cstr = updates_json.c_str();
	const char *token_json_cstr = token_json.empty() ? "" : token_json.c_str();

	const auto status =
	    ol_delta_update(table_uri_cstr, predicate_cstr, updates_json_cstr, token_json_cstr, error_buffer.data(),
	                    error_buffer.size(), &rows_updated, &files_removed, &files_added);

	if (status != static_cast<int>(OlDeltaStatus::Ok)) {
		string error_msg = error_buffer[0] ? string(error_buffer.data()) : "Unknown update error";
		throw IOException("Delta UPDATE failed: %s", error_msg.c_str());
	}

	OneLakeDeltaUpdateMetrics metrics;
	metrics.rows_updated = static_cast<idx_t>(rows_updated);
	metrics.files_removed = static_cast<idx_t>(files_removed);
	metrics.files_added = static_cast<idx_t>(files_added);

	return metrics;
}

} // namespace duckdb
