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

} // namespace duckdb
