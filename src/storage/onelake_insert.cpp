#include "storage/onelake_insert.hpp"

#include "onelake_api.hpp"
#include "onelake_delta_writer.hpp"
#include "onelake_logging.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_path_util.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <cstdlib>
#include <mutex>

namespace duckdb {
using namespace duckdb_yyjson; // NOLINT

namespace {

struct OneLakeInsertGlobalState final : public GlobalSinkState {
	OneLakeInsertGlobalState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(context, types), insert_count(0) {
	}

	ColumnDataCollection collection;
	std::mutex append_lock;
	idx_t insert_count;
};

struct OneLakeInsertSourceState final : public GlobalSourceState {
	bool emitted = false;
};

string SerializeTokenJson(const string &token) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	if (!token.empty()) {
		yyjson_mut_obj_add_str(doc, root, "storageToken", token.c_str());
	}
	char *buffer = yyjson_mut_write(doc, 0, nullptr);
	string result = buffer ? string(buffer) : string();
	if (buffer) {
		free(buffer);
	}
	yyjson_mut_doc_free(doc);
	return result;
}

string SerializeWriteOptions(const vector<string> &partition_columns) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_str(doc, root, "mode", "append");
	if (!partition_columns.empty()) {
		auto *arr = yyjson_mut_arr(doc);
		for (auto &col : partition_columns) {
			yyjson_mut_arr_add_strcpy(doc, arr, col.c_str());
		}
		yyjson_mut_obj_add(root, yyjson_mut_strcpy(doc, "partitionColumns"), arr);
	}
	char *buffer = yyjson_mut_write(doc, 0, nullptr);
	string result = buffer ? string(buffer) : string();
	if (buffer) {
		free(buffer);
	}
	yyjson_mut_doc_free(doc);
	return result;
}

string ResolveTableUri(ClientContext &context, OneLakeCatalog &catalog, OneLakeTableEntry &table_entry) {
	auto &schema_entry = table_entry.ParentSchema().Cast<OneLakeSchemaEntry>();
	auto cached_path = table_entry.GetCachedResolvedPath();
	auto candidates = BuildLocationCandidates(catalog, schema_entry, table_entry, cached_path);
	if (candidates.empty()) {
		throw InvalidInputException("Unable to resolve storage location for OneLake table '%s'", table_entry.name);
	}
	auto is_abfs = [](const string &candidate) {
		return IsValidAbfssPath(candidate);
	};
	std::stable_partition(candidates.begin(), candidates.end(), is_abfs);
	for (auto &candidate : candidates) {
		if (IsValidAbfssPath(candidate)) {
			table_entry.RememberResolvedPath(candidate);
			return candidate;
		}
	}
	ONELAKE_LOG_WARN(&context, "[delta] Falling back to non-abfss path for writes: %s", candidates.front().c_str());
	ONELAKE_LOG_WARN(&context, "[delta] %s", GetAbfssPathDiagnostic(candidates.front()).c_str());
	table_entry.RememberResolvedPath(candidates.front());
	return candidates.front();
}

OneLakeDeltaWriteRequest BuildWriteRequest(ClientContext &context, OneLakeCatalog &catalog,
                                           OneLakeTableEntry &table_entry) {
	OneLakeDeltaWriteRequest request;
	request.table_uri = ResolveTableUri(context, catalog, table_entry);
	request.token_json = SerializeTokenJson(
	    OneLakeAPI::GetAccessToken(&context, catalog.GetCredentials(), OneLakeTokenAudience::OneLakeDfs));
	request.options_json = SerializeWriteOptions(table_entry.GetPartitionColumns());
	request.column_names = table_entry.GetColumns().GetColumnNames();
	return request;
}

} // namespace

PhysicalOneLakeInsert::PhysicalOneLakeInsert(PhysicalPlan &plan, OneLakeTableEntry &table_entry_p,
                                             OneLakeCatalog &catalog_p, vector<LogicalType> types,
                                             idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_entry(table_entry_p), catalog(catalog_p) {
}

unique_ptr<GlobalSinkState> PhysicalOneLakeInsert::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!children.empty());
	return make_uniq<OneLakeInsertGlobalState>(context, children[0].get().types);
}

unique_ptr<LocalSinkState> PhysicalOneLakeInsert::GetLocalSinkState(ExecutionContext &) const {
	return make_uniq<LocalSinkState>();
}

SinkResultType PhysicalOneLakeInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &state) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &gstate = state.global_state.Cast<OneLakeInsertGlobalState>();
	std::lock_guard<std::mutex> guard(gstate.append_lock);
	gstate.collection.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalOneLakeInsert::Finalize(Pipeline &, Event &, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<OneLakeInsertGlobalState>();
	gstate.insert_count = gstate.collection.Count();
	if (gstate.insert_count == 0) {
		return SinkFinalizeType::READY;
	}
	OneLakeDeltaWriteRequest request = BuildWriteRequest(context, catalog, table_entry);
	for (auto &chunk : gstate.collection.Chunks()) {
		chunk.Flatten();
		OneLakeDeltaWriter::Append(context, chunk, request);
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalOneLakeInsert::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<OneLakeInsertSourceState>();
}

SourceResultType PhysicalOneLakeInsert::GetData(ExecutionContext &, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<OneLakeInsertSourceState>();
	if (state.emitted) {
		chunk.SetCardinality(0);
		return SourceResultType::FINISHED;
	}
	auto &gstate = sink_state->Cast<OneLakeInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	state.emitted = true;
	return SourceResultType::FINISHED;
}

} // namespace duckdb
