#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_transaction.hpp"
#include "storage/onelake_http_util.hpp"
#include "onelake_api.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include <algorithm>
#include <unordered_set>

namespace duckdb {

namespace {

constexpr const char *DELTA_FUNCTION_NAME = "delta_scan";

bool HasScheme(const string &path) {
	return path.find("://") != string::npos;
}

string NormalizeSlashes(const string &path) {
	string result = path;
	std::replace(result.begin(), result.end(), '\\', '/');
	return result;
}

string TrimLeadingSlashes(const string &path) {
	idx_t pos = 0;
	while (pos < path.size() && path[pos] == '/') {
		pos++;
	}
	return path.substr(pos);
}

string JoinPath(const string &lhs, const string &rhs) {
	if (lhs.empty()) {
		return rhs;
	}
	if (rhs.empty()) {
		return lhs;
	}
	bool lhs_slash = StringUtil::EndsWith(lhs, "/");
	bool rhs_slash = StringUtil::StartsWith(rhs, "/");
	if (lhs_slash && rhs_slash) {
		return lhs + rhs.substr(1);
	}
	if (!lhs_slash && !rhs_slash) {
		return lhs + "/" + rhs;
	}
	return lhs + rhs;
}

vector<string> BuildRelativePaths(const string &raw_location, const string &table_name) {
	vector<string> result;
	auto normalized = TrimLeadingSlashes(NormalizeSlashes(raw_location));
	if (!normalized.empty()) {
		result.push_back(normalized);
	}

	vector<string> parts = StringUtil::Split(table_name, '.');
	string default_relative;
	if (parts.empty()) {
		default_relative = "Tables/" + table_name;
	} else {
		default_relative =
		    "Tables/" + StringUtil::Join(parts, parts.size(), "/", [](const string &entry) { return entry; });
	}
	if (std::find(result.begin(), result.end(), default_relative) == result.end()) {
		result.push_back(default_relative);
	}
	return result;
}

string ToHttps(const string &input) {
	const string abfss = "abfss://";
	const string abfs = "abfs://";
	if (StringUtil::StartsWith(input, "https://")) {
		return input;
	}
	string path = input;
	if (StringUtil::StartsWith(path, abfss)) {
		path = path.substr(abfss.size());
	} else if (StringUtil::StartsWith(path, abfs)) {
		path = path.substr(abfs.size());
	} else {
		return string();
	}
	auto at_pos = path.find('@');
	if (at_pos == string::npos) {
		return string();
	}
	auto container = path.substr(0, at_pos);
	auto host_and_path = path.substr(at_pos + 1);
	if (host_and_path.empty()) {
		host_and_path = "onelake.dfs.fabric.microsoft.com";
	}
	string host = host_and_path;
	string tail;
	auto slash_pos = host_and_path.find('/');
	if (slash_pos != string::npos) {
		host = host_and_path.substr(0, slash_pos);
		tail = host_and_path.substr(slash_pos);
	}
	if (!tail.empty() && tail[0] != '/') {
		tail = "/" + tail;
	}
	return "https://" + host + "/" + container + tail;
}

void AddCandidate(vector<string> &candidates, std::unordered_set<string> &seen, const string &candidate,
                  bool prepend = false) {
	if (candidate.empty()) {
		return;
	}
	if (!seen.insert(candidate).second) {
		return;
	}
	if (prepend) {
		candidates.insert(candidates.begin(), candidate);
	} else {
		candidates.push_back(candidate);
	}
}

vector<string> BuildLocationCandidates(const OneLakeCatalog &catalog, const OneLakeSchemaEntry &schema_entry,
                                       const OneLakeTableEntry &table_entry, const string &cached_path) {
	std::unordered_set<string> seen;
	vector<string> candidates;
	AddCandidate(candidates, seen, cached_path);

	string raw_location;
	if (table_entry.table_data && !table_entry.table_data->location.empty()) {
		raw_location = table_entry.table_data->location;
	}
	if (raw_location.empty()) {
		raw_location = table_entry.name;
	}

	auto normalized = NormalizeSlashes(raw_location);
	if (HasScheme(normalized)) {
		AddCandidate(candidates, seen, normalized);
		AddCandidate(candidates, seen, ToHttps(normalized), true);
		return candidates;
	}

	auto relative_paths = BuildRelativePaths(normalized, table_entry.name);
	const string& workspace_id = catalog.GetWorkspaceId();
	vector<string> prefixes;
	std::unordered_set<string> prefix_seen;
	auto add_prefix = [&](const string &value) {
		if (value.empty()) {
			return;
		}
		if (!prefix_seen.insert(value).second) {
			return;
		}
		prefixes.push_back(value);
	};
	string base_prefix = "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com";

	if (schema_entry.schema_data) {
		const auto &lakehouse_id = schema_entry.schema_data->id;
		const auto &lakehouse_name = schema_entry.schema_data->name;
		if (!lakehouse_id.empty()) {
			add_prefix(base_prefix + "/" + lakehouse_id);
			add_prefix(base_prefix + "/" + lakehouse_id + ".Lakehouse");
		}
		if (!lakehouse_name.empty()) {
			add_prefix(base_prefix + "/" + lakehouse_name);
			add_prefix(base_prefix + "/" + lakehouse_name + ".Lakehouse");
		}
	}
	add_prefix(base_prefix);

	for (auto &prefix : prefixes) {
		for (auto &relative : relative_paths) {
			auto joined = JoinPath(prefix, relative);
			AddCandidate(candidates, seen, ToHttps(joined), true);
			AddCandidate(candidates, seen, joined);
		}
	}

	return candidates;
}

TableFunction ResolveDeltaFunction(ClientContext &context) {
	auto table_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA,
	                                                                DELTA_FUNCTION_NAME, OnEntryNotFound::RETURN_NULL);
	if (!table_entry) {
		ExtensionHelper::AutoLoadExtension(context, "delta");
		table_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA,
		                                                           DELTA_FUNCTION_NAME, OnEntryNotFound::RETURN_NULL);
	}
	if (!table_entry) {
		throw CatalogException("The 'delta' extension is required to query OneLake tables. Install it using INSTALL "
		                       "delta; then LOAD delta;");
	}
	if (table_entry->functions.Size() == 0) {
		throw InternalException("delta_scan function set is empty");
	}
	return table_entry->functions.GetFunctionByOffset(0);
}

unique_ptr<FunctionData> BindDeltaFunction(ClientContext &context, TableFunction &delta_function, const string &path,
                                           vector<LogicalType> &return_types, vector<string> &return_names) {
	if (!delta_function.bind) {
		throw InternalException("delta_scan function does not expose a bind callback");
	}
	vector<Value> inputs;
	inputs.emplace_back(Value(path));
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	TableFunctionRef ref;
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(path)));
	ref.function = make_uniq<FunctionExpression>(DELTA_FUNCTION_NAME, std::move(children));
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr, nullptr,
	                                  delta_function, ref);
	return delta_function.bind(context, bind_input, return_types, return_names);
}

bool IsDeltaFormat(const string &format) {
	return format.empty() || StringUtil::CIEquals(format, "delta");
}

} // namespace

OneLakeTableEntry::OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
	table_data = make_uniq<OneLakeTable>();
	table_data->name = info.table;
}

OneLakeTableEntry::OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, OneLakeTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
	table_data = make_uniq<OneLakeTable>();
	table_data->name = info.name.empty() ? info.create_info->table : info.name;
	table_data->format = info.format;
	table_data->location = info.location;
	table_data->type = "Table";
	SetPartitionColumns(info.partition_columns);
}

void OneLakeTableEntry::SetPartitionColumns(vector<string> columns) {
	std::lock_guard<std::mutex> guard(bind_lock);
	partition_columns = std::move(columns);
}

void OneLakeTableEntry::UpdateColumnDefinitions(const vector<string> &names, const vector<LogicalType> &types) {
	ColumnList new_columns;
	for (idx_t i = 0; i < types.size(); i++) {
		ColumnDefinition column(names[i], types[i]);
		new_columns.AddColumn(std::move(column));
	}
	new_columns.Finalize();
	columns = std::move(new_columns);
}

unique_ptr<BaseStatistics> OneLakeTableEntry::GetStatistics(ClientContext &, column_t) {
	// OneLake doesn't provide column statistics through standard APIs
	return nullptr;
}

void OneLakeTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                              ClientContext &) {
	throw NotImplementedException("OneLake tables do not support UPDATE operations");
}

TableFunction OneLakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	std::lock_guard<std::mutex> guard(bind_lock);
	auto &catalog = ParentCatalog().Cast<OneLakeCatalog>();
	auto &schema_entry = ParentSchema().Cast<OneLakeSchemaEntry>();
	if (!table_data) {
		throw InternalException("Table metadata missing for OneLake table '%s'", name);
	}
	if (!IsDeltaFormat(table_data->format)) {
		throw InvalidInputException("OneLake table '%s' uses unsupported format '%s'", name, table_data->format);
	}

	ExtensionHelper::TryAutoLoadExtension(context, "httpfs");
	EnsureHttpBearerSecret(context, catalog, &schema_entry);

	auto delta_function = ResolveDeltaFunction(context);
	auto candidate_paths = BuildLocationCandidates(catalog, schema_entry, *this, resolved_path);

	vector<string> errors;
	for (auto &candidate : candidate_paths) {
		try {
			vector<LogicalType> return_types;
			vector<string> return_names;
			auto delta_bind = BindDeltaFunction(context, delta_function, candidate, return_types, return_names);
			UpdateColumnDefinitions(return_names, return_types);
			resolved_path = candidate;
			bind_data = std::move(delta_bind);
			return delta_function;
		} catch (const Exception &ex) {
			errors.push_back(StringUtil::Format("%s (path=%s)", ex.what(), candidate));
		}
	}

	string error_summary = errors.empty() ? "no candidate paths resolved" : StringUtil::Join(errors, "; ");
	throw IOException("Failed to bind Delta scan for OneLake table '%s'. Errors: %s", name, error_summary);
}

TableStorageInfo OneLakeTableEntry::GetStorageInfo(ClientContext &) {
	TableStorageInfo result;
	result.cardinality = 0; // Unknown cardinality
	return result;
}

} // namespace duckdb
