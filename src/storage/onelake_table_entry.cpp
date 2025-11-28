#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_transaction.hpp"
#include "storage/onelake_http_util.hpp"
#include "storage/onelake_path_util.hpp"
#include "onelake_api.hpp"
#include "onelake_logging.hpp"

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

namespace duckdb {

namespace {

constexpr char DELTA_FUNCTION_NAME[] = "delta_scan";
constexpr char ICEBERG_FUNCTION_NAME[] = "iceberg_scan";

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

TableFunction ResolveIcebergFunction(ClientContext &context) {
	auto table_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context, INVALID_CATALOG, DEFAULT_SCHEMA, ICEBERG_FUNCTION_NAME, OnEntryNotFound::RETURN_NULL);
	if (!table_entry) {
		ExtensionHelper::AutoLoadExtension(context, "iceberg");
		table_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA,
		                                                           ICEBERG_FUNCTION_NAME, OnEntryNotFound::RETURN_NULL);
	}
	if (!table_entry) {
		throw CatalogException(
		    "The 'iceberg' extension is required to query OneLake tables in Iceberg format. Install it using INSTALL "
		    "iceberg; then LOAD iceberg;");
	}
	if (table_entry->functions.Size() == 0) {
		throw InternalException("iceberg_scan function set is empty");
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
	auto delta_bind = delta_function.bind(context, bind_input, return_types, return_names);
	ONELAKE_LOG_DEBUG(&context, "[delta] delta_scan bind succeeded for path %s with %llu columns", path.c_str(),
	                  static_cast<long long>(return_names.size()));
	return delta_bind;
}

unique_ptr<FunctionData> BindIcebergFunction(ClientContext &context, TableFunction &iceberg_function,
                                             const string &path, vector<LogicalType> &return_types,
                                             vector<string> &return_names) {
	if (!iceberg_function.bind) {
		throw InternalException("iceberg_scan function does not expose a bind callback");
	}
	vector<Value> inputs;
	inputs.emplace_back(Value(path));
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	TableFunctionRef ref;
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(path)));
	ref.function = make_uniq<FunctionExpression>(ICEBERG_FUNCTION_NAME, std::move(children));
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr, nullptr,
	                                  iceberg_function, ref);
	auto iceberg_bind = iceberg_function.bind(context, bind_input, return_types, return_names);
	ONELAKE_LOG_DEBUG(&context, "[delta] iceberg_scan bind succeeded for path %s with %llu columns", path.c_str(),
	                  static_cast<long long>(return_names.size()));
	return iceberg_bind;
}

bool IsDeltaFormat(const string &format) {
	return format.empty() || StringUtil::CIEquals(format, "delta");
}

bool IsIcebergFormat(const string &format) {
	return StringUtil::CIEquals(format, "iceberg");
}

} // namespace

OneLakeTableEntry::OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
	table_data = make_uniq<OneLakeTable>();
	table_data->name = info.table;
	this->details_loaded = false;
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
	this->details_loaded = true;
}

void OneLakeTableEntry::SetPartitionColumns(vector<string> columns) {
	std::lock_guard<std::mutex> guard(bind_lock);
	partition_columns = std::move(columns);
}

void OneLakeTableEntry::SetCreateMetadata(unique_ptr<OneLakeCreateTableMetadata> metadata) {
	std::lock_guard<std::mutex> guard(bind_lock);
	create_metadata = std::move(metadata);
}

OneLakeCreateTableMetadata *OneLakeTableEntry::GetCreateMetadata() {
	std::lock_guard<std::mutex> guard(bind_lock);
	return create_metadata.get();
}

const OneLakeCreateTableMetadata *OneLakeTableEntry::GetCreateMetadata() const {
	std::lock_guard<std::mutex> guard(bind_lock);
	return create_metadata.get();
}

string OneLakeTableEntry::GetCachedResolvedPath() const {
	std::lock_guard<std::mutex> guard(bind_lock);
	return resolved_path;
}

void OneLakeTableEntry::RememberResolvedPath(const string &path) {
	std::lock_guard<std::mutex> guard(bind_lock);
	resolved_path = path;
}

bool OneLakeTableEntry::EnsureColumnDefinitions(ClientContext &context) {
	if (GetColumns().PhysicalColumnCount() > 0) {
		return true;
	}
	unique_ptr<FunctionData> temp_bind_data;
	auto table_function = GetScanFunction(context, temp_bind_data);
	(void)table_function;
	if (GetColumns().PhysicalColumnCount() == 0) {
		throw CatalogException("OneLake table '%s' did not expose any columns after binding", name);
	}
	return true;
}

void OneLakeTableEntry::UpdateColumnDefinitions(const vector<string> &names, const vector<LogicalType> &types) {
	ColumnList new_columns;
	for (idx_t i = 0; i < names.size(); i++) {
		new_columns.AddColumn(ColumnDefinition(names[i], types[i]));
	}
	new_columns.Finalize();
	columns = std::move(new_columns);
}

void OneLakeTableEntry::EnsureDetailsLoaded(ClientContext &context) {
	if (details_loaded) {
		return;
	}
	if (!table_data) {
		return;
	}

	// bool missing_location = table_data->location.empty();
	// bool missing_partitions = partition_columns.empty();

	auto &catalog = ParentCatalog().Cast<OneLakeCatalog>();
	auto &schema_entry = ParentSchema().Cast<OneLakeSchemaEntry>();

	if (!schema_entry.schema_data) {
		return;
	}

	try {
		auto table_info =
		    OneLakeAPI::GetTableInfo(context, catalog.GetWorkspaceId(), *schema_entry.schema_data, schema_entry.name,
		                             name, table_data->format, catalog.GetCredentials());

		if (table_info.has_metadata) {
			if (!table_info.location.empty()) {
				table_data->location = table_info.location;
			}
			if (!table_info.format.empty()) {
				table_data->format = table_info.format;
			}
			SetPartitionColumns(table_info.partition_columns);
		}
	} catch (const Exception &ex) {
		// Log warning but continue - might be able to discover via DFS or guess location
		ONELAKE_LOG_WARN(&context, "[onelake] Lazy detail fetch failed for '%s': %s", name.c_str(), ex.what());
	}

	// Fallback location logic if still empty
	try {
		if (table_data->location.empty()) {
			if (schema_entry.schema_data && schema_entry.schema_data->schema_enabled) {
				table_data->location = "Schemas/" + schema_entry.name + "/Tables/" + name;
			} else if (StringUtil::CIEquals(table_data->format, "iceberg") && !table_data->schema_name.empty()) {
				table_data->location = "Tables/" + table_data->schema_name + "/" + name;
			}
		}
	} catch (const Exception &ex) {
		ONELAKE_LOG_WARN(&context, "[onelake] Failed to retrieve table metadata from OneLake API for '%s': %s",
		                 name.c_str(), ex.what());
	}

	details_loaded = true;
}

unique_ptr<BaseStatistics> OneLakeTableEntry::GetStatistics(ClientContext &, column_t) {
	// OneLake doesn't provide column statistics through standard APIs
	return nullptr;
}

void OneLakeTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                              ClientContext &) {
}

TableFunction OneLakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	std::lock_guard<std::mutex> guard(bind_lock);
	EnsureDetailsLoaded(context);
	auto &catalog = ParentCatalog().Cast<OneLakeCatalog>();
	auto &schema_entry = ParentSchema().Cast<OneLakeSchemaEntry>();
	if (!table_data) {
		throw InternalException("Table metadata missing for OneLake table '%s'", name);
	}
	string table_format = table_data->format;
	if (table_format.empty()) {
		table_format = "delta";
	}

	ExtensionHelper::TryAutoLoadExtension(context, "httpfs");
	EnsureHttpBearerSecret(context, catalog, &schema_entry);
	ONELAKE_LOG_INFO(&context, "[delta] Binding table '%s' (format=%s)", name.c_str(), table_format.c_str());

	if (!IsDeltaFormat(table_format) && !IsIcebergFormat(table_format)) {
		throw InvalidInputException("OneLake table '%s' uses unsupported format '%s'", name, table_format);
	}

	// Optimization: Try the cached resolved path first to avoid rebuilding candidate lists
	if (!resolved_path.empty()) {
		try {
			if (IsIcebergFormat(table_format)) {
				auto iceberg_function = ResolveIcebergFunction(context);
				vector<LogicalType> return_types;
				vector<string> return_names;
				auto iceberg_bind =
				    BindIcebergFunction(context, iceberg_function, resolved_path, return_types, return_names);
				UpdateColumnDefinitions(return_names, return_types);
				bind_data = std::move(iceberg_bind);
				return iceberg_function;
			} else {
				auto delta_function = ResolveDeltaFunction(context);
				vector<LogicalType> return_types;
				vector<string> return_names;
				auto delta_bind = BindDeltaFunction(context, delta_function, resolved_path, return_types, return_names);
				UpdateColumnDefinitions(return_names, return_types);
				bind_data = std::move(delta_bind);
				return delta_function;
			}
		} catch (const Exception &ex) {
			ONELAKE_LOG_WARN(&context, "[onelake] Cached path '%s' failed: %s. Falling back to discovery.",
			                 resolved_path.c_str(), ex.what());
		}
	}

	auto candidate_paths = BuildLocationCandidates(catalog, schema_entry, *this, resolved_path);

	if (IsIcebergFormat(table_format)) {
		auto iceberg_function = ResolveIcebergFunction(context);

		auto is_abfs = [](const string &candidate) {
			return StringUtil::StartsWith(candidate, "abfss://");
		};
		std::stable_partition(candidate_paths.begin(), candidate_paths.end(), is_abfs);

		vector<string> errors;
		for (auto &candidate : candidate_paths) {
			try {
				vector<LogicalType> return_types;
				vector<string> return_names;
				auto iceberg_bind =
				    BindIcebergFunction(context, iceberg_function, candidate, return_types, return_names);
				UpdateColumnDefinitions(return_names, return_types);
				resolved_path = candidate;
				bind_data = std::move(iceberg_bind);
				return iceberg_function;
			} catch (const Exception &ex) {
				errors.push_back(StringUtil::Format("%s (path=%s)", ex.what(), candidate));
			}
		}

		string error_summary = errors.empty() ? "no candidate paths resolved" : StringUtil::Join(errors, "; ");
		throw IOException("Failed to bind Iceberg scan for OneLake table '%s'. Errors: %s", name, error_summary);
	}

	auto delta_function = ResolveDeltaFunction(context);

	auto is_abfs = [](const string &candidate) {
		return StringUtil::StartsWith(candidate, "abfs://") || StringUtil::StartsWith(candidate, "abfss://");
	};
	std::stable_partition(candidate_paths.begin(), candidate_paths.end(), is_abfs);
	ONELAKE_LOG_DEBUG(&context, "[delta] Candidate paths for '%s': %s", name.c_str(),
	                  StringUtil::Join(candidate_paths, ", ").c_str());

	vector<string> errors;
	for (auto &candidate : candidate_paths) {
		ONELAKE_LOG_DEBUG(&context, "[delta] Trying path %s", candidate.c_str());
		try {
			vector<LogicalType> return_types;
			vector<string> return_names;
			auto delta_bind = BindDeltaFunction(context, delta_function, candidate, return_types, return_names);
			UpdateColumnDefinitions(return_names, return_types);
			resolved_path = candidate;
			bind_data = std::move(delta_bind);
			return delta_function;
		} catch (const Exception &ex) {
			string path_info = GetAbfssPathDiagnostic(candidate);
			errors.push_back(StringUtil::Format("%s (%s)", ex.what(), path_info));
			ONELAKE_LOG_WARN(&context, "[delta] Path failed: %s (%s)", ex.what(), candidate.c_str());
		}
	}

	string error_summary = errors.empty() ? "no candidate paths resolved" : StringUtil::Join(errors, "; ");
	ONELAKE_LOG_ERROR(&context, "[delta] Failed to bind table '%s': %s", name.c_str(), error_summary.c_str());
	throw IOException("Failed to bind Delta scan for OneLake table '%s'. Errors: %s", name, error_summary);
}

TableStorageInfo OneLakeTableEntry::GetStorageInfo(ClientContext &) {
	TableStorageInfo result;
	result.cardinality = 0; // Unknown cardinality
	return result;
}

} // namespace duckdb
