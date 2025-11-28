#include "onelake_api.hpp"
#include "onelake_delta_writer.hpp"
#include "onelake_logging.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_http_util.hpp"
#include "storage/onelake_table_set.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include <algorithm>
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/constants.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include <unordered_set>
#include <memory>

namespace {

using duckdb::ClientContext;
using duckdb::CreateTableInfo;
using duckdb::DefaultLogType;
using duckdb::EnsureHttpBearerSecret;
using duckdb::Exception;
using duckdb::ExtensionHelper;
using duckdb::FunctionData;
using duckdb::idx_t;
using duckdb::InsertionOrderPreservingMap;
using duckdb::Logger;
using duckdb::LogLevel;
using duckdb::make_uniq;
using duckdb::OneLakeAPI;
using duckdb::OneLakeCatalog;
using duckdb::OneLakeSchemaEntry;
using duckdb::OneLakeTableEntry;
using duckdb::OneLakeTableSet;
using duckdb::Printer;
using duckdb::string;
using duckdb::StringUtil;
using duckdb::unique_ptr;
using duckdb::unique_ptr_cast;
using duckdb::vector;
using std::unordered_set;

string GetTagValue(const InsertionOrderPreservingMap<string> &tags, const string &key) {
	auto entry = tags.find(key);
	if (entry == tags.end()) {
		return string();
	}
	return entry->second;
}

vector<string> ParseListValue(const string &value) {
	vector<string> result;
	auto entries = StringUtil::Split(value, ',');
	for (auto &entry : entries) {
		auto trimmed = entry;
		StringUtil::Trim(trimmed);
		if (trimmed.empty()) {
			continue;
		}
		if (std::find(result.begin(), result.end(), trimmed) != result.end()) {
			continue;
		}
		result.push_back(trimmed);
	}
	return result;
}

vector<string> ExtractPartitionColumns(ClientContext &context, const CreateTableInfo &create_info) {
	vector<string> partitions;

	// First check session variable onelake_partition_columns
	duckdb::Value partition_val;
	if (context.TryGetCurrentSetting("onelake_partition_columns", partition_val)) {
		if (!partition_val.IsNull()) {
			string partition_str = partition_val.ToString();
			auto parts = StringUtil::Split(partition_str, ',');
			for (auto &part : parts) {
				StringUtil::Trim(part);
				if (!part.empty()) {
					partitions.push_back(part);
				}
			}
			if (!partitions.empty()) {
				return partitions;
			}
		}
	}

	// Check for ONELAKE_PARTITIONS in comment
	if (!create_info.comment.IsNull()) {
		string comment = create_info.comment.ToString();
		auto pos = comment.find("ONELAKE_PARTITIONS=");
		if (pos != string::npos) {
			pos += 19; // strlen("ONELAKE_PARTITIONS=")
			string partition_str;
			while (pos < comment.size() && comment[pos] != ' ' && comment[pos] != ']') {
				partition_str += comment[pos];
				pos++;
			}
			auto parts = StringUtil::Split(partition_str, ',');
			for (auto &part : parts) {
				StringUtil::Trim(part);
				if (!part.empty()) {
					partitions.push_back(part);
				}
			}
			if (!partitions.empty()) {
				return partitions;
			}
		}
	}

	// Fallback to tags (for compatibility)
	static const vector<string> candidate_keys = {"partition_columns", "partition_by", "onelake_partition_columns"};
	for (const auto &key : candidate_keys) {
		auto raw = GetTagValue(create_info.tags, key);
		if (raw.empty()) {
			continue;
		}
		auto parsed = ParseListValue(raw);
		partitions.insert(partitions.end(), parsed.begin(), parsed.end());
	}
	return partitions;
}

string ExtractExplicitLocation(const CreateTableInfo &create_info) {
	static const vector<string> location_keys = {"location", "path", "onelake_location"};
	for (const auto &key : location_keys) {
		auto value = GetTagValue(create_info.tags, key);
		StringUtil::Trim(value);
		if (!value.empty()) {
			return value;
		}
	}
	return string();
}

string EnsureTrailingSlash(const string &path) {
	if (path.empty()) {
		return "/";
	}
	if (StringUtil::EndsWith(path, "/")) {
		return path;
	}
	return path + "/";
}

vector<string> BuildTableRootCandidates(const OneLakeCatalog &catalog, const OneLakeSchemaEntry &schema_entry) {
	vector<string> result;
	unordered_set<string> seen;
	const string &workspace_id = catalog.GetWorkspaceId();
	string base_prefix = "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com";
	auto add_candidate = [&](const string &candidate) {
		if (candidate.empty()) {
			return;
		}
		auto normalized = EnsureTrailingSlash(candidate);
		if (!seen.insert(normalized).second) {
			return;
		}
		result.push_back(normalized);
	};

	if (schema_entry.schema_data) {
		const auto &lakehouse_id = schema_entry.schema_data->id;
		const auto &lakehouse_name = schema_entry.schema_data->name;
		if (!lakehouse_id.empty()) {
			if (schema_entry.schema_data->schema_enabled) {
				// For schema-enabled lakehouses, tables are under /Schemas/{schema_name}/Tables/
				add_candidate(base_prefix + "/" + lakehouse_id + "/Schemas/" + schema_entry.name + "/Tables");
			} else {
				// For regular lakehouses, tables are under /Tables/
				add_candidate(base_prefix + "/" + lakehouse_id + "/Tables");
			}
		}
	}

	return result;
}

void AddDiscoveredTable(ClientContext &context, OneLakeCatalog &catalog, OneLakeSchemaEntry &schema,
                        OneLakeTableSet &table_set, const string &table_name, const string &format,
                        const string &relative_location) {
	CreateTableInfo info;
	info.table = table_name;
	auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, info);
	table_entry->table_data->format = format.empty() ? "Delta" : format;
	table_entry->table_data->type = "Table";
	if (!relative_location.empty()) {
		table_entry->table_data->location = relative_location;
	} else {
		if (schema.schema_data && schema.schema_data->schema_enabled) {
			table_entry->table_data->location = "Schemas/" + schema.name + "/Tables/" + table_name;
		} else {
			table_entry->table_data->location = "Tables/" + table_name;
		}
	}
	table_set.CreateEntry(std::move(table_entry));
}

idx_t DiscoverTablesFromStorage(ClientContext &context, OneLakeCatalog &catalog, OneLakeSchemaEntry &schema,
                                OneLakeTableSet &table_set, unordered_set<string> &known_tables) {
	ExtensionHelper::TryAutoLoadExtension(context, "httpfs");
	EnsureHttpBearerSecret(context, catalog, &schema);
	auto roots = BuildTableRootCandidates(catalog, schema);
	idx_t discovered = 0;

	for (auto &root : roots) {
		vector<string> entries;
		try {
			entries = OneLakeAPI::ListDirectory(context, root, catalog.GetCredentials());
			// Printer::Print(StringUtil::Format("[onelake] scanned directory '%s' via DFS API (entries=%llu)", root,
			//                                   static_cast<uint64_t>(entries.size())));
		} catch (const Exception &ex) {
			// Printer::Print(StringUtil::Format("[onelake] failed to list '%s': %s", root, ex.what()));
			continue;
		}
		for (auto &leaf : entries) {
			if (leaf == "." || leaf == "..") {
				continue;
			}
			if (leaf.find('/') != string::npos || leaf.find('\\') != string::npos) {
				continue;
			}
			if (StringUtil::StartsWith(leaf, "_")) {
				continue;
			}
			bool has_delta_log = false;
			try {
				string delta_dir = root + leaf;
				if (!StringUtil::EndsWith(delta_dir, "/")) {
					delta_dir += "/";
				}
				delta_dir += "_delta_log/";
				auto delta_entries = OneLakeAPI::ListDirectory(context, delta_dir, catalog.GetCredentials());
				has_delta_log = !delta_entries.empty();
			} catch (const Exception &) {
				// Printer::Print(StringUtil::Format("[onelake] failed to inspect '%s/%s'", root, leaf));
				has_delta_log = false;
			}
			bool has_iceberg_metadata = false;
			if (!has_delta_log) {
				try {
					string table_root = root + leaf;
					if (!StringUtil::EndsWith(table_root, "/")) {
						table_root += "/";
					}
					auto child_dirs = OneLakeAPI::ListDirectory(context, table_root, catalog.GetCredentials());
					for (auto &child : child_dirs) {
						if (StringUtil::CIEquals(child, "metadata")) {
							has_iceberg_metadata = true;
							break;
						}
					}
				} catch (const Exception &) {
					// Printer::Print(StringUtil::Format("[onelake] failed to inspect metadata directory for '%s/%s'",
					// root, leaf));
					has_iceberg_metadata = false;
				}
			}
			if (!has_delta_log && !has_iceberg_metadata) {
				continue;
			}
			if (!known_tables.insert(StringUtil::Lower(leaf)).second) {
				continue;
			}
			string detected_format = has_delta_log ? "Delta" : "iceberg";
			string relative_location;
			if (schema.schema_data && schema.schema_data->schema_enabled) {
				relative_location = "Schemas/" + schema.name + "/Tables/" + leaf;
			} else {
				relative_location = "Tables/" + leaf;
			}
			AddDiscoveredTable(context, catalog, schema, table_set, leaf, detected_format, relative_location);
			discovered++;
			// Printer::Print(
			//     StringUtil::Format("[onelake] registered storage table '%s' from '%s/%s'", leaf, root, leaf));
		}
	}

	return discovered;
}

} // namespace

namespace duckdb {

OneLakeTableSet::OneLakeTableSet(OneLakeSchemaEntry &schema) : OneLakeInSchemaSet(schema) {
}

void OneLakeTableSet::LoadEntries(ClientContext &context) {
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();

	// Get the lakehouse ID from the schema data
	if (!schema.schema_data) {
		throw InternalException("Schema data not available for OneLake schema");
	}

	// Printer::Print(StringUtil::Format("[onelake] loading tables for lakehouse '%s'", schema.schema_data->name));

	// Get tables from the OneLake API
	auto &credentials = onelake_catalog.GetCredentials();
	auto tables = OneLakeAPI::GetTables(context, onelake_catalog.GetWorkspaceId(), *schema.schema_data, credentials);
	std::unordered_set<string> seen_names;
	idx_t api_count = 0;
	delta_detail_supported = true;
	iceberg_detail_supported = true;
	detail_endpoint_reported = false;

	for (auto &table : tables) {
		// For schema-enabled lakehouses, filter tables by current schema
		if (schema.schema_data->schema_enabled && !table.schema_name.empty()) {
			if (table.schema_name != schema.name) {
				continue; // Skip tables from other schemas
			}
		}
		CreateTableInfo info;
		info.table = table.name;
		auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, info);
		table_entry->table_data = make_uniq<OneLakeTable>(table);
		bool is_iceberg_table = StringUtil::CIEquals(table_entry->table_data->format, "iceberg");

		if (table_entry->table_data->location.empty()) {
			if (schema.schema_data && schema.schema_data->schema_enabled) {
				table_entry->table_data->location = "Schemas/" + schema.name + "/Tables/" + table.name;
			} else if (is_iceberg_table && !table_entry->table_data->schema_name.empty()) {
				table_entry->table_data->location = "Tables/" + table_entry->table_data->schema_name + "/" + table.name;
			} else {
				table_entry->table_data->location = "Tables/" + table.name;
			}
		}

		const auto resolved_location = table_entry->table_data->location;
		const auto resolved_format =
		    table_entry->table_data->format.empty() ? "<unknown>" : table_entry->table_data->format;

		CreateEntry(std::move(table_entry));
		seen_names.insert(StringUtil::Lower(table.name));
		api_count++;
		// Printer::Print(StringUtil::Format("[onelake] API table '%s' (format=%s, location=%s)", table.name,
		//                                   resolved_format, resolved_location));
	}

	idx_t storage_count = 0;
	if (api_count == 0) {
		storage_count = DiscoverTablesFromStorage(context, onelake_catalog, schema, *this, seen_names);
	}

	// if (api_count == 0 && storage_count == 0) {
	// 	Printer::Print(StringUtil::Format(
	// 	    "[onelake] no tables discovered for lakehouse '%s'. Verify the Fabric API permissions and that tables "
	// 	    "exist under the 'Tables/' directory.",
	// 	    lakehouse_name));
	// } else {
	// 	// Printer::Print(
	// 	//     StringUtil::Format("[onelake] registered %llu tables for lakehouse '%s' (api=%llu, storage=%llu)",
	// 	//                        static_cast<uint64_t>(api_count + storage_count), lakehouse_name,
	// 	//                        static_cast<uint64_t>(api_count), static_cast<uint64_t>(storage_count)));
	// }
}

optional_ptr<CatalogEntry> OneLakeTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, table_name);
	auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

bool OneLakeTableSet::ShouldRefreshForMissingTable(ClientContext &context, const string &table_name) {
	transaction_t active_query_id = MAXIMUM_QUERY_ID;
	if (context.transaction.HasActiveTransaction()) {
		active_query_id = context.transaction.GetActiveQuery();
	}
	lock_guard<mutex> guard(missing_tables_lock);
	if (missing_tables_query_id != active_query_id) {
		missing_tables.clear();
		missing_tables_query_id = active_query_id;
	}
	auto normalized = StringUtil::Lower(table_name);
	return missing_tables.insert(std::move(normalized)).second;
}

void OneLakeTableSet::EnsureFresh(ClientContext &context) {
	transaction_t active_query_id = MAXIMUM_QUERY_ID;
	if (context.transaction.HasActiveTransaction()) {
		active_query_id = context.transaction.GetActiveQuery();
	}

	// Only treat differing query ids as a refresh trigger when both ids are meaningful.
	// The engine resets the active query id back to MAXIMUM_QUERY_ID once execution finishes,
	// which previously caused us to invalidate the cache repeatedly while SHOW TABLES consumed
	// its result. By ignoring the sentinel value we refresh exactly once per real query id.
	bool query_id_mismatch = false;
	if (active_query_id != MAXIMUM_QUERY_ID) {
		query_id_mismatch = (last_refresh_query_id == MAXIMUM_QUERY_ID) || (last_refresh_query_id != active_query_id);
	}

	bool reload = refresh_forced || !IsLoaded() || query_id_mismatch;
	if (reload) {
		ClearEntries();
		refresh_forced = false;
		last_refresh_query_id = active_query_id;
	}
	EnsureLoaded(context);

	if (active_query_id != MAXIMUM_QUERY_ID) {
		last_refresh_query_id = active_query_id;
	}
}

void OneLakeTableSet::MarkRefreshRequired() {
	refresh_forced = true;
}

unique_ptr<OneLakeTableInfo> OneLakeTableSet::GetTableInfo(ClientContext &context, const string &table_name) {
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();

	if (!schema.schema_data) {
		throw InternalException("Schema data not available for OneLake schema");
	}

	// Get detailed table info from OneLake API
	auto &credentials = onelake_catalog.GetCredentials();
	OneLakeTableInfo table_info_api;
	if ((delta_detail_supported || iceberg_detail_supported) && schema.schema_data) {
		try {
			table_info_api = OneLakeAPI::GetTableInfo(context, onelake_catalog.GetWorkspaceId(), *schema.schema_data,
			                                          schema.name, table_name, string(), credentials);
		} catch (const Exception &ex) {
			(void)ex;
			if (!detail_endpoint_reported) {
				// Printer::Print(StringUtil::Format(
				//     "[onelake] table detail lookup failed for '%s': %s.", table_name, ex.what()));
				detail_endpoint_reported = true;
			}
		}
	}

	auto table_info = make_uniq<OneLakeTableInfo>();
	table_info->create_info->table = table_name;
	table_info->name = table_info_api.name.empty() ? table_name : table_info_api.name;
	table_info->format = table_info_api.format.empty() ? "Delta" : table_info_api.format;
	table_info->location = table_info_api.location;
	if (table_info_api.has_metadata) {
		table_info->partition_columns = table_info_api.partition_columns;
	}
	if (table_info->location.empty()) {
		if (schema.schema_data && schema.schema_data->schema_enabled) {
			table_info->location = "Schemas/" + schema.name + "/Tables/" + table_name;
		} else {
			table_info->location = "Tables/" + table_name;
		}
	}

	// Attempt to hydrate the column definitions by binding through the OneLake table entry logic
	OneLakeTableEntry temp_entry(catalog, schema, *table_info);
	unique_ptr<FunctionData> temp_bind_data;
	auto table_function = temp_entry.GetScanFunction(context, temp_bind_data);
	(void)table_function;
	for (auto &column : temp_entry.GetColumns().Logical()) {
		table_info->create_info->columns.AddColumn(column.Copy());
	}

	return table_info;
}

optional_ptr<CatalogEntry> OneLakeTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	if (base_info.temporary) {
		throw BinderException("TEMPORARY tables are not supported in OneLake catalogs");
	}
	EnsureFresh(context);
	auto existing_entry = GetEntry(context, base_info.table);
	if (existing_entry) {
		switch (base_info.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return existing_entry;
		case OnCreateConflict::REPLACE_ON_CONFLICT:
			throw NotImplementedException("CREATE OR REPLACE TABLE is not supported in OneLake catalogs yet");
		default:
			throw BinderException("Table with name \"%s\" already exists in OneLake schema \"%s\"", base_info.table,
			                      schema.name);
		}
	}

	// Extract partition columns and custom location before creating table entry
	ONELAKE_LOG_INFO(&context, "[create_table] Table tags count: %zu", base_info.tags.size());
	for (const auto &tag : base_info.tags) {
		ONELAKE_LOG_INFO(&context, "[create_table] Tag: %s = %s", tag.first.c_str(), tag.second.c_str());
	}

	auto partition_columns = ExtractPartitionColumns(context, base_info);
	auto custom_location = ExtractExplicitLocation(base_info);

	ONELAKE_LOG_INFO(&context, "[create_table] Partition columns extracted: %zu columns", partition_columns.size());
	for (const auto &col : partition_columns) {
		ONELAKE_LOG_INFO(&context, "[create_table]   - %s", col.c_str());
	}

	// Build table URI for physical creation
	auto &schema_entry = schema.Cast<OneLakeSchemaEntry>();
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
	string table_uri;
	if (!custom_location.empty()) {
		table_uri = custom_location;
	} else {
		// Build default URI: abfss://workspace_id@onelake.dfs.fabric.microsoft.com/lakehouse_id/Tables/table_name/
		string workspace_id = onelake_catalog.GetWorkspaceId();
		if (!schema_entry.schema_data || schema_entry.schema_data->id.empty()) {
			throw InternalException("Schema entry missing lakehouse data for table creation");
		}
		string lakehouse_id = schema_entry.schema_data->id;
		table_uri = "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com/" + lakehouse_id + "/Tables/" +
		            base_info.table + "/";
	}

	// Serialize write options with partition columns
	string options_json = "{\"partitionColumns\":[";
	for (idx_t i = 0; i < partition_columns.size(); i++) {
		if (i > 0)
			options_json += ",";
		options_json += "\"" + partition_columns[i] + "\"";
	}
	options_json += "],\"tableName\":\"" + base_info.table + "\"";
	if (!base_info.comment.IsNull()) {
		options_json += ",\"description\":\"" + StringValue::Get(base_info.comment) + "\"";
	}
	options_json += "}";

	ONELAKE_LOG_INFO(&context, "[create_table] Options JSON: %s", options_json.c_str());

	// Get access token
	string token_json = "{\"storageToken\":\"" +
	                    OneLakeAPI::GetAccessToken(onelake_catalog.GetCredentials(), OneLakeTokenAudience::OneLakeDfs) +
	                    "\"}";

	// Convert columns to vector for CreateTable
	vector<ColumnDefinition> columns_vec;
	for (auto &col : base_info.columns.Physical()) {
		columns_vec.push_back(col.Copy());
	}

	// Create physical Delta table in OneLake storage
	ONELAKE_LOG_INFO(&context, "[create_table] Creating Delta table: %s", table_uri.c_str());
	try {
		OneLakeDeltaWriter::CreateTable(context, table_uri, columns_vec, token_json, options_json);
		ONELAKE_LOG_INFO(&context, "[create_table] Successfully created table: %s", base_info.table.c_str());
	} catch (std::exception &ex) {
		ONELAKE_LOG_ERROR(&context, "[create_table] Failed to create table %s: %s", base_info.table.c_str(), ex.what());
		throw;
	}

	// Create catalog entry
	auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, base_info);
	if (!partition_columns.empty()) {
		table_entry->SetPartitionColumns(partition_columns);
	}
	if (table_entry->table_data) {
		table_entry->table_data->type = "Table";
		if (table_entry->table_data->format.empty()) {
			table_entry->table_data->format = "Delta";
		}
		if (!custom_location.empty()) {
			table_entry->table_data->location = custom_location;
		} else {
			table_entry->table_data->location = table_uri;
		}
	}
	auto metadata = make_uniq<OneLakeCreateTableMetadata>();
	metadata->create_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(base_info.Copy());
	metadata->partition_columns = partition_columns;
	metadata->table_properties = metadata->create_info->tags;
	metadata->is_ctas = info.query.get() != nullptr;
	table_entry->SetCreateMetadata(std::move(metadata));
	auto result = CreateEntry(std::move(table_entry));
	return result;
}

void OneLakeTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("OneLake table renaming not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("OneLake column renaming not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("OneLake add column not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("OneLake remove column not supported");
}

void OneLakeTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(context, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(context, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(context, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(context, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw NotImplementedException("OneLake alter table operation not supported");
	}
}

void OneLakeTableSet::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type != CatalogType::TABLE_ENTRY) {
		throw NotImplementedException("Only DROP TABLE is supported for OneLake tables");
	}

	// CRITICAL: Check permission FIRST, before EnsureFresh or any API calls
	bool allow_destructive = false;
	Value setting_value;
	if (context.TryGetCurrentSetting("onelake_allow_destructive_operations", setting_value)) {
		allow_destructive = BooleanValue::Get(setting_value);
	}
	if (!allow_destructive) {
		throw PermissionException(
		    "DROP TABLE is disabled. Set 'onelake_allow_destructive_operations = true' to enable.");
	}

	// Permission granted - now proceed with normal DROP logic
	EnsureFresh(context);
	auto existing_entry = GetEntry(context, info.name);
	if (!existing_entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw BinderException("Table '%s' not found in OneLake schema '%s'", info.name, schema.name);
	}

	auto &table_entry = existing_entry->Cast<OneLakeTableEntry>();
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
	auto &schema_entry = schema.Cast<OneLakeSchemaEntry>();

	// Build table URI
	string workspace_id = onelake_catalog.GetWorkspaceId();
	if (!schema_entry.schema_data || schema_entry.schema_data->id.empty()) {
		throw InternalException("Schema entry missing lakehouse data for table drop");
	}
	string lakehouse_id = schema_entry.schema_data->id;
	string table_uri =
	    "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com/" + lakehouse_id + "/Tables/" + info.name + "/";

	// Build token/options and drop from storage
	string token_json = "{\"storageToken\":\"" +
	                    OneLakeAPI::GetAccessToken(onelake_catalog.GetCredentials(), OneLakeTokenAudience::OneLakeDfs) +
	                    "\"}";
	string options_json = "{\"deleteData\":true}";

	// --- Performance profiling: log timing for storage delete ---
	auto t_start_storage = std::chrono::high_resolution_clock::now();
	ONELAKE_LOG_INFO(&context, "[drop_table] Dropping table: %s", info.name.c_str());
	try {
		OneLakeDeltaWriter::DropTable(context, table_uri, token_json, options_json);
	} catch (std::exception &ex) {
		ONELAKE_LOG_ERROR(&context, "[drop_table] Failed to drop table %s storage: %s", info.name.c_str(), ex.what());
		throw;
	}
	auto t_end_storage = std::chrono::high_resolution_clock::now();
	double storage_secs = std::chrono::duration<double>(t_end_storage - t_start_storage).count();
	ONELAKE_LOG_INFO(&context, "[drop_table] Storage delete time: %.3fs", storage_secs);

	// --- Performance profiling: log timing for catalog delete ---
	auto t_start_catalog = std::chrono::high_resolution_clock::now();
	try {
		const auto &lakehouse = *schema_entry.schema_data;
		// Prefer explicit schema if schema-enabled; otherwise, attempt default schema if present
		string uc_schema = lakehouse.schema_enabled ? schema.name : lakehouse.default_schema;
		OneLakeAPI::DropUnityCatalogTable(context, workspace_id, lakehouse, uc_schema, info.name,
		                                  onelake_catalog.GetCredentials(), /*allow_not_found=*/true);
		ONELAKE_LOG_INFO(&context, "[drop_table] Dropped Unity Catalog entry: %s", info.name.c_str());
	} catch (std::exception &ex) {
		// Don't fail the DROP if catalog cleanup fails; the storage was already removed.
		ONELAKE_LOG_WARN(&context, "[drop_table] Unity Catalog cleanup failed for %s: %s", info.name.c_str(),
		                 ex.what());
	}
	auto t_end_catalog = std::chrono::high_resolution_clock::now();
	double catalog_secs = std::chrono::duration<double>(t_end_catalog - t_start_catalog).count();
	ONELAKE_LOG_INFO(&context, "[drop_table] Catalog delete time: %.3fs", catalog_secs);

	// TODO: If table is partitioned or has many files, consider bulk/parallel delete for storage for further speedup.

	// Remove from catalog cache
	EraseEntryInternal(info.name);

	ONELAKE_LOG_INFO(&context, "[drop_table] Dropped table successfully: %s", info.name.c_str());
}

} // namespace duckdb
