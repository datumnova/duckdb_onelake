#include "onelake_api.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_http_util.hpp"
#include "storage/onelake_table_set.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/onelake_schema_entry.hpp"
#include <unordered_set>

namespace {

using namespace duckdb;

using duckdb::string;
using duckdb::vector;
using std::unordered_set;

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
    string workspace_id = catalog.GetWorkspaceId();
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
            add_candidate(base_prefix + "/" + lakehouse_id + ".Lakehouse/Tables");
            add_candidate(base_prefix + "/" + lakehouse_id + "/Tables");
        }
        if (!lakehouse_name.empty()) {
            add_candidate(base_prefix + "/" + lakehouse_name + ".Lakehouse/Tables");
            add_candidate(base_prefix + "/" + lakehouse_name + "/Tables");
        }
    }

    return result;
}

void AddDiscoveredTable(OneLakeCatalog &catalog, OneLakeSchemaEntry &schema, OneLakeTableSet &table_set,
                        const string &table_name) {
    CreateTableInfo info;
    info.table = table_name;
    auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, info);
    table_entry->table_data->format = "Delta";
    table_entry->table_data->type = "Table";
    table_entry->table_data->location = "Tables/" + table_name;
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
            Printer::Print(StringUtil::Format("[onelake] scanned directory '%s' via DFS API (entries=%llu)", root,
                                              static_cast<long long unsigned>(entries.size())));
        } catch (const Exception &ex) {
            Printer::Print(StringUtil::Format("[onelake] failed to list '%s': %s", root, ex.what()));
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
            } catch (const Exception &ex) {
                Printer::Print(StringUtil::Format("[onelake] failed to inspect '%s/%s': %s", root, leaf, ex.what()));
                has_delta_log = false;
            }
            if (!has_delta_log) {
                Printer::Print(StringUtil::Format("[onelake] skipping '%s/%s' (missing _delta_log)", root, leaf));
                continue;
            }
            if (!known_tables.insert(StringUtil::Lower(leaf)).second) {
                continue;
            }
            AddDiscoveredTable(catalog, schema, table_set, leaf);
            discovered++;
            Printer::Print(StringUtil::Format("[onelake] registered storage table '%s' from '%s/%s'", leaf, root, leaf));
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

    auto lakehouse_id = schema.schema_data->id;
    auto lakehouse_name = schema.schema_data->name;

    Printer::Print(StringUtil::Format("[onelake] loading tables for lakehouse '%s' (id=%s)", lakehouse_name,
                                      lakehouse_id.empty() ? "<unknown>" : lakehouse_id));

    // Get tables from the OneLake API
    auto &credentials = onelake_catalog.GetCredentials();
    auto tables = OneLakeAPI::GetTables(context, onelake_catalog.GetWorkspaceId(), lakehouse_id, credentials);
    std::unordered_set<string> seen_names;
    idx_t api_count = 0;
    detail_endpoint_supported = true;
    detail_endpoint_reported = false;
    
    for (auto &table : tables) {
        CreateTableInfo info;
        info.table = table.name;
        auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, info);
        table_entry->table_data = make_uniq<OneLakeTable>(table);

        OneLakeTableInfo table_info;
        if (detail_endpoint_supported) {
            try {
                table_info = OneLakeAPI::GetTableInfo(context, onelake_catalog.GetWorkspaceId(), lakehouse_id,
                                                      table.name, credentials);
                if (!table_info.has_metadata) {
                    detail_endpoint_supported = false;
                    if (!detail_endpoint_reported) {
                        Printer::Print("[onelake] table detail endpoint returned no metadata; continuing without it");
                        detail_endpoint_reported = true;
                    }
                }
            } catch (const Exception &ex) {
                detail_endpoint_supported = false;
                if (!detail_endpoint_reported) {
                    Printer::Print(StringUtil::Format(
                        "[onelake] table detail lookup failed for '%s': %s. Skipping further detail requests.",
                        table.name, ex.what()));
                    detail_endpoint_reported = true;
                }
            }
        }

        if (table_info.has_metadata) {
            if (!table_info.location.empty()) {
                table_entry->table_data->location = table_info.location;
            }
            if (!table_info.format.empty()) {
                table_entry->table_data->format = table_info.format;
            }
            table_entry->SetPartitionColumns(table_info.partition_columns);
        }
        if (table_entry->table_data->location.empty()) {
            table_entry->table_data->location = "Tables/" + table.name;
        }

        const auto resolved_location = table_entry->table_data->location;
        const auto resolved_format = table_entry->table_data->format.empty() ? "<unknown>" : table_entry->table_data->format;

        CreateEntry(std::move(table_entry));
        seen_names.insert(StringUtil::Lower(table.name));
        api_count++;
        Printer::Print(StringUtil::Format("[onelake] API table '%s' (format=%s, location=%s)", table.name,
                                          resolved_format, resolved_location));
    }

    auto storage_count = DiscoverTablesFromStorage(context, onelake_catalog, schema, *this, seen_names);

    if (api_count == 0 && storage_count == 0) {
        Printer::Print(StringUtil::Format(
            "[onelake] no tables discovered for lakehouse '%s'. Verify the Fabric API permissions and that tables "
            "exist under the 'Tables/' directory.",
            lakehouse_name));
    } else {
        Printer::Print(StringUtil::Format(
            "[onelake] registered %llu tables for lakehouse '%s' (api=%llu, storage=%llu)",
            static_cast<long long unsigned>(api_count + storage_count), lakehouse_name,
            static_cast<long long unsigned>(api_count), static_cast<long long unsigned>(storage_count)));
    }
}

optional_ptr<CatalogEntry> OneLakeTableSet::RefreshTable(ClientContext &context, const string &table_name) {
    auto table_info = GetTableInfo(context, table_name);
    auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, *table_info);
    auto table_ptr = table_entry.get();
    CreateEntry(std::move(table_entry));
    return table_ptr;
}

unique_ptr<OneLakeTableInfo> OneLakeTableSet::GetTableInfo(ClientContext &context, const string &table_name) {
    auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();

    if (!schema.schema_data) {
        throw InternalException("Schema data not available for OneLake schema");
    }

    auto lakehouse_id = schema.schema_data->id;

    // Get detailed table info from OneLake API
    auto &credentials = onelake_catalog.GetCredentials();
    OneLakeTableInfo table_info_api;
    if (detail_endpoint_supported) {
        try {
            table_info_api = OneLakeAPI::GetTableInfo(context, onelake_catalog.GetWorkspaceId(), lakehouse_id,
                                                      table_name, credentials);
            if (!table_info_api.has_metadata) {
                detail_endpoint_supported = false;
                if (!detail_endpoint_reported) {
                    Printer::Print("[onelake] table detail endpoint returned no metadata; continuing without it");
                    detail_endpoint_reported = true;
                }
            }
        } catch (const Exception &ex) {
            detail_endpoint_supported = false;
            if (!detail_endpoint_reported) {
                Printer::Print(StringUtil::Format(
                    "[onelake] table detail lookup failed for '%s': %s. Skipping further detail requests.",
                    table_name, ex.what()));
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
        table_info->location = "Tables/" + table_name;
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
    throw NotImplementedException("OneLake table creation not supported - tables are managed through Fabric");
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

} // namespace duckdb