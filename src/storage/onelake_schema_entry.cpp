#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

OneLakeSchemaEntry::OneLakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}

OneLakeSchemaEntry::~OneLakeSchemaEntry() {
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("REPLACE ON CONFLICT in CreateTable not supported in OneLake");
	}
	return tables.CreateTable(transaction.GetContext(), info);
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating functions");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	throw NotImplementedException("OneLake lakehouses do not support creating indexes");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty()) {
		throw BinderException("Cannot create view in OneLake that originated from an empty SQL statement");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (current_entry) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return current_entry;
			}
			throw NotImplementedException("REPLACE ON CONFLICT in CreateView not supported in OneLake");
		}
	}
	// For now, we don't support view creation in OneLake
	throw NotImplementedException("OneLake view creation not yet supported");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating types");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating sequences");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating table functions");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating copy functions");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating pragma functions");
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
	throw BinderException("OneLake lakehouses do not support creating collations");
}

void OneLakeSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for OneLake lakehouses");
	}
	auto &alter = info.Cast<AlterTableInfo>();
	tables.AlterTable(transaction.GetContext(), alter);
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void OneLakeSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	if (type == CatalogType::TABLE_ENTRY) {
		tables.EnsureFresh(context);
	}
	GetCatalogSet(type).Scan(context, callback);
}

void OneLakeSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void OneLakeSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	GetCatalogSet(info.type).DropEntry(context, info);
}

optional_ptr<CatalogEntry> OneLakeSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return nullptr;
	}
	auto &catalog_set = GetCatalogSet(lookup_info.GetCatalogType());
	auto result = catalog_set.GetEntry(transaction.GetContext(), lookup_info.GetEntryName());
	if (result || lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY || !transaction.HasContext()) {
		return result;
	}

	auto &context = transaction.GetContext();
	tables.MarkRefreshRequired();
	tables.EnsureFresh(context);
	return catalog_set.GetEntry(context, lookup_info.GetEntryName());
}

void OneLakeSchemaEntry::EnsureTablesLoaded(ClientContext &context) {
	tables.EnsureLoaded(context);
}

OneLakeCatalogSet &OneLakeSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
