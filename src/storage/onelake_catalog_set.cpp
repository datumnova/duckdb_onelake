#include "storage/onelake_catalog_set.hpp"
#include "storage/onelake_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/onelake_schema_entry.hpp"

namespace duckdb {

OneLakeCatalogSet::OneLakeCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> OneLakeCatalogSet::GetEntry(ClientContext &context, const string &name) {
    if (!is_loaded) {
        is_loaded = true;
        LoadEntries(context);
    }
    lock_guard<mutex> l(entry_lock);
    auto entry = entries.find(name);
    if (entry == entries.end()) {
        return nullptr;
    }
    return entry->second.get();
}

void OneLakeCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("OneLakeCatalogSet::DropEntry");
}

void OneLakeCatalogSet::EraseEntryInternal(const string &name) {
    lock_guard<mutex> l(entry_lock);
    entries.erase(name);
}

void OneLakeCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
    if (!is_loaded) {
        is_loaded = true;
        LoadEntries(context);
    }
    lock_guard<mutex> l(entry_lock);
    for (auto &entry : entries) {
        callback(*entry.second);
    }
}

optional_ptr<CatalogEntry> OneLakeCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
    lock_guard<mutex> l(entry_lock);
    auto result = entry.get();
    if (result->name.empty()) {
        throw InternalException("OneLakeCatalogSet::CreateEntry called with empty name");
    }
    entries.insert(make_pair(result->name, std::move(entry)));
    return result;
}

void OneLakeCatalogSet::ClearEntries() {
    entries.clear();
    is_loaded = false;
}

OneLakeInSchemaSet::OneLakeInSchemaSet(OneLakeSchemaEntry &schema) : OneLakeCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> OneLakeInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
    if (!entry->internal) {
        entry->internal = schema.internal;
    }
    return OneLakeCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb