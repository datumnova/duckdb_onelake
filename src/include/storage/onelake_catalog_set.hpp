#pragma once
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct DropInfo;
class OneLakeSchemaEntry;
class OneLakeTransaction;

class OneLakeCatalogSet {
public:
    explicit OneLakeCatalogSet(Catalog &catalog);
	virtual ~OneLakeCatalogSet() = default;

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	virtual void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void ClearEntries();
	void EnsureLoaded(ClientContext &context);
	bool IsLoaded() const;

protected:
	virtual void LoadEntries(ClientContext &context) = 0;
	void EraseEntryInternal(const string &name);

protected:
	Catalog &catalog;

private:
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	bool is_loaded;
};

class OneLakeInSchemaSet : public OneLakeCatalogSet {
public:
	explicit OneLakeInSchemaSet(OneLakeSchemaEntry &schema);

	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry) override;

protected:
	OneLakeSchemaEntry &schema;
};

} // namespace duckdb
