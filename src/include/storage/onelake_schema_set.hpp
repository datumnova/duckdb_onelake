#pragma once
#include "onelake_catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

class OneLakeSchemaSet : public OneLakeCatalogSet {
public:
    OneLakeSchemaSet(Catalog &catalog);
    
protected:
    void LoadEntries(ClientContext &context) override;
    
private:
    optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);
};

} // namespace duckdb