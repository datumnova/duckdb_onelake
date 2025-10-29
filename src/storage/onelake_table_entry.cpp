#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_transaction.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {

struct OneLakeScanBindData : public TableFunctionData {
    unique_ptr<FunctionData> Copy() const override {
        return make_uniq<OneLakeScanBindData>(*this);
    }

    bool Equals(const FunctionData &other) const override {
        return dynamic_cast<const OneLakeScanBindData *>(&other) != nullptr;
    }
};

struct OneLakeGlobalState : public GlobalTableFunctionState {
};

struct OneLakeLocalState : public LocalTableFunctionState {
};

static unique_ptr<FunctionData> OneLakeScanBind(ClientContext &, TableFunctionBindInput &,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("data");
    return make_uniq<OneLakeScanBindData>();
}

static unique_ptr<GlobalTableFunctionState> OneLakeScanInitGlobal(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<OneLakeGlobalState>();
}

static unique_ptr<LocalTableFunctionState> OneLakeScanInitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                                GlobalTableFunctionState *) {
    return make_uniq<OneLakeLocalState>();
}

static void OneLakeScanFunction(ClientContext &, TableFunctionInput &, DataChunk &output) {
    // Placeholder: return empty chunk until scan implementation is available
    output.SetCardinality(0);
}

static TableFunction CreateOneLakeScanFunction() {
    return TableFunction("onelake_scan", {}, OneLakeScanFunction, OneLakeScanBind, OneLakeScanInitGlobal,
                         OneLakeScanInitLocal);
}

} // namespace

OneLakeTableEntry::OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
    this->internal = false;
}

OneLakeTableEntry::OneLakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, OneLakeTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
    this->internal = false;
}

unique_ptr<BaseStatistics> OneLakeTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    // OneLake doesn't provide column statistics through standard APIs
    return nullptr;
}

void OneLakeTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                             ClientContext &) {
    throw NotImplementedException("OneLake tables do not support UPDATE operations");
}

TableFunction OneLakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    bind_data = make_uniq<OneLakeScanBindData>();
    return CreateOneLakeScanFunction();
}

TableStorageInfo OneLakeTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo result;
    result.cardinality = 0; // Unknown cardinality
    return result;
}

} // namespace duckdb