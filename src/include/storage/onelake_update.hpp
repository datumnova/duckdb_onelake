#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

class OneLakeCatalog;
class OneLakeTableEntry;

class PhysicalOneLakeUpdate : public PhysicalOperator {
public:
	PhysicalOneLakeUpdate(PhysicalPlan &plan, OneLakeTableEntry &table_entry, OneLakeCatalog &catalog,
	                      vector<LogicalType> types, unique_ptr<Expression> condition, vector<string> update_columns,
	                      vector<unique_ptr<Expression>> update_expressions, idx_t estimated_cardinality,
	                      vector<column_t> column_ids = {}, TableFilterSet table_filters = TableFilterSet());

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	// Pipeline construction
	vector<const_reference<PhysicalOperator>> GetSources() const override;

public:
	// Helpers
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	OneLakeTableEntry &table_entry;
	OneLakeCatalog &catalog;
	unique_ptr<Expression> condition;
	vector<string> update_columns;
	vector<unique_ptr<Expression>> update_expressions;
	vector<column_t> column_ids;
	TableFilterSet table_filters;
};

} // namespace duckdb
