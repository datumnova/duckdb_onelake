#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class OneLakeCatalog;
class OneLakeTableEntry;

class PhysicalOneLakeDelete : public PhysicalOperator {
public:
	PhysicalOneLakeDelete(PhysicalPlan &plan, OneLakeTableEntry &table_entry, OneLakeCatalog &catalog,
	                      vector<LogicalType> types, unique_ptr<Expression> condition, idx_t estimated_cardinality);

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
};

} // namespace duckdb
