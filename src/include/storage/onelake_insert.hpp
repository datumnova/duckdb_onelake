#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class OneLakeCatalog;
class OneLakeTableEntry;

class PhysicalOneLakeInsert : public PhysicalOperator {
public:
	PhysicalOneLakeInsert(PhysicalPlan &plan, OneLakeTableEntry &table_entry, OneLakeCatalog &catalog,
	                      vector<LogicalType> types, idx_t estimated_cardinality);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &state) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return true;
	}

private:
	OneLakeTableEntry &table_entry;
	OneLakeCatalog &catalog;
};

} // namespace duckdb
