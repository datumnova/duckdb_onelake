#include "storage/onelake_catalog.hpp"
#include "storage/onelake_insert.hpp"
#include "storage/onelake_delete.hpp"
#include "storage/onelake_update.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_transaction.hpp"
#include "onelake_logging.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

OneLakeCatalog::OneLakeCatalog(AttachedDatabase &db_p, const string &workspace_id, const string &catalog_name,
                               OneLakeCredentials credentials, string default_schema_p)
    : Catalog(db_p), workspace_id(workspace_id), access_mode(AccessMode::AUTOMATIC),
      credentials(std::move(credentials)), schemas(*this), default_schema(std::move(default_schema_p)),
      configured_default_preference(default_schema), user_configured_default(!default_schema.empty()) {
}

OneLakeCatalog::~OneLakeCatalog() = default;

void OneLakeCatalog::Initialize(bool load_builtin) {
	// OneLake catalogs don't need built-in schema initialization
}

optional_ptr<CatalogEntry> OneLakeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("OneLake catalog does not support CREATE SCHEMA");
}

void OneLakeCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void OneLakeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<OneLakeSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> OneLakeCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA) {
		if (default_schema.empty() && transaction.HasContext()) {
			auto &context = transaction.GetContext();
			schemas.Scan(context, [&](CatalogEntry &schema) {
				if (default_schema.empty()) {
					default_schema = schema.name;
				}
			});
		}
		if (default_schema.empty()) {
			if (if_not_found == OnEntryNotFound::RETURN_NULL) {
				return nullptr;
			}
			throw InvalidInputException(
			    "Attempting to fetch the default schema - but no default lakehouse was provided in the connection "
			    "string");
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Lakehouse with name \"%s\" not found", schema_lookup.GetEntryName());
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool OneLakeCatalog::InMemory() {
	return false;
}

string OneLakeCatalog::GetDBPath() {
	return workspace_id;
}

DatabaseSize OneLakeCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	// OneLake doesn't provide size information through standard APIs
	return size;
}

void OneLakeCatalog::ClearCache() {
	schemas.ClearEntries();
}

PhysicalOperator &OneLakeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	if (op.children.empty()) {
		throw InternalException("PlanCreateTableAs invoked without a projection plan");
	}
	auto &schema_entry = op.schema.Cast<OneLakeSchemaEntry>();
	auto entry = GetEntry(context, CatalogType::TABLE_ENTRY, schema_entry.name, op.info->Base().table,
	                      OnEntryNotFound::THROW_EXCEPTION);
	auto &table_entry = entry->Cast<OneLakeTableEntry>();
	auto &insert = planner.Make<PhysicalOneLakeInsert>(table_entry, *this, op.types, op.estimated_cardinality);
	insert.children.push_back(plan);
	return insert;
}

PhysicalOperator &OneLakeCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	if (!plan) {
		throw NotImplementedException("INSERT ... DEFAULT VALUES is not supported for OneLake tables yet");
	}
	if (op.return_chunk) {
		throw NotImplementedException("INSERT ... RETURNING is not supported for OneLake tables yet");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("INSERT ... ON CONFLICT is not supported for OneLake tables yet");
	}
	if (!op.column_index_map.empty()) {
		plan = &planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &table_entry = op.table.Cast<OneLakeTableEntry>();
	auto &insert = planner.Make<PhysicalOneLakeInsert>(table_entry, *this, op.types, op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &OneLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	// We do not support DELETE ... RETURNING for OneLake
	if (op.return_chunk) {
		throw NotImplementedException("DELETE ... RETURNING is not supported for OneLake tables");
	}
	auto &table_entry = op.table.Cast<OneLakeTableEntry>();

	// For OneLake DELETE, we extract the WHERE condition from the logical operator tree
	// and push it down to Delta. We do NOT execute the child plan (no table scan needed).
	unique_ptr<Expression> condition;

	// Navigate the logical operator tree to find the filter
	if (op.children.size() > 0) {
		auto child = op.children[0].get();
		// Check if there's a LogicalFilter in the tree
		if (child->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = child->Cast<LogicalFilter>();
			if (!filter.expressions.empty()) {
				// Use the first expression (WHERE condition)
				condition = filter.expressions[0]->Copy();
			}
		}
	}

	// Create DELETE operator without children - we don't need to scan the table
	// We pass BIGINT as output type because the operator acts as a source returning the delete count
	auto &delete_op = planner.Make<PhysicalOneLakeDelete>(table_entry, *this, vector<LogicalType> {LogicalType::BIGINT},
	                                                      std::move(condition), op.estimated_cardinality);
	// DO NOT add children - this prevents the table scan that causes the virtual column error
	return delete_op;
}

namespace {

PhysicalOperator &PlanOneLakeUpdateInternal(OneLakeCatalog &catalog, ClientContext &context,
                                            PhysicalPlanGenerator &planner, LogicalUpdate &op) {
	if (op.return_chunk) {
		throw NotImplementedException("UPDATE ... RETURNING is not supported for OneLake tables");
	}
	auto &table_entry = op.table.Cast<OneLakeTableEntry>();

	// Extract WHERE condition and column mapping
	unique_ptr<Expression> condition;
	vector<column_t> column_ids;
	TableFilterSet table_filters;

	LogicalOperator *current = op.children[0].get();
	while (current) {
		if (current->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = current->Cast<LogicalFilter>();
			for (auto &expr : filter.expressions) {
				if (condition) {
					auto new_condition = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
					new_condition->children.push_back(std::move(condition));
					new_condition->children.push_back(expr->Copy());
					condition = std::move(new_condition);
				} else {
					condition = expr->Copy();
				}
			}
		} else if (current->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = current->Cast<LogicalGet>();
			for (auto &col_idx : get.GetColumnIds()) {
				column_ids.push_back(col_idx.GetPrimaryIndex());
			}
			// Copy table filters
			for (auto &entry : get.table_filters.filters) {
				if (!entry.second) {
					continue;
				}
				table_filters.filters.emplace(entry.first, entry.second->Copy());
			}
		}

		if (current->children.empty()) {
			break;
		}
		current = current->children[0].get();
	}

	// Extract update columns and expressions
	vector<string> update_columns;
	vector<unique_ptr<Expression>> update_expressions;

	auto &columns = table_entry.GetColumns();
	for (size_t i = 0; i < op.columns.size(); i++) {
		auto &col_idx = op.columns[i];
		if (col_idx.index >= columns.LogicalColumnCount()) {
			throw InternalException("Invalid column index in UPDATE");
		}
		auto &col = columns.GetColumn(LogicalIndex(col_idx.index));
		update_columns.push_back(col.Name());

		auto expr = op.expressions[i]->Copy();
		if (expr->type == ExpressionType::BOUND_REF) {
			auto &bound_ref = expr->Cast<BoundReferenceExpression>();
			if (op.children.size() > 0 && op.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &proj = op.children[0]->Cast<LogicalProjection>();
				if (bound_ref.index < proj.expressions.size()) {
					expr = proj.expressions[bound_ref.index]->Copy();
				}
			}
		}
		update_expressions.push_back(std::move(expr));
	}

	auto &update_op = planner.Make<PhysicalOneLakeUpdate>(
	    table_entry, catalog, vector<LogicalType> {LogicalType::BIGINT}, std::move(condition),
	    std::move(update_columns), std::move(update_expressions), op.estimated_cardinality, std::move(column_ids),
	    std::move(table_filters));
	return update_op;
}

} // namespace

PhysicalOperator &OneLakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
	(void)plan;
	return PlanOneLakeUpdateInternal(*this, context, planner, op);
}

PhysicalOperator &OneLakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalUpdate &op) {
	return PlanOneLakeUpdateInternal(*this, context, planner, op);
}

unique_ptr<LogicalOperator> OneLakeCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                            TableCatalogEntry &table,
                                                            unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("OneLake catalog does not support CREATE INDEX operations");
}

bool OneLakeCatalog::HasDefaultSchema() const {
	return !default_schema.empty();
}

void OneLakeCatalog::SetDefaultSchema(const string &schema_name) {
	default_schema = schema_name;
}

string OneLakeCatalog::GetDefaultSchema() const {
	return default_schema.empty() ? Catalog::GetDefaultSchema() : default_schema;
}

} // namespace duckdb
