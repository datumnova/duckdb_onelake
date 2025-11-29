#include "storage/onelake_update.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_path_util.hpp"
#include "onelake_delta_writer.hpp"
#include "onelake_logging.hpp"
#include "onelake_api.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

#include <chrono>
#include <algorithm>

namespace duckdb {
using namespace duckdb_yyjson; // NOLINT

namespace {

string SerializeTokenJson(const string &token) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	if (!token.empty()) {
		yyjson_mut_obj_add_str(doc, root, "storageToken", token.c_str());
	}
	char *buffer = yyjson_mut_write(doc, 0, nullptr);
	string result = buffer ? string(buffer) : string();
	if (buffer) {
		free(buffer);
	}
	yyjson_mut_doc_free(doc);
	return result;
}

string SerializeUpdatesJson(const vector<string> &columns, const vector<string> &expressions) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (size_t i = 0; i < columns.size(); i++) {
		yyjson_mut_obj_add_str(doc, root, columns[i].c_str(), expressions[i].c_str());
	}

	char *buffer = yyjson_mut_write(doc, 0, nullptr);
	string result = buffer ? string(buffer) : string();
	if (buffer) {
		free(buffer);
	}
	yyjson_mut_doc_free(doc);
	return result;
}

string ResolveTableUri(ClientContext &context, OneLakeCatalog &catalog, OneLakeTableEntry &table_entry) {
	auto &schema_entry = table_entry.ParentSchema().Cast<OneLakeSchemaEntry>();
	auto cached_path = table_entry.GetCachedResolvedPath();
	auto candidates = BuildLocationCandidates(catalog, schema_entry, table_entry, cached_path);
	if (candidates.empty()) {
		throw InvalidInputException("Unable to resolve storage location for OneLake table '%s'", table_entry.name);
	}
	auto is_abfs = [](const string &candidate) {
		return IsValidAbfssPath(candidate);
	};
	std::stable_partition(candidates.begin(), candidates.end(), is_abfs);
	for (auto &candidate : candidates) {
		if (IsValidAbfssPath(candidate)) {
			table_entry.RememberResolvedPath(candidate);
			return candidate;
		}
	}
	ONELAKE_LOG_WARN(&context, "[update] Falling back to non-abfss path for updates: %s", candidates.front().c_str());
	ONELAKE_LOG_WARN(&context, "[update] %s", GetAbfssPathDiagnostic(candidates.front()).c_str());
	table_entry.RememberResolvedPath(candidates.front());
	return candidates.front();
}

struct OneLakeUpdateSourceState : public GlobalSourceState {
	bool finished = false;
};

} // namespace

PhysicalOneLakeUpdate::PhysicalOneLakeUpdate(PhysicalPlan &plan, OneLakeTableEntry &table_entry_p,
                                             OneLakeCatalog &catalog_p, vector<LogicalType> types,
                                             unique_ptr<Expression> condition_p, vector<string> update_columns_p,
                                             vector<unique_ptr<Expression>> update_expressions_p,
                                             idx_t estimated_cardinality, vector<column_t> column_ids_p,
                                             TableFilterSet table_filters_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_entry(table_entry_p), catalog(catalog_p), condition(std::move(condition_p)),
      update_columns(std::move(update_columns_p)), update_expressions(std::move(update_expressions_p)),
      column_ids(std::move(column_ids_p)), table_filters(std::move(table_filters_p)) {
}

unique_ptr<GlobalSourceState> PhysicalOneLakeUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<OneLakeUpdateSourceState>();
}

// Helper function to translate DuckDB expression to Delta SQL
static string TranslateExpressionToDeltaSQL(const Expression &expr, const vector<string> &column_names,
                                            const vector<column_t> &column_ids) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT: {
		auto &const_expr = expr.Cast<BoundConstantExpression>();
		auto value = const_expr.value;

		if (value.IsNull()) {
			return "NULL";
		}

		switch (value.type().id()) {
		case LogicalTypeId::BOOLEAN:
			return value.GetValue<bool>() ? "true" : "false";
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			return value.ToString();
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return value.ToString();
		case LogicalTypeId::VARCHAR:
			return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
		case LogicalTypeId::DATE:
			return "DATE '" + value.ToString() + "'";
		case LogicalTypeId::TIMESTAMP:
			return "TIMESTAMP '" + value.ToString() + "'";
		default:
			return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
		}
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		idx_t col_idx = col_ref.binding.column_index;
		if (col_idx < column_names.size()) {
			return "`" + column_names[col_idx] + "`";
		}
		return "col" + to_string(col_idx);
	}
	case ExpressionClass::BOUND_REF: {
		auto &col_ref = expr.Cast<BoundReferenceExpression>();
		idx_t col_idx = col_ref.index;
		if (!column_ids.empty() && col_idx < column_ids.size()) {
			col_idx = column_ids[col_idx];
		}
		if (col_idx < column_names.size()) {
			return "`" + column_names[col_idx] + "`";
		}
		return "col" + to_string(col_idx);
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		if (!comp.left || !comp.right) {
			throw InternalException("BoundComparisonExpression has null children");
		}
		string left = TranslateExpressionToDeltaSQL(*comp.left, column_names, column_ids);
		string right = TranslateExpressionToDeltaSQL(*comp.right, column_names, column_ids);

		string op;
		switch (comp.type) {
		case ExpressionType::COMPARE_EQUAL:
			op = "=";
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			op = "!=";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			op = "<";
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			op = ">";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			op = "<=";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			op = ">=";
			break;
		default:
			throw NotImplementedException("Comparison type not supported in UPDATE predicate");
		}

		return "(" + left + " " + op + " " + right + ")";
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		vector<string> parts;

		for (auto &child : conj.children) {
			if (child) {
				parts.push_back(TranslateExpressionToDeltaSQL(*child, column_names, column_ids));
			}
		}

		string conjunction = (conj.type == ExpressionType::CONJUNCTION_AND) ? " AND " : " OR ";
		string result = StringUtil::Join(parts, conjunction);

		return "(" + result + ")";
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name == "+" || func.function.name == "-" || func.function.name == "*" ||
		    func.function.name == "/") {
			if (func.children.size() == 2) {
				if (!func.children[0] || !func.children[1]) {
					throw InternalException("Function expression has null children");
				}
				string left = TranslateExpressionToDeltaSQL(*func.children[0], column_names, column_ids);
				string right = TranslateExpressionToDeltaSQL(*func.children[1], column_names, column_ids);
				return "(" + left + " " + func.function.name + " " + right + ")";
			}
		}
		throw NotImplementedException("Function not supported in UPDATE expression: %s", func.function.name);
	}
	default:
		throw NotImplementedException("Expression type not supported in UPDATE: %s", ExpressionTypeToString(expr.type));
	}
}

SourceResultType PhysicalOneLakeUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<OneLakeUpdateSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}

	// Check if destructive operations are allowed
	Value setting_value;
	if (context.client.TryGetCurrentSetting("onelake_allow_destructive_operations", setting_value)) {
		if (!BooleanValue::Get(setting_value)) {
			throw PermissionException(
			    "UPDATE is disabled. Set 'onelake_allow_destructive_operations = true' to enable.");
		}
	} else {
		throw PermissionException("UPDATE is disabled. Set 'onelake_allow_destructive_operations = true' to enable.");
	}

	auto column_names = table_entry.GetColumns().GetColumnNames();

	string predicate_sql;
	// Translate condition to Delta SQL predicate
	if (condition) {
		predicate_sql = TranslateExpressionToDeltaSQL(*condition, column_names, column_ids);
	}

	// Translate table filters to Delta SQL predicate
	for (auto &entry : table_filters.filters) {
		auto col_idx = entry.first;
		auto &filter = entry.second;
		if (col_idx < column_names.size()) {
			ONELAKE_LOG_DEBUG(&context.client, "[update] Processing filter for column %llu", col_idx);
			if (!filter) {
				ONELAKE_LOG_WARN(&context.client, "[update] Filter is null for column %llu", col_idx);
				continue;
			}
			auto col_ref = make_uniq<BoundColumnRefExpression>(
			    table_entry.GetColumns().GetColumn(LogicalIndex(col_idx)).Type(), ColumnBinding(0, col_idx));
			auto filter_expr = filter->ToExpression(*col_ref);
			if (filter_expr) {
				ONELAKE_LOG_DEBUG(&context.client, "[update] Filter expression generated: %s",
				                  filter_expr->ToString().c_str());
				string filter_sql = TranslateExpressionToDeltaSQL(*filter_expr, column_names,
				                                                  {}); // Pass empty column_ids to use direct index
				if (!predicate_sql.empty()) {
					predicate_sql += " AND ";
				}
				predicate_sql += filter_sql;
			} else {
				ONELAKE_LOG_WARN(&context.client, "[update] Failed to generate expression for filter on column %llu",
				                 col_idx);
			}
		}
	}

	// Translate update expressions
	vector<string> update_expr_sqls;
	for (auto &expr : update_expressions) {
		update_expr_sqls.push_back(TranslateExpressionToDeltaSQL(*expr, column_names, column_ids));
	}

	string updates_json = SerializeUpdatesJson(update_columns, update_expr_sqls);

	// Resolve table URI
	auto table_uri = ResolveTableUri(context.client, catalog, table_entry);

	ONELAKE_LOG_INFO(&context.client, "[update] Executing UPDATE: table=%s.%s, predicate=%s", catalog.GetName().c_str(),
	                 table_entry.name.c_str(), predicate_sql.c_str());

	auto update_start = std::chrono::high_resolution_clock::now();

	// Execute update via Rust
	auto token = OneLakeAPI::GetAccessToken(catalog.GetCredentials(), OneLakeTokenAudience::OneLakeDfs);
	auto metrics =
	    OneLakeDeltaWriter::Update(context.client, table_uri, predicate_sql, updates_json, SerializeTokenJson(token));

	auto update_end = std::chrono::high_resolution_clock::now();
	auto update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(update_end - update_start).count();

	ONELAKE_LOG_INFO(&context.client, "[update] Updated %llu rows (removed %llu files, added %llu files) in %lld ms",
	                 metrics.rows_updated, metrics.files_removed, metrics.files_added, update_ms);

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(metrics.rows_updated));

	state.finished = true;
	return SourceResultType::FINISHED;
}

vector<const_reference<PhysicalOperator>> PhysicalOneLakeUpdate::GetSources() const {
	return {*this};
}

string PhysicalOneLakeUpdate::GetName() const {
	return "ONELAKE_UPDATE";
}

InsertionOrderPreservingMap<string> PhysicalOneLakeUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table_entry.name;
	if (condition) {
		result["Condition"] = condition->ToString();
	}
	return result;
}

} // namespace duckdb
