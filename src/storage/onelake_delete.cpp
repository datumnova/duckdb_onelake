#include "storage/onelake_delete.hpp"
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
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
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
	ONELAKE_LOG_WARN(&context, "[delete] Falling back to non-abfss path for deletes: %s", candidates.front().c_str());
	ONELAKE_LOG_WARN(&context, "[delete] %s", GetAbfssPathDiagnostic(candidates.front()).c_str());
	table_entry.RememberResolvedPath(candidates.front());
	return candidates.front();
}

struct OneLakeDeleteSourceState : public GlobalSourceState {
	bool finished = false;
};

} // namespace

PhysicalOneLakeDelete::PhysicalOneLakeDelete(PhysicalPlan &plan, OneLakeTableEntry &table_entry_p,
                                             OneLakeCatalog &catalog_p, vector<LogicalType> types,
                                             unique_ptr<Expression> condition_p, idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_entry(table_entry_p), catalog(catalog_p), condition(std::move(condition_p)) {
}

unique_ptr<GlobalSourceState> PhysicalOneLakeDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<OneLakeDeleteSourceState>();
}

// Helper function to translate DuckDB expression to Delta SQL
static string TranslateExpressionToDeltaSQL(const Expression &expr, const vector<string> &column_names) {
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
		auto &col_ref = expr.Cast<BoundReferenceExpression>();
		if (col_ref.index < column_names.size()) {
			return "`" + column_names[col_ref.index] + "`";
		}
		return "col" + to_string(col_ref.index);
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		string left = TranslateExpressionToDeltaSQL(*comp.left, column_names);
		string right = TranslateExpressionToDeltaSQL(*comp.right, column_names);

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
			throw NotImplementedException("Comparison type not supported in DELETE predicate");
		}

		return "(" + left + " " + op + " " + right + ")";
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		vector<string> parts;

		for (auto &child : conj.children) {
			parts.push_back(TranslateExpressionToDeltaSQL(*child, column_names));
		}

		string conjunction = (conj.type == ExpressionType::CONJUNCTION_AND) ? " AND " : " OR ";
		string result = StringUtil::Join(parts, conjunction);

		return "(" + result + ")";
	}
	default:
		throw NotImplementedException("Expression type not supported in DELETE predicate: %s",
		                              ExpressionTypeToString(expr.type));
	}
}

SourceResultType PhysicalOneLakeDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<OneLakeDeleteSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}

	// Check if destructive operations are allowed
	Value setting_value;
	if (context.client.TryGetCurrentSetting("onelake_allow_destructive_operations", setting_value)) {
		if (!BooleanValue::Get(setting_value)) {
			throw PermissionException(
			    "DELETE is disabled. Set 'onelake_allow_destructive_operations = true' to enable.");
		}
	} else {
		throw PermissionException("DELETE is disabled. Set 'onelake_allow_destructive_operations = true' to enable.");
	}

	string predicate_sql;
	// Translate condition to Delta SQL predicate
	if (condition) {
		auto column_names = table_entry.GetColumns().GetColumnNames();
		predicate_sql = TranslateExpressionToDeltaSQL(*condition, column_names);
	}

	// Resolve table URI
	auto table_uri = ResolveTableUri(context.client, catalog, table_entry);

	ONELAKE_LOG_INFO(&context.client, "[delete] Executing DELETE: table=%s.%s, predicate=%s", catalog.GetName().c_str(),
	                 table_entry.name.c_str(), predicate_sql.c_str());

	auto delete_start = std::chrono::high_resolution_clock::now();

	// Execute delete via Rust
	auto token = OneLakeAPI::GetAccessToken(catalog.GetCredentials(), OneLakeTokenAudience::OneLakeDfs);
	auto metrics = OneLakeDeltaWriter::Delete(context.client, table_uri, predicate_sql, SerializeTokenJson(token));

	auto delete_end = std::chrono::high_resolution_clock::now();
	auto delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(delete_end - delete_start).count();

	ONELAKE_LOG_INFO(&context.client, "[delete] Deleted %llu rows (removed %llu files, added %llu files) in %lld ms",
	                 metrics.rows_deleted, metrics.files_removed, metrics.files_added, delete_ms);

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(metrics.rows_deleted));

	state.finished = true;
	return SourceResultType::FINISHED;
}

vector<const_reference<PhysicalOperator>> PhysicalOneLakeDelete::GetSources() const {
	return {*this};
}

string PhysicalOneLakeDelete::GetName() const {
	return "ONELAKE_DELETE";
}

InsertionOrderPreservingMap<string> PhysicalOneLakeDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table_entry.name;
	if (condition) {
		result["Condition"] = condition->ToString();
	}
	return result;
}

} // namespace duckdb
