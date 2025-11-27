#include "onelake_parser_extension.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/function/table_function.hpp"
#include <algorithm>

namespace duckdb {

namespace {

const string ONELAKE_TABLES_PATH_SEGMENT = "/Tables/";

string BuildAbfssPath(const string &workspace_id, const string &lakehouse_id, const string &table_name) {
	return "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com/" + lakehouse_id +
	       ONELAKE_TABLES_PATH_SEGMENT + table_name;
}

struct OneLakeCreateTableParseData : public ParserExtensionParseData {
	string partition_columns;
	string original_query;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		auto result = make_uniq<OneLakeCreateTableParseData>();
		result->partition_columns = partition_columns;
		result->original_query = original_query;
		return result;
	}

	string ToString() const override {
		string result = "SET onelake_partition_columns = '" + partition_columns + "'; ";
		result += original_query + "; ";
		result += "SET onelake_partition_columns = NULL;";
		return result;
	}
};

struct OneLakeIcebergParseData : public ParserExtensionParseData {
	string catalog;
	string schema;
	string table;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		auto result = make_uniq<OneLakeIcebergParseData>();
		result->catalog = catalog;
		result->schema = schema;
		result->table = table;
		return result;
	}

	string ToString() const override {
		string qualified;
		if (!catalog.empty()) {
			qualified += catalog + ".";
		}
		if (!schema.empty()) {
			qualified += schema + ".";
		}
		qualified += table;
		return StringUtil::Format("SELECT * FROM %s USING ICEBERG", qualified);
	}
};

string TrimmedWithoutSemicolon(const string &query) {
	string result = query;
	StringUtil::Trim(result);
	while (!result.empty() && result.back() == ';') {
		result.pop_back();
		StringUtil::RTrim(result);
	}
	return result;
}

bool TryParseQualifiedName(const string &input, OneLakeIcebergParseData &out) {
	auto parts = StringUtil::Split(input, '.');
	if (parts.empty() || parts.size() > 3) {
		return false;
	}
	for (auto &part : parts) {
		StringUtil::Trim(part);
		if (part.empty() || std::any_of(part.begin(), part.end(), [](unsigned char c) { return std::isspace(c); })) {
			return false;
		}
	}
	if (parts.size() == 3) {
		out.catalog = std::move(parts[0]);
		out.schema = std::move(parts[1]);
		out.table = std::move(parts[2]);
	} else if (parts.size() == 2) {
		out.schema = std::move(parts[0]);
		out.table = std::move(parts[1]);
	} else {
		out.table = std::move(parts[0]);
	}
	return true;
}

} // namespace

// Extract PARTITION BY clause and remove it from query, storing columns in modified query
string ExtractAndRemovePartitionBy(const string &query, vector<string> &partition_columns) {
	auto upper = StringUtil::Upper(query);
	auto pos = upper.find("PARTITION BY");
	if (pos == string::npos) {
		return query;
	}
	
	// Find opening parenthesis after PARTITION BY
	size_t paren_start = pos + 12; // strlen("PARTITION BY")
	while (paren_start < query.size() && isspace(query[paren_start])) {
		paren_start++;
	}
	if (paren_start >= query.size() || query[paren_start] != '(') {
		return query;
	}
	
	// Find matching closing parenthesis
	size_t paren_end = paren_start + 1;
	int depth = 1;
	while (paren_end < query.size() && depth > 0) {
		if (query[paren_end] == '(') {
			depth++;
		} else if (query[paren_end] == ')') {
			depth--;
		}
		paren_end++;
	}
	if (depth != 0) {
		return query;
	}
	paren_end--; // Point to the ')'
	
	// Extract column names
	string col_list = query.substr(paren_start + 1, paren_end - paren_start - 1);
	StringUtil::Trim(col_list);
	auto parts = StringUtil::Split(col_list, ',');
	for (auto &part : parts) {
		StringUtil::Trim(part);
		if (!part.empty()) {
			partition_columns.push_back(part);
		}
	}
	
	// Remove PARTITION BY clause from query - just return the CREATE TABLE without it
	string result = query.substr(0, pos);
	StringUtil::RTrim(result);
	
	// Append any remaining content after PARTITION BY (like semicolons)
	if (paren_end + 1 < query.size()) {
		string remainder = query.substr(paren_end + 1);
		StringUtil::Trim(remainder);
		if (!remainder.empty()) {
			result += remainder;
		}
	}
	
	return result;
}

ParserExtensionParseResult OneLakeUsingIcebergParse(ParserExtensionInfo *, const string &query) {
	auto normalized = TrimmedWithoutSemicolon(query);
	if (normalized.empty()) {
		return ParserExtensionParseResult();
	}
	
	auto upper = StringUtil::Upper(normalized);
	
	// Check for PARTITION BY in CREATE TABLE - return error with rewritten syntax
	if (StringUtil::StartsWith(upper, "CREATE TABLE") && upper.find("PARTITION BY") != string::npos) {
		vector<string> partition_columns;
		string modified_query = ExtractAndRemovePartitionBy(normalized, partition_columns);
		
		if (!partition_columns.empty()) {
			string error_msg = "PARTITION BY syntax is not directly supported. Use this instead:\n\n";
			error_msg += "SET onelake_partition_columns = '" + StringUtil::Join(partition_columns, ",") + "';\n";
			error_msg += modified_query + ";";
			return ParserExtensionParseResult(error_msg);
		}
	}
	
	// Handle USING ICEBERG queries
	const string prefix = "SELECT * FROM ";
	const string suffix = " USING ICEBERG";
	if (!StringUtil::StartsWith(upper, prefix) || !StringUtil::EndsWith(upper, suffix)) {
		return ParserExtensionParseResult();
	}
	auto identifier = normalized.substr(prefix.size(), normalized.size() - prefix.size() - suffix.size());
	StringUtil::Trim(identifier);
	if (identifier.empty()) {
		return ParserExtensionParseResult();
	}
	OneLakeIcebergParseData parse_data;
	if (!TryParseQualifiedName(identifier, parse_data)) {
		return ParserExtensionParseResult();
	}
	auto result = make_uniq<OneLakeIcebergParseData>();
	result->catalog = std::move(parse_data.catalog);
	result->schema = std::move(parse_data.schema);
	result->table = std::move(parse_data.table);
	return ParserExtensionParseResult(std::move(result));
}

ParserExtensionPlanResult OneLakeUsingIcebergPlan(ParserExtensionInfo *, ClientContext &context,
                                                  unique_ptr<ParserExtensionParseData> parse_data) {
	auto &iceberg_data = static_cast<OneLakeIcebergParseData &>(*parse_data);
	if (iceberg_data.table.empty()) {
		throw InvalidInputException("OneLake ICEBERG query requires a table name");
	}

	// Ensure iceberg extension is loaded
	ExtensionHelper::TryAutoLoadExtension(context, "iceberg");

	auto table_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA,
	                                                                "iceberg_scan", OnEntryNotFound::RETURN_NULL);
	if (!table_entry) {
		throw CatalogException("The 'iceberg' extension is required to run USING ICEBERG queries. Install it via "
		                       "INSTALL iceberg; then LOAD iceberg;");
	}
	if (table_entry->functions.Size() == 0) {
		throw InternalException("iceberg_scan function set is empty");
	}
	auto iceberg_function = table_entry->functions.GetFunctionByOffset(0);

	string catalog_name = iceberg_data.catalog.empty() ? INVALID_CATALOG : iceberg_data.catalog;
	string schema_name = iceberg_data.schema.empty() ? DEFAULT_SCHEMA : iceberg_data.schema;
	auto &table_catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(context, catalog_name, schema_name, iceberg_data.table);
	if (table_catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("Object '%s' is not a table", iceberg_data.table);
	}
	auto &onelake_table = static_cast<OneLakeTableEntry &>(table_catalog_entry);
	auto &onelake_catalog = static_cast<OneLakeCatalog &>(onelake_table.ParentCatalog());
	auto &schema_entry = static_cast<OneLakeSchemaEntry &>(onelake_table.ParentSchema());

	auto workspace_id = onelake_catalog.GetWorkspaceId();
	if (workspace_id.empty()) {
		throw InvalidInputException("Unable to resolve OneLake workspace identifier for Iceberg scan");
	}

	string lakehouse_id =
	    schema_entry.schema_data && !schema_entry.schema_data->id.empty()
	        ? schema_entry.schema_data->id
	        : (schema_entry.schema_data && !schema_entry.schema_data->name.empty() ? schema_entry.schema_data->name
	                                                                               : schema_entry.name);
	if (lakehouse_id.empty()) {
		throw InvalidInputException("Unable to resolve OneLake lakehouse identifier for Iceberg scan");
	}

	string path = BuildAbfssPath(workspace_id, lakehouse_id, iceberg_data.table);

	ParserExtensionPlanResult result;
	result.function = iceberg_function;
	result.parameters.emplace_back(Value(path));
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

ParserExtension CreateOneLakeParserExtension() {
	ParserExtension extension;
	extension.parse_function = OneLakeUsingIcebergParse;
	extension.plan_function = OneLakeUsingIcebergPlan;
	return extension;
}

} // namespace duckdb
