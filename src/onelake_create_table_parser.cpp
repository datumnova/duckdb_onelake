#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

// Parser extension to capture PARTITION BY in CREATE TABLE
struct OneLakeCreateTableParserExtension : public ParserExtension {
	OneLakeCreateTableParserExtension() {
		parse_function = OneLakeParseCreateTable;
	}

	static ParserExtensionParseResult OneLakeParseCreateTable(ParserExtensionInfo *info,
	                                                            const string &query) {
		// Look for "PARTITION BY" in CREATE TABLE statements
		auto partition_pos = query.find("PARTITION BY");
		if (partition_pos == string::npos) {
			return ParserExtensionParseResult();
		}

		// Check if this is in a CREATE TABLE context
		auto create_pos = query.find("CREATE");
		auto table_pos = query.find("TABLE");
		if (create_pos == string::npos || table_pos == string::npos || 
		    partition_pos < table_pos) {
			return ParserExtensionParseResult();
		}

		// Extract partition columns between parentheses after PARTITION BY
		auto paren_start = query.find('(', partition_pos);
		if (paren_start == string::npos) {
			return ParserExtensionParseResult();
		}

		auto paren_end = query.find(')', paren_start);
		if (paren_end == string::npos) {
			return ParserExtensionParseResult();
		}

		// Extract the column list
		auto columns_str = query.substr(paren_start + 1, paren_end - paren_start - 1);
		
		// Remove "PARTITION BY (...)" from the query to make it valid SQL
		auto modified_query = query.substr(0, partition_pos);
		auto after_partition = query.find(';', paren_end);
		if (after_partition != string::npos) {
			modified_query += query.substr(after_partition);
		}

		ParserExtensionParseResult result;
		result.modified_query = modified_query;
		result.tags["partition_columns"] = columns_str;
		
		return result;
	}
};

} // namespace duckdb
