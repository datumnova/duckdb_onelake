#include "storage/onelake_schema_set.hpp"
#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "onelake_api.hpp"
#include "storage/onelake_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

OneLakeSchemaSet::OneLakeSchemaSet(Catalog &catalog) : OneLakeCatalogSet(catalog) {
}

static bool IsInternalLakehouse(const string &lakehouse_name) {
	// Mark system lakehouses as internal if needed
	return false;
}

void OneLakeSchemaSet::LoadEntries(ClientContext &context) {
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
	auto lakehouses =
	    OneLakeAPI::GetLakehouses(context, onelake_catalog.GetWorkspaceId(), onelake_catalog.GetCredentials());
	const bool has_configured_default = onelake_catalog.HasDefaultSchema();
	const string configured_default = onelake_catalog.GetConfiguredDefaultSchema();
	bool matched_configured_default = false;
	vector<string> available_lakehouses;

	for (const auto &lakehouse : lakehouses) {
		CreateSchemaInfo info;
		info.schema = lakehouse.name;
		info.internal = IsInternalLakehouse(lakehouse.name);
		auto schema_entry = make_uniq<OneLakeSchemaEntry>(catalog, info);
		schema_entry->schema_data = make_uniq<OneLakeLakehouse>(lakehouse);
		auto created_entry = CreateEntry(std::move(schema_entry));
		if (created_entry) {
			Printer::Print(StringUtil::Format("[onelake] registered lakehouse '%s' (id=%s)", lakehouse.name,
			                                  lakehouse.id.empty() ? "<unknown>" : lakehouse.id));
		}

		available_lakehouses.push_back(lakehouse.name);

		if (has_configured_default && !matched_configured_default) {
			if (StringUtil::CIEquals(configured_default, lakehouse.name) ||
			    StringUtil::CIEquals(configured_default, lakehouse.id)) {
				onelake_catalog.SetDefaultSchema(lakehouse.name);
				matched_configured_default = true;
			}
		}

		if (!has_configured_default && !onelake_catalog.HasDefaultSchema()) {
			onelake_catalog.SetDefaultSchema(lakehouse.name);
		}
	}

	if (has_configured_default && !matched_configured_default) {
		string available;
		if (available_lakehouses.empty()) {
			available = "<none>";
		} else {
			available = StringUtil::Join(available_lakehouses, ", ");
		}
		throw InvalidInputException("Lakehouse '%s' not found in workspace '%s'. Available lakehouses: %s",
		                            configured_default, onelake_catalog.GetWorkspaceId(), available);
	}

	if (!has_configured_default && !lakehouses.empty() && !onelake_catalog.HasDefaultSchema()) {
		onelake_catalog.SetDefaultSchema(lakehouses.front().name);
	}
}

} // namespace duckdb
