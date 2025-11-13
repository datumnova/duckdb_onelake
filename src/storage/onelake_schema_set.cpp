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

static string NormalizeLakehousePreferenceToken(string token) {
	StringUtil::Trim(token);
	if (token.empty()) {
		return token;
	}
	const string suffix = ".lakehouse";
	if (token.size() >= suffix.size()) {
		auto tail = token.substr(token.size() - suffix.size());
		if (StringUtil::CIEquals(tail, suffix)) {
			token = token.substr(0, token.size() - suffix.size());
			StringUtil::Trim(token);
		}
	}
	return token;
}

static string NormalizeLakehouseForComparison(const string &value) {
	string normalized;
	normalized.reserve(value.size());
	for (char ch : value) {
		if (StringUtil::CharacterIsAlphaNumeric(ch)) {
			normalized.push_back(StringUtil::CharacterToLower(ch));
		}
	}
	return normalized;
}

static bool LakehouseMatchesPreference(const OneLakeLakehouse &lakehouse, const string &preference) {
	if (preference.empty()) {
		return false;
	}
	auto token = NormalizeLakehousePreferenceToken(preference);
	if (token.empty()) {
		return false;
	}
	if (!lakehouse.name.empty() && StringUtil::CIEquals(token, lakehouse.name)) {
		return true;
	}
	if (!lakehouse.display_name.empty() && StringUtil::CIEquals(token, lakehouse.display_name)) {
		return true;
	}
	if (!lakehouse.id.empty() && StringUtil::CIEquals(token, lakehouse.id)) {
		return true;
	}
	auto normalized_token = NormalizeLakehouseForComparison(token);
	if (normalized_token.empty()) {
		return false;
	}
	if (!lakehouse.name.empty() && NormalizeLakehouseForComparison(lakehouse.name) == normalized_token) {
		return true;
	}
	if (!lakehouse.display_name.empty() &&
	    NormalizeLakehouseForComparison(lakehouse.display_name) == normalized_token) {
		return true;
	}
	return false;
}

void OneLakeSchemaSet::LoadEntries(ClientContext &context) {
	auto &onelake_catalog = catalog.Cast<OneLakeCatalog>();
	auto lakehouses =
	    OneLakeAPI::GetLakehouses(context, onelake_catalog.GetWorkspaceId(), onelake_catalog.GetCredentials());
	const bool has_configured_default = onelake_catalog.HasUserConfiguredDefault();
	const string configured_default = onelake_catalog.GetConfiguredDefaultPreference();
	bool matched_configured_default = false;
	vector<string> available_lakehouses;

	for (const auto &lakehouse : lakehouses) {
		available_lakehouses.push_back(lakehouse.name);

		// With Option 3 format, we should only process the specific lakehouse mentioned in the attach path
		if (!LakehouseMatchesPreference(lakehouse, configured_default)) {
			continue;
		}

		if (lakehouse.schema_enabled) {
			// For schema-enabled lakehouses, create a schema entry for each schema within the lakehouse
			auto schemas = OneLakeAPI::GetSchemas(context, onelake_catalog.GetWorkspaceId(), lakehouse.id,
			                                      lakehouse.name, onelake_catalog.GetCredentials());
			for (const auto &schema : schemas) {
				CreateSchemaInfo info;
				info.schema = schema.name;
				info.internal = IsInternalLakehouse(schema.name);
				auto schema_entry = make_uniq<OneLakeSchemaEntry>(catalog, info);
				schema_entry->schema_data = make_uniq<OneLakeLakehouse>(lakehouse);
				auto created_entry = CreateEntry(std::move(schema_entry));

				if (!onelake_catalog.HasDefaultSchema()) {
					onelake_catalog.SetDefaultSchema(schema.name);
				}
			}
			matched_configured_default = true;
			break;
		} else {
			// For non-schema-enabled lakehouses, create a single "default" schema
			CreateSchemaInfo info;
			info.schema = "default";
			info.internal = IsInternalLakehouse(lakehouse.name);
			auto schema_entry = make_uniq<OneLakeSchemaEntry>(catalog, info);
			schema_entry->schema_data = make_uniq<OneLakeLakehouse>(lakehouse);
			auto created_entry = CreateEntry(std::move(schema_entry));

			if (!onelake_catalog.HasDefaultSchema()) {
				onelake_catalog.SetDefaultSchema("default");
			}
			matched_configured_default = true;
			break;
		}
	}

	if (!matched_configured_default) {
		string available;
		if (available_lakehouses.empty()) {
			available = "<none>";
		} else {
			available = StringUtil::Join(available_lakehouses, ", ");
		}
		throw InvalidInputException("Lakehouse '%s' not found in workspace '%s'. Available lakehouses: %s",
		                            configured_default, onelake_catalog.GetWorkspaceId(), available);
	}
}

} // namespace duckdb
