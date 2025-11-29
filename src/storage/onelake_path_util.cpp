#include "storage/onelake_path_util.hpp"

#include "storage/onelake_catalog.hpp"
#include "storage/onelake_schema_entry.hpp"
#include "storage/onelake_table_entry.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <unordered_set>

namespace duckdb {
namespace {

bool HasScheme(const string &path) {
	return path.find("://") != string::npos;
}

string NormalizeSlashes(const string &path) {
	string result = path;
	std::replace(result.begin(), result.end(), '\\', '/');
	return result;
}

string TrimLeadingSlashes(const string &path) {
	idx_t pos = 0;
	while (pos < path.size() && path[pos] == '/') {
		pos++;
	}
	return path.substr(pos);
}

string JoinPath(const string &lhs, const string &rhs) {
	if (lhs.empty()) {
		return rhs;
	}
	if (rhs.empty()) {
		return lhs;
	}
	bool lhs_slash = StringUtil::EndsWith(lhs, "/");
	bool rhs_slash = StringUtil::StartsWith(rhs, "/");
	if (lhs_slash && rhs_slash) {
		return lhs + rhs.substr(1);
	}
	if (!lhs_slash && !rhs_slash) {
		return lhs + "/" + rhs;
	}
	return lhs + rhs;
}

vector<string> BuildRelativePaths(const string &raw_location, const string &table_name,
                                  const OneLakeSchemaEntry &schema_entry) {
	vector<string> result;
	auto normalized = TrimLeadingSlashes(NormalizeSlashes(raw_location));
	if (!normalized.empty()) {
		result.push_back(normalized);
	}

	vector<string> parts = StringUtil::Split(table_name, '.');
	string default_relative;
	if (schema_entry.schema_data && schema_entry.schema_data->schema_enabled) {
		if (parts.empty()) {
			default_relative = "Schemas/" + schema_entry.name + "/Tables/" + table_name;
		} else {
			default_relative = "Schemas/" + schema_entry.name + "/Tables/" +
			                   StringUtil::Join(parts, parts.size(), "/", [](const string &entry) { return entry; });
		}
	} else {
		if (parts.empty()) {
			default_relative = "Tables/" + table_name;
		} else {
			default_relative =
			    "Tables/" + StringUtil::Join(parts, parts.size(), "/", [](const string &entry) { return entry; });
		}
	}
	if (std::find(result.begin(), result.end(), default_relative) == result.end()) {
		result.push_back(default_relative);
	}
	return result;
}

string ToHttps(const string &input) {
	const string abfss = "abfss://";
	const string abfs = "abfs://";
	if (StringUtil::StartsWith(input, "https://")) {
		return input;
	}
	string path = input;
	if (StringUtil::StartsWith(path, abfss)) {
		path = path.substr(abfss.size());
	} else if (StringUtil::StartsWith(path, abfs)) {
		path = path.substr(abfs.size());
	} else {
		return string();
	}
	auto at_pos = path.find('@');
	if (at_pos == string::npos) {
		return string();
	}
	auto container = path.substr(0, at_pos);
	auto host_and_path = path.substr(at_pos + 1);
	if (host_and_path.empty()) {
		host_and_path = "onelake.dfs.fabric.microsoft.com";
	}
	string host = host_and_path;
	string tail;
	auto slash_pos = host_and_path.find('/');
	if (slash_pos != string::npos) {
		host = host_and_path.substr(0, slash_pos);
		tail = host_and_path.substr(slash_pos);
	}
	if (!tail.empty() && tail[0] != '/') {
		tail = "/" + tail;
	}
	return "https://" + host + "/" + container + tail;
}

void AddCandidate(vector<string> &candidates, std::unordered_set<string> &seen, const string &candidate,
                  bool prepend = false) {
	if (candidate.empty()) {
		return;
	}
	if (!seen.insert(candidate).second) {
		return;
	}
	if (prepend) {
		candidates.insert(candidates.begin(), candidate);
	} else {
		candidates.push_back(candidate);
	}
}

} // namespace

vector<string> BuildLocationCandidates(const OneLakeCatalog &catalog, const OneLakeSchemaEntry &schema_entry,
                                       const OneLakeTableEntry &table_entry, const string &cached_path) {
	std::unordered_set<string> seen;
	vector<string> candidates;
	AddCandidate(candidates, seen, cached_path);

	string raw_location;
	if (table_entry.table_data && !table_entry.table_data->location.empty()) {
		raw_location = table_entry.table_data->location;
	}
	if (raw_location.empty()) {
		raw_location = table_entry.name;
	}

	auto normalized = NormalizeSlashes(raw_location);
	if (HasScheme(normalized)) {
		AddCandidate(candidates, seen, normalized);
		AddCandidate(candidates, seen, ToHttps(normalized), true);
		return candidates;
	}

	auto relative_paths = BuildRelativePaths(normalized, table_entry.name, schema_entry);
	const string &workspace_id = catalog.GetWorkspaceId();
	vector<string> prefixes;
	std::unordered_set<string> prefix_seen;
	auto add_prefix = [&](const string &value) {
		if (value.empty()) {
			return;
		}
		if (!prefix_seen.insert(value).second) {
			return;
		}
		prefixes.push_back(value);
	};
	string base_prefix = "abfss://" + workspace_id + "@onelake.dfs.fabric.microsoft.com";

	if (schema_entry.schema_data) {
		const auto &lakehouse_id = schema_entry.schema_data->id;
		const auto &lakehouse_name = schema_entry.schema_data->name;
		if (!lakehouse_id.empty()) {
			add_prefix(base_prefix + "/" + lakehouse_id);
			add_prefix(base_prefix + "/" + lakehouse_id + ".Lakehouse");
		}
		if (!lakehouse_name.empty()) {
			add_prefix(base_prefix + "/" + lakehouse_name);
			add_prefix(base_prefix + "/" + lakehouse_name + ".Lakehouse");
		}
	}
	add_prefix(base_prefix);

	for (auto &prefix : prefixes) {
		for (auto &relative : relative_paths) {
			auto joined = JoinPath(prefix, relative);
			AddCandidate(candidates, seen, ToHttps(joined), true);
			AddCandidate(candidates, seen, joined);
		}
	}

	return candidates;
}

bool IsValidAbfssPath(const string &path) {
	return StringUtil::StartsWith(path, "abfs://") || StringUtil::StartsWith(path, "abfss://");
}

string GetAbfssPathDiagnostic(const string &path) {
	if (IsValidAbfssPath(path)) {
		return StringUtil::Format("Valid abfss path: %s", path);
	}
	return StringUtil::Format("Non-abfss path (may be slower): %s", path);
}

} // namespace duckdb
