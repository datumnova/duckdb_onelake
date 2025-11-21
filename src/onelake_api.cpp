#include "onelake_api.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include <azure/core/context.hpp>
#include <azure/core/credentials/credentials.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <curl/curl.h>
#include <json/json.h>
#include <unordered_set>
#include <memory>
#include <chrono>
#include <iostream>

namespace duckdb {

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, string *userp) {
	size_t totalSize = size * nmemb;
	userp->append((char *)contents, totalSize);
	return totalSize;
}

namespace {

struct AbfssPathComponents {
	string container;
	string host;
	string path;
};

bool ParseAbfssPath(const string &abfss_path, AbfssPathComponents &out) {
	const string abfss = "abfss://";
	const string abfs = "abfs://";
	string path = abfss_path;
	if (StringUtil::StartsWith(path, abfss)) {
		path = path.substr(abfss.size());
	} else if (StringUtil::StartsWith(path, abfs)) {
		path = path.substr(abfs.size());
	} else {
		return false;
	}
	auto at_pos = path.find('@');
	if (at_pos == string::npos) {
		return false;
	}
	out.container = path.substr(0, at_pos);
	auto host_and_path = path.substr(at_pos + 1);
	if (host_and_path.empty()) {
		return false;
	}
	auto slash_pos = host_and_path.find('/');
	if (slash_pos == string::npos) {
		out.host = host_and_path;
		out.path.clear();
	} else {
		out.host = host_and_path.substr(0, slash_pos);
		out.path = host_and_path.substr(slash_pos + 1);
	}
	return !out.container.empty() && !out.host.empty();
}

bool JsonIsDirectory(const Json::Value &value) {
	if (!value.isMember("isDirectory")) {
		return false;
	}
	const auto &node = value["isDirectory"];
	if (node.isBool()) {
		return node.asBool();
	}
	if (node.isString()) {
		return StringUtil::CIEquals(node.asString(), "true");
	}
	return false;
}

string ComposeLeaf(const string &base_path, const string &full_name) {
	string normalized_base = base_path;
	if (StringUtil::EndsWith(normalized_base, "/")) {
		normalized_base = normalized_base.substr(0, normalized_base.size() - 1);
	}
	if (!normalized_base.empty()) {
		if (!StringUtil::StartsWith(full_name, normalized_base + "/")) {
			return string();
		}
		auto relative = full_name.substr(normalized_base.size() + 1);
		auto slash_pos = relative.find('/');
		if (slash_pos != string::npos) {
			relative = relative.substr(0, slash_pos);
		}
		return relative;
	}
	auto slash_pos = full_name.find('/');
	if (slash_pos != string::npos) {
		return full_name.substr(0, slash_pos);
	}
	return full_name;
}

} // namespace

static const char *ICEBERG_TABLE_API_BASE = "https://onelake.table.fabric.microsoft.com/iceberg";
static const char *FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default";
static const char *DFS_SCOPE = "https://storage.azure.com/.default";

static string BuildWarehouseScope(const string &workspace_id, const OneLakeLakehouse &lakehouse) {
	if (workspace_id.empty()) {
		return string();
	}
	string data_item = lakehouse.id;
	if (data_item.empty()) {
		data_item = lakehouse.name;
		if (!data_item.empty() && !StringUtil::EndsWith(data_item, ".Lakehouse")) {
			data_item += ".Lakehouse";
		}
	}
	if (data_item.empty()) {
		return string();
	}
	return workspace_id + "/" + data_item;
}

static string UrlEncodeComponent(const string &value) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for URL encoding");
	}
	char *escaped = curl_easy_escape(curl, value.c_str(), static_cast<int>(value.size()));
	string result;
	if (escaped) {
		result = string(escaped);
		curl_free(escaped);
	}
	curl_easy_cleanup(curl);
	return result;
}

static vector<string> SplitPathSegments(const string &path) {
	if (path.empty()) {
		return {};
	}
	return StringUtil::Split(path, '/');
}

static string EncodePathSegments(const vector<string> &segments) {
	string encoded;
	for (idx_t i = 0; i < segments.size(); i++) {
		if (i > 0) {
			encoded += "/";
		}
		encoded += UrlEncodeComponent(segments[i]);
	}
	return encoded;
}

static string BuildIcebergPath(const string &prefix, const vector<string> &suffix_segments) {
	vector<string> segments;
	segments.push_back("v1");
	auto prefix_parts = SplitPathSegments(prefix);
	segments.insert(segments.end(), prefix_parts.begin(), prefix_parts.end());
	segments.insert(segments.end(), suffix_segments.begin(), suffix_segments.end());
	return EncodePathSegments(segments);
}

static string JoinNamespaceName(const vector<string> &namespace_parts) {
	if (namespace_parts.empty()) {
		return string();
	}
	return StringUtil::Join(namespace_parts, namespace_parts.size(), ".", [](const string &entry) { return entry; });
}

static string EnsureTrailingSlash(const string &input) {
	if (input.empty()) {
		return string();
	}
	if (StringUtil::EndsWith(input, "/")) {
		return input;
	}
	return input + "/";
}

static const string &ScopeForAudience(OneLakeTokenAudience audience) {
	static const string fabric_scope = FABRIC_SCOPE;
	static const string dfs_scope = DFS_SCOPE;
	switch (audience) {
	case OneLakeTokenAudience::Fabric:
		return fabric_scope;
	case OneLakeTokenAudience::OneLakeDfs:
		return dfs_scope;
	default:
		return fabric_scope;
	}
}

static string PerformBearerGet(const string &url, const string &token, long timeout_seconds = 60L) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for OneLake HTTP request");
	}
	string response_string;
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_seconds);

	struct curl_slist *headers = nullptr;
	string auth_header = "Authorization: Bearer " + token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "Accept: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	CURLcode res = curl_easy_perform(curl);
	long response_code = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("OneLake HTTP request failed: %s", curl_easy_strerror(res));
	}
	if (response_code < 200 || response_code >= 300) {
		throw IOException("OneLake HTTP request returned HTTP %ld - %s", response_code, response_string.c_str());
	}

	return response_string;
}

static vector<vector<string>> IcebergListNamespaces(const string &prefix, const string &token) {
	vector<vector<string>> namespaces;
	string next_token;
	do {
		vector<string> suffix = {"namespaces"};
		string path = BuildIcebergPath(prefix, suffix);
		string url = string(ICEBERG_TABLE_API_BASE) + "/" + path;
		if (!next_token.empty()) {
			url += "?page-token=" + UrlEncodeComponent(next_token);
		}
		string response = PerformBearerGet(url, token);
		Json::Value root;
		Json::Reader reader;
		if (!reader.parse(response, root)) {
			throw InvalidInputException("Failed to parse Iceberg namespaces response");
		}
		if (root.isMember("namespaces") && root["namespaces"].isArray()) {
			for (const auto &ns_value : root["namespaces"]) {
				if (!ns_value.isArray()) {
					continue;
				}
				vector<string> ns_parts;
				for (const auto &part : ns_value) {
					if (part.isString()) {
						ns_parts.push_back(part.asString());
					}
				}
				if (!ns_parts.empty()) {
					namespaces.push_back(std::move(ns_parts));
				}
			}
		}
		next_token.clear();
		if (root.isMember("next-page-token") && root["next-page-token"].isString()) {
			next_token = root["next-page-token"].asString();
		}
	} while (!next_token.empty());
	return namespaces;
}

static string IcebergGetNamespaceLocation(const string &prefix, const vector<string> &namespace_parts, const string &token) {
	vector<string> suffix = {"namespaces"};
	suffix.insert(suffix.end(), namespace_parts.begin(), namespace_parts.end());
	string path = BuildIcebergPath(prefix, suffix);
	string url = string(ICEBERG_TABLE_API_BASE) + "/" + path;
	string response = PerformBearerGet(url, token);
	Json::Value root;
	Json::Reader reader;
	if (!reader.parse(response, root)) {
		throw InvalidInputException("Failed to parse Iceberg namespace response");
	}
	if (root.isMember("properties") && root["properties"].isObject()) {
		const auto &props = root["properties"];
		if (props.isMember("location") && props["location"].isString()) {
			return props["location"].asString();
		}
	}
	return string();
}

static vector<string> IcebergListTables(const string &prefix, const vector<string> &namespace_parts, const string &token) {
	vector<string> tables;
	string next_token;
	do {
		vector<string> suffix = {"namespaces"};
		suffix.insert(suffix.end(), namespace_parts.begin(), namespace_parts.end());
		suffix.push_back("tables");
		string path = BuildIcebergPath(prefix, suffix);
		string url = string(ICEBERG_TABLE_API_BASE) + "/" + path;
		if (!next_token.empty()) {
			url += "?page-token=" + UrlEncodeComponent(next_token);
		}
		string response = PerformBearerGet(url, token);
		Json::Value root;
		Json::Reader reader;
		if (!reader.parse(response, root)) {
			throw InvalidInputException("Failed to parse Iceberg tables response");
		}
		if (root.isMember("identifiers") && root["identifiers"].isArray()) {
			for (const auto &identifier : root["identifiers"]) {
				if (!identifier.isObject() || !identifier.isMember("name")) {
					continue;
				}
				tables.push_back(identifier["name"].asString());
			}
		}
		next_token.clear();
		if (root.isMember("next-page-token") && root["next-page-token"].isString()) {
			next_token = root["next-page-token"].asString();
		}
	} while (!next_token.empty());
	return tables;
}

static vector<OneLakeTable> FetchIcebergTables(const string &workspace_id, const OneLakeLakehouse &lakehouse,
	                                            OneLakeCredentials &credentials) {
	vector<OneLakeTable> tables;
	string warehouse = BuildWarehouseScope(workspace_id, lakehouse);
	if (warehouse.empty()) {
		return tables;
	}
	string token = OneLakeAPI::GetAccessToken(credentials, OneLakeTokenAudience::OneLakeDfs);
	string config_url = string(ICEBERG_TABLE_API_BASE) + "/v1/config?warehouse=" + UrlEncodeComponent(warehouse);
	string prefix = warehouse;
	try {
		string config_response = PerformBearerGet(config_url, token);
		Json::Value config_json;
		Json::Reader reader;
		if (reader.parse(config_response, config_json)) {
			if (config_json.isMember("overrides") && config_json["overrides"].isObject()) {
				const auto &overrides = config_json["overrides"];
				if (overrides.isMember("prefix") && overrides["prefix"].isString()) {
					prefix = overrides["prefix"].asString();
				}
			}
		}
	} catch (const Exception &) {
		// Ignore prefix fetch failures and fall back to warehouse identifier
	}
	auto namespaces = IcebergListNamespaces(prefix, token);
	for (auto &ns_parts : namespaces) {
		string display_name = JoinNamespaceName(ns_parts);
		string namespace_location;
		try {
			namespace_location = IcebergGetNamespaceLocation(prefix, ns_parts, token);
		} catch (const Exception &) {
			// Continue even if schema location is unavailable
		}
		auto table_names = IcebergListTables(prefix, ns_parts, token);
		for (auto &table_name : table_names) {
			OneLakeTable table;
			table.name = table_name;
			table.type = "Table";
			table.schema_name = display_name;
			table.format = "Iceberg";
			if (!namespace_location.empty()) {
				string base = EnsureTrailingSlash(namespace_location);
				table.location = base + table_name;
			}
			tables.push_back(std::move(table));
		}
	}
	return tables;
}

string OneLakeAPI::GetAccessToken(OneLakeCredentials &credentials, OneLakeTokenAudience audience) {
	const auto &scope = ScopeForAudience(audience);
	auto current_time = Timestamp::GetCurrentTimestamp();
	auto cache_entry = credentials.token_cache.find(scope);
	if (cache_entry != credentials.token_cache.end()) {
		if (!cache_entry->second.token.empty() && current_time < cache_entry->second.expiry) {
			return cache_entry->second.token;
		}
	}

	if (credentials.provider == OneLakeCredentialProvider::CredentialChain) {
		try {
			auto credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
			Azure::Core::Credentials::TokenRequestContext request_context;
			request_context.Scopes = {scope};
			Azure::Core::Context azure_context;
			auto access_token_result = credential->GetToken(request_context, azure_context);
			auto access_token = access_token_result.Token;
			auto expires_on = access_token_result.ExpiresOn.time_since_epoch();
			auto expires_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(expires_on).count();
			auto expiry = Timestamp::FromEpochMicroSeconds(expires_microseconds);

			OneLakeCredentials::TokenCacheEntry entry;
			entry.token = access_token;
			entry.expiry = expiry;
			credentials.token_cache[scope] = entry;

			return access_token;
		} catch (const std::exception &ex) {
			throw IOException("Failed to obtain OneLake access token via Azure credential chain: %s", ex.what());
		}
	}

	if (credentials.provider != OneLakeCredentialProvider::ServicePrincipal) {
		throw IOException("Unsupported OneLake credential provider encountered when requesting token");
	}

	CURL *curl;
	CURLcode res;
	string response_string;
	string post_data;

	curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for OneLake API");
	}

	// Microsoft Entra ID token endpoint
	string token_url = "https://login.microsoftonline.com/" + credentials.tenant_id + "/oauth2/v2.0/token";

	// Prepare POST data for client credentials flow
	post_data = "grant_type=client_credentials";
	post_data += "&client_id=" + credentials.client_id;
	post_data += "&client_secret=" + credentials.client_secret;
	post_data += "&scope=" + scope;

	curl_easy_setopt(curl, CURLOPT_URL, token_url.c_str());
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	res = curl_easy_perform(curl);

	long response_code;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	if (res != CURLE_OK || response_code != 200) {
		throw IOException("Failed to obtain OneLake access token: HTTP %ld", response_code);
	}

	// Parse JSON response to get access token
	try {
		Json::Value root;
		Json::Reader reader;

		if (!reader.parse(response_string, root)) {
			throw InvalidInputException("Failed to parse JSON token response");
		}

		if (!root.isMember("access_token")) {
			throw InvalidInputException("Invalid token response from Microsoft Entra ID");
		}

		auto access_token = root["access_token"].asString();

		// Compute token expiry with a small safety margin if the response provides expires_in
		auto expires_in_seconds = root.isMember("expires_in") ? root["expires_in"].asInt64() : 3600;
		const int64_t safety_margin = 60;
		if (expires_in_seconds > safety_margin) {
			expires_in_seconds -= safety_margin;
		}
		auto expiry = Timestamp::GetCurrentTimestamp();
		expiry += expires_in_seconds * Interval::MICROS_PER_SEC;

		OneLakeCredentials::TokenCacheEntry entry;
		entry.token = access_token;
		entry.expiry = expiry;
		credentials.token_cache[scope] = entry;

		return access_token;
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to parse token response: %s", e.what());
	}
}

string OneLakeAPI::MakeAPIRequest(ClientContext &context, const string &url, OneLakeCredentials &credentials,
                                  bool allow_not_found) {
	(void)context;

	string access_token = GetAccessToken(credentials, OneLakeTokenAudience::Fabric);

	CURL *curl;
	CURLcode res;
	string response_string;

	curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for OneLake API");
	}

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

	struct curl_slist *headers = nullptr;
	string auth_header = "Authorization: Bearer " + access_token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	res = curl_easy_perform(curl);

	long response_code;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	if (res != CURLE_OK) {
		throw IOException("OneLake API request failed: %s", curl_easy_strerror(res));
	}

	if (response_code == 404 && allow_not_found) {
		return string();
	}
	if (response_code < 200 || response_code >= 300) {
		throw IOException("OneLake API returned error: HTTP %ld - %s", response_code, response_string.c_str());
	}

	return response_string;
}

string OneLakeAPI::BuildAPIUrl(const string &workspace_id, const string &endpoint) {
	return "https://api.fabric.microsoft.com/v1/workspaces/" + workspace_id + "/" + endpoint;
}

vector<OneLakeWorkspace> OneLakeAPI::GetWorkspaces(ClientContext &context, OneLakeCredentials &credentials) {
	vector<OneLakeWorkspace> workspaces;
	std::unordered_set<string> visited_urls;
	string next_url = "https://api.fabric.microsoft.com/v1/workspaces";

	while (!next_url.empty()) {
		if (!visited_urls.insert(next_url).second) {
			break;
		}

		string response = MakeAPIRequest(context, next_url, credentials);

		try {
			Json::Value root;
			Json::Reader reader;

			if (!reader.parse(response, root)) {
				throw InvalidInputException("Failed to parse JSON workspaces response");
			}

			if (root.isMember("error")) {
				const auto &error_obj = root["error"];
				if (error_obj.isObject() && error_obj.isMember("message")) {
					throw InvalidInputException("OneLake API error while listing workspaces: %s",
					                            error_obj["message"].asString());
				}
				throw InvalidInputException("OneLake API error while listing workspaces: %s",
				                            error_obj.toStyledString());
			}

			const Json::Value *workspace_array = nullptr;
			if (root.isMember("value") && root["value"].isArray()) {
				workspace_array = &root["value"];
			} else if (root.isMember("data") && root["data"].isArray()) {
				workspace_array = &root["data"];
			}

			if (workspace_array) {
				for (const auto &workspace_json : *workspace_array) {
					if (!workspace_json.isMember("id")) {
						continue;
					}

					OneLakeWorkspace workspace;
					workspace.id = workspace_json["id"].asString();
					if (workspace_json.isMember("name") && workspace_json["name"].isString()) {
						workspace.name = workspace_json["name"].asString();
					}
					if (workspace_json.isMember("displayName") && workspace_json["displayName"].isString()) {
						workspace.display_name = workspace_json["displayName"].asString();
						if (workspace.name.empty()) {
							workspace.name = workspace.display_name;
						}
					}
					if (workspace_json.isMember("description") && workspace_json["description"].isString()) {
						workspace.description = workspace_json["description"].asString();
					}

					workspaces.push_back(std::move(workspace));
				}
			}

			if (root.isMember("continuationUri") && root["continuationUri"].isString()) {
				auto candidate = root["continuationUri"].asString();
				if (!candidate.empty()) {
					next_url = candidate;
					continue;
				}
			}

			if (root.isMember("continuationToken") && root["continuationToken"].isString()) {
				auto token = root["continuationToken"].asString();
				if (!token.empty()) {
					CURL *curl = curl_easy_init();
					string encoded_token = token;
					if (curl) {
						char *escaped = curl_easy_escape(curl, token.c_str(), static_cast<int>(token.size()));
						if (escaped) {
							encoded_token = string(escaped);
							curl_free(escaped);
						}
						curl_easy_cleanup(curl);
					}
					next_url = "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=" + encoded_token;
					continue;
				}
			}

			next_url.clear();
		} catch (const std::exception &e) {
			throw InvalidInputException("Failed to parse workspaces response: %s", e.what());
		}
	}

	return workspaces;
}

vector<OneLakeLakehouse> OneLakeAPI::GetLakehouses(ClientContext &context, const string &workspace_id,
                                                   OneLakeCredentials &credentials) {
	string url = BuildAPIUrl(workspace_id, "lakehouses");
	string response = MakeAPIRequest(context, url, credentials);

	vector<OneLakeLakehouse> lakehouses;

	try {
		Json::Value root;
		Json::Reader reader;

		if (!reader.parse(response, root)) {
			throw InvalidInputException("Failed to parse JSON lakehouses response");
		}

		if (!root.isMember("value") || !root["value"].isArray()) {
			throw InvalidInputException("Unexpected API response format for lakehouses");
		}

		auto &lakehouse_array = root["value"];
		for (const auto &lakehouse_json : lakehouse_array) {
			OneLakeLakehouse lakehouse;
			lakehouse.id = lakehouse_json["id"].asString();
			lakehouse.name = lakehouse_json["displayName"].asString();
			lakehouse.display_name = lakehouse.name;

			if (lakehouse_json.isMember("description") && !lakehouse_json["description"].isNull()) {
				lakehouse.description = lakehouse_json["description"].asString();
			}

			// Check if this is a schema-enabled lakehouse
			if (lakehouse_json.isMember("properties") && lakehouse_json["properties"].isObject()) {
				const auto &properties = lakehouse_json["properties"];
				if (properties.isMember("defaultSchema") && properties["defaultSchema"].isString()) {
					lakehouse.schema_enabled = true;
					lakehouse.default_schema = properties["defaultSchema"].asString();
				}
			}

			lakehouses.push_back(lakehouse);
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to parse lakehouses response: %s", e.what());
	}

	return lakehouses;
}

vector<OneLakeTable> OneLakeAPI::GetTables(ClientContext &context, const string &workspace_id,
                                           const string &lakehouse_id, OneLakeCredentials &credentials) {
	vector<OneLakeTable> tables;
	std::unordered_set<string> seen_names;

	string next_url = BuildAPIUrl(workspace_id, "lakehouses/" + lakehouse_id + "/tables");
	std::unordered_set<string> visited_urls;

	while (!next_url.empty()) {
		if (!visited_urls.insert(next_url).second) {
			// Prevent potential loops if the service returns the same continuation URL repeatedly
			break;
		}

		string response = MakeAPIRequest(context, next_url, credentials, true);
		if (response.empty()) {
			break;
		}

		try {
			Json::Value root;
			Json::Reader reader;

			if (!reader.parse(response, root)) {
				throw InvalidInputException("Failed to parse JSON tables response");
			}

			if (root.isMember("error")) {
				const auto &error_obj = root["error"];
				if (error_obj.isObject() && error_obj.isMember("message")) {
					throw InvalidInputException("OneLake API error while listing tables: %s",
					                            error_obj["message"].asString());
				}
				throw InvalidInputException("OneLake API error while listing tables: %s", error_obj.toStyledString());
			}

			const Json::Value *table_array = nullptr;
			if (root.isMember("value") && root["value"].isArray()) {
				table_array = &root["value"];
			} else if (root.isMember("data") && root["data"].isArray()) {
				table_array = &root["data"];
			}

			if (table_array) {
				for (const auto &table_json : *table_array) {
					if (!table_json.isMember("name")) {
						continue;
					}
					auto table_name = table_json["name"].asString();
					if (!seen_names.insert(StringUtil::Lower(table_name)).second) {
						continue;
					}

					OneLakeTable table;
					table.name = std::move(table_name);
					table.type = table_json.isMember("type") ? table_json["type"].asString() : "Table";
					table.format = table_json.isMember("format") ? table_json["format"].asString() : "Delta";

					if (table_json.isMember("location")) {
						table.location = table_json["location"].asString();
					}

					tables.push_back(std::move(table));
				}
			}

			if (root.isMember("continuationUri") && root["continuationUri"].isString()) {
				auto candidate = root["continuationUri"].asString();
				if (!candidate.empty()) {
					next_url = candidate;
					continue;
				}
			}

			if (root.isMember("continuationToken") && root["continuationToken"].isString()) {
				auto token = root["continuationToken"].asString();
				if (!token.empty()) {
					CURL *curl = curl_easy_init();
					string encoded_token = token;
					if (curl) {
						char *escaped = curl_easy_escape(curl, token.c_str(), static_cast<int>(token.size()));
						if (escaped) {
							encoded_token = string(escaped);
							curl_free(escaped);
						}
						curl_easy_cleanup(curl);
					}
					next_url = BuildAPIUrl(workspace_id,
					                       "lakehouses/" + lakehouse_id + "/tables?continuationToken=" + encoded_token);
					continue;
				}
			}

			next_url.clear();
		} catch (const std::exception &e) {
			throw InvalidInputException("Failed to parse tables response: %s", e.what());
		}
	}

	return tables;
}

vector<OneLakeTable> OneLakeAPI::GetTables(ClientContext &context, const string &workspace_id,
                                           const OneLakeLakehouse &lakehouse, OneLakeCredentials &credentials) {
	vector<OneLakeTable> all_tables;
	std::unordered_set<string> seen_names;
	auto register_table = [&](OneLakeTable table) {
		string key = StringUtil::Lower(table.name);
		if (!table.schema_name.empty()) {
			key = StringUtil::Lower(table.schema_name) + "." + key;
		}
		if (!seen_names.insert(key).second) {
			return;
		}
		all_tables.push_back(std::move(table));
	};

	bool delta_api_success = false;
	try {
		auto schemas = GetSchemas(context, workspace_id, lakehouse.id, lakehouse.name, credentials);
		for (const auto &schema : schemas) {
			auto schema_tables =
			    GetTablesFromSchema(context, workspace_id, lakehouse.id, lakehouse.name, schema.name, credentials);
			for (auto &table : schema_tables) {
				table.schema_name = schema.name;
				register_table(std::move(table));
			}
		}
		delta_api_success = true;
	} catch (const Exception &) {
		// Unity Catalog API not available; will fall back to legacy Fabric endpoint
	}

	if (!delta_api_success) {
		auto legacy_tables = GetTables(context, workspace_id, lakehouse.id, credentials);
		for (auto &table : legacy_tables) {
			register_table(std::move(table));
		}
	}

	try {
		auto iceberg_tables = FetchIcebergTables(workspace_id, lakehouse, credentials);
		for (auto &table : iceberg_tables) {
			register_table(std::move(table));
		}
	} catch (const Exception &) {
		// Iceberg REST catalog may be disabled; ignore failures and rely on DFS fallback later
	}

	return all_tables;
}

vector<OneLakeSchema> OneLakeAPI::GetSchemas(ClientContext &context, const string &workspace_id,
                                             const string &lakehouse_id, const string &lakehouse_name,
                                             OneLakeCredentials &credentials) {
	vector<OneLakeSchema> schemas;

	// Build Unity Catalog API URL for schemas
	string url = "https://onelake.table.fabric.microsoft.com/delta/" + workspace_id + "/" + lakehouse_id +
	             "/api/2.1/unity-catalog/schemas?catalog_name=" + lakehouse_name + ".Lakehouse";

	// Use DFS scope for Unity Catalog API
	string access_token = GetAccessToken(credentials, OneLakeTokenAudience::OneLakeDfs);

	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for Unity Catalog schemas API");
	}

	string response_string;
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

	struct curl_slist *headers = nullptr;
	string auth_header = "Authorization: Bearer " + access_token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	CURLcode res = curl_easy_perform(curl);
	long response_code;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("Unity Catalog schemas API request failed: %s", curl_easy_strerror(res));
	}

	if (response_code < 200 || response_code >= 300) {
		throw IOException("Unity Catalog schemas API returned error: HTTP %ld - %s", response_code,
		                  response_string.c_str());
	}

	try {
		Json::Value root;
		Json::Reader reader;

		if (!reader.parse(response_string, root)) {
			throw InvalidInputException("Failed to parse JSON schemas response");
		}

		if (root.isMember("schemas") && root["schemas"].isArray()) {
			for (const auto &schema_json : root["schemas"]) {
				if (!schema_json.isMember("name")) {
					continue;
				}

				OneLakeSchema schema;
				schema.name = schema_json["name"].asString();
				if (schema_json.isMember("catalog_name")) {
					schema.catalog_name = schema_json["catalog_name"].asString();
				}
				if (schema_json.isMember("full_name")) {
					schema.full_name = schema_json["full_name"].asString();
				}

				schemas.push_back(std::move(schema));
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to parse schemas response: %s", e.what());
	}

	return schemas;
}

vector<OneLakeTable> OneLakeAPI::GetTablesFromSchema(ClientContext &context, const string &workspace_id,
                                                     const string &lakehouse_id, const string &lakehouse_name,
                                                     const string &schema_name, OneLakeCredentials &credentials) {
	vector<OneLakeTable> tables;

	// Build Unity Catalog API URL for tables in schema
	string url = "https://onelake.table.fabric.microsoft.com/delta/" + workspace_id + "/" + lakehouse_id +
	             "/api/2.1/unity-catalog/tables?catalog_name=" + lakehouse_name +
	             ".Lakehouse&schema_name=" + schema_name;

	// Use DFS scope for Unity Catalog API
	string access_token = GetAccessToken(credentials, OneLakeTokenAudience::OneLakeDfs);

	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for Unity Catalog tables API");
	}

	string response_string;
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

	struct curl_slist *headers = nullptr;
	string auth_header = "Authorization: Bearer " + access_token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	CURLcode res = curl_easy_perform(curl);
	long response_code;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("Unity Catalog tables API request failed: %s", curl_easy_strerror(res));
	}

	if (response_code < 200 || response_code >= 300) {
		throw IOException("Unity Catalog tables API returned error: HTTP %ld - %s", response_code,
		                  response_string.c_str());
	}

	try {
		Json::Value root;
		Json::Reader reader;

		if (!reader.parse(response_string, root)) {
			throw InvalidInputException("Failed to parse JSON schema tables response");
		}

		if (root.isMember("tables") && root["tables"].isArray()) {
			for (const auto &table_json : root["tables"]) {
				if (!table_json.isMember("name")) {
					continue;
				}

				OneLakeTable table;
				table.name = table_json["name"].asString();
				table.schema_name = schema_name;
				table.type = "Table"; // Unity Catalog tables are typically "Table"

				if (table_json.isMember("data_source_format")) {
					table.format = table_json["data_source_format"].asString();
				} else {
					table.format = "Delta"; // Default for Unity Catalog
				}

				if (table_json.isMember("storage_location")) {
					string storage_location = table_json["storage_location"].asString();
					if (StringUtil::StartsWith(storage_location, "https://")) {
						// Convert from
						// https://onelake.dfs.fabric.microsoft.com/<workspaceID>/<LakehouseID>/Tables/<schema>/<table_name>
						// to
						// abfss://<workspaceID>@onelake.dfs.fabric.microsoft.com/<LakehouseID>/Tables/<schema>/<table_name>
						const string https_prefix = "https://onelake.dfs.fabric.microsoft.com/";
						if (StringUtil::StartsWith(storage_location, https_prefix)) {
							string path_part = storage_location.substr(https_prefix.size());
							auto slash_pos = path_part.find('/');
							if (slash_pos != string::npos) {
								string workspace_part = path_part.substr(0, slash_pos);
								string remaining_path = path_part.substr(slash_pos + 1);
								table.location =
								    "abfss://" + workspace_part + "@onelake.dfs.fabric.microsoft.com/" + remaining_path;
							} else {
								table.location = storage_location;
							}
						} else {
							table.location = storage_location;
						}
					} else {
						table.location = storage_location;
					}
				}

				tables.push_back(std::move(table));
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to parse schema tables response: %s", e.what());
	}

	return tables;
}

OneLakeTableInfo OneLakeAPI::GetTableInfo(ClientContext &context, const string &workspace_id,
                                          const string &lakehouse_id, const string &table_name,
                                          OneLakeCredentials &credentials) {
	OneLakeTableInfo table_info;
	table_info.name = table_name;
	table_info.format = "Delta";

	string url = BuildAPIUrl(workspace_id, "lakehouses/" + lakehouse_id + "/tables/" + table_name);
	string response = MakeAPIRequest(context, url, credentials, true);
	if (response.empty()) {
		// Endpoint not available for this table; fall back to defaults
		return table_info;
	}

	try {
		Json::Value root;
		Json::Reader reader;

		if (!reader.parse(response, root)) {
			throw InvalidInputException("Failed to parse JSON table info response");
		}

		if (root.isMember("error")) {
			const auto &error_obj = root["error"];
			if (error_obj.isObject() && error_obj.isMember("message")) {
				throw InvalidInputException("OneLake API error while fetching table '%s': %s", table_name,
				                            error_obj["message"].asString());
			}
			throw InvalidInputException("OneLake API error while fetching table '%s': %s", table_name,
			                            error_obj.toStyledString());
		}

		table_info.has_metadata = true;
		if (root.isMember("name") && root["name"].isString()) {
			table_info.name = root["name"].asString();
		}
		if (root.isMember("format") && root["format"].isString()) {
			table_info.format = root["format"].asString();
		}
		if (root.isMember("location") && root["location"].isString()) {
			table_info.location = root["location"].asString();
		}

		// Get partition columns if available
		if (root.isMember("partitionColumns") && root["partitionColumns"].isArray()) {
			for (const auto &col : root["partitionColumns"]) {
				table_info.partition_columns.push_back(col.asString());
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to parse table info response: %s", e.what());
	}

	return table_info;
}

vector<string> OneLakeAPI::ListDirectory(ClientContext &context, const string &abfss_path,
                                         OneLakeCredentials &credentials) {
	(void)context;
	AbfssPathComponents components;
	if (!ParseAbfssPath(abfss_path, components)) {
		throw InvalidInputException("Invalid abfss path: %s", abfss_path);
	}

	auto token = GetAccessToken(credentials, OneLakeTokenAudience::OneLakeDfs);
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL for OneLake directory listing");
	}

	string directory = components.path;
	if (StringUtil::EndsWith(directory, "/")) {
		directory = directory.substr(0, directory.size() - 1);
	}

	string url = "https://" + components.host + "/" + components.container + "?resource=filesystem&recursive=false";
	char *escaped_directory = nullptr;
	if (!directory.empty()) {
		escaped_directory = curl_easy_escape(curl, directory.c_str(), static_cast<int>(directory.size()));
		if (!escaped_directory) {
			curl_easy_cleanup(curl);
			throw InternalException("Failed to URL-encode directory path for OneLake");
		}
		url += "&directory=" + string(escaped_directory);
	}

	string response_string;
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

	struct curl_slist *headers = nullptr;
	string auth_header = "Authorization: Bearer " + token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "x-ms-version: 2021-08-06");
	headers = curl_slist_append(headers, "Accept: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	CURLcode res = curl_easy_perform(curl);
	long response_code = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

	if (escaped_directory) {
		curl_free(escaped_directory);
	}
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		throw IOException("OneLake DFS list request failed: %s", curl_easy_strerror(res));
	}
	if (response_code < 200 || response_code >= 300) {
		throw IOException("OneLake DFS list returned HTTP %ld - %s", response_code, response_string.c_str());
	}

	vector<string> directories;
	std::unordered_set<string> seen;

	try {
		Json::Value root;
		Json::Reader reader;
		if (!reader.parse(response_string, root)) {
			throw InvalidInputException("Failed to parse OneLake DFS list response");
		}
		if (!root.isMember("paths") || !root["paths"].isArray()) {
			return directories;
		}
		for (const auto &entry : root["paths"]) {
			if (!JsonIsDirectory(entry)) {
				continue;
			}
			if (!entry.isMember("name") || !entry["name"].isString()) {
				continue;
			}
			auto name = entry["name"].asString();
			auto leaf = ComposeLeaf(directory, name);
			if (leaf.empty()) {
				continue;
			}
			if (!seen.insert(leaf).second) {
				continue;
			}
			directories.push_back(leaf);
		}
	} catch (const std::exception &ex) {
		throw InvalidInputException("Failed to parse OneLake DFS list response: %s", ex.what());
	}

	return directories;
}

} // namespace duckdb
