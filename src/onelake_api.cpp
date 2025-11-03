#include "onelake_api.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include <curl/curl.h>
#include <json/json.h>
#include <unordered_set>

namespace duckdb {

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, string *userp) {
    size_t totalSize = size * nmemb;
    userp->append((char*)contents, totalSize);
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

static const char *FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default";
static const char *DFS_SCOPE = "https://storage.azure.com/.default";

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

string OneLakeAPI::GetAccessToken(OneLakeCredentials &credentials, OneLakeTokenAudience audience) {
    const auto &scope = ScopeForAudience(audience);
    auto current_time = Timestamp::GetCurrentTimestamp();
    auto cache_entry = credentials.token_cache.find(scope);
    if (cache_entry != credentials.token_cache.end()) {
        if (!cache_entry->second.token.empty() && current_time < cache_entry->second.expiry) {
            return cache_entry->second.token;
        }
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

vector<OneLakeLakehouse> OneLakeAPI::GetLakehouses(ClientContext &context, const string &workspace_id, OneLakeCredentials &credentials) {
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
            
            lakehouses.push_back(lakehouse);
        }
    } catch (const std::exception &e) {
        throw InvalidInputException("Failed to parse lakehouses response: %s", e.what());
    }
    
    return lakehouses;
}

vector<OneLakeTable> OneLakeAPI::GetTables(ClientContext &context, const string &workspace_id, const string &lakehouse_id, OneLakeCredentials &credentials) {
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
                throw InvalidInputException("OneLake API error while listing tables: %s",
                                            error_obj.toStyledString());
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
                    next_url = BuildAPIUrl(workspace_id, "lakehouses/" + lakehouse_id + "/tables?continuationToken=" + encoded_token);
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

OneLakeTableInfo OneLakeAPI::GetTableInfo(ClientContext &context, const string &workspace_id, const string &lakehouse_id, const string &table_name, OneLakeCredentials &credentials) {
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

vector<string> OneLakeAPI::ListDirectory(ClientContext &context, const string &abfss_path, OneLakeCredentials &credentials) {
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