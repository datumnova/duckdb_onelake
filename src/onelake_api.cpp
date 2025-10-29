#include "onelake_api.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <curl/curl.h>
#include <json/json.h>

namespace duckdb {

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, string *userp) {
    size_t totalSize = size * nmemb;
    userp->append((char*)contents, totalSize);
    return totalSize;
}

string OneLakeAPI::GetAccessToken(const OneLakeCredentials &credentials) {
    if (!credentials.IsTokenExpired() && !credentials.access_token.empty()) {
        return credentials.access_token;
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
    post_data += "&scope=https://api.fabric.microsoft.com/.default";
    
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
        
        return root["access_token"].asString();
    } catch (const std::exception &e) {
        throw InvalidInputException("Failed to parse token response: %s", e.what());
    }
}

string OneLakeAPI::MakeAPIRequest(ClientContext &context, const string &url, const OneLakeCredentials &credentials) {
    string access_token = GetAccessToken(credentials);
    
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

    if (response_code < 200 || response_code >= 300) {
        throw IOException("OneLake API returned error: HTTP %ld - %s", response_code, response_string.c_str());
    }
    
    return response_string;
}

string OneLakeAPI::BuildAPIUrl(const string &workspace_id, const string &endpoint) {
    return "https://api.fabric.microsoft.com/v1/workspaces/" + workspace_id + "/" + endpoint;
}

vector<OneLakeLakehouse> OneLakeAPI::GetLakehouses(ClientContext &context, const string &workspace_id, const OneLakeCredentials &credentials) {
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

vector<OneLakeTable> OneLakeAPI::GetTables(ClientContext &context, const string &workspace_id, const string &lakehouse_id, const OneLakeCredentials &credentials) {
    string url = BuildAPIUrl(workspace_id, "lakehouses/" + lakehouse_id + "/tables");
    string response = MakeAPIRequest(context, url, credentials);
    
    vector<OneLakeTable> tables;
    
    try {
        Json::Value root;
        Json::Reader reader;
        
        if (!reader.parse(response, root)) {
            throw InvalidInputException("Failed to parse JSON tables response");
        }
        
        if (!root.isMember("value") || !root["value"].isArray()) {
            if (root.isMember("error")) {
                const auto &error_obj = root["error"];
                if (error_obj.isObject() && error_obj.isMember("message")) {
                    throw InvalidInputException("OneLake API error while listing tables: %s",
                                                error_obj["message"].asString());
                }
                throw InvalidInputException("OneLake API error while listing tables: %s",
                                            error_obj.toStyledString());
            }
            if (root.isMember("message")) {
                throw InvalidInputException("OneLake API returned message while listing tables: %s",
                                            root["message"].asString());
            }
            // Some responses might omit the value array when there are no tables
            return tables;
        }
        
        auto &table_array = root["value"];
        for (const auto &table_json : table_array) {
            OneLakeTable table;
            table.name = table_json["name"].asString();
            table.type = table_json.isMember("type") ? table_json["type"].asString() : "Table";
            table.format = table_json.isMember("format") ? table_json["format"].asString() : "Delta";
            
            if (table_json.isMember("location")) {
                table.location = table_json["location"].asString();
            }
            
            tables.push_back(table);
        }
    } catch (const std::exception &e) {
        throw InvalidInputException("Failed to parse tables response: %s", e.what());
    }
    
    return tables;
}

OneLakeTableInfo OneLakeAPI::GetTableInfo(ClientContext &context, const string &workspace_id, const string &lakehouse_id, const string &table_name, const OneLakeCredentials &credentials) {
    string url = BuildAPIUrl(workspace_id, "lakehouses/" + lakehouse_id + "/tables/" + table_name);
    string response = MakeAPIRequest(context, url, credentials);
    
    OneLakeTableInfo table_info;
    
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

        if (!root.isMember("name")) {
            throw InvalidInputException("Unexpected API response format for table '%s'", table_name);
        }

        table_info.name = root["name"].asString();
        table_info.format = root.isMember("format") ? root["format"].asString() : "Delta";
        
        if (root.isMember("location")) {
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

} // namespace duckdb