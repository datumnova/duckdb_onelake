#pragma once
#include "onelake_credentials.hpp"
#include "onelake_types.hpp"
#include "duckdb.hpp"

namespace duckdb {

enum class OneLakeTokenAudience { Fabric, OneLakeDfs, OneLakeBlob };

class OneLakeAPI {
public:
	static vector<OneLakeWorkspace> GetWorkspaces(ClientContext &context, OneLakeCredentials &credentials);

	static vector<OneLakeLakehouse> GetLakehouses(ClientContext &context, const string &workspace_id,
	                                              OneLakeCredentials &credentials);

	static vector<OneLakeTable> GetTables(ClientContext &context, const string &workspace_id,
	                                      const string &lakehouse_id, OneLakeCredentials &credentials);

	static vector<OneLakeTable> GetTables(ClientContext &context, const string &workspace_id,
	                                      const OneLakeLakehouse &lakehouse, OneLakeCredentials &credentials);

	static vector<OneLakeSchema> GetSchemas(ClientContext &context, const string &workspace_id,
	                                        const string &lakehouse_id, const string &lakehouse_name,
	                                        OneLakeCredentials &credentials);

	static vector<OneLakeTable> GetTablesFromSchema(ClientContext &context, const string &workspace_id,
	                                                const string &lakehouse_id, const string &lakehouse_name,
	                                                const string &schema_name, OneLakeCredentials &credentials);

	static OneLakeTableInfo GetTableInfo(ClientContext &context, const string &workspace_id, const string &lakehouse_id,
	                                     const string &table_name, OneLakeCredentials &credentials);
	static vector<string> ListDirectory(ClientContext &context, const string &abfss_path,
	                                    OneLakeCredentials &credentials);

	static string GetAccessToken(ClientContext *context, OneLakeCredentials &credentials,
	                             OneLakeTokenAudience audience = OneLakeTokenAudience::Fabric);

private:
	static string MakeAPIRequest(ClientContext &context, const string &url, OneLakeCredentials &credentials,
	                             bool allow_not_found = false);
	static string BuildAPIUrl(const string &workspace_id, const string &endpoint);
};

} // namespace duckdb
