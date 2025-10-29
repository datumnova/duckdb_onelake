#pragma once
#include "onelake_credentials.hpp"
#include "onelake_types.hpp"
#include "duckdb.hpp"

namespace duckdb {

class OneLakeAPI {
public:
    static vector<OneLakeLakehouse> GetLakehouses(ClientContext &context,
                                                 const string &workspace_id,
                                                 const OneLakeCredentials &credentials);
    
    static vector<OneLakeTable> GetTables(ClientContext &context,
                                         const string &workspace_id,
                                         const string &lakehouse_id,
                                         const OneLakeCredentials &credentials);
    
    static OneLakeTableInfo GetTableInfo(ClientContext &context,
                                        const string &workspace_id,
                                        const string &lakehouse_id,
                                        const string &table_name,
                                        const OneLakeCredentials &credentials);

private:
    static string MakeAPIRequest(ClientContext &context,
                               const string &url,
                               const OneLakeCredentials &credentials);
    
    static string GetAccessToken(const OneLakeCredentials &credentials);
    static string BuildAPIUrl(const string &workspace_id, const string &endpoint);
};

} // namespace duckdb