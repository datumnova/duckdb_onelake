#pragma once
#include "onelake_credentials.hpp"
#include "onelake_types.hpp"
#include "duckdb.hpp"

namespace duckdb {

/// @brief Enum representing the target audience for OneLake authentication tokens.
enum class OneLakeTokenAudience {
	/// @brief Token for Microsoft Fabric Management APIs (https://api.fabric.microsoft.com)
	Fabric,
	/// @brief Token for OneLake DFS / Azure Data Lake Storage (https://storage.azure.com)
	OneLakeDfs
};

/// @brief Static helper class for interacting with Microsoft Fabric and OneLake APIs.
class OneLakeAPI {
public:
	/// @brief Retrieves a list of accessible workspaces for the authenticated user.
	/// @param context The client context.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of OneLakeWorkspace objects.
	static vector<OneLakeWorkspace> GetWorkspaces(ClientContext &context, OneLakeCredentials &credentials);

	/// @brief Retrieves a list of lakehouses within a specific workspace.
	/// @param context The client context.
	/// @param workspace_id The ID of the workspace.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of OneLakeLakehouse objects.
	static vector<OneLakeLakehouse> GetLakehouses(ClientContext &context, const string &workspace_id,
	                                              OneLakeCredentials &credentials);

	/// @brief Retrieves a list of tables within a specific lakehouse (legacy/flat structure).
	/// @param context The client context.
	/// @param workspace_id The ID of the workspace.
	/// @param lakehouse_id The ID of the lakehouse.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of OneLakeTable objects.
	static vector<OneLakeTable> GetTables(ClientContext &context, const string &workspace_id,
	                                      const string &lakehouse_id, OneLakeCredentials &credentials);

	/// @brief Retrieves a list of tables within a lakehouse, supporting both flat and schema-enabled structures.
	/// @param context The client context.
	/// @param workspace_id The ID of the workspace.
	/// @param lakehouse The lakehouse object containing ID and name.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of OneLakeTable objects.
	static vector<OneLakeTable> GetTables(ClientContext &context, const string &workspace_id,
	                                      const OneLakeLakehouse &lakehouse, OneLakeCredentials &credentials);

	/// @brief Retrieves a list of schemas within a schema-enabled lakehouse.
	/// @param context The client context.
	/// @param workspace_id The ID of the workspace.
	/// @param lakehouse_id The ID of the lakehouse.
	/// @param lakehouse_name The name of the lakehouse.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of OneLakeSchema objects.
	static vector<OneLakeSchema> GetSchemas(ClientContext &context, const string &workspace_id,
	                                        const string &lakehouse_id, const string &lakehouse_name,
	                                        OneLakeCredentials &credentials);

	/// @brief Retrieves a list of tables within a specific schema of a lakehouse.
	/// @param context The client context.
	/// @param workspace_id The ID of the workspace.
	/// @param lakehouse_id The ID of the lakehouse.
	/// @param lakehouse_name The name of the lakehouse.
	/// @param schema_name The name of the schema.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of OneLakeTable objects.
	static vector<OneLakeTable> GetTablesFromSchema(ClientContext &context, const string &workspace_id,
	                                                const string &lakehouse_id, const string &lakehouse_name,
	                                                const string &schema_name, OneLakeCredentials &credentials);

	/// @brief Retrieves detailed information about a specific table.
	/// @param context The client context.
	/// @param workspace_id The ID of the workspace.
	/// @param lakehouse The lakehouse object.
	/// @param schema_name The schema name (optional).
	/// @param table_name The name of the table.
	/// @param format_hint A hint for the table format (e.g., "Delta", "Iceberg").
	/// @param credentials The credentials to use for authentication.
	/// @return A OneLakeTableInfo object containing metadata.
	static OneLakeTableInfo GetTableInfo(ClientContext &context, const string &workspace_id,
	                                     const OneLakeLakehouse &lakehouse, const string &schema_name,
	                                     const string &table_name, const string &format_hint,
	                                     OneLakeCredentials &credentials);

	/// @brief Lists the contents of a directory in OneLake DFS.
	/// @param context The client context.
	/// @param abfss_path The ABFSS path to list.
	/// @param credentials The credentials to use for authentication.
	/// @return A vector of strings representing the directory contents.
	static vector<string> ListDirectory(ClientContext &context, const string &abfss_path,
	                                    OneLakeCredentials &credentials);

	/// @brief Retrieves an access token for the specified audience.
	/// @param credentials The credentials object to use (and cache tokens in).
	/// @param audience The target audience for the token (Fabric or DFS).
	/// @return The access token string.
	static string GetAccessToken(OneLakeCredentials &credentials,
	                             OneLakeTokenAudience audience = OneLakeTokenAudience::Fabric);

	/// @brief Drops a Unity Catalog-backed Delta table from Fabric catalog (not storage).
	/// @param context The client context.
	/// @param workspace_id The workspace ID.
	/// @param lakehouse The lakehouse object (id and name required).
	/// @param schema_name Schema name for schema-enabled lakehouses; may be empty for flat lakehouses.
	/// @param table_name Table to drop.
	/// @param credentials Credentials for auth.
	/// @param allow_not_found If true, 404 is ignored.
	static void DropUnityCatalogTable(ClientContext &context, const string &workspace_id,
	                                 const OneLakeLakehouse &lakehouse, const string &schema_name,
	                                 const string &table_name, OneLakeCredentials &credentials,
	                                 bool allow_not_found = true);

private:
	static string MakeAPIRequest(ClientContext &context, const string &url, OneLakeCredentials &credentials,
	                             bool allow_not_found = false);
	static string BuildAPIUrl(const string &workspace_id, const string &endpoint);
};

} // namespace duckdb
