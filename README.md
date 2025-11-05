# Onelake

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Onelake, allow you to connect DuckDB to OneLake workspaces and lakehouses, enabling you to query data stored in OneLake directly from DuckDB.

DISCLAIMER: Currently, this extension is in an experimental phase and only supports reading Delta Lake tables in lakehouses created without schema.

## Features
- Authentication using:
    - Azure service principal credentials (or Fabric Workspace Managed Identity).
    - Credentials from environment variables.
    - Credentials picked up from the Azure CLI logged in user.
- Connect to OneLake workspaces and lakehouses.
- Attach multiple lakehouses from the same OneLake workspace.
- Set a default lakehouse for queries as a schema.
- Query Delta Lake tables stored in OneLake lakehouses with SQL syntax.

## Running the extension

# Prerequisites
To use the Onelake extension, you need to have access to a OneLake workspace and lakehouse. You will also need to have the necessary credentials for a service principal (tenant ID, client ID, and client secret) to authenticate with Azure, this extension was tested using a Workspace identity. Please follow the steps documented here : https://learn.microsoft.com/en-us/fabric/security/workspace-identity.

To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. For example, to load the extension, run:
Before starting the shell, export the following environment variable to point to your CA certificates file:
```sh
export CURL_CA_PATH=/etc/ssl/certs
# This path may vary based on your operating system and installation but is necessary for successful connections through the delta extension when attempting to validate tokens.

# Optional if using the CLI authentication method
az login

```
Then start the DuckDB shell:
```sh
./build/release/duckdb --unsigned
```
Then, in the DuckDB shell, run:
```sql
LOAD './extension/onelake/onelake.duckdb_extension';
set azure_transport_option_type = 'curl';
CREATE SECRET  (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID '<your_tenant_id>',
    CLIENT_ID '<your_client_id>',
    CLIENT_SECRET '<your_client_secret>'
);
-- CREATE SECRET  (
--     TYPE azure,
--     PROVIDER credential_chain,
--     CHAIN 'cli'
-- );

CREATE SECRET onelake (
    TYPE ONELAKE,
    TENANT_ID '<your_tenant_id>',
    CLIENT_ID '<your_client_id>',
    CLIENT_SECRET '<your_client_secret>'
);

-- CREATE SECRET  onelake(
--     TYPE ONELAKE,
--     PROVIDER credential_chain,
--     CHAIN 'cli'
-- );

ATTACH 'onelake://<your_workspace_id>'
      AS <your_connection_name>
      (TYPE ONELAKE, DEFAULT_LAKEHOUSE '<your_lakehouse_id_or_name>');

USE <your_connection_name>;

SHOW TABLES;

SELECT * FROM <your_table_name> LIMIT 10;
SELECT * FROM <second_lakehouse>.<your_table_name> LIMIT 10;
```

Optionally, you can replace the secret creation and authentication steps by setting the following environment variables before starting the DuckDB shell:

```sh
export ONELAKE_TENANT_ID='<your_tenant_id>'
export ONELAKE_CLIENT_ID='<your_client_id>'
export ONELAKE_CLIENT_SECRET='<your_client_secret>'
```
And then in the DuckDB shell, you can replace the `CREATE SECRET` statements with:
```sql
CREATE SECRET onelake (
    TYPE ONELAKE,
    TENANT_ID '${ONELAKE_TENANT_ID}',
    CLIENT_ID '${ONELAKE_CLIENT_ID}',
    CLIENT_SECRET '${ONELAKE_CLIENT_SECRET}'
);
CREATE SECRET  (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID '${ONELAKE_TENANT_ID}',
    CLIENT_ID '${ONELAKE_CLIENT_ID}',
    CLIENT_SECRET '${ONELAKE_CLIENT_SECRET}'
);
```

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/onelake/onelake.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `onelake.duckdb_extension` is the loadable binary as it would be distributed.