# Onelake

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Onelake, allow you to connect DuckDB to OneLake workspaces and lakehouses, enabling you to query data stored in OneLake directly from DuckDB.

DISCLAIMER: Currently, this extension is in an experimental phase..

## Features
- Authentication using:
    - Azure service principal credentials (or Fabric Workspace Managed Identity).
    - Credentials from environment variables via a configurable credential chain (`CHAIN 'env'`).
    - Credentials picked up from the Azure CLI logged in user.
- Connect to OneLake workspaces and lakehouses.
- Attach multiple lakehouses from the same OneLake workspace.
- Set a default lakehouse.schema for queries .
- Query Delta and Iceberg tables stored in OneLake lakehouses with SQL syntax.
- Append to existing Delta tables via standard DuckDB `INSERT` statements (append-only writes).

### Current Limitations

- The Extension only works with normal Lakehouses, schema enabled Lakehouses fail to attach due to the Fabric API limitation [More here](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas#public-preview-limitations)

### Detailed Documentation
For more detailed documentation on the Onelake extension, including architecture, authentication, database attachment, table discovery, Apache Iceberg support, code reference, API integration, and limitations, please refer to the [DOCUMENTATION.md](DOCUMENTATION.md) file.


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

-- Required format: workspace-name/lakehouse-name.Lakehouse
ATTACH '<your_workspace_name>/<your_lakehouse_name>.Lakehouse'
    AS <your_connection_name>
    (TYPE ONELAKE);

USE <your_connection_name>.<schema_name>;

SHOW TABLES;

SELECT * FROM <your_table_name>;

-- SELECT * FROM <your_iceberg_table_name> USING ICEBERG;

-- Append to a Delta table that lives in the current schema
INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes');
-- Or insert the results of another query
INSERT INTO fact_sales
SELECT * FROM staging_sales;
```

Optionally, you can replace the secret creation and authentication steps by setting the following environment variables before starting the DuckDB shell:

```sh
export ONELAKE_TENANT_ID='<your_tenant_id>'
export ONELAKE_CLIENT_ID='<your_client_id>'
export ONELAKE_CLIENT_SECRET='<your_client_secret>'
export AZURE_STORAGE_TOKEN='<preissued_onelake_access_token>'
export FABRIC_API_TOKEN='<preissued_fabric_api_token>'
```
And then in the DuckDB shell, you can replace the `CREATE SECRET` statements with:
```sql
CREATE SECRET onelake (
    TYPE ONELAKE,
    TENANT_ID getenv('ONELAKE_TENANT_ID'),
    CLIENT_ID getenv('ONELAKE_CLIENT_ID'),
    CLIENT_SECRET getenv('ONELAKE_CLIENT_SECRET')
);
CREATE SECRET  (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID getenv('ONELAKE_TENANT_ID'),
    CLIENT_ID getenv('ONELAKE_CLIENT_ID'),
    CLIENT_SECRET getenv('ONELAKE_CLIENT_SECRET')
);

-- Optional: use preissued tokens stored in env variables (defaults shown)
SET onelake_env_fabric_token_variable = 'FABRIC_API_TOKEN';
SET onelake_env_storage_token_variable = 'AZURE_STORAGE_TOKEN';
CREATE SECRET onelake_env (
    TYPE ONELAKE,
    PROVIDER credential_chain,
    CHAIN 'env'
);
-- Combine chain steps if you want CLI fallback
CREATE SECRET onelake_env_chain (
    TYPE ONELAKE,
    PROVIDER credential_chain,
    CHAIN 'cli, env'
);

-- Optionally keep the token in-session instead of touching the shell
SET VARIABLE AZURE_STORAGE_TOKEN = '<preissued_onelake_access_token>';
```

Every `ATTACH ... (TYPE ONELAKE)` run now tries to auto-create the secrets it needs from those tokens:

- a temporary OneLake secret named `__onelake_env_secret` whose credential chain is `env` for Fabric API calls, and
- an Azure access-token secret named `env_secret` so `httpfs`/`delta` can authenticate against the DFS endpoint.

Both secrets reuse the variable names captured when you issued `CREATE SECRET`, so you only need to provide
`FABRIC_API_TOKEN`/`AZURE_STORAGE_TOKEN` (or their overrides) via environment variables or `SET VARIABLE`. If either
token is missing, `ATTACH` raises an error that points to the exact variable to populate, which avoids ambiguous
"no secret found" failures. Manual Azure secret creation is no longer required in the env-token flow. Token lookup
prefers values from `SET VARIABLE <name>` before falling back to the surrounding environment, which keeps tokens
scoped to the DuckDB session when desired.

The `onelake_env_fabric_token_variable` and `onelake_env_storage_token_variable` options are scoped like any other
DuckDB setting. Set them *before* `CREATE SECRET` when you want the extension to remember different variable names.
When left untouched they continue to default to `FABRIC_API_TOKEN` and `AZURE_STORAGE_TOKEN` respectively.

### Writing to OneLake tables

`INSERT` now streams chunks into the Delta writer that ships with this repository. A few tips:

- **Target format**: writes are currently limited to Delta tables that already exist in the lakehouse. Iceberg writes
    remain read-only for now.
- **Schema alignment**: DuckDB reuses the table schema discovered during `ATTACH`, so column order and types must match.
    Use an explicit column list (`INSERT INTO people(id, name) VALUES ...`) when you need to reorder or omit columns.
- **Authentication**: the same secrets created for reading are reused for writes. No extra configuration is required
    beyond ensuring the service principal (or issued token) has write permissions in the lakehouse.
- **Row count**: the DuckDB shell shows a single-row result with the number of appended rows (e.g., `2`). If you are
    running in a non-interactive context, capture the result set to read the inserted row count.

Any insertable DuckDB query works, including `INSERT INTO table SELECT ...` pipelines and parameterized statements. The
extension batches the input into Arrow streams and hands them to the Rust-based Delta writer, so large inserts benefit
from DuckDB's vectorized execution without additional configuration.

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

#### Platform-specific notes

- **macOS cross-compilation**: the Rust-based Delta writer now follows the architecture requested through `CMAKE_OSX_ARCHITECTURES`. If you are building Intel artifacts from an Apple Silicon host, pass `-DCMAKE_OSX_ARCHITECTURES=x86_64` (or `arm64` for native builds) when configuring DuckDB. The build invokes `rustup target add <triple>` automatically, so `rustup` must be available on the PATH.
- **macOS deployment target**: Cargo inherits `MACOSX_DEPLOYMENT_TARGET`, so the build injects a default of `11.0` whenever `CMAKE_OSX_DEPLOYMENT_TARGET` is unset. Override it via `-DCMAKE_OSX_DEPLOYMENT_TARGET=12.3` (for example) if you need a newer baseline to match your distribution policy.
- **Windows builds**: the same Rust target detection produces a `.lib` that matches MSVC expectations, so no manual renaming is required. Simply run the standard `make` (or the corresponding CMake/Ninja invocation) inside a Developer Command Prompt.

### Rust delta writer prerequisites

Write support is implemented in a companion Rust crate (`rust/onelake_delta_writer`) that exposes a
static library consumed by the C++ extension. CMake runs `cargo build --release` automatically, so
remote builds keep working as long as the builder image has `cargo` available. For local builds you
must install a Rust toolchain once (the default stable channel is sufficient):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustup update stable
```

After installing Rust, re-run `make` and the build will place `libonelake_delta_writer.a` plus the
generated C header under `build/release/onelake_delta_writer/`, making the C++ library available.

To iterate on the Rust writer itself (for example before running an end-to-end local build) you can
use the crate's standard workflow:

```sh
cd rust/onelake_delta_writer
SKIP_ONELAKE_HEADER=1 cargo test
```

Setting `SKIP_ONELAKE_HEADER=1` skips header generation during tests, which keeps the inner loop
fast while still exercising the Delta writer logic.