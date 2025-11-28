# OneLake Extension Write API Documentation

**Version:** 1.2  
**Last Updated:** November 27, 2025  
**Status:** Phase 3 - DROP TABLE Complete

---

## Overview

This document describes the C++/Rust FFI (Foreign Function Interface) for write operations in the DuckDB OneLake extension. The write functionality is split across two layers:

1. **C++ Layer** - DuckDB integration, query planning, data collection
2. **Rust Layer** - Delta Lake operations via the `deltalake` crate

## Architecture

```
┌─────────────────────────────────────────────────────┐
│           DuckDB Query Execution                    │
│  (SQL Parser → Planner → Physical Operators)        │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│       C++ Write Operators                           │
│  - PhysicalOneLakeInsert                           │
│  - PhysicalOneLakeDelete (future)                  │
│  - PhysicalOneLakeUpdate (future)                  │
│  - PhysicalOneLakeMerge (future)                   │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│       OneLakeDeltaWriter (C++ wrapper)              │
│  - Serializes requests to JSON                     │
│  - Converts DataChunks to Arrow streams            │
│  - Handles error translation                       │
└──────────────────┬──────────────────────────────────┘
                   │ FFI Boundary (C ABI)
                   ▼
┌─────────────────────────────────────────────────────┐
│       Rust Delta Writer Library                     │
│  - onelake_delta_writer crate                      │
│  - Arrow stream processing                         │
│  - Delta transaction log updates                   │
│  - Azure storage integration                       │
└─────────────────────────────────────────────────────┘
```

---

## C++ Layer API

### OneLakeDeltaWriteRequest

Primary structure for passing write requests from C++ to Rust.

**File:** `src/include/onelake_delta_writer.hpp`

```cpp
struct OneLakeDeltaWriteRequest {
    // Required fields
    string table_uri;           // ABFSS URI: abfss://workspace@onelake.../table/
    string token_json;          // JSON with Azure tokens (see Token Format)
    string options_json;        // JSON with write options (see Options Format)
    vector<string> column_names;// Column names for schema alignment
    
    // Optional fields (Phase 1+)
    string write_mode;          // "append"|"overwrite"|"error_if_exists"|"ignore"
    string schema_mode;         // ""|"merge"|"overwrite"
    string replace_where;       // SQL predicate for conditional replacement
    bool safe_cast;             // Enable safe type casting
    optional_idx target_file_size;    // Target Parquet file size in bytes
    optional_idx write_batch_size;    // Rows per batch
};
```

### OneLakeDeltaWriter Class

**File:** `src/onelake_delta_writer.cpp`

#### Append Method (Current)

```cpp
static void Append(
    ClientContext &context,
    DataChunk &chunk,
    const OneLakeDeltaWriteRequest &request
);
```

**Purpose:** Appends a batch of rows to an existing Delta table.

**Parameters:**
- `context` - DuckDB client context for error handling and logging
- `chunk` - DataChunk containing rows to insert
- `request` - Write request configuration

**Throws:** `IOException` on write failure

**Example Usage:**
```cpp
OneLakeDeltaWriteRequest request;
request.table_uri = "abfss://workspace@onelake.../lakehouse/Tables/my_table/";
request.token_json = SerializeTokenJson(access_token);
request.options_json = SerializeWriteOptions(partition_columns);
request.column_names = {"id", "name", "value"};

for (auto &chunk : data_collection.Chunks()) {
    chunk.Flatten();
    OneLakeDeltaWriter::Append(context, chunk, request);
}
```

#### CreateTable Method (Phase 2 - Implemented)

```cpp
static void CreateTable(
    ClientContext &context,
    const string &table_uri,
    const vector<ColumnDefinition> &columns,
    const string &token_json,
    const string &options_json
);
```

**Purpose:** Creates a new Delta table in OneLake storage.

**Parameters:**
- `context` - DuckDB client context for error handling and logging
- `table_uri` - ABFSS URI where the table will be created
- `columns` - Table schema (column names and types)
- `token_json` - JSON with Azure authentication tokens
- `options_json` - JSON with creation options (partitions, description, config)

**Throws:** `IOException` on creation failure

**Status:** ✅ Implemented (Phase 2 complete)

**Supported Types:**
- Numeric: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE
- String: VARCHAR
- Temporal: DATE, TIMESTAMP
- Decimal: DECIMAL(precision, scale)

**Example Usage:**
```cpp
vector<ColumnDefinition> columns;
columns.push_back(ColumnDefinition("id", LogicalType::INTEGER));
columns.push_back(ColumnDefinition("name", LogicalType::VARCHAR));
columns.push_back(ColumnDefinition("amount", LogicalType::DECIMAL(18, 2)));

string options_json = R"({
    "tableName": "sales",
    "description": "Sales transactions",
    "partitionColumns": ["region"]
})";

OneLakeDeltaWriter::CreateTable(context, table_uri, columns, 
                                token_json, options_json);
```

#### DropTable Method (Phase 3 - Future)

```cpp
static void DropTable(
    ClientContext &context,
    const string &table_uri,
    const OneLakeCredentials &credentials,
    bool delete_data
);
```

**Purpose:** Deletes a Delta table from storage.

**Status:** Not yet implemented

---

## Rust Layer API

### FFI Entry Points

**File:** `rust/onelake_delta_writer/src/lib.rs`

#### ol_delta_append

```rust
#[no_mangle]
pub extern "C" fn ol_delta_append(
    stream: *mut FFI_ArrowArrayStream,
    table_uri: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int
```

**Purpose:** Appends Arrow record batches to a Delta table.

**Parameters:**
- `stream` - Pointer to Arrow C stream interface containing row data
- `table_uri` - UTF-8 encoded table URI (ABFSS format)
- `token_json` - JSON with authentication tokens
- `options_json` - JSON with write configuration
- `error_buffer` - Buffer for error messages (output parameter)
- `error_buffer_len` - Size of error buffer

**Return Values:**
- `0` (OlDeltaStatus::Ok) - Success
- `1` (OlDeltaStatus::InvalidInput) - Invalid parameters
- `3` (OlDeltaStatus::ArrowError) - Arrow processing error
- `4` (OlDeltaStatus::DeltaError) - Delta operation error
- `5` (OlDeltaStatus::JsonError) - JSON parsing error
- `6` (OlDeltaStatus::RuntimeError) - Tokio runtime error

**Error Handling:**
On non-zero return, `error_buffer` contains a null-terminated error message.

**Thread Safety:** Function is thread-safe. Multiple concurrent calls are supported.

---

## JSON Payload Formats

### Token JSON Format

Provides Azure authentication tokens for accessing OneLake storage.

```json
{
    "storageToken": "eyJ0eXAiOiJKV1QiLCJhbGc...",  // Azure Storage access token
    "fabricToken": "eyJ0eXAiOiJKV1QiLCJhbGc..."   // Fabric API token (optional)
}
```

**Notes:**
- `storageToken` is required for write operations
- `fabricToken` is used for Fabric API calls (metadata, catalog)
- Tokens are typically obtained via OneLakeCredentials::GetAccessToken()

**Alternative Keys Supported:**
- `storage_token`, `token` → maps to `storageToken`
- `fabric_token` → maps to `fabricToken`

### Options JSON Format (Current)

**Phase 0-1 Format:**

```json
{
    "mode": "append",
    "partitionColumns": ["region", "date"],
    "safeCast": false,
    "targetFileSize": 134217728,
    "writeBatchSize": 10000,
    "schemaMode": "merge",
    "replaceWhere": "date < '2025-01-01'"
}
```

**Field Descriptions:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | string | "append" | Write mode: append, overwrite, error_if_exists, ignore |
| `partitionColumns` | array[string] | [] | Columns to partition by |
| `safeCast` | boolean | false | Enable type casting with validation |
| `targetFileSize` | integer | - | Target size for Parquet files in bytes |
| `writeBatchSize` | integer | - | Number of rows per write batch |
| `schemaMode` | string | - | Schema evolution: merge, overwrite |
| `replaceWhere` | string | - | SQL predicate for conditional replacement |
| `tableName` | string | - | Table name for metadata |
| `description` | string | - | Table description |
| `configuration` | object | {} | Delta table properties |
| `createCheckpoint` | boolean | - | Create checkpoint after write |
| `cleanupExpiredLogs` | boolean | - | Cleanup old transaction logs |

### Options JSON Format (Phase 2+ - Future)

**CREATE TABLE Options:**

```json
{
    "tableName": "sales_data",
    "description": "Sales transactions by region",
    "partitionColumns": ["region", "year"],
    "configuration": {
        "delta.appendOnly": "true",
        "delta.checkpointInterval": "10",
        "delta.enableChangeDataFeed": "false"
    }
}
```

**DELETE Options:**

```json
{
    "predicate": "date < '2024-01-01'",
    "dryRun": false
}
```

**UPDATE Options:**

```json
{
    "predicate": "status = 'pending'",
    "updates": {
        "status": "'processed'",
        "updated_at": "CURRENT_TIMESTAMP()"
    }
}
```

---

## Arrow Stream Interface

### Arrow C Stream ABI

The extension uses the Arrow C Stream interface for zero-copy data transfer.

**Structure:**
```c
struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};
```

### C++ Arrow Stream Creation

**File:** `src/onelake_delta_writer.cpp`

```cpp
class SingleBatchStreamHolder {
public:
    SingleBatchStreamHolder(ClientContext &context, 
                           DataChunk &chunk,
                           const vector<string> &column_names);
    
    ArrowArrayStream* GetStream();
    void Close();
};
```

**Lifecycle:**
1. Create holder with DuckDB DataChunk
2. Convert to Arrow via `ArrowConverter::ToArrowArray`
3. Pass stream pointer to Rust via FFI
4. Rust reads batches via `get_next` callback
5. Close stream to free resources

### Rust Arrow Stream Processing

**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
unsafe fn collect_batches(
    stream: *mut FFI_ArrowArrayStream
) -> Result<Vec<RecordBatch>, DeltaFfiError> {
    let mut reader = ArrowArrayStreamReader::from_raw(stream)?;
    let mut batches = Vec::new();
    
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }
    
    Ok(batches)
}
```

---

## Error Handling

### Error Code Enumeration

```rust
#[repr(i32)]
pub enum OlDeltaStatus {
    Ok = 0,
    InvalidInput = 1,
    ArrowError = 3,
    DeltaError = 4,
    JsonError = 5,
    RuntimeError = 6,
    InternalError = 100,
}
```

### C++ Error Translation

```cpp
if (status != static_cast<int>(OlDeltaStatus::Ok)) {
    string error_msg = error_buffer[0] ? string(error_buffer.data()) 
                                       : "Unknown delta writer error";
    throw IOException("Delta writer failed: %s", error_msg.c_str());
}
```

### Common Error Scenarios

| Error Type | Cause | Resolution |
|------------|-------|------------|
| InvalidInput | Null pointers, empty URIs | Validate request before FFI call |
| ArrowError | Schema mismatch, invalid data | Check column types align with table |
| DeltaError | Transaction conflict, storage failure | Retry with backoff, check permissions |
| JsonError | Malformed JSON payloads | Validate JSON serialization |
| RuntimeError | Tokio runtime failure | Check system resources |

---

## Logging and Debugging

### C++ Logging Macros

**File:** `src/include/onelake_logging.hpp`

```cpp
// Standard logging
ONELAKE_LOG_INFO(&context, "[write] Starting INSERT: table=%s, rows=%llu", 
                 table_name, row_count);

// Write-specific logging
ONELAKE_LOG_WRITE_START(&context, "insert", table_name, "mode=append");
ONELAKE_LOG_WRITE_SUCCESS(&context, "insert", table_name, rows_inserted, 
                          "files_added=3");
ONELAKE_LOG_WRITE_ERROR(&context, "insert", table_name, error_msg);

// Delta transaction logging
ONELAKE_LOG_DELTA_COMMIT(&context, table_name, version, files_added, files_removed);
ONELAKE_LOG_DELTA_URI(&context, table_name, resolved_uri);
```

### Rust Logging

The Rust layer uses standard error messages returned via `error_buffer`. For internal debugging:

```rust
// Add debug prints (remove before production)
eprintln!("DEBUG: Processing {} batches", batches.len());
```

**Log Levels:**
- `TRACE` - Detailed execution flow
- `DEBUG` - URI resolution, path building
- `INFO` - Operation start/success
- `WARN` - Non-fatal issues (fallbacks, retries)
- `ERROR` - Operation failures

---

## Performance Considerations

### Batch Size Tuning

**Default:** DuckDB's internal chunking (typically 2048 rows)

**Tuning:**
```cpp
// Collect larger batches before writing
request.write_batch_size = optional_idx(10000);
```

**Trade-offs:**
- Larger batches → Fewer Parquet files → Better query performance
- Larger batches → Higher memory usage → Risk of OOM
- Smaller batches → More frequent I/O → Higher transaction overhead

### File Size Optimization

```cpp
// Target 128 MB Parquet files
request.target_file_size = optional_idx(134217728);
```

**Impact:**
- Optimal for query engines (balance between scan efficiency and parallelism)
- Too small → Excessive file count → Slow table scans
- Too large → Poor parallelism → Slower queries

### Partition Strategy

```cpp
// Partition by frequently filtered columns
request.partition_columns = {"region", "year", "month"};
```

**Benefits:**
- Partition pruning during queries
- Improved write parallelism
- Better data organization

**Cautions:**
- Too many partitions → Small files → Performance degradation
- Cardinality explosion → Thousands of directories

---

## Catalog Integration (Phase 2)

### OneLakeTableSet::CreateTable

**File:** `src/storage/onelake_table_set.cpp`

The catalog's `CreateTable` method orchestrates the complete table creation process:

```cpp
optional_ptr<CatalogEntry> OneLakeTableSet::CreateTable(
    ClientContext &context, 
    BoundCreateTableInfo &info
)
```

**Workflow:**
1. **Validation** - Check for temporary tables (not supported), existing tables
2. **URI Construction** - Build ABFSS path from workspace_id/lakehouse_id
3. **Options Serialization** - Package partition columns, description, config as JSON
4. **Token Acquisition** - Get Azure storage token from credentials
5. **Physical Creation** - Call `OneLakeDeltaWriter::CreateTable()`
6. **Catalog Entry** - Create `OneLakeTableEntry` with metadata
7. **Metadata Update** - Set table type, format (Delta), location

**URI Format:**
```
abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/<table_name>/
```

**Error Handling:**
- `BinderException` - Table already exists (unless IF NOT EXISTS)
- `InternalException` - Missing schema metadata
- `IOException` - Delta table creation failure (propagated from Rust)

**Example SQL:**
```sql
-- Basic table creation
-- Basic table
CREATE TABLE sales (
    id INTEGER,
    product VARCHAR,
    amount DECIMAL(18,2)
);

-- Partitioned table (PARTITION BY gets rewritten automatically)
CREATE TABLE sales_partitioned (
    id INTEGER,
    region VARCHAR,
    date DATE,
    amount DOUBLE
) PARTITION BY (region, date)
COMMENT 'Regional sales by date';

-- With IF NOT EXISTS
CREATE TABLE IF NOT EXISTS sales (id INTEGER);
```

**Note:** For partitioning details (including the transparent `PARTITION BY` rewrite), see
[PARTITION_SYNTAX.md](../PARTITION_SYNTAX.md).

---

## Future Extensions (Phases 3-8)

### Phase 2: Table Creation (Implemented)

**Rust FFI Function:**
```rust
#[no_mangle]
pub extern "C" fn ol_delta_create_table(
    table_uri: *const c_char,
    schema_json: *const c_char,  // Arrow schema as JSON
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int
```

**Purpose:** Creates a new Delta table with specified schema.

**Status:** ✅ Implemented

**Arrow Schema JSON Format:**
```json
{
  "fields": [
    {
      "name": "id",
      "data_type": "Int32",
      "nullable": false,
      "dict_id": 0,
      "dict_is_ordered": false
    },
    {
      "name": "name",
      "data_type": "Utf8",
      "nullable": true,
      "dict_id": 0,
      "dict_is_ordered": false
    },
    {
      "name": "amount",
      "data_type": {
        "Decimal": [18, 2]
      },
      "nullable": true,
      "dict_id": 0,
      "dict_is_ordered": false
    }
  ]
}
```

**Type Mapping (DuckDB → Arrow → Delta):**
| DuckDB Type | Arrow DataType | Delta StructField |
|-------------|----------------|-------------------|
| BOOLEAN | Boolean | boolean |
| TINYINT | Int8 | byte |
| SMALLINT | Int16 | short |
| INTEGER | Int32 | integer |
| BIGINT | Int64 | long |
| FLOAT | Float32 | float |
| DOUBLE | Float64 | double |
| VARCHAR | Utf8 | string |
| DATE | Date32 | date |
| TIMESTAMP | Timestamp(Microsecond, None) | timestamp |
| DECIMAL(p,s) | Decimal128(p,s) | decimal(p,s) |

**Implementation:**
- Uses `DeltaOps::create().with_table_name()` builder pattern
- Converts Arrow schema fields to Delta `StructField`
- Supports partition columns via `with_partition_columns()`
- Supports table properties via `with_configuration()`
- Creates `_delta_log/00000000000000000000.json` transaction log

### Phase 3: Table Deletion

```rust
pub extern "C" fn ol_delta_drop_table(
    table_uri: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize
) -> c_int
```

**Purpose:** Drops a Delta table by deleting transaction log and optionally data files.

**Status:** ✅ Implemented

**Options JSON Format:**
```json
{
  "deleteData": true
}
```

**Parameters:**
- `deleteData`: When `true`, deletes both `_delta_log/` and data Parquet files; when `false`, only deletes `_delta_log/` (leaves orphaned data files)

**Implementation:**
- Lists objects in `_delta_log/` prefix via `ObjectStore::list()`
- Lists objects at table root if `deleteData: true`
- Filters out `_delta_log/` paths when deleting data files to avoid duplicate deletion
- Uses `ObjectStore::delete()` for each object
- Streams results with `StreamExt::next()` to handle large tables

**Safety Setting:**
DuckDB users must explicitly enable destructive operations:
```sql
SET onelake_allow_destructive_operations = true;
DROP TABLE my_table;
```

**Error Handling:**
- Returns non-zero status code on failure
- Populates `error_buffer` with detailed error messages
- C++ wrapper (`OneLakeDeltaWriter::DropTable`) translates to DuckDB exceptions

**Catalog Integration:**
- `OneLakeTableSet::DropEntry` checks safety setting
- Builds Azure DFS token and options JSON
- Calls Rust FFI via `ol_delta_drop_table`
- Removes entry from in-memory catalog cache via `EraseEntryInternal`
- Logs drop operation with table name and status

### Phase 4: DELETE Operations

```cpp
// DROP TABLE via FFI
int ol_delta_drop_table(
    const char *table_uri,
    const char *token_json,
    const char *options_json,   // {"deleteData": true}
    char *error_buffer,
    size_t error_buffer_len
);
```

### Phase 4: DELETE Operations

```cpp
// DELETE with predicate
int ol_delta_delete(
    const char *table_uri,
    const char *predicate,      // SQL WHERE clause
    const char *token_json,
    const char *options_json,
    char *error_buffer,
    size_t error_buffer_len
);
```

### Phase 5: UPDATE Operations

```cpp
// UPDATE with predicate and column updates
int ol_delta_update(
    const char *table_uri,
    const char *predicate,
    const char *updates_json,   // {"col": "expression"}
    const char *token_json,
    const char *options_json,
    char *error_buffer,
    size_t error_buffer_len
);
```

### Phase 6: MERGE Operations

```cpp
// MERGE (UPSERT)
int ol_delta_merge(
    struct FFI_ArrowArrayStream *source_stream,
    const char *table_uri,
    const char *merge_spec_json,  // Join condition + actions
    const char *token_json,
    const char *options_json,
    char *error_buffer,
    size_t error_buffer_len
);
```

---

## Testing Guidelines

### Unit Testing

**C++ Layer:**
```bash
./build/release/test/unittest --test-dir=test/sql/onelake_writes.test
```

**Rust Layer:**
```bash
cd rust/onelake_delta_writer
SKIP_ONELAKE_HEADER=1 cargo test
```

### Integration Testing

Requires live OneLake connection:

```bash
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
export TEST_WORKSPACE="my-workspace"
export TEST_LAKEHOUSE="test-lakehouse"

./build/release/duckdb --unsigned < test/sql/onelake_writes.test
```

### Performance Benchmarking

```sql
-- Benchmark INSERT performance
.timer on
INSERT INTO large_table SELECT * FROM source_table;  -- 1M rows
.timer off
```

---

## Security Considerations

### Token Handling

- **Never log tokens** - Tokens in JSON payloads contain sensitive data
- **Token lifetime** - Tokens expire (typically 1 hour); refresh as needed
- **Least privilege** - Use service principals with minimal required permissions

### Input Validation

```cpp
// Validate URIs
if (table_uri.empty() || !IsValidAbfssPath(table_uri)) {
    throw InvalidInputException("Invalid table URI");
}

// Sanitize predicates (prevent SQL injection)
ValidateSqlExpression(predicate);
```

### Permissions

Required Azure permissions for writes:
- `Storage Blob Data Contributor` or higher
- `Fabric Workspace Contributor` for table creation
- `Fabric Lakehouse Contributor` for schema modifications

---

## References

- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [deltalake-rs Documentation](https://docs.rs/deltalake/)
- [DuckDB Extension Development](https://duckdb.org/docs/extensions/overview)
- [OneLake Documentation](https://learn.microsoft.com/en-us/fabric/onelake/)

---

## Changelog

### Version 1.1 (November 28, 2025)
- **Phase 2 Complete**: Physical Table Creation
- Added `ol_delta_create_table` FFI function in Rust layer
- Added `OneLakeDeltaWriter::CreateTable` in C++ wrapper
- Integrated CREATE TABLE with OneLakeTableSet catalog
- Type mappings: BOOLEAN, INT8/16/32/64, FLOAT, DOUBLE, VARCHAR, DATE, TIMESTAMP, DECIMAL
- Support for partition columns, table descriptions, and Delta configuration
- Comprehensive test suite in `test/sql/onelake_create_table.test`

### Version 1.0 (November 26, 2025)
- Initial documentation
- Phase 0: Foundation complete
- Documents current INSERT implementation
- Outlines future phases (2-8)

---

**Maintained by:** DuckDB OneLake Extension Team  
**Issues:** Report at https://github.com/datumnova/duckdb_onelake/issues
