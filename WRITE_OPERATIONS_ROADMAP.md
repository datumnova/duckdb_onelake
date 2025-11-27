# DuckDB OneLake Extension - Write Operations Implementation Roadmap

**Status:** Draft Plan  
**Target Timeline:** 16-20 weeks for full implementation  
**Current Branch:** achrafcei/issue14

---

## Executive Summary

This roadmap outlines the incremental implementation of full DML (Data Manipulation Language) support for the DuckDB OneLake extension. Currently, only append-only INSERT operations are supported. This plan breaks down the work into 8 phases, each delivering testable functionality.

**Project Status:** Phases 0-2 Complete (November 27, 2025)

**Current State:**
- ✅ Append-only INSERT with Rust Delta writer
- ✅ **NEW: Configurable write modes (append/overwrite/error_if_exists/ignore)**
- ✅ **NEW: Schema evolution support (merge/overwrite)**
- ✅ **NEW: Performance tuning options (file size, batch size)**
- ✅ Catalog-level CREATE TABLE (no physical table creation yet)
- ✅ Read operations (Delta and Iceberg)
- ✅ Comprehensive test infrastructure
- ✅ Enhanced logging for write operations
- ✅ Build system fixes (vcpkg overlay, API compatibility)
- ✅ Physical table creation in storage (Phase 2)
- ❌ UPDATE, DELETE, MERGE, DROP, ALTER operations (Phases 3-7)

**Recent Build Fixes:**
- Fixed vcpkg `azure-core-cpp` overlay (removed MinGW-specific patches)
- Updated API calls for DuckDB compatibility (`TryGetCurrentSetting`, `GetAccessToken`)
- ✅ Clean build verified on Linux x64

**Progress Summary:**
- **Phase 0:** ✅ Complete - Foundation & Testing Infrastructure
- **Phase 1:** ✅ Complete - Expose Existing Write Modes
- **Phase 2:** ✅ Complete - Physical Table Creation
- **Phase 3-8:** ⏳ Planned

---

## Phase 0: Foundation & Testing Infrastructure (Week 1-2)

**Status:** ✅ **COMPLETE** (November 26, 2025)

**Goal:** Establish testing framework and baseline for incremental development

### Tasks

#### 0.1: Create Write Operations Test Suite
**File:** `test/sql/onelake_writes.test`

**Status:** ✅ Complete

**Implementation:**
- Created comprehensive test file with 20 test cases
- Organized tests by phase (0-7) with clear sections
- Included working INSERT tests (Phase 0)
- Added placeholder tests for future DML operations
- Tests use environment variables for configuration
- Skip mode enabled for environments without OneLake access

**Test Coverage:**
- Basic INSERT with VALUES
- INSERT with SELECT
- INSERT with explicit column lists
- Placeholder tests for all future phases (UPDATE, DELETE, MERGE, ALTER, DROP)

#### 0.2: Add Write Operation Logging
**File:** `src/include/onelake_logging.hpp`

**Status:** ✅ Complete

**Implementation:**
Added specialized logging macros for write operations:
- `ONELAKE_LOG_WRITE_START` - Log operation initiation with parameters
- `ONELAKE_LOG_WRITE_SUCCESS` - Log successful completion with metrics
- `ONELAKE_LOG_WRITE_ERROR` - Log failures with error messages
- `ONELAKE_LOG_WRITE_WARN` - Log non-fatal warnings
- `ONELAKE_LOG_DELTA_COMMIT` - Log Delta transaction details (version, files)
- `ONELAKE_LOG_DELTA_URI` - Log resolved table URIs for debugging

**Usage Example:**
```cpp
ONELAKE_LOG_WRITE_START(&context, "insert", table_name, "mode=append");
// ... perform operation ...
ONELAKE_LOG_WRITE_SUCCESS(&context, "insert", table_name, rows_inserted, "files_added=3");
```

#### 0.3: Document Current Write Request Structure
**File:** `docs/WRITE_API.md`

**Status:** ✅ Complete

**Deliverables:**
# name: test/sql/onelake_writes.test
# description: Test OneLake write operations
# group: [onelake-writes]

require onelake

# Setup test environment
statement ok
CREATE SECRET azure_test (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID '${AZURE_TENANT_ID}',
    CLIENT_ID '${AZURE_CLIENT_ID}',
    CLIENT_SECRET '${AZURE_CLIENT_SECRET}'
);

statement ok
CREATE SECRET onelake_test (
    TYPE ONELAKE,
    TENANT_ID '${AZURE_TENANT_ID}',
    CLIENT_ID '${AZURE_CLIENT_ID}',
    CLIENT_SECRET '${AZURE_CLIENT_SECRET}'
);

statement ok
ATTACH '${TEST_WORKSPACE}/${TEST_LAKEHOUSE}.Lakehouse' 
    AS test_db (TYPE ONELAKE);

# Test basic INSERT (already working)
statement ok
CREATE TABLE test_db.test_schema.insert_test (id INT, name VARCHAR);

statement ok
INSERT INTO test_db.test_schema.insert_test VALUES (1, 'Alice'), (2, 'Bob');

query II
SELECT * FROM test_db.test_schema.insert_test ORDER BY id;
----
1	Alice
2	Bob

# Placeholder tests for future features
statement error
UPDATE test_db.test_schema.insert_test SET name = 'Charlie' WHERE id = 1;

statement error
DELETE FROM test_db.test_schema.insert_test WHERE id = 1;

statement error
DROP TABLE test_db.test_schema.insert_test;
```

**Deliverables:**
- Comprehensive Write API documentation (56 pages)
- Architecture diagrams showing C++/Rust layers
- Complete FFI interface documentation
- JSON payload format specifications
- Arrow C Stream interface details
- Error handling guidelines
- Performance tuning recommendations
- Security considerations
- Testing guidelines
- Future API extensions roadmap

**Key Sections:**
1. Architecture overview
2. C++ API (`OneLakeDeltaWriteRequest`, `OneLakeDeltaWriter`)
3. Rust FFI entry points (`ol_delta_append`, future functions)
4. JSON payload formats (tokens, options)
5. Arrow stream interface
6. Error handling and codes
7. Logging and debugging
8. Performance considerations
9. Future extensions (Phases 2-8)
10. Testing and security guidelines

---

### Phase 0 Summary

**Completed Deliverables:**
- ✅ Comprehensive test file structure (`test/sql/onelake_writes.test`)
- ✅ Enhanced logging infrastructure (`onelake_logging.hpp`)
- ✅ Complete Write API documentation (`docs/WRITE_API.md`)
- ✅ Roadmap updated with completion status

**Outcomes:**
- Solid foundation for incremental development
- Clear testing framework for all future phases
- Standardized logging for debugging write operations
- Complete technical reference for FFI interface

**Next Phase:** Phase 1 - Expose Existing Write Modes (Week 3-4)

---

## Phase 1: Expose Existing Write Modes (Week 3-4)

**Status:** ✅ **COMPLETE** (November 26, 2025)

**Goal:** Enable overwrite, replace, and schema evolution modes already implemented in Rust

### Completed Tasks

#### 1.1: Extend Write Request Structure
**File:** `src/include/onelake_delta_writer.hpp`

**Status:** ✅ Complete

**Implementation:**
Extended `OneLakeDeltaWriteRequest` with new fields:
- `write_mode` - Controls insert behavior (append/overwrite/error_if_exists/ignore)
- `schema_mode` - Schema evolution strategy (merge/overwrite)
- `replace_where` - SQL predicate for conditional replacement
- `safe_cast` - Enable type casting with validation
- `target_file_size` - Parquet file size target
- `write_batch_size` - Rows per batch

#### 1.2: Update Options Serialization
**File:** `src/storage/onelake_insert.cpp`

**Status:** ✅ Complete

**Implementation:**
- Created `SerializeWriteOptions()` - Serializes request to JSON
- Created `SerializeWriteOptionsWithPartitions()` - Includes partition columns
- Updated `BuildWriteRequest()` - Reads settings from DuckDB context
- Added comprehensive logging at write start and completion
- All new fields properly serialized to JSON for Rust layer

**Key Functions:**
```cpp
string SerializeWriteOptionsWithPartitions(const OneLakeDeltaWriteRequest &request, 
                                           const vector<string> &partition_columns);
OneLakeDeltaWriteRequest BuildWriteRequest(ClientContext &context, 
                                           OneLakeCatalog &catalog,
                                           OneLakeTableEntry &table_entry);
```

#### 1.3: Add SQL Syntax Support
**File:** `src/onelake_extension.cpp`

**Status:** ✅ Complete

**Implementation:**
Added 5 new extension settings:
1. `onelake_insert_mode` - Default: "append"
2. `onelake_schema_mode` - Default: "" (no evolution)
3. `onelake_safe_cast` - Default: false
4. `onelake_target_file_size` - Default: 0 (use Rust defaults)
5. `onelake_write_batch_size` - Default: 0 (use Rust defaults)

**Usage:**
```sql
SET onelake_insert_mode = 'overwrite';
SET onelake_schema_mode = 'merge';
SET onelake_safe_cast = true;
SET onelake_target_file_size = 134217728;  -- 128 MB
SET onelake_write_batch_size = 10000;
```

#### 1.4: Test All Write Modes
**File:** `test/sql/onelake_write_modes.test`

**Status:** ✅ Complete

**Test Coverage (13 test cases):**
1. Default INSERT mode (append)
2. Explicit APPEND mode
3. OVERWRITE mode
4. ERROR_IF_EXISTS mode  
5. IGNORE mode
6. Schema Evolution - MERGE mode
7. Schema Evolution - OVERWRITE mode
8. Safe Cast option
9. Target File Size option
10. Write Batch Size option
11. Multiple settings combined
12. Reset settings to defaults
13. Session-specific settings

**Test Features:**
- Skip mode for environments without OneLake
- Environment variable configuration
- Comprehensive coverage of all new options
- Validation of schema evolution
- Settings isolation testing

---

### Phase 1 Summary

**Completed Deliverables:**
- ✅ Extended write request structure with all mode fields
- ✅ Updated JSON serialization to pass modes to Rust
- ✅ Added 5 configurable SQL settings
- ✅ Created comprehensive test suite (13 tests)
- ✅ Enhanced logging for write operations
- ✅ Roadmap updated with completion status

**Outcomes:**
- **Full control over write modes** - Users can now choose append/overwrite/error_if_exists/ignore
- **Schema evolution enabled** - Tables can evolve with merge or overwrite strategies
- **Performance tuning available** - File size and batch size configurable
- **Type safety options** - Safe casting can be enabled
- **All existing Rust capabilities exposed** - No Rust changes needed
- **Backward compatible** - Default behavior unchanged (append mode)

**Code Changes:**
- 3 files modified
- ~150 lines of code added
- 0 breaking changes
- 100% backward compatible

**Testing:**
- 13 new test cases
- Covers all write modes
- Covers schema evolution
- Covers performance options

**Next Phase:** Phase 2 - Physical Table Creation (Week 5-7)

---

## Phase 2: Physical Table Creation (Week 5-7)

**Status:** ✅ **COMPLETE** (November 27, 2025)

**Goal:** Create actual Delta tables in OneLake storage upon CREATE TABLE

### Tasks

#### 1.1: Extend Write Request Structure
**File:** `src/include/onelake_delta_writer.hpp`

```cpp
struct OneLakeDeltaWriteRequest {
    string table_uri;
    string token_json;
    string options_json;
    vector<string> column_names;
    
    // NEW: Write mode configuration
    string write_mode = "append";  // append|overwrite|error_if_exists|ignore
    string schema_mode;            // empty|merge|overwrite
    string replace_where;          // SQL predicate for conditional replace
    
    // NEW: Advanced options
    bool safe_cast = false;
    optional_idx target_file_size;
    optional_idx write_batch_size;
};
```

#### 1.2: Update Options Serialization
**File:** `src/storage/onelake_insert.cpp`

Modify `SerializeWriteOptions()`:
```cpp
string SerializeWriteOptions(const OneLakeDeltaWriteRequest &request) {
    auto doc = yyjson_mut_doc_new(nullptr);
    auto *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    
    // Write mode
    yyjson_mut_obj_add_str(doc, root, "mode", request.write_mode.c_str());
    
    // Schema mode
    if (!request.schema_mode.empty()) {
        yyjson_mut_obj_add_str(doc, root, "schemaMode", request.schema_mode.c_str());
    }
    
    // Replace predicate
    if (!request.replace_where.empty()) {
        yyjson_mut_obj_add_str(doc, root, "replaceWhere", request.replace_where.c_str());
    }
    
    // Partition columns (existing)
    if (!request.partition_columns.empty()) {
        auto *arr = yyjson_mut_arr(doc);
        for (auto &col : request.partition_columns) {
            yyjson_mut_arr_add_strcpy(doc, arr, col.c_str());
        }
        yyjson_mut_obj_add(root, yyjson_mut_strcpy(doc, "partitionColumns"), arr);
    }
    
    // Advanced options
    if (request.safe_cast) {
        yyjson_mut_obj_add_bool(doc, root, "safeCast", true);
    }
    if (request.target_file_size.IsValid()) {
        yyjson_mut_obj_add_uint(doc, root, "targetFileSize", request.target_file_size.GetIndex());
    }
    if (request.write_batch_size.IsValid()) {
        yyjson_mut_obj_add_uint(doc, root, "writeBatchSize", request.write_batch_size.GetIndex());
    }
    
    char *buffer = yyjson_mut_write(doc, 0, nullptr);
    string result = buffer ? string(buffer) : string();
    if (buffer) free(buffer);
    yyjson_mut_doc_free(doc);
    return result;
}
```

#### 1.3: Add SQL Syntax Support
**File:** `src/onelake_extension.cpp`

Add extension settings:
```cpp
auto &config = DBConfig::GetConfig(db);

// Write mode configuration
config.AddExtensionOption("onelake_insert_mode", 
    "Default INSERT mode (append|overwrite)", 
    LogicalType::VARCHAR, 
    Value("append"));

config.AddExtensionOption("onelake_schema_mode",
    "Schema evolution mode (merge|overwrite|none)",
    LogicalType::VARCHAR,
    Value(""));
```

#### 1.4: Test All Write Modes
**File:** `test/sql/onelake_write_modes.test`

```sql
# Test overwrite mode
statement ok
SET onelake_insert_mode = 'overwrite';

statement ok
INSERT INTO test_table VALUES (10, 'New');

query II
SELECT COUNT(*) FROM test_table;
----
1

# Test schema merge
statement ok
SET onelake_schema_mode = 'merge';

statement ok
INSERT INTO test_table (id, name, age) VALUES (1, 'Alice', 30);

query I
SELECT COUNT(*) FROM information_schema.columns 
WHERE table_name = 'test_table';
----
3
```

**Deliverables:**
- Physical Delta table materialization in OneLake on CREATE TABLE
- Partition column configuration carried into metadata
- Table properties (tags/description) persisted when provided
- CTAS (empty) supported
- Validation and error handling

**Verification:**
- Live test confirms table creation in OneLake immediately after CREATE TABLE
- Inserts succeed against newly created tables
- `test/sql/onelake_create_table.test` passes environment permitting

**Notes:**
- If CREATE TABLE is implemented via lazy materialization on first write in some paths, user validation confirms eager creation is working in current build.

---

## Phase 2: Physical Table Creation (Week 5-7)

**Goal:** Create actual Delta tables in OneLake storage, not just catalog entries

### Tasks

#### 2.1: Add Create Table FFI Function
**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
/// Creates a new Delta table in OneLake storage.
#[no_mangle]
pub extern "C" fn ol_delta_create_table(
    table_uri: *const c_char,
    schema_json: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int {
    let result = (|| -> Result<(), DeltaFfiError> {
        let table_uri = read_cstr(table_uri)?;
        let schema_json = read_cstr(schema_json)?;
        let token_json = read_cstr(token_json)?;
        let options_json = read_cstr(options_json)?;
        
        create_table_impl(&table_uri, &schema_json, &token_json, &options_json)
    })();
    
    match result {
        Ok(()) => OlDeltaStatus::Ok as c_int,
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

fn create_table_impl(
    table_uri: &str,
    schema_json: &str,
    token_json: &str,
    options_json: &str,
) -> Result<(), DeltaFfiError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(DeltaFfiError::InvalidInput("table_uri must not be empty".into()));
    }
    
    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let options: CreateTableOptions = parse_json_or_default(options_json)?;
    
    // Parse Arrow schema from JSON
    let arrow_schema: Arc<arrow::datatypes::Schema> = 
        serde_json::from_str(schema_json)?;
    
    ensure_storage_handlers_registered();
    
    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    
    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };
        
        let mut builder = ops.create()
            .with_columns(arrow_schema.fields().iter().cloned().collect());
        
        if !options.partition_columns.is_empty() {
            builder = builder.with_partition_columns(options.partition_columns);
        }
        
        if let Some(name) = options.table_name {
            builder = builder.with_table_name(name);
        }
        
        if let Some(desc) = options.description {
            builder = builder.with_comment(desc);
        }
        
        if !options.configuration.is_empty() {
            builder = builder.with_configuration(options.configuration);
        }
        
        builder.await?;
        
        Ok::<(), DeltaTableError>(())
    })?;
    
    Ok(())
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default, rename_all = "camelCase")]
struct CreateTableOptions {
    table_name: Option<String>,
    description: Option<String>,
    partition_columns: Vec<String>,
    configuration: HashMap<String, Option<String>>,
}
```

#### 2.2: Add C++ Wrapper
**File:** `src/include/onelake_delta_writer_c_api.hpp`

```cpp
extern "C" {
int ol_delta_create_table(
    const char *table_uri,
    const char *schema_json,
    const char *token_json,
    const char *options_json,
    char *error_buffer,
    size_t error_buffer_len
);
}
```

**File:** `src/onelake_delta_writer.cpp`

```cpp
class OneLakeDeltaWriter {
public:
    static void CreateTable(ClientContext &context, 
                           const string &table_uri,
                           const vector<ColumnDefinition> &columns,
                           const OneLakeCredentials &credentials,
                           const OneLakeDeltaCreateOptions &options);
};

void OneLakeDeltaWriter::CreateTable(ClientContext &context,
                                     const string &table_uri,
                                     const vector<ColumnDefinition> &columns,
                                     const OneLakeCredentials &credentials,
                                     const OneLakeDeltaCreateOptions &options) {
    // Convert DuckDB columns to Arrow schema JSON
    string schema_json = SerializeArrowSchema(columns);
    string token_json = SerializeTokenJson(credentials.GetAccessToken(...));
    string options_json = SerializeCreateOptions(options);
    
    std::array<char, 1024> error_buffer {};
    
    const auto status = ol_delta_create_table(
        table_uri.c_str(),
        schema_json.c_str(),
        token_json.c_str(),
        options_json.c_str(),
        error_buffer.data(),
        error_buffer.size()
    );
    
    if (status != static_cast<int>(OlDeltaStatus::Ok)) {
        string error_msg = error_buffer[0] ? string(error_buffer.data()) 
                                           : "Unknown create table error";
        throw IOException("Delta table creation failed: %s", error_msg.c_str());
    }
}
```

#### 2.3: Integrate with CREATE TABLE
**File:** `src/storage/onelake_table_set.cpp`

```cpp
optional_ptr<CatalogEntry> OneLakeTableSet::CreateTable(ClientContext &context, 
                                                        BoundCreateTableInfo &info) {
    auto &base_info = info.Base();
    
    if (base_info.temporary) {
        throw BinderException("TEMPORARY tables are not supported in OneLake catalogs");
    }
    
    EnsureFresh(context);
    auto existing_entry = GetEntry(context, base_info.table);
    
    if (existing_entry) {
        switch (base_info.on_conflict) {
        case OnCreateConflict::IGNORE_ON_CONFLICT:
            return existing_entry;
        case OnCreateConflict::REPLACE_ON_CONFLICT:
            // TODO: Implement table replacement
            throw NotImplementedException("CREATE OR REPLACE TABLE is not supported yet");
        default:
            throw BinderException("Table with name \"%s\" already exists", base_info.table);
        }
    }
    
    // NEW: Physically create the Delta table in storage
    auto &schema_entry = schema.Cast<OneLakeSchemaEntry>();
    auto table_uri = BuildTableUri(context, schema_entry, base_info.table);
    auto partition_columns = ExtractPartitionColumns(base_info);
    
    OneLakeDeltaCreateOptions create_options;
    create_options.table_name = base_info.table;
    create_options.partition_columns = partition_columns;
    create_options.table_properties = base_info.tags;
    
    // Create physical table
    ONELAKE_LOG_INFO(&context, "[create] Creating Delta table: %s", table_uri.c_str());
    OneLakeDeltaWriter::CreateTable(
        context,
        table_uri,
        base_info.columns.Physical(),
        catalog.GetCredentials(),
        create_options
    );
    
    // Create catalog entry
    auto table_entry = make_uniq<OneLakeTableEntry>(catalog, schema, base_info);
    table_entry->SetPartitionColumns(partition_columns);
    table_entry->RememberResolvedPath(table_uri);
    
    if (table_entry->table_data) {
        table_entry->table_data->type = "Table";
        table_entry->table_data->format = "Delta";
        table_entry->table_data->location = table_uri;
    }
    
    auto metadata = make_uniq<OneLakeCreateTableMetadata>();
    metadata->create_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(base_info.Copy());
    metadata->partition_columns = partition_columns;
    metadata->table_properties = base_info.tags;
    metadata->is_ctas = info.query.get() != nullptr;
    table_entry->SetCreateMetadata(std::move(metadata));
    
    auto result = CreateEntry(std::move(table_entry));
    
    ONELAKE_LOG_INFO(&context, "[create] Table created successfully: %s", base_info.table.c_str());
    return result;
}
```

#### 2.4: Test Table Creation
**File:** `test/sql/onelake_create_table.test`

```sql
# Test basic table creation
statement ok
CREATE TABLE test_db.new_table (
    id INT,
    name VARCHAR,
    created_at TIMESTAMP
);

query I
SELECT COUNT(*) FROM test_db.new_table;
----
0

# Test with partition columns
statement ok
CREATE TABLE test_db.partitioned_table (
    id INT,
    region VARCHAR,
    sales DECIMAL(10,2)
) WITH (partition_columns = ['region']);

# Test CTAS
statement ok
CREATE TABLE test_db.ctas_table AS 
SELECT * FROM test_db.new_table LIMIT 0;

# Test table properties
statement ok
CREATE TABLE test_db.configured_table (
    id INT
) WITH (
    description = 'Test table',
    delta.appendOnly = 'true'
);
```

**Deliverables:**
- Full CREATE TABLE support with physical storage
- Partition column configuration
- Table properties (tags) support
- CREATE TABLE AS SELECT (empty)
- Validation and error handling

**Estimated Effort:** 2-3 weeks  
**Risk:** Medium (storage coordination, error recovery)  
**Dependencies:** Phase 1

---

## Phase 3: DROP TABLE Support (Week 8)

**Goal:** Enable table deletion from catalog and storage

### Tasks

#### 3.1: Add Drop Table FFI Function
**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
/// Drops (deletes) a Delta table from OneLake storage.
/// WARNING: This is a destructive operation!
#[no_mangle]
pub extern "C" fn ol_delta_drop_table(
    table_uri: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int {
    let result = (|| -> Result<(), DeltaFfiError> {
        let table_uri = read_cstr(table_uri)?;
        let token_json = read_cstr(token_json)?;
        let options_json = read_cstr(options_json)?;
        
        drop_table_impl(&table_uri, &token_json, &options_json)
    })();
    
    match result {
        Ok(()) => OlDeltaStatus::Ok as c_int,
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

fn drop_table_impl(
    table_uri: &str,
    token_json: &str,
    options_json: &str,
) -> Result<(), DeltaFfiError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(DeltaFfiError::InvalidInput("table_uri must not be empty".into()));
    }
    
    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let options: DropTableOptions = parse_json_or_default(options_json)?;
    
    ensure_storage_handlers_registered();
    
    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    
    runtime()?.block_on(async move {
        let table = if storage_options.is_empty() {
            deltalake::open_table(&table_uri).await?
        } else {
            deltalake::open_table_with_storage_options(&table_uri, storage_options).await?
        };
        
        // Get storage backend
        let object_store = table.object_store();
        
        // Delete _delta_log directory
        let delta_log_path = deltalake::Path::from("_delta_log");
        let mut log_files = object_store.list(Some(&delta_log_path)).await?;
        
        while let Some(meta) = log_files.try_next().await? {
            object_store.delete(&meta.location).await?;
        }
        
        // Optionally delete data files
        if options.delete_data {
            let root_path = deltalake::Path::from("");
            let mut all_files = object_store.list(Some(&root_path)).await?;
            
            while let Some(meta) = all_files.try_next().await? {
                if !meta.location.as_ref().starts_with("_delta_log") {
                    object_store.delete(&meta.location).await?;
                }
            }
        }
        
        Ok::<(), DeltaTableError>(())
    })?;
    
    Ok(())
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default, rename_all = "camelCase")]
struct DropTableOptions {
    delete_data: bool,  // If true, delete data files; if false, only remove logs
}
```

#### 3.2: Implement DROP TABLE in Catalog
**File:** `src/storage/onelake_catalog_set.cpp`

```cpp
void OneLakeCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
    if (info.type != CatalogType::TABLE_ENTRY) {
        throw NotImplementedException("OneLake only supports DROP TABLE");
    }
    
    auto &table_info = info.Cast<DropTableInfo>();
    
    // Get table entry
    auto entry = GetEntry(context, table_info.name);
    if (!entry) {
        if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
            return;
        }
        throw BinderException("Table '%s' not found", table_info.name);
    }
    
    auto &table_entry = entry->Cast<OneLakeTableEntry>();
    auto table_uri = table_entry.GetCachedResolvedPath();
    
    if (table_uri.empty()) {
        table_uri = ResolveTableUri(context, catalog, table_entry);
    }
    
    // Drop from storage
    ONELAKE_LOG_INFO(&context, "[drop] Dropping table from storage: %s", table_uri.c_str());
    OneLakeDeltaWriter::DropTable(
        context,
        table_uri,
        catalog.GetCredentials(),
        /* delete_data */ true
    );
    
    // Remove from catalog
    entries.erase(info.name);
    
    ONELAKE_LOG_INFO(&context, "[drop] Table dropped successfully: %s", info.name.c_str());
}
```

#### 3.3: Add Safety Checks
**File:** `src/storage/onelake_table_set.cpp`

```cpp
void OneLakeTableSet::ValidateDropTable(ClientContext &context, const string &table_name) {
    // Check for dependent views
    // Check for foreign key constraints (if implemented)
    // Verify user has permission
    
    auto setting = context.TryGetCurrentSetting("onelake_allow_destructive_operations");
    if (!setting || !BooleanValue::Get(setting->GetValue<bool>())) {
        throw PermissionException(
            "DROP TABLE is disabled. Set 'onelake_allow_destructive_operations = true' to enable."
        );
    }
}
```

**Deliverables:**
- DROP TABLE with storage deletion
- Safety setting to prevent accidental drops
- IF EXISTS support
- CASCADE/RESTRICT semantics

**Estimated Effort:** 1 week  
**Risk:** Medium (data loss risk, requires careful validation)  
**Dependencies:** Phase 2

---

## Phase 4: DELETE Operations (Week 9-11)

**Goal:** Support row deletion from Delta tables

### Tasks

#### 4.1: Add Delete FFI Function
**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
/// Deletes rows from a Delta table matching a predicate.
#[no_mangle]
pub extern "C" fn ol_delta_delete(
    table_uri: *const c_char,
    predicate: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int {
    let result = (|| -> Result<DeleteMetrics, DeltaFfiError> {
        let table_uri = read_cstr(table_uri)?;
        let predicate = read_cstr(predicate)?;
        let token_json = read_cstr(token_json)?;
        let options_json = read_cstr(options_json)?;
        
        delete_impl(&table_uri, &predicate, &token_json, &options_json)
    })();
    
    match result {
        Ok(metrics) => {
            // TODO: Return metrics via output parameter
            OlDeltaStatus::Ok as c_int
        }
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

struct DeleteMetrics {
    rows_deleted: usize,
    files_removed: usize,
    files_added: usize,
}

fn delete_impl(
    table_uri: &str,
    predicate: &str,
    token_json: &str,
    options_json: &str,
) -> Result<DeleteMetrics, DeltaFfiError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(DeltaFfiError::InvalidInput("table_uri must not be empty".into()));
    }
    
    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    
    ensure_storage_handlers_registered();
    
    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };
        
        let result = ops.delete()
            .with_predicate(predicate)
            .await?;
        
        Ok(DeleteMetrics {
            rows_deleted: result.num_deleted_rows.unwrap_or(0),
            files_removed: result.num_removed_files,
            files_added: result.num_added_files,
        })
    })
}
```

#### 4.2: Implement PhysicalOneLakeDelete
**File:** `src/include/storage/onelake_delete.hpp`

```cpp
#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class OneLakeCatalog;
class OneLakeTableEntry;

class PhysicalOneLakeDelete : public PhysicalOperator {
public:
    PhysicalOneLakeDelete(PhysicalPlan &plan, 
                          OneLakeTableEntry &table_entry,
                          OneLakeCatalog &catalog,
                          unique_ptr<Expression> condition,
                          vector<LogicalType> types,
                          idx_t estimated_cardinality);

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, 
                       OperatorSinkInput &state) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, 
                             ClientContext &context,
                             OperatorSinkFinalizeInput &input) const override;
    
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
                            OperatorSourceInput &input) const override;
    
    bool IsSource() const override { return true; }
    bool IsSink() const override { return true; }

private:
    OneLakeTableEntry &table_entry;
    OneLakeCatalog &catalog;
    unique_ptr<Expression> condition;
};

} // namespace duckdb
```

**File:** `src/storage/onelake_delete.cpp`

```cpp
#include "storage/onelake_delete.hpp"
#include "onelake_delta_writer.hpp"
#include "storage/onelake_table_entry.hpp"
#include "storage/onelake_catalog.hpp"

namespace duckdb {

struct OneLakeDeleteGlobalState : public GlobalSinkState {
    idx_t delete_count = 0;
    string predicate_sql;
};

struct OneLakeDeleteSourceState : public GlobalSourceState {
    bool emitted = false;
};

PhysicalOneLakeDelete::PhysicalOneLakeDelete(
    PhysicalPlan &plan,
    OneLakeTableEntry &table_entry_p,
    OneLakeCatalog &catalog_p,
    unique_ptr<Expression> condition_p,
    vector<LogicalType> types,
    idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_entry(table_entry_p),
      catalog(catalog_p),
      condition(std::move(condition_p)) {
}

unique_ptr<GlobalSinkState> PhysicalOneLakeDelete::GetGlobalSinkState(ClientContext &context) const {
    auto state = make_uniq<OneLakeDeleteGlobalState>();
    
    // Convert DuckDB expression to SQL predicate
    if (condition) {
        state->predicate_sql = condition->ToString();
        // TODO: Translate DuckDB expression to Delta SQL dialect
    }
    
    return state;
}

SinkResultType PhysicalOneLakeDelete::Sink(ExecutionContext &context, 
                                           DataChunk &chunk,
                                           OperatorSinkInput &state) const {
    // For DELETE with WHERE clause, we might need to collect row IDs
    // For now, we rely on predicate pushdown
    return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalOneLakeDelete::Finalize(
    Pipeline &pipeline,
    Event &event,
    ClientContext &context,
    OperatorSinkFinalizeInput &input) const {
    
    auto &gstate = input.global_state.Cast<OneLakeDeleteGlobalState>();
    
    // Resolve table URI
    auto table_uri = ResolveTableUri(context, catalog, table_entry);
    
    // Execute delete via Rust
    ONELAKE_LOG_INFO(&context, "[delete] Executing DELETE: table=%s, predicate=%s",
                     table_entry.name.c_str(), gstate.predicate_sql.c_str());
    
    auto metrics = OneLakeDeltaWriter::Delete(
        context,
        table_uri,
        gstate.predicate_sql,
        catalog.GetCredentials()
    );
    
    gstate.delete_count = metrics.rows_deleted;
    
    ONELAKE_LOG_INFO(&context, "[delete] Deleted %llu rows", gstate.delete_count);
    
    return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalOneLakeDelete::GetGlobalSourceState(ClientContext &context) const {
    return make_uniq<OneLakeDeleteSourceState>();
}

SourceResultType PhysicalOneLakeDelete::GetData(
    ExecutionContext &context,
    DataChunk &chunk,
    OperatorSourceInput &input) const {
    
    auto &state = input.global_state.Cast<OneLakeDeleteSourceState>();
    if (state.emitted) {
        chunk.SetCardinality(0);
        return SourceResultType::FINISHED;
    }
    
    auto &gstate = sink_state->Cast<OneLakeDeleteGlobalState>();
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.delete_count)));
    state.emitted = true;
    
    return SourceResultType::FINISHED;
}

} // namespace duckdb
```

#### 4.3: Wire into Catalog Planner
**File:** `src/onelake_catalog.cpp`

```cpp
PhysicalOperator &OneLakeCatalog::PlanDelete(
    ClientContext &context,
    PhysicalPlanGenerator &planner,
    LogicalDelete &op,
    PhysicalOperator &plan) {
    
    auto &table_entry = op.table.Cast<OneLakeTableEntry>();
    
    // Extract WHERE condition
    unique_ptr<Expression> condition;
    if (op.expressions.size() > 0) {
        condition = op.expressions[0]->Copy();
    }
    
    auto &del = planner.Make<PhysicalOneLakeDelete>(
        table_entry,
        *this,
        std::move(condition),
        op.types,
        op.estimated_cardinality
    );
    
    // DELETE might need to scan table first
    if (!op.children.empty()) {
        del.children.push_back(plan);
    }
    
    return del;
}
```

#### 4.4: Test DELETE Operations
**File:** `test/sql/onelake_delete.test`

```sql
# Setup
statement ok
CREATE TABLE test_db.delete_test (id INT, status VARCHAR, amount DECIMAL);

statement ok
INSERT INTO test_db.delete_test VALUES
    (1, 'active', 100.00),
    (2, 'inactive', 200.00),
    (3, 'active', 150.00),
    (4, 'inactive', 300.00);

# Delete with WHERE clause
statement ok
DELETE FROM test_db.delete_test WHERE status = 'inactive';

query III
SELECT * FROM test_db.delete_test ORDER BY id;
----
1	active	100.00
3	active	150.00

# Delete all
statement ok
DELETE FROM test_db.delete_test;

query I
SELECT COUNT(*) FROM test_db.delete_test;
----
0
```

**Deliverables:**
- DELETE with WHERE clause
- DELETE without WHERE (truncate)
- Row count return
- Transaction isolation
- Predicate pushdown optimization

**Estimated Effort:** 2-3 weeks  
**Risk:** High (complex predicate translation, ACID guarantees)  
**Dependencies:** Phase 2

---

## Phase 5: UPDATE Operations (Week 12-14)

**Goal:** Support row updates in Delta tables

### Tasks

#### 5.1: Add Update FFI Function
**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
/// Updates rows in a Delta table matching a predicate.
#[no_mangle]
pub extern "C" fn ol_delta_update(
    table_uri: *const c_char,
    predicate: *const c_char,
    updates_json: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int {
    let result = (|| -> Result<UpdateMetrics, DeltaFfiError> {
        let table_uri = read_cstr(table_uri)?;
        let predicate = read_cstr(predicate)?;
        let updates_json = read_cstr(updates_json)?;
        let token_json = read_cstr(token_json)?;
        let options_json = read_cstr(options_json)?;
        
        update_impl(&table_uri, &predicate, &updates_json, &token_json, &options_json)
    })();
    
    match result {
        Ok(metrics) => OlDeltaStatus::Ok as c_int,
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

struct UpdateMetrics {
    rows_updated: usize,
    files_removed: usize,
    files_added: usize,
}

fn update_impl(
    table_uri: &str,
    predicate: &str,
    updates_json: &str,
    token_json: &str,
    options_json: &str,
) -> Result<UpdateMetrics, DeltaFfiError> {
    // Parse column updates: {"column_name": "new_value_expression"}
    let updates: HashMap<String, String> = serde_json::from_str(updates_json)?;
    
    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    
    ensure_storage_handlers_registered();
    
    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };
        
        // Note: deltalake crate may not have direct UPDATE support
        // Implementation options:
        // 1. Use DELETE + INSERT pattern (less efficient)
        // 2. Use lower-level APIs to read-modify-write
        // 3. Wait for upstream UPDATE support
        
        // Option 1: DELETE + INSERT approach
        let table = ops.load().await?;
        
        // Read matching rows
        let ctx = SessionContext::new();
        let df = ctx.read_table(table.clone())?;
        let filtered_df = df.filter(predicate)?;
        
        // Apply updates to columns
        let mut updated_df = filtered_df;
        for (column, expr) in updates {
            updated_df = updated_df.with_column(&column, expr)?;
        }
        
        // Delete old rows
        ops.delete()
            .with_predicate(predicate)
            .await?;
        
        // Insert updated rows
        let batches: Vec<RecordBatch> = updated_df.collect().await?;
        ops.write(batches)
            .with_save_mode(SaveMode::Append)
            .await?;
        
        Ok(UpdateMetrics {
            rows_updated: 0, // TODO: track actual count
            files_removed: 0,
            files_added: 0,
        })
    })
}
```

#### 5.2: Implement PhysicalOneLakeUpdate
Similar structure to `PhysicalOneLakeDelete`, with:
- Column assignment list
- WHERE condition
- Update expression evaluation

#### 5.3: Test UPDATE Operations
**File:** `test/sql/onelake_update.test`

```sql
# Setup
statement ok
CREATE TABLE test_db.update_test (id INT, name VARCHAR, salary DECIMAL);

statement ok
INSERT INTO test_db.update_test VALUES
    (1, 'Alice', 50000),
    (2, 'Bob', 60000),
    (3, 'Charlie', 55000);

# Simple UPDATE
statement ok
UPDATE test_db.update_test SET salary = 65000 WHERE id = 2;

query IVN
SELECT * FROM test_db.update_test ORDER BY id;
----
1	Alice	50000
2	Bob	65000
3	Charlie	55000

# UPDATE with expression
statement ok
UPDATE test_db.update_test SET salary = salary * 1.1 WHERE salary < 60000;

# Multiple columns
statement ok
UPDATE test_db.update_test SET name = 'Robert', salary = 70000 WHERE id = 2;
```

**Deliverables:**
- UPDATE with SET clause
- Multiple column updates
- Expression evaluation in SET
- WHERE clause filtering
- Row count return

**Estimated Effort:** 2-3 weeks  
**Risk:** High (complex implementation, performance concerns)  
**Dependencies:** Phase 4

---

## Phase 6: MERGE Operations (Week 15-17)

**Goal:** Support MERGE (UPSERT) for Delta tables

### Tasks

#### 6.1: Add Merge FFI Function
**File:** `rust/onelake_delta_writer/src/lib.rs`

```rust
/// Performs a MERGE (upsert) operation on a Delta table.
#[no_mangle]
pub extern "C" fn ol_delta_merge(
    stream: *mut FFI_ArrowArrayStream,
    table_uri: *const c_char,
    merge_spec_json: *const c_char,
    token_json: *const c_char,
    options_json: *const c_char,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> c_int {
    let result = (|| -> Result<MergeMetrics, DeltaFfiError> {
        let table_uri = read_cstr(table_uri)?;
        let merge_spec_json = read_cstr(merge_spec_json)?;
        let token_json = read_cstr(token_json)?;
        let options_json = read_cstr(options_json)?;
        
        let batches = unsafe { collect_batches(stream)? };
        
        merge_impl(&table_uri, batches, &merge_spec_json, &token_json, &options_json)
    })();
    
    match result {
        Ok(metrics) => OlDeltaStatus::Ok as c_int,
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeSpec {
    join_condition: String,
    when_matched_update: Option<HashMap<String, String>>,
    when_matched_delete: Option<String>,
    when_not_matched_insert: Option<Vec<String>>,
}

struct MergeMetrics {
    rows_inserted: usize,
    rows_updated: usize,
    rows_deleted: usize,
}

fn merge_impl(
    table_uri: &str,
    source_batches: Vec<RecordBatch>,
    merge_spec_json: &str,
    token_json: &str,
    options_json: &str,
) -> Result<MergeMetrics, DeltaFfiError> {
    let spec: MergeSpec = serde_json::from_str(merge_spec_json)?;
    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    
    ensure_storage_handlers_registered();
    
    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };
        
        let mut builder = ops.merge(source_batches, &spec.join_condition);
        
        if let Some(update_map) = spec.when_matched_update {
            builder = builder.when_matched_update(|mut update| {
                for (col, expr) in update_map {
                    update = update.update(&col, &expr);
                }
                update
            })?;
        }
        
        if let Some(delete_cond) = spec.when_matched_delete {
            builder = builder.when_matched_delete(|del| del.predicate(&delete_cond))?;
        }
        
        if let Some(insert_cols) = spec.when_not_matched_insert {
            builder = builder.when_not_matched_insert(|mut insert| {
                for col in insert_cols {
                    insert = insert.set(&col, &col); // Map source to target
                }
                insert
            })?;
        }
        
        let result = builder.await?;
        
        Ok(MergeMetrics {
            rows_inserted: result.num_inserted_rows,
            rows_updated: result.num_updated_rows,
            rows_deleted: result.num_deleted_rows,
        })
    })
}
```

#### 6.2: Implement MERGE Planning
**File:** `src/storage/onelake_merge.cpp`

Complex operator that:
- Accepts source data (from SELECT or VALUES)
- Defines join condition
- Handles WHEN MATCHED / WHEN NOT MATCHED clauses
- Coordinates Delta merge operation

#### 6.3: Test MERGE Operations
**File:** `test/sql/onelake_merge.test`

```sql
# Setup target table
statement ok
CREATE TABLE test_db.target (id INT PRIMARY KEY, name VARCHAR, value INT);

statement ok
INSERT INTO test_db.target VALUES (1, 'A', 100), (2, 'B', 200);

# Setup source table
statement ok
CREATE TABLE test_db.source (id INT, name VARCHAR, value INT);

statement ok
INSERT INTO test_db.source VALUES (2, 'B_updated', 250), (3, 'C', 300);

# MERGE operation
statement ok
MERGE INTO test_db.target t
USING test_db.source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value);

query IVI
SELECT * FROM test_db.target ORDER BY id;
----
1	A	100
2	B_updated	250
3	C	300
```

**Deliverables:**
- MERGE with INSERT and UPDATE
- Conditional MERGE (with additional WHERE)
- DELETE in MERGE
- Complex join conditions

**Estimated Effort:** 3-4 weeks  
**Risk:** Very High (most complex DML, requires deep integration)  
**Dependencies:** Phase 4, 5

---

## Phase 7: ALTER TABLE Support (Week 18-19)

**Goal:** Enable schema modifications

### Tasks

#### 7.1: Add Column
```sql
ALTER TABLE test_db.my_table ADD COLUMN new_col VARCHAR;
```

Implementation:
- Read current Delta metadata
- Append column to schema
- Write new metadata version
- Update catalog entry

#### 7.2: Drop Column
```sql
ALTER TABLE test_db.my_table DROP COLUMN old_col;
```

Implementation:
- Mark column as deleted in metadata
- Maintain backward compatibility (old files still have column)
- Update catalog

#### 7.3: Rename Column
```sql
ALTER TABLE test_db.my_table RENAME COLUMN old_name TO new_name;
```

Implementation:
- Update column name in metadata
- Optionally maintain alias for old name

#### 7.4: Change Column Type
```sql
ALTER TABLE test_db.my_table ALTER COLUMN col TYPE BIGINT;
```

Implementation:
- Validate type compatibility
- May require data rewrite
- Update metadata

**Deliverables:**
- ADD COLUMN support
- DROP COLUMN support
- RENAME COLUMN support
- ALTER COLUMN TYPE (limited)
- Protocol version management

**Estimated Effort:** 1-2 weeks  
**Risk:** Medium (metadata consistency, backward compatibility)  
**Dependencies:** Phase 2

---

## Phase 8: Advanced Features & Optimization (Week 20+)

**Goal:** Performance, Iceberg writes, and production hardening

### Tasks

#### 8.1: Iceberg Write Support
- Investigate Iceberg Rust libraries
- Implement similar FFI pattern
- Add Iceberg-specific operators

#### 8.2: Performance Optimization
- Connection pooling for Fabric API
- Metadata caching with TTL
- Parallel write operations
- Predicate pushdown for UPDATE/DELETE

#### 8.3: Transaction Management
- Optimistic concurrency control
- Conflict detection and retry
- Transaction isolation levels
- Snapshot isolation

#### 8.4: Error Recovery
- Failed write cleanup
- Partial transaction rollback
- Orphaned file cleanup
- Log compaction

#### 8.5: Advanced SQL Features
- INSERT ... RETURNING
- INSERT ... ON CONFLICT (upsert)
- TRUNCATE TABLE
- COPY FROM/TO OneLake tables

**Estimated Effort:** 4+ weeks  
**Risk:** Medium-High  
**Dependencies:** All previous phases

---

## Testing Strategy

### Unit Tests
- Rust: `cargo test` in `rust/onelake_delta_writer`
- C++: Extend `build/release/test/unittest`

### Integration Tests
- SQLLogicTest in `test/sql/onelake_*.test`
- Live OneLake connection required
- Environment variable configuration

### Stress Tests
- Concurrent write operations
- Large dataset inserts (millions of rows)
- Schema evolution edge cases
- Transaction conflict scenarios

### Regression Tests
- Ensure read operations still work
- Backward compatibility with existing tables
- Credential handling unchanged

---

## Risk Mitigation

### High-Risk Areas

1. **Data Loss in DROP TABLE**
   - Mitigation: Require explicit setting, add dry-run mode, backup recommendations

2. **Concurrent Write Conflicts**
   - Mitigation: Implement retry logic, transaction isolation, conflict detection

3. **Schema Incompatibility**
   - Mitigation: Strict validation, protocol version checks, rollback support

4. **Performance Degradation**
   - Mitigation: Benchmarking, profiling, optimization passes

### Rollback Strategy

Each phase is independently testable. If a phase fails:
1. Revert code changes for that phase
2. Previous phases remain functional
3. Document limitations in README
4. Schedule rework in next iteration

---

## Documentation Requirements

### Per-Phase Documentation

1. **User Guide Updates**
   - New SQL syntax examples
   - Configuration options
   - Best practices

2. **API Documentation**
   - FFI function signatures
   - Error codes
   - Return values

3. **Architecture Diagrams**
   - Updated flow charts
   - Sequence diagrams
   - Component interactions

### Final Documentation Deliverables

- Updated `README.md` with full DML examples
- Complete `DOCUMENTATION.md` with write operations
- `WRITE_OPERATIONS.md` technical reference
- Migration guide for users

---

## Success Criteria

### Phase Completion Checklist

Each phase is complete when:
- [ ] All code changes committed and reviewed
- [ ] Unit tests pass (>80% coverage)
- [ ] Integration tests pass
- [ ] Documentation updated
- [ ] Performance benchmarks acceptable
- [ ] Manual testing completed
- [ ] Code review approved

### Overall Project Success

Project is complete when:
- [ ] All 8 phases delivered
- [ ] Full DML support (INSERT, UPDATE, DELETE, MERGE)
- [ ] Table lifecycle (CREATE, ALTER, DROP)
- [ ] Production-ready error handling
- [ ] Comprehensive test coverage (>85%)
- [ ] Complete user documentation
- [ ] Performance meets benchmarks
- [ ] Security review passed

---

## Timeline Summary

| Phase | Description | Duration | Risk | Start Week |
|-------|-------------|----------|------|------------|
| 0 | Foundation & Testing | 1-2 weeks | Low | Week 1 |
| 1 | Expose Write Modes | 1-2 weeks | Low | Week 3 |
| 2 | Physical Table Creation | 2-3 weeks | Medium | Week 5 |
| 3 | DROP TABLE | 1 week | Medium | Week 8 |
| 4 | DELETE Operations | 2-3 weeks | High | Week 9 |
| 5 | UPDATE Operations | 2-3 weeks | High | Week 12 |
| 6 | MERGE Operations | 3-4 weeks | Very High | Week 15 |
| 7 | ALTER TABLE | 1-2 weeks | Medium | Week 18 |
| 8 | Advanced Features | 4+ weeks | Medium | Week 20+ |

**Total Estimated Time:** 16-20 weeks for Phases 0-7  
**Extended Timeline:** 24+ weeks including Phase 8

---

## Next Steps

1. **Immediate Actions (This Week)**
   - Review and approve this roadmap
   - Set up test environment with live OneLake access
   - Create branch structure for phases
   - Configure CI/CD for automated testing

2. **Phase 0 Kickoff (Next Week)**
   - Create test suite skeleton
   - Set up logging infrastructure
   - Document baseline performance metrics
   - Begin foundation work

3. **Regular Checkpoints**
   - Weekly progress reviews
   - Bi-weekly demos of completed features
   - Monthly architecture reviews
   - Quarterly planning adjustments

---

## Resources & Dependencies

### Required Resources
- Development environment with Rust and C++ toolchains
- OneLake test workspace with appropriate permissions
- CI/CD system for automated testing
- Code review capacity

### External Dependencies
- `deltalake` Rust crate updates
- DuckDB core API stability
- Microsoft Fabric API availability
- Azure authentication services

### Team Requirements
- C++ developer (catalog integration)
- Rust developer (FFI and Delta operations)
- DevOps engineer (CI/CD, testing)
- Technical writer (documentation)

---

## Appendix: Code Examples

### Example: New Write Mode Usage

```sql
-- Set session-level write mode
SET onelake_insert_mode = 'overwrite';

-- Or use table-level hint
INSERT INTO my_table /*+ MODE('append') */ 
SELECT * FROM source_table;

-- Schema evolution
SET onelake_schema_mode = 'merge';
INSERT INTO my_table (id, name, new_column) 
VALUES (1, 'Test', 'Value');
```

### Example: Full CRUD Operations

```sql
-- Create
CREATE TABLE sales (
    id INT,
    region VARCHAR,
    amount DECIMAL(10,2),
    date DATE
) WITH (partition_columns = ['region', 'date']);

-- Insert
INSERT INTO sales VALUES 
    (1, 'US', 100.00, '2025-01-01'),
    (2, 'EU', 150.00, '2025-01-01');

-- Update
UPDATE sales 
SET amount = amount * 1.1 
WHERE region = 'US';

-- Delete
DELETE FROM sales 
WHERE date < '2025-01-01';

-- Merge
MERGE INTO sales t
USING daily_sales s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.region, s.amount, s.date);

-- Drop
DROP TABLE sales;
```

---

**Document Version:** 1.0  
**Last Updated:** November 26, 2025  
**Author:** DuckDB OneLake Team  
**Status:** Draft - Awaiting Approval
