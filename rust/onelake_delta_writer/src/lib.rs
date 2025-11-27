//! onelake_delta_writer
//!
//! This crate exposes a tiny FFI surface that allows the DuckDB OneLake extension to
//! forward DuckDB `DataChunk`s (converted to Arrow streams) into the `deltalake`
//! writer. All heavy lifting (Arrow import, async runtime management, storage option
//! wiring, etc.) lives here so the C++ code can stay lean.

use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::os::raw::c_int;
use std::slice;
use std::sync::Arc;

use deltalake::arrow::error::ArrowError;
use deltalake::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::errors::DeltaTableError;
use deltalake::operations::write::SchemaMode;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use once_cell::sync::OnceCell;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tokio::runtime::Runtime;
use futures_util::StreamExt; // for .next() on streams during drop table

/// Result codes returned over the C ABI.
#[repr(i32)]
pub enum OlDeltaStatus {
    /// Call succeeded.
    Ok = 0,
    /// Input arguments were invalid.
    InvalidInput = 1,
    /// Legacy value kept for backwards compatibility.
    Unimplemented = 2,
    /// Arrow import failed.
    ArrowError = 3,
    /// The Delta writer returned an error.
    DeltaError = 4,
    /// Options JSON could not be parsed.
    JsonError = 5,
    /// Tokio runtime failed to initialize.
    RuntimeError = 6,
    /// An unexpected internal error occurred.
    InternalError = 100,
}

static TOKIO_RUNTIME: OnceCell<Runtime> = OnceCell::new();
static STORAGE_HANDLERS: OnceCell<()> = OnceCell::new();

fn runtime() -> Result<&'static Runtime, DeltaFfiError> {
    TOKIO_RUNTIME.get_or_try_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("onelake-delta-writer")
            .build()
            .map_err(|err| DeltaFfiError::Runtime(err.to_string()))
    })
}

fn ensure_storage_handlers_registered() {
    STORAGE_HANDLERS.get_or_init(|| {
        deltalake::azure::register_handlers(None);
    });
}

#[derive(thiserror::Error, Debug)]
enum DeltaFfiError {
    #[error("{0}")]
    InvalidInput(String),
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Delta error: {0}")]
    Delta(#[from] DeltaTableError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Runtime error: {0}")]
    Runtime(String),
}

impl DeltaFfiError {
    fn status(&self) -> OlDeltaStatus {
        match self {
            DeltaFfiError::InvalidInput(_) => OlDeltaStatus::InvalidInput,
            DeltaFfiError::Arrow(_) => OlDeltaStatus::ArrowError,
            DeltaFfiError::Delta(_) => OlDeltaStatus::DeltaError,
            DeltaFfiError::Json(_) => OlDeltaStatus::JsonError,
            DeltaFfiError::Runtime(_) => OlDeltaStatus::RuntimeError,
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default, rename_all = "camelCase")]
struct TokenPayload {
    #[serde(alias = "storage_token", alias = "storageToken", alias = "token")]
    storage_token: Option<String>,
    #[allow(dead_code)]
    #[serde(alias = "fabric_token", alias = "fabricToken")]
    fabric_token: Option<String>,
}

impl TokenPayload {
    fn storage_token(&self) -> Option<String> {
        self.storage_token
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
    }
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default, rename_all = "camelCase")]
struct WriteOptions {
    mode: Option<String>,
    schema_mode: Option<String>,
    replace_where: Option<String>,
    partition_columns: Vec<String>,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    safe_cast: Option<bool>,
    storage_options: HashMap<String, String>,
    table_name: Option<String>,
    description: Option<String>,
    configuration: HashMap<String, Option<String>>,
    commit_info: HashMap<String, Value>,
    create_checkpoint: Option<bool>,
    cleanup_expired_logs: Option<bool>,
}

fn write_error_message(buffer: *mut c_char, buffer_len: usize, message: &str) {
    if buffer.is_null() || buffer_len == 0 {
        return;
    }
    let bytes = message.as_bytes();
    let target_len = bytes.len().min(buffer_len.saturating_sub(1));
    // SAFETY: caller promises `buffer` points to a valid, writable region of length `buffer_len`.
    unsafe {
        let target = slice::from_raw_parts_mut(buffer as *mut u8, buffer_len);
        target[..target_len].copy_from_slice(&bytes[..target_len]);
        target[target_len] = 0; // NUL terminator
    }
}

fn read_cstr(ptr: *const c_char) -> Result<String, DeltaFfiError> {
    if ptr.is_null() {
        return Err(DeltaFfiError::InvalidInput(
            "ol_delta_append received a null pointer".to_string(),
        ));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_owned())
        .map_err(|_| DeltaFfiError::InvalidInput("input strings must be valid UTF-8".into()))
}

fn parse_json_or_default<T>(payload: &str) -> Result<T, DeltaFfiError>
where
    T: DeserializeOwned + Default,
{
    if payload.trim().is_empty() {
        Ok(T::default())
    } else {
        let value = serde_json::from_str(payload)?;
        Ok(value)
    }
}

unsafe fn collect_batches(
    stream: *mut FFI_ArrowArrayStream,
) -> Result<Vec<RecordBatch>, DeltaFfiError> {
    if stream.is_null() {
        return Err(DeltaFfiError::InvalidInput(
            "Arrow stream pointer cannot be null".into(),
        ));
    }

    let mut reader = ArrowArrayStreamReader::from_raw(stream)?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }
    Ok(batches)
}

fn normalize_option(input: Option<String>) -> Option<String> {
    input
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn parse_save_mode(raw: Option<String>) -> Result<SaveMode, DeltaFfiError> {
    let Some(value) = raw else {
        return Ok(SaveMode::Append);
    };
    match value.to_ascii_lowercase().as_str() {
        "append" => Ok(SaveMode::Append),
        "overwrite" => Ok(SaveMode::Overwrite),
        "error_if_exists" | "error-if-exists" | "errorifexists" => Ok(SaveMode::ErrorIfExists),
        "ignore" => Ok(SaveMode::Ignore),
        other => Err(DeltaFfiError::InvalidInput(format!(
            "Unsupported save mode '{other}'"
        ))),
    }
}

fn parse_schema_mode(raw: Option<String>) -> Result<Option<SchemaMode>, DeltaFfiError> {
    let Some(value) = raw else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "merge" => Ok(Some(SchemaMode::Merge)),
        "overwrite" => Ok(Some(SchemaMode::Overwrite)),
        other => Err(DeltaFfiError::InvalidInput(format!(
            "Unsupported schema mode '{other}'"
        ))),
    }
}

fn merge_storage_options(
    mut base: HashMap<String, String>,
    tokens: &TokenPayload,
) -> HashMap<String, String> {
    if let Some(token) = tokens.storage_token() {
        base.entry("azure_storage_token".to_string())
            .or_insert(token);
    }
    base
}

fn build_commit_properties(
    options: &WriteOptions,
) -> deltalake::operations::transaction::CommitProperties {
    use deltalake::operations::transaction::CommitProperties;

    let mut props = CommitProperties::default();
    if !options.commit_info.is_empty() {
        props = props.with_metadata(options.commit_info.clone());
    }
    if let Some(flag) = options.create_checkpoint {
        props = props.with_create_checkpoint(flag);
    }
    if let Some(flag) = options.cleanup_expired_logs {
        props = props.with_cleanup_expired_logs(Some(flag));
    }
    props
}

fn perform_write(
    table_uri: String,
    mut options: WriteOptions,
    tokens: TokenPayload,
    batches: Vec<RecordBatch>,
) -> Result<(), DeltaFfiError> {
    if batches.is_empty() {
        return Ok(());
    }

    ensure_storage_handlers_registered();

    let save_mode = parse_save_mode(options.mode.take())?;
    let schema_mode = parse_schema_mode(options.schema_mode.take())?;
    let replace_where = normalize_option(options.replace_where.take());
    let safe_cast = options.safe_cast.unwrap_or(false);
    let storage_options =
        merge_storage_options(std::mem::take(&mut options.storage_options), &tokens);
    let commit_properties = build_commit_properties(&options);
    let table_name = options.table_name.take();
    let description = options.description.take();
    let configuration = std::mem::take(&mut options.configuration);
    let partition_columns = std::mem::take(&mut options.partition_columns);
    let target_file_size = options.target_file_size;
    let write_batch_size = options.write_batch_size;

    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };

        let mut builder = ops.write(batches).with_save_mode(save_mode);

        if let Some(mode) = schema_mode {
            builder = builder.with_schema_mode(mode);
        }
        if !partition_columns.is_empty() {
            builder = builder.with_partition_columns(partition_columns);
        }
        if let Some(predicate) = replace_where {
            builder = builder.with_replace_where(predicate);
        }
        if let Some(size) = target_file_size {
            builder = builder.with_target_file_size(size);
        }
        if let Some(batch_size) = write_batch_size {
            builder = builder.with_write_batch_size(batch_size);
        }
        if safe_cast {
            builder = builder.with_cast_safety(true);
        }
        if let Some(name) = table_name {
            builder = builder.with_table_name(name);
        }
        if let Some(desc) = description {
            builder = builder.with_description(desc);
        }
        if !configuration.is_empty() {
            builder = builder.with_configuration(configuration);
        }

        builder.with_commit_properties(commit_properties).await?;

        Ok::<(), DeltaTableError>(())
    })?;

    Ok(())
}

fn append_impl(
    stream: *mut FFI_ArrowArrayStream,
    table_uri: &str,
    token_json: &str,
    options_json: &str,
) -> Result<(), DeltaFfiError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(DeltaFfiError::InvalidInput(
            "table_uri must not be empty".into(),
        ));
    }

    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let options: WriteOptions = parse_json_or_default(options_json)?;
    let batches = unsafe { collect_batches(stream)? };

    perform_write(table_uri.to_string(), options, tokens, batches)
}

fn create_table_impl(
    table_uri: &str,
    schema_json: &str,
    token_json: &str,
    options_json: &str,
) -> Result<(), DeltaFfiError> {
    let table_uri = table_uri.trim();
    if table_uri.is_empty() {
        return Err(DeltaFfiError::InvalidInput(
            "table_uri must not be empty".into(),
        ));
    }

    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let options: WriteOptions = parse_json_or_default(options_json)?;

    // Parse Arrow schema from JSON
    let arrow_schema: Arc<deltalake::arrow::datatypes::Schema> = 
        Arc::new(serde_json::from_str(schema_json)?);

    ensure_storage_handlers_registered();

    let storage_options = merge_storage_options(HashMap::new(), &tokens);
    let partition_columns = options.partition_columns.clone();
    let table_name = options.table_name.clone();
    let description = options.description.clone();
    let configuration = options.configuration.clone();

    runtime()?.block_on(async move {
        let ops = if storage_options.is_empty() {
            DeltaOps::try_from_uri(&table_uri).await?
        } else {
            DeltaOps::try_from_uri_with_storage_options(&table_uri, storage_options).await?
        };

        let mut builder = ops.create()
            .with_columns(
                arrow_schema.fields().iter()
                    .map(|f| deltalake::kernel::StructField::try_from(f.as_ref()))
                    .collect::<Result<Vec<_>, _>>()?
            );

        if !partition_columns.is_empty() {
            builder = builder.with_partition_columns(partition_columns);
        }

        if let Some(name) = table_name {
            builder = builder.with_table_name(name);
        }

        if let Some(desc) = description {
            builder = builder.with_comment(desc);
        }

        if !configuration.is_empty() {
            builder = builder.with_configuration(configuration);
        }

        builder.await?;

        Ok::<(), DeltaTableError>(())
    })?;

    Ok(())
}

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

/// Appends a batch of rows (represented as an ArrowArrayStream) into a Delta table.
#[no_mangle]
pub extern "C" fn ol_delta_append(
    stream: *mut FFI_ArrowArrayStream,
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
        append_impl(stream, &table_uri, &token_json, &options_json)
    })();

    match result {
        Ok(()) => OlDeltaStatus::Ok as c_int,
        Err(err) => {
            write_error_message(error_buffer, error_buffer_len, &err.message());
            err.status() as c_int
        }
    }
}

/// Drops (deletes) a Delta table from storage.
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
        return Err(DeltaFfiError::InvalidInput(
            "table_uri must not be empty".into(),
        ));
    }

    let tokens: TokenPayload = parse_json_or_default(token_json)?;
    let options: DropTableOptions = parse_json_or_default(options_json)?;

    ensure_storage_handlers_registered();

    let storage_options = merge_storage_options(HashMap::new(), &tokens);

    runtime()?.block_on(async move {
        // Open the table to get access to object store
        let table = if storage_options.is_empty() {
            deltalake::open_table(&table_uri).await?
        } else {
            deltalake::open_table_with_storage_options(&table_uri, storage_options).await?
        };

        // Get object store for deletion operations
        let object_store = table.object_store();

        // Delete _delta_log directory and its contents
        use deltalake::storage::ObjectStoreError;

        let delta_log_path = deltalake::Path::from("_delta_log");
        let mut log_files = object_store.list(Some(&delta_log_path));

        // Collect all paths to delete
        let mut paths_to_delete = Vec::new();
        while let Some(result) = log_files.next().await {
            let meta = result?;
            paths_to_delete.push(meta.location.clone());
        }

        // Delete all log files
        for path in paths_to_delete {
            object_store.delete(&path).await.map_err(|e| {
                DeltaTableError::ObjectStore {
                    source: ObjectStoreError::Generic {
                        store: "azure",
                        source: Box::new(e),
                    },
                }
            })?;
        }

        // Optionally delete data files
        if options.delete_data {
            let root_path = deltalake::Path::from("");
            let mut all_files = object_store.list(Some(&root_path));

            let mut data_paths = Vec::new();
            while let Some(result) = all_files.next().await {
                let meta = result?;
                // Skip _delta_log entries (already deleted)
                if !meta.location.as_ref().starts_with("_delta_log") {
                    data_paths.push(meta.location.clone());
                }
            }

            for path in data_paths {
                object_store.delete(&path).await.map_err(|e| {
                    DeltaTableError::ObjectStore {
                        source: ObjectStoreError::Generic {
                            store: "azure",
                            source: Box::new(e),
                        },
                    }
                })?;
            }
        }

        Ok::<(), DeltaTableError>(())
    })?;

    Ok(())
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default, rename_all = "camelCase")]
struct DropTableOptions {
    /// If true, delete data files; if false, only remove transaction logs
    delete_data: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use deltalake::operations::DeltaOps;
    use deltalake::DeltaTable;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    static TEST_COUNTER: once_cell::sync::Lazy<AtomicUsize> =
        once_cell::sync::Lazy::new(|| AtomicUsize::new(0));

    struct TempDirGuard(PathBuf);

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn temp_table_dir() -> (String, TempDirGuard) {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("onelake-delta-writer-test-{id}"));
        fs::create_dir_all(&path).expect("temp dir should be creatable");
        let uri = path.to_string_lossy().to_string();
        (uri, TempDirGuard(path))
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let names = Arc::new(StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            Some("carol"),
        ]));

        RecordBatch::try_new(schema, vec![ids, names]).expect("failed to build test batch")
    }

    #[test]
    fn writes_to_local_table() {
        let (uri, _guard) = temp_table_dir();

        perform_write(
            uri.clone(),
            WriteOptions::default(),
            TokenPayload::default(),
            vec![sample_batch()],
        )
        .expect("write should succeed");

        runtime()
            .expect("runtime available")
            .block_on(async {
                let mut table: DeltaTable = DeltaOps::try_from_uri(&uri)
                    .await
                    .expect("table should load")
                    .into();
                table.load().await.expect("load succeeds");
                assert_eq!(table.version(), 0, "first write should produce version 0");
            });
    }

    #[test]
    fn writes_multiple_versions() {
        let (uri, _guard) = temp_table_dir();

        perform_write(
            uri.clone(),
            WriteOptions::default(),
            TokenPayload::default(),
            vec![sample_batch()],
        )
        .expect("first write should succeed");

        perform_write(
            uri.clone(),
            WriteOptions::default(),
            TokenPayload::default(),
            vec![sample_batch()],
        )
        .expect("second write should succeed");

        runtime().expect("runtime available").block_on(async {
            let mut table: DeltaTable = DeltaOps::try_from_uri(&uri)
                .await
                .expect("table should load")
                .into();
            table.load().await.expect("load succeeds");
            assert_eq!(table.version(), 1, "two successful writes should reach version 1");
        });
    }
}
