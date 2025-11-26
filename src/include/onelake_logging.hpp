#pragma once

#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"

// Helper macros that only emit logs when a ClientContext pointer is available.
#define ONELAKE_LOG_TRACE(ctx_ptr, ...)                                                                                \
	do {                                                                                                               \
		if (ctx_ptr) {                                                                                                 \
			DUCKDB_LOG_TRACE(*(ctx_ptr), __VA_ARGS__);                                                                 \
		}                                                                                                              \
	} while (0)

#define ONELAKE_LOG_DEBUG(ctx_ptr, ...)                                                                                \
	do {                                                                                                               \
		if (ctx_ptr) {                                                                                                 \
			DUCKDB_LOG_DEBUG(*(ctx_ptr), __VA_ARGS__);                                                                 \
		}                                                                                                              \
	} while (0)

#define ONELAKE_LOG_INFO(ctx_ptr, ...)                                                                                 \
	do {                                                                                                               \
		if (ctx_ptr) {                                                                                                 \
			DUCKDB_LOG_INFO(*(ctx_ptr), __VA_ARGS__);                                                                  \
		}                                                                                                              \
	} while (0)

#define ONELAKE_LOG_WARN(ctx_ptr, ...)                                                                                 \
	do {                                                                                                               \
		if (ctx_ptr) {                                                                                                 \
			DUCKDB_LOG_WARN(*(ctx_ptr), __VA_ARGS__);                                                                  \
		}                                                                                                              \
	} while (0)

#define ONELAKE_LOG_ERROR(ctx_ptr, ...)                                                                                \
	do {                                                                                                               \
		if (ctx_ptr) {                                                                                                 \
			DUCKDB_LOG_ERROR(*(ctx_ptr), __VA_ARGS__);                                                                 \
		}                                                                                                              \
	} while (0)

// Write operation specific logging macros with standardized format
// Format: [operation_type] message: key1=value1, key2=value2

#define ONELAKE_LOG_WRITE_START(ctx_ptr, operation, table_name, ...)                                                  \
	ONELAKE_LOG_INFO(ctx_ptr, "[write:%s] Starting operation: table=%s, " __VA_ARGS__, operation, table_name)

#define ONELAKE_LOG_WRITE_SUCCESS(ctx_ptr, operation, table_name, rows, ...)                                          \
	ONELAKE_LOG_INFO(ctx_ptr, "[write:%s] Operation succeeded: table=%s, rows=%llu, " __VA_ARGS__, operation,         \
	                 table_name, (unsigned long long)(rows))

#define ONELAKE_LOG_WRITE_ERROR(ctx_ptr, operation, table_name, error_msg)                                            \
	ONELAKE_LOG_ERROR(ctx_ptr, "[write:%s] Operation failed: table=%s, error=%s", operation, table_name, error_msg)

#define ONELAKE_LOG_WRITE_WARN(ctx_ptr, operation, table_name, warning_msg)                                           \
	ONELAKE_LOG_WARN(ctx_ptr, "[write:%s] Warning: table=%s, message=%s", operation, table_name, warning_msg)

// Delta-specific logging for transaction details
#define ONELAKE_LOG_DELTA_COMMIT(ctx_ptr, table_name, version, files_added, files_removed)                            \
	ONELAKE_LOG_INFO(ctx_ptr, "[delta:commit] table=%s, version=%lld, files_added=%zu, files_removed=%zu",            \
	                 table_name, (long long)(version), (size_t)(files_added), (size_t)(files_removed))

#define ONELAKE_LOG_DELTA_URI(ctx_ptr, table_name, uri)                                                               \
	ONELAKE_LOG_DEBUG(ctx_ptr, "[delta:uri] table=%s, resolved_uri=%s", table_name, uri)
