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
