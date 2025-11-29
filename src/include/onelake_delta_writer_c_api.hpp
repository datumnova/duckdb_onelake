#pragma once

#include <cstdint>

#include "duckdb/common/arrow/arrow.hpp"

extern "C" {

enum class OlDeltaStatus : int32_t {
	Ok = 0,
	InvalidInput = 1,
	Unimplemented = 2,
	ArrowError = 3,
	DeltaError = 4,
	JsonError = 5,
	RuntimeError = 6,
	InternalError = 100,
};

int ol_delta_create_table(const char *table_uri, const char *schema_json, const char *token_json,
                          const char *options_json, char *error_buffer, uintptr_t error_buffer_len);

int ol_delta_drop_table(const char *table_uri, const char *token_json, const char *options_json, char *error_buffer,
                        uintptr_t error_buffer_len);

int ol_delta_append(ArrowArrayStream *stream, const char *table_uri, const char *token_json, const char *options_json,
                    char *error_buffer, uintptr_t error_buffer_len);

int ol_delta_delete(const char *table_uri, const char *predicate, const char *token_json, char *error_buffer,
                    uintptr_t error_buffer_len, uint64_t *rows_deleted, uint64_t *files_removed, uint64_t *files_added);

int ol_delta_update(const char *table_uri, const char *predicate, const char *updates_json, const char *token_json,
                    char *error_buffer, uintptr_t error_buffer_len, uint64_t *rows_updated, uint64_t *files_removed,
                    uint64_t *files_added);

} // extern "C"
