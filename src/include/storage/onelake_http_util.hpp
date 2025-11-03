#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {

class OneLakeCatalog;
class OneLakeSchemaEntry;

void EnsureHttpBearerSecret(ClientContext &context, OneLakeCatalog &catalog,
                            const OneLakeSchemaEntry *schema_entry);

} // namespace duckdb
