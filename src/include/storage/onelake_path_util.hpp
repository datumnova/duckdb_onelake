#pragma once

#include "duckdb/common/vector.hpp"
#include <string>

namespace duckdb {

class OneLakeCatalog;
class OneLakeSchemaEntry;
class OneLakeTableEntry;

//! Build the ordered list of candidate storage locations for a given OneLake table.
vector<string> BuildLocationCandidates(const OneLakeCatalog &catalog, const OneLakeSchemaEntry &schema_entry,
                                       const OneLakeTableEntry &table_entry, const string &cached_path);

//! Returns true if the provided path points to ABFS/ABFSS storage.
bool IsValidAbfssPath(const string &path);

//! Produces a short diagnostic string describing the path flavor (abfss vs https, etc.).
string GetAbfssPathDiagnostic(const string &path);

} // namespace duckdb
