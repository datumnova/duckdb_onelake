#pragma once
#include "duckdb.hpp"

namespace duckdb {

struct OneLakeCredentials {
    string tenant_id;
    string client_id;
    string client_secret;
    string access_token; // Cached token
    timestamp_t token_expiry;
    
    bool IsValid() const;
    bool IsTokenExpired() const;
};

} // namespace duckdb