#pragma once
#include "duckdb.hpp"
#include <unordered_map>

namespace duckdb {

struct OneLakeCredentials {
    string tenant_id;
    string client_id;
    string client_secret;
    struct TokenCacheEntry {
        string token;
        timestamp_t expiry;
    };
    std::unordered_map<string, TokenCacheEntry> token_cache;
    
    bool IsValid() const;
};

} // namespace duckdb