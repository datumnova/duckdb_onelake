#include "onelake_credentials.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

bool OneLakeCredentials::IsValid() const {
    return !tenant_id.empty() && !client_id.empty() && !client_secret.empty();
}

bool OneLakeCredentials::IsTokenExpired() const {
    if (access_token.empty()) {
        return true;
    }
    
    auto current_time = Timestamp::GetCurrentTimestamp();
    return current_time >= token_expiry;
}

} // namespace duckdb