#include "onelake_credentials.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

bool OneLakeCredentials::IsValid() const {
    return !tenant_id.empty() && !client_id.empty() && !client_secret.empty();
}

} // namespace duckdb