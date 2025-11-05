#include "onelake_credentials.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

bool OneLakeCredentials::IsValid() const {
	switch (provider) {
	case OneLakeCredentialProvider::ServicePrincipal:
		return !tenant_id.empty() && !client_id.empty() && !client_secret.empty();
	case OneLakeCredentialProvider::CredentialChain:
		return !credential_chain.empty();
	default:
		return false;
	}
}

} // namespace duckdb
