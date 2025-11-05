#pragma once
#include "duckdb.hpp"
#include <unordered_map>

namespace duckdb {

enum class OneLakeCredentialProvider { ServicePrincipal, CredentialChain };

struct OneLakeCredentials {
	string tenant_id;
	string client_id;
	string client_secret;
	string credential_chain;
	OneLakeCredentialProvider provider = OneLakeCredentialProvider::ServicePrincipal;
	struct TokenCacheEntry {
		string token;
		timestamp_t expiry;
	};
	std::unordered_map<string, TokenCacheEntry> token_cache;

	bool IsValid() const;
};

} // namespace duckdb
