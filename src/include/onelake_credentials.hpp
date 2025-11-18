#pragma once
#include "duckdb.hpp"
#include <unordered_map>
#include <vector>

namespace duckdb {

enum class OneLakeCredentialProvider { ServicePrincipal, CredentialChain };

struct OneLakeCredentials {
	string tenant_id;
	string client_id;
	string client_secret;
	string credential_chain;
	string env_fabric_variable;
	string env_storage_variable;
	OneLakeCredentialProvider provider = OneLakeCredentialProvider::ServicePrincipal;
	struct TokenCacheEntry {
		string token;
		timestamp_t expiry;
	};
	std::unordered_map<string, TokenCacheEntry> token_cache;

	bool IsValid() const;
};

vector<string> ParseOneLakeCredentialChain(const string &chain_value);
string NormalizeOneLakeCredentialChain(const string &chain_value);

} // namespace duckdb
