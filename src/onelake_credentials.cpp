#include "onelake_credentials.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/string_util.hpp"

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

static string NormalizeChainSeparators(string input) {
	for (auto &ch : input) {
		if (ch == ',') {
			ch = ';';
		}
	}
	return input;
}

vector<string> ParseOneLakeCredentialChain(const string &chain_value) {
	vector<string> parts;
	if (chain_value.empty()) {
		return parts;
	}
	auto normalized = NormalizeChainSeparators(StringUtil::Lower(chain_value));
	auto raw_parts = StringUtil::Split(normalized, ';');
	for (auto &part : raw_parts) {
		StringUtil::Trim(part);
		if (part.empty()) {
			continue;
		}
		parts.push_back(part);
	}
	return parts;
}

string NormalizeOneLakeCredentialChain(const string &chain_value) {
	auto parts = ParseOneLakeCredentialChain(chain_value);
	if (parts.empty()) {
		return string();
	}
	return StringUtil::Join(parts, ";");
}

} // namespace duckdb
