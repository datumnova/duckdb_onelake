#pragma once
#include "duckdb.hpp"

namespace duckdb {

class OnelakeExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override;
    std::string Version() const override;

private:
    void RegisterFunctions();
    void RegisterTypes();
};

} // namespace duckdb