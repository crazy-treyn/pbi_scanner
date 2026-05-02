#pragma once

#include "connection_string.hpp"
#include "powerbi_resolver.hpp"
#include "xmla.hpp"

#include <string>
#include <vector>

namespace duckdb {

bool TryGetCachedTarget(const PowerBIConnectionConfig &config,
                        PowerBIResolvedTarget &target);
void StoreCachedTarget(const PowerBIConnectionConfig &config,
                       const PowerBIResolvedTarget &target);
void InvalidateCachedTarget(const PowerBIConnectionConfig &config);

bool TryGetCachedSchema(const PowerBIResolvedTarget &target,
                        const std::string &dax_text,
                        const std::string &effective_user_name,
                        std::vector<XmlaColumn> &columns);
void StoreCachedSchema(const PowerBIResolvedTarget &target,
                       const std::string &dax_text,
                       const std::string &effective_user_name,
                       const std::vector<XmlaColumn> &columns);
void InvalidateCachedSchema(const PowerBIResolvedTarget &target,
                            const std::string &dax_text,
                            const std::string &effective_user_name);

bool TestMetadataCacheRoundTrip();

} // namespace duckdb
