#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

std::string BuildLimitedDaxSchemaProbe(const std::string &statement,
                                       int64_t row_limit);
std::string BuildDaxSchemaProbeForTesting(const std::string &statement,
                                          int64_t row_limit);

} // namespace duckdb
