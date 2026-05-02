#pragma once

#include "duckdb/function/table_function.hpp"

#include <string>

namespace duckdb {

TableFunction CreateDaxQueryFunction();
TableFunction CreatePbiTablesFunction();
TableFunction CreatePbiColumnsFunction();
TableFunction CreatePbiMeasuresFunction();
TableFunction CreatePbiRelationshipsFunction();
bool TestMetadataCacheRoundTrip();
std::string BuildDaxSchemaProbeForTesting(const std::string &statement,
                                          int64_t row_limit);

} // namespace duckdb
