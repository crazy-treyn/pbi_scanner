#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

TableFunction CreateDaxQueryFunction();
TableFunction CreatePbiTablesFunction();
TableFunction CreatePbiColumnsFunction();
TableFunction CreatePbiMeasuresFunction();
TableFunction CreatePbiRelationshipsFunction();
bool TestMetadataCacheRoundTrip();

} // namespace duckdb
