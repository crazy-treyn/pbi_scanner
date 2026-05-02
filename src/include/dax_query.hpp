#pragma once

#include "duckdb/function/table_function.hpp"

#include <string>

namespace duckdb {

TableFunction CreateDaxQueryFunction();
TableFunction CreatePbiTablesFunction();
TableFunction CreatePbiColumnsFunction();
TableFunction CreatePbiMeasuresFunction();
TableFunction CreatePbiRelationshipsFunction();

} // namespace duckdb
