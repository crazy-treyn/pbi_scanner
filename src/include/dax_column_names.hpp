#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;

bool ResolveNormalizeDaxColumnNames(ClientContext &context);
string FormatDaxColumnNameForDuckDB(const string &raw_name, bool normalize);

} // namespace duckdb
