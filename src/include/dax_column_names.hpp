#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/function/function.hpp"

#include <vector>

namespace duckdb {

class ClientContext;

bool ResolveNormalizeDaxColumnNames(ClientContext &context,
                                    const named_parameter_map_t &named_parameters);
string FormatDaxColumnNameForDuckDB(const string &raw_name, bool normalize);
std::vector<string> FormatDaxColumnNamesForDuckDB(const std::vector<string> &raw_names,
                                                  bool normalize);

} // namespace duckdb
