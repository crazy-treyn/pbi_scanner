#pragma once

#include "duckdb/common/named_parameter_map.hpp"

#include <string>

namespace duckdb {

class ClientContext;
struct PowerBIConnectionConfig;

string
ResolvePowerBIAccessToken(ClientContext &context,
                          const PowerBIConnectionConfig &connection_config,
                          const named_parameter_map_t &named_parameters);

string TestServicePrincipalAuthErrorMessage(const string &test_case);

} // namespace duckdb
