#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

struct PowerBIEndpoint {
  std::string raw;
  std::string host;
  std::string workspace_name;
};

struct PowerBIConnectionConfig {
  std::string raw;
  std::string data_source;
  std::string initial_catalog;
  bool is_direct_xmla = false;
  std::string secret_name;
  std::string effective_user_name;
  int64_t timeout_ms = 0;
  bool has_timeout_ms = false;
  PowerBIEndpoint endpoint;
};

PowerBIEndpoint ParsePowerBIEndpoint(const std::string &raw);
PowerBIConnectionConfig ParsePowerBIConnectionString(const std::string &raw);

} // namespace duckdb
