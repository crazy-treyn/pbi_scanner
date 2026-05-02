#pragma once

#include "connection_string.hpp"

#include <string>

namespace duckdb {

struct PowerBIResolvedTarget {
  std::string workspace_name;
  std::string workspace_id;
  std::string workspace_type;
  std::string capacity_object_id;
  std::string capacity_uri;
  std::string dataset_name;
  std::string dataset_id;
  std::string internal_catalog;
  std::string aixl_url;
  std::string fixed_cluster_uri;
  std::string core_server_name;
};

PowerBIResolvedTarget ResolvePowerBITarget(const PowerBIEndpoint &endpoint,
                                           const std::string &database,
                                           const std::string &access_token,
                                           int64_t timeout_ms);

std::string GeneratePowerBIXmlaToken(const PowerBIEndpoint &endpoint,
                                     const PowerBIResolvedTarget &target,
                                     const std::string &access_token,
                                     int64_t timeout_ms);

std::string ResolveLegacyPowerBIXmlaUrl(const PowerBIEndpoint &endpoint,
                                        const PowerBIResolvedTarget &target,
                                        const std::string &access_token,
                                        int64_t timeout_ms);

} // namespace duckdb
