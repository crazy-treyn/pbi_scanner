#include "powerbi_resolver.hpp"

#include "http_client.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>

namespace duckdb {
using namespace duckdb_yyjson; // NOLINT

namespace {

static constexpr const char *WORKSPACES_PATH =
    "/powerbi/databases/v201606/workspaces?includeMyWorkspace=true";
static constexpr const char *CLUSTER_PATH =
    "/spglobalservice/GetOrInsertClusterUrisByTenantLocation";

static HttpHeaders PowerBIHeaders(const string &access_token) {
  return HttpHeaders{std::make_pair("Authorization", "Bearer " + access_token)};
}

static HttpHeaders JsonHeaders(const string &access_token) {
  auto headers = PowerBIHeaders(access_token);
  headers.emplace_back("Content-Type", "application/json");
  return headers;
}

static std::unique_ptr<yyjson_doc, void (*)(yyjson_doc *)>
ReadJsonDocument(const HttpResponse &response, const string &operation) {
  if (response.HasRequestError()) {
    throw IOException("%s request failed: %s", operation,
                      response.request_error);
  }
  if (response.status >= 400) {
    throw IOException("%s http %d: %s", operation, response.status,
                      response.body);
  }

  auto *document = yyjson_read(response.body.c_str(), response.body.size(),
                               YYJSON_READ_NOFLAG);
  if (!document) {
    throw IOException("%s returned invalid JSON", operation);
  }
  return std::unique_ptr<yyjson_doc, void (*)(yyjson_doc *)>(document,
                                                             yyjson_doc_free);
}

static string GetRequiredJSONString(yyjson_val *object, const char *key,
                                    const string &operation) {
  auto *value = yyjson_obj_get(object, key);
  if (!value || !yyjson_is_str(value) || !yyjson_get_str(value)) {
    throw IOException("%s response was missing required field \"%s\"",
                      operation, key);
  }
  return yyjson_get_str(value);
}

static string GetOptionalJSONString(yyjson_val *object, const char *key) {
  auto *value = yyjson_obj_get(object, key);
  if (!value || !yyjson_is_str(value) || !yyjson_get_str(value)) {
    return string();
  }
  return yyjson_get_str(value);
}

static string EscapeJSON(const string &value) {
  string result;
  result.reserve(value.size() + 8);
  for (auto ch : value) {
    switch (ch) {
    case '\\':
      result += "\\\\";
      break;
    case '"':
      result += "\\\"";
      break;
    case '\b':
      result += "\\b";
      break;
    case '\f':
      result += "\\f";
      break;
    case '\n':
      result += "\\n";
      break;
    case '\r':
      result += "\\r";
      break;
    case '\t':
      result += "\\t";
      break;
    default:
      result.push_back(ch);
      break;
    }
  }
  return result;
}

} // namespace

PowerBIResolvedTarget ResolvePowerBITarget(const PowerBIEndpoint &endpoint,
                                           const string &database,
                                           const string &access_token,
                                           int64_t timeout_ms) {
  if (Trimmed(access_token).empty()) {
    throw InvalidInputException("access_token is required");
  }
  if (Trimmed(database).empty()) {
    throw InvalidInputException("Initial Catalog is required");
  }

  HttpClient client(timeout_ms);
  auto base_url = string("https://") + endpoint.host;

  auto workspace_start = std::chrono::steady_clock::now();
  auto workspace_response =
      client.Get(base_url + WORKSPACES_PATH, PowerBIHeaders(access_token));
  DebugTiming("ResolvePowerBITarget workspace GET", workspace_start);
  auto workspace_parse_start = std::chrono::steady_clock::now();
  auto workspace_document =
      ReadJsonDocument(workspace_response, "resolve workspace");
  DebugTiming("ResolvePowerBITarget workspace JSON parse",
              workspace_parse_start);
  auto *workspace_root = yyjson_doc_get_root(workspace_document.get());
  if (!workspace_root || !yyjson_is_arr(workspace_root)) {
    throw IOException("resolve workspace response was not a JSON array");
  }

  string workspace_id;
  string workspace_type;
  string capacity_object_id;
  idx_t index, max;
  yyjson_val *workspace_value;
  yyjson_arr_foreach(workspace_root, index, max, workspace_value) {
    if (!yyjson_is_obj(workspace_value)) {
      continue;
    }
    auto workspace_name = GetOptionalJSONString(workspace_value, "name");
    if (StringUtil::CIEquals(workspace_name, endpoint.workspace_name)) {
      workspace_id =
          GetRequiredJSONString(workspace_value, "id", "resolve workspace");
      workspace_type =
          GetRequiredJSONString(workspace_value, "type", "resolve workspace");
      capacity_object_id =
          GetOptionalJSONString(workspace_value, "capacityObjectId");
      break;
    }
  }
  if (workspace_id.empty()) {
    throw IOException("workspace \"%s\" was not found",
                      endpoint.workspace_name);
  }

  string dataset_lookup_body =
      string("{\"datasetName\":\"") + EscapeJSON(database) +
      "\",\"workspaceType\":\"" + EscapeJSON(workspace_type) + "\"}";
  auto dataset_url = base_url + "/powerbi/databases/v201606/workspaces/" +
                     workspace_id + "/getDatabaseName";
  auto dataset_start = std::chrono::steady_clock::now();
  auto dataset_response = client.Post(dataset_url, JsonHeaders(access_token),
                                      dataset_lookup_body, "application/json");
  DebugTiming("ResolvePowerBITarget dataset POST", dataset_start);
  auto dataset_parse_start = std::chrono::steady_clock::now();
  auto dataset_document = ReadJsonDocument(dataset_response, "resolve dataset");
  DebugTiming("ResolvePowerBITarget dataset JSON parse", dataset_parse_start);
  auto *dataset_root = yyjson_doc_get_root(dataset_document.get());
  if (!dataset_root || !yyjson_is_arr(dataset_root)) {
    throw IOException("resolve dataset response was not a JSON array");
  }

  string database_name;
  idx_t dataset_index, dataset_max;
  yyjson_val *dataset_value;
  yyjson_arr_foreach(dataset_root, dataset_index, dataset_max, dataset_value) {
    if (!yyjson_is_obj(dataset_value)) {
      continue;
    }
    auto dataset_name = GetOptionalJSONString(dataset_value, "datasetName");
    if (StringUtil::CIEquals(dataset_name, database)) {
      database_name = GetRequiredJSONString(dataset_value, "databaseName",
                                            "resolve dataset");
      break;
    }
  }
  if (database_name.empty()) {
    throw IOException("dataset \"%s\" was not found", database);
  }

  auto cluster_start = std::chrono::steady_clock::now();
  auto cluster_response =
      client.Put(base_url + CLUSTER_PATH, JsonHeaders(access_token), string(),
                 "application/json");
  DebugTiming("ResolvePowerBITarget cluster PUT", cluster_start);
  auto cluster_parse_start = std::chrono::steady_clock::now();
  auto cluster_document = ReadJsonDocument(cluster_response, "resolve cluster");
  DebugTiming("ResolvePowerBITarget cluster JSON parse", cluster_parse_start);
  auto *cluster_root = yyjson_doc_get_root(cluster_document.get());
  if (!cluster_root || !yyjson_is_obj(cluster_root)) {
    throw IOException("resolve cluster response was not a JSON object");
  }
  auto fixed_cluster_uri =
      GetRequiredJSONString(cluster_root, "FixedClusterUri", "resolve cluster");
  while (!fixed_cluster_uri.empty() && fixed_cluster_uri.back() == '/') {
    fixed_cluster_uri.pop_back();
  }
  fixed_cluster_uri += "/";

  PowerBIResolvedTarget result;
  result.workspace_name = endpoint.workspace_name;
  result.workspace_id = workspace_id;
  result.workspace_type = workspace_type;
  result.capacity_object_id = capacity_object_id;
  result.dataset_name = database;
  result.dataset_id = database_name;
  result.fixed_cluster_uri = fixed_cluster_uri;
  result.internal_catalog = "sobe_wowvirtualserver-" + database_name;
  result.aixl_url =
      fixed_cluster_uri + "xmla?vs=sobe_wowvirtualserver&db=" + database_name;
  if (DebugTimingsEnabled()) {
    std::fprintf(stderr,
                 "[pbi_scanner] ResolvePowerBITarget direct connection "
                 "string: Data Source=%s;Initial Catalog=%s;\n",
                 result.aixl_url.c_str(), result.internal_catalog.c_str());
  }
  return result;
}

} // namespace duckdb
