#include "powerbi_resolver.hpp"

#include "http_client.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <unordered_map>

namespace duckdb {
using namespace duckdb_yyjson; // NOLINT

namespace {

static constexpr const char *WORKSPACES_PATH =
    "/powerbi/databases/v201606/workspaces?includeMyWorkspace=true";
static constexpr const char *CLUSTER_PATH =
    "/spglobalservice/GetOrInsertClusterUrisByTenantLocation";
static constexpr const char *GENERATE_AS_TOKEN_PATH =
    "/metadata/v201606/generateastoken?PreferClientRouting=true";
static constexpr const char *CLUSTER_RESOLVE_PATH = "/webapi/clusterResolve";
static constexpr const char *WEBAPI_XMLA_PATH = "/webapi/xmla";
static constexpr int64_t MWC_TOKEN_CACHE_TTL_SECONDS = 5 * 60;

struct CachedMwcToken {
  CachedMwcToken() = default;
  CachedMwcToken(string token_p, int64_t expires_at_unix_seconds_p)
      : token(std::move(token_p)),
        expires_at_unix_seconds(expires_at_unix_seconds_p) {}

  string token;
  int64_t expires_at_unix_seconds = 0;
};

std::mutex mwc_token_cache_lock;
std::unordered_map<string, CachedMwcToken> mwc_token_cache;

static int64_t CurrentUnixSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

static string HashSensitiveValue(const string &value) {
  uint64_t hash = 1469598103934665603ULL;
  for (auto ch : value) {
    hash ^= static_cast<uint8_t>(ch);
    hash *= 1099511628211ULL;
  }
  return std::to_string(hash);
}

static HttpHeaders PowerBIHeaders(const string &access_token) {
  return HttpHeaders{std::make_pair("Authorization", "Bearer " + access_token)};
}

static HttpHeaders JsonHeaders(const string &access_token) {
  auto headers = PowerBIHeaders(access_token);
  headers.emplace_back("Content-Type", "application/json");
  return headers;
}

static HttpHeaders JsonHeaders() {
  return HttpHeaders{std::make_pair("Content-Type", "application/json")};
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

static string BuildMwcTokenRequestBody(const PowerBIResolvedTarget &target) {
  string body;
  body.reserve(target.capacity_object_id.size() + target.workspace_id.size() +
               target.dataset_name.size() + 192);
  body += "{\"capacityObjectId\":\"";
  body += EscapeJSON(target.capacity_object_id);
  body += "\",\"workspaceObjectId\":\"";
  body += EscapeJSON(target.workspace_id);
  body += "\",\"datasetName\":\"";
  body += EscapeJSON(target.dataset_name);
  body += "\",\"applyAuxiliaryPermission\":false";
  body += ",\"bypassBuildPermission\":false";
  body += ",\"intendedUsage\":0}";
  return body;
}

static string ExtractUrlHost(const string &url) {
  auto scheme_pos = url.find("://");
  auto host_start = scheme_pos == string::npos ? 0 : scheme_pos + 3;
  auto host_end = url.find('/', host_start);
  auto host =
      url.substr(host_start, host_end == string::npos ? string::npos
                                                      : host_end - host_start);
  auto port_pos = host.rfind(':');
  if (port_pos != string::npos && host.find(']') == string::npos) {
    return host.substr(0, port_pos);
  }
  return host;
}

static string ExtractFirstUrlPathSegment(const string &url) {
  auto scheme_pos = url.find("://");
  auto host_start = scheme_pos == string::npos ? 0 : scheme_pos + 3;
  auto path_start = url.find('/', host_start);
  if (path_start == string::npos) {
    return string();
  }
  while (path_start < url.size() && url[path_start] == '/') {
    path_start++;
  }
  auto path_end = url.find('/', path_start);
  return url.substr(path_start, path_end == string::npos
                                    ? string::npos
                                    : path_end - path_start);
}

static string
BuildClusterResolveRequestBody(const PowerBIResolvedTarget &target) {
  string body;
  body.reserve(target.capacity_object_id.size() + 96);
  body += "{\"serverName\":\"";
  body += EscapeJSON(target.capacity_object_id);
  body += "\",\"databaseName\":null";
  body += ",\"premiumPublicXmlaEndpoint\":true}";
  return body;
}

static string ResolveCapacityXmlaUrl(PowerBIResolvedTarget &target,
                                     int64_t timeout_ms) {
  auto capacity_host = ExtractUrlHost(target.capacity_uri);
  if (capacity_host.empty()) {
    return string();
  }
  HttpClient client(timeout_ms);
  auto response = client.Post(
      string("https://") + capacity_host + CLUSTER_RESOLVE_PATH, JsonHeaders(),
      BuildClusterResolveRequestBody(target), "application/json");
  auto document = ReadJsonDocument(response, "resolve capacity XMLA cluster");
  auto *root = yyjson_doc_get_root(document.get());
  if (!root || !yyjson_is_obj(root)) {
    throw IOException(
        "resolve capacity XMLA cluster response was not a JSON object");
  }
  auto cluster_fqdn = GetRequiredJSONString(root, "clusterFQDN",
                                            "resolve capacity XMLA cluster");
  target.core_server_name = GetRequiredJSONString(
      root, "coreServerName", "resolve capacity XMLA cluster");
  return string("https://") + cluster_fqdn + WEBAPI_XMLA_PATH;
}

static string ResponseExcerpt(const string &body) {
  static constexpr idx_t MAX_EXCERPT_LENGTH = 512;
  if (body.size() <= MAX_EXCERPT_LENGTH) {
    return body;
  }
  return body.substr(0, MAX_EXCERPT_LENGTH) + "...";
}

static string MwcTokenCacheKey(const PowerBIEndpoint &endpoint,
                               const PowerBIResolvedTarget &target,
                               const string &access_token) {
  return endpoint.host + "|workspace=" + target.workspace_id +
         "|capacity=" + target.capacity_object_id +
         "|dataset=" + target.dataset_name +
         "|aad_hash=" + HashSensitiveValue(access_token);
}

static bool TryGetCachedMwcToken(const string &cache_key, string &token) {
  std::lock_guard<std::mutex> guard(mwc_token_cache_lock);
  auto entry = mwc_token_cache.find(cache_key);
  if (entry == mwc_token_cache.end()) {
    return false;
  }
  if (entry->second.expires_at_unix_seconds <= CurrentUnixSeconds()) {
    mwc_token_cache.erase(entry);
    return false;
  }
  token = entry->second.token;
  if (DebugTimingsEnabled()) {
    std::fprintf(stderr, "[pbi_scanner] MWC token cache hit\n");
  }
  return true;
}

static void StoreCachedMwcToken(const string &cache_key, const string &token) {
  std::lock_guard<std::mutex> guard(mwc_token_cache_lock);
  mwc_token_cache[cache_key] =
      CachedMwcToken{token, CurrentUnixSeconds() + MWC_TOKEN_CACHE_TTL_SECONDS};
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
  string capacity_uri;
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
      capacity_uri = GetOptionalJSONString(workspace_value, "capacityUri");
      auto capacity_id_from_uri = ExtractFirstUrlPathSegment(capacity_uri);
      if (!capacity_id_from_uri.empty()) {
        capacity_object_id = capacity_id_from_uri;
      }
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

  PowerBIResolvedTarget result;
  result.workspace_name = endpoint.workspace_name;
  result.workspace_id = workspace_id;
  result.workspace_type = workspace_type;
  result.capacity_object_id = capacity_object_id;
  result.capacity_uri = capacity_uri;
  result.dataset_name = database;
  result.dataset_id = database_name;
  result.internal_catalog = "sobe_wowvirtualserver-" + database_name;
  if (!capacity_object_id.empty() && !capacity_uri.empty()) {
    auto capacity_cluster_start = std::chrono::steady_clock::now();
    result.aixl_url = ResolveCapacityXmlaUrl(result, timeout_ms);
    DebugTiming("ResolvePowerBITarget capacity cluster POST",
                capacity_cluster_start);
    result.fixed_cluster_uri = result.aixl_url;
  } else {
    auto cluster_start = std::chrono::steady_clock::now();
    auto cluster_response =
        client.Put(base_url + CLUSTER_PATH, JsonHeaders(access_token), string(),
                   "application/json");
    DebugTiming("ResolvePowerBITarget cluster PUT", cluster_start);
    auto cluster_parse_start = std::chrono::steady_clock::now();
    auto cluster_document =
        ReadJsonDocument(cluster_response, "resolve cluster");
    DebugTiming("ResolvePowerBITarget cluster JSON parse", cluster_parse_start);
    auto *cluster_root = yyjson_doc_get_root(cluster_document.get());
    if (!cluster_root || !yyjson_is_obj(cluster_root)) {
      throw IOException("resolve cluster response was not a JSON object");
    }
    auto fixed_cluster_uri = GetRequiredJSONString(
        cluster_root, "FixedClusterUri", "resolve cluster");
    while (!fixed_cluster_uri.empty() && fixed_cluster_uri.back() == '/') {
      fixed_cluster_uri.pop_back();
    }
    fixed_cluster_uri += "/";
    result.fixed_cluster_uri = fixed_cluster_uri;
    result.aixl_url =
        fixed_cluster_uri + "xmla?vs=sobe_wowvirtualserver&db=" + database_name;
  }
  if (DebugTimingsEnabled()) {
    std::fprintf(stderr,
                 "[pbi_scanner] ResolvePowerBITarget direct connection "
                 "string: Data Source=%s;Initial Catalog=%s;\n",
                 result.aixl_url.c_str(), result.internal_catalog.c_str());
  }
  return result;
}

std::string ResolveLegacyPowerBIXmlaUrl(const PowerBIEndpoint &endpoint,
                                        const PowerBIResolvedTarget &target,
                                        const string &access_token,
                                        int64_t timeout_ms) {
  HttpClient client(timeout_ms);
  auto base_url = string("https://") + endpoint.host;
  auto cluster_response =
      client.Put(base_url + CLUSTER_PATH, JsonHeaders(access_token), string(),
                 "application/json");
  auto cluster_document = ReadJsonDocument(cluster_response, "resolve cluster");
  auto *cluster_root = yyjson_doc_get_root(cluster_document.get());
  if (!cluster_root || !yyjson_is_obj(cluster_root)) {
    throw IOException("resolve cluster response was not a JSON object");
  }
  auto fixed_cluster_uri =
      GetRequiredJSONString(cluster_root, "FixedClusterUri", "resolve cluster");
  while (!fixed_cluster_uri.empty() && fixed_cluster_uri.back() == '/') {
    fixed_cluster_uri.pop_back();
  }
  return fixed_cluster_uri +
         "/xmla?vs=sobe_wowvirtualserver&db=" + target.dataset_id;
}

std::string GeneratePowerBIXmlaToken(const PowerBIEndpoint &endpoint,
                                     const PowerBIResolvedTarget &target,
                                     const string &access_token,
                                     int64_t timeout_ms,
                                     bool bypass_mwc_cache) {
  if (Trimmed(access_token).empty()) {
    throw InvalidInputException("access_token is required");
  }
  if (Trimmed(target.workspace_id).empty() ||
      Trimmed(target.capacity_object_id).empty() ||
      Trimmed(target.dataset_name).empty()) {
    throw IOException("resolved Power BI target did not include the workspace, "
                      "capacity, and dataset identifiers required for XMLA "
                      "token generation");
  }
  auto cache_key = MwcTokenCacheKey(endpoint, target, access_token);
  string cached_token;
  if (!bypass_mwc_cache && TryGetCachedMwcToken(cache_key, cached_token)) {
    return cached_token;
  }

  HttpClient client(timeout_ms);
  auto token_start = std::chrono::steady_clock::now();
  auto token_url = string("https://") + endpoint.host + GENERATE_AS_TOKEN_PATH;
  auto token_body = BuildMwcTokenRequestBody(target);
  HttpResponse response;
  for (idx_t redirect_count = 0; redirect_count < 3; redirect_count++) {
    response = client.Post(token_url, JsonHeaders(access_token), token_body,
                           "application/json");
    if (response.status != 307) {
      break;
    }
    auto location = response.GetHeader("Location");
    if (location.empty()) {
      break;
    }
    token_url = location;
  }
  DebugTiming("GeneratePowerBIXmlaToken POST", token_start);
  auto parse_start = std::chrono::steady_clock::now();
  if (response.HasRequestError()) {
    throw IOException("generate XMLA token request failed: %s",
                      response.request_error);
  }
  if (response.status >= 400) {
    throw IOException("generate XMLA token http %d: %s", response.status,
                      response.body);
  }
  auto *raw_document = yyjson_read(response.body.c_str(), response.body.size(),
                                   YYJSON_READ_NOFLAG);
  if (!raw_document) {
    throw IOException("generate XMLA token returned invalid JSON (http %d, "
                      "Content-Type \"%s\", body: %s)",
                      response.status,
                      response.GetHeader("Content-Type").c_str(),
                      ResponseExcerpt(response.body).c_str());
  }
  std::unique_ptr<yyjson_doc, void (*)(yyjson_doc *)> document(raw_document,
                                                               yyjson_doc_free);
  DebugTiming("GeneratePowerBIXmlaToken JSON parse", parse_start);
  auto *root = yyjson_doc_get_root(document.get());
  if (!root || !yyjson_is_obj(root)) {
    throw IOException("generate XMLA token response was not a JSON object");
  }
  auto token = GetRequiredJSONString(root, "Token", "generate XMLA token");
  StoreCachedMwcToken(cache_key, token);
  return token;
}

} // namespace duckdb
