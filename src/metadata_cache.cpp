#include "metadata_cache.hpp"

#include "pbi_scanner_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <unordered_map>
#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace duckdb {

namespace {

std::mutex query_cache_lock;
std::unordered_map<string, PowerBIResolvedTarget> resolved_target_cache;
std::unordered_map<string, std::vector<XmlaColumn>> schema_cache;

static constexpr int64_t DEFAULT_METADATA_CACHE_TTL_SECONDS = 24 * 60 * 60;
static constexpr const char *TARGET_CACHE_VERSION =
    "pbi_scanner_target_cache_v3";
static constexpr const char *SCHEMA_CACHE_VERSION =
    "pbi_scanner_schema_cache_v1";
static constexpr const char *CACHE_FORMAT_VERSION = "v1";

static string TargetCacheKey(const PowerBIConnectionConfig &config) {
  return config.data_source + "\n" + config.initial_catalog;
}

static string SchemaCacheKey(const PowerBIResolvedTarget &target,
                             const string &dax_text,
                             const string &effective_user_name) {
  return target.aixl_url + "\n" + target.internal_catalog + "\n" +
         effective_user_name + "\n" + dax_text;
}

static bool MetadataCacheDisabled() {
  auto *value = std::getenv("PBI_SCANNER_DISABLE_METADATA_CACHE");
  if (!value || !*value) {
    return false;
  }
  string normalized = value;
  StringUtil::Trim(normalized);
  normalized = StringUtil::Lower(normalized);
  return normalized == "1" || normalized == "true" || normalized == "yes" ||
         normalized == "on";
}

static int64_t ResolveCacheTtlSeconds(const char *name) {
  auto *value = std::getenv(name);
  if (!value || !*value) {
    return DEFAULT_METADATA_CACHE_TTL_SECONDS;
  }
  try {
    auto ttl = std::stoll(value);
    return ttl < 0 ? 0 : ttl;
  } catch (const std::exception &) {
    return DEFAULT_METADATA_CACHE_TTL_SECONDS;
  }
}

static bool CacheEntryExpired(int64_t created_at_seconds, int64_t ttl_seconds) {
  if (ttl_seconds == 0) {
    return true;
  }
  auto now = CurrentUnixSeconds();
  return created_at_seconds <= 0 || created_at_seconds > now ||
         now - created_at_seconds > ttl_seconds;
}

static bool CreateDirectoryIfMissing(const string &path) {
  if (path.empty()) {
    return false;
  }
#ifdef _WIN32
  if (_mkdir(path.c_str()) == 0 || errno == EEXIST) {
    return true;
  }
#else
  if (mkdir(path.c_str(), 0700) == 0 || errno == EEXIST) {
    return true;
  }
#endif
  return false;
}

static bool IsPathSeparator(char value) {
  return value == '/' || value == '\\';
}

static idx_t PathRootLength(const string &path) {
#ifdef _WIN32
  if (path.size() >= 2 && IsPathSeparator(path[0]) &&
      IsPathSeparator(path[1])) {
    auto server_end = path.find_first_of("/\\", 2);
    if (server_end == string::npos) {
      return path.size();
    }
    auto share_end = path.find_first_of("/\\", server_end + 1);
    if (share_end == string::npos) {
      return path.size();
    }
    return share_end + 1;
  }
  if (path.size() >= 2 && path[1] == ':') {
    if (path.size() >= 3 && IsPathSeparator(path[2])) {
      return 3;
    }
    return 2;
  }
#endif
  if (!path.empty() && IsPathSeparator(path[0])) {
    return 1;
  }
  return 0;
}

static string JoinPath(const string &left, const string &right) {
  if (left.empty()) {
    return right;
  }
  auto last = left[left.size() - 1];
  if (IsPathSeparator(last)) {
    return left + right;
  }
#ifdef _WIN32
  return left + "\\" + right;
#else
  return left + "/" + right;
#endif
}

static string DefaultMetadataCacheDirectory() {
  auto *configured = std::getenv("PBI_SCANNER_CACHE_DIR");
  if (configured && *configured) {
    return configured;
  }
#ifdef _WIN32
  auto *local_app_data = std::getenv("LOCALAPPDATA");
  if (local_app_data && *local_app_data) {
    return JoinPath(local_app_data, "pbi_scanner");
  }
  auto *app_data = std::getenv("APPDATA");
  if (app_data && *app_data) {
    return JoinPath(app_data, "pbi_scanner");
  }
#else
  auto *xdg_cache_home = std::getenv("XDG_CACHE_HOME");
  if (xdg_cache_home && *xdg_cache_home) {
    return JoinPath(xdg_cache_home, "pbi_scanner");
  }
  auto *home = std::getenv("HOME");
  if (home && *home) {
#ifdef __APPLE__
    return JoinPath(JoinPath(home, "Library"), "Caches/pbi_scanner");
#else
    return JoinPath(JoinPath(home, ".cache"), "pbi_scanner");
#endif
  }
#endif
  return string();
}

static string HexByte(uint8_t value) {
  constexpr const char *digits = "0123456789abcdef";
  string result;
  result.push_back(digits[value >> 4]);
  result.push_back(digits[value & 0x0F]);
  return result;
}

static string EscapeCacheField(const string &value) {
  string result;
  result.reserve(value.size());
  for (auto ch : value) {
    auto byte = static_cast<uint8_t>(ch);
    if (ch == '%' || ch == '\t' || ch == '\n' || ch == '\r' || byte < 0x20) {
      result.push_back('%');
      result += HexByte(byte);
    } else {
      result.push_back(ch);
    }
  }
  return result;
}

static string UnescapeCacheField(const string &value) {
  string result;
  result.reserve(value.size());
  for (idx_t i = 0; i < value.size(); i++) {
    if (value[i] != '%') {
      result.push_back(value[i]);
      continue;
    }
    if (i + 2 >= value.size()) {
      throw InvalidInputException("invalid cache escape sequence");
    }
    auto byte = static_cast<uint8_t>(
        (DecodeHexDigit(value[i + 1], "invalid cache escape sequence") << 4) |
        DecodeHexDigit(value[i + 2], "invalid cache escape sequence"));
    result.push_back(static_cast<char>(byte));
    i += 2;
  }
  return result;
}

static vector<string> SplitCacheLine(const string &line) {
  vector<string> result;
  idx_t start = 0;
  for (idx_t i = 0; i <= line.size(); i++) {
    if (i == line.size() || line[i] == '\t') {
      result.push_back(UnescapeCacheField(line.substr(start, i - start)));
      start = i + 1;
    }
  }
  return result;
}

static string CacheLine(std::initializer_list<string> fields) {
  string result;
  bool first = true;
  for (const auto &field : fields) {
    if (!first) {
      result.push_back('\t');
    }
    result += EscapeCacheField(field);
    first = false;
  }
  result.push_back('\n');
  return result;
}

static string HashCacheKey(const string &key) {
  uint64_t hash = 1469598103934665603ULL;
  for (auto ch : key) {
    hash ^= static_cast<uint8_t>(ch);
    hash *= 1099511628211ULL;
  }
  std::ostringstream stream;
  stream << std::hex << std::setfill('0') << std::setw(16) << hash;
  return stream.str();
}

static bool EnsureCacheDirectory(const string &cache_dir) {
  if (cache_dir.empty()) {
    return false;
  }
  auto root_length = PathRootLength(cache_dir);
  if (root_length >= cache_dir.size()) {
    return true;
  }
  for (idx_t i = root_length; i < cache_dir.size(); i++) {
    if (IsPathSeparator(cache_dir[i])) {
      auto current = cache_dir.substr(0, i);
      if (current.size() > root_length && !CreateDirectoryIfMissing(current)) {
        return false;
      }
    }
  }
  return CreateDirectoryIfMissing(cache_dir);
}

static string CacheFilePath(const string &prefix, const string &key) {
  auto cache_dir = DefaultMetadataCacheDirectory();
  if (!EnsureCacheDirectory(cache_dir)) {
    return string();
  }
  return JoinPath(cache_dir, prefix + "_" + HashCacheKey(key) + ".cache");
}

static string LogicalTypeCacheName(const LogicalType &type) {
  switch (type.id()) {
  case LogicalTypeId::BOOLEAN:
    return "BOOLEAN";
  case LogicalTypeId::BIGINT:
    return "BIGINT";
  case LogicalTypeId::UBIGINT:
    return "UBIGINT";
  case LogicalTypeId::DOUBLE:
    return "DOUBLE";
  case LogicalTypeId::DATE:
    return "DATE";
  case LogicalTypeId::TIME:
    return "TIME";
  case LogicalTypeId::TIMESTAMP:
    return "TIMESTAMP";
  case LogicalTypeId::TIMESTAMP_TZ:
    return "TIMESTAMP_TZ";
  case LogicalTypeId::HUGEINT:
    return "HUGEINT";
  default:
    return "VARCHAR";
  }
}

static LogicalType LogicalTypeFromCacheName(const string &value) {
  if (value == "BOOLEAN") {
    return LogicalType::BOOLEAN;
  }
  if (value == "BIGINT") {
    return LogicalType::BIGINT;
  }
  if (value == "UBIGINT") {
    return LogicalType::UBIGINT;
  }
  if (value == "DOUBLE") {
    return LogicalType::DOUBLE;
  }
  if (value == "DATE") {
    return LogicalType::DATE;
  }
  if (value == "TIME") {
    return LogicalType::TIME;
  }
  if (value == "TIMESTAMP") {
    return LogicalType::TIMESTAMP;
  }
  if (value == "TIMESTAMP_TZ") {
    return LogicalType::TIMESTAMP_TZ;
  }
  if (value == "HUGEINT") {
    return LogicalType::HUGEINT;
  }
  return LogicalType::VARCHAR;
}

static string CoercionKindCacheName(XmlaCoercionKind kind) {
  return std::to_string(static_cast<uint8_t>(kind));
}

static XmlaCoercionKind CoercionKindFromCacheName(const string &value) {
  auto parsed = std::stoul(value);
  if (parsed > static_cast<uint8_t>(XmlaCoercionKind::TIMESTAMP_TZ)) {
    return XmlaCoercionKind::INFER;
  }
  return static_cast<XmlaCoercionKind>(parsed);
}

static bool TryReadPersistentTarget(const string &key,
                                    PowerBIResolvedTarget &target) {
  if (MetadataCacheDisabled()) {
    return false;
  }
  auto path = CacheFilePath("target", key);
  if (path.empty()) {
    return false;
  }
  try {
    std::ifstream input(path);
    if (!input.good()) {
      return false;
    }
    string line;
    if (!std::getline(input, line) || line != TARGET_CACHE_VERSION) {
      return false;
    }
    if (!std::getline(input, line)) {
      return false;
    }
    auto header = SplitCacheLine(line);
    if (header.size() != 3 || header[0] != CACHE_FORMAT_VERSION) {
      return false;
    }
    auto created_at = std::stoll(header[1]);
    if (CacheEntryExpired(
            created_at,
            ResolveCacheTtlSeconds("PBI_SCANNER_TARGET_CACHE_TTL_SECONDS"))) {
      return false;
    }
    PowerBIResolvedTarget cached;
    if (!std::getline(input, line)) {
      return false;
    }
    auto fields = SplitCacheLine(line);
    if (fields.size() != 11) {
      return false;
    }
    cached.workspace_name = fields[0];
    cached.workspace_id = fields[1];
    cached.workspace_type = fields[2];
    cached.capacity_object_id = fields[3];
    cached.capacity_uri = fields[4];
    cached.dataset_name = fields[5];
    cached.dataset_id = fields[6];
    cached.internal_catalog = fields[7];
    cached.aixl_url = fields[8];
    cached.fixed_cluster_uri = fields[9];
    cached.core_server_name = fields[10];
    if (cached.internal_catalog.empty() || cached.aixl_url.empty()) {
      return false;
    }
    target = std::move(cached);
    if (DebugTimingsEnabled()) {
      std::fprintf(stderr, "[pbi_scanner] metadata cache target hit\n");
    }
    return true;
  } catch (const Exception &) {
    return false;
  } catch (const std::exception &) {
    return false;
  }
}

static void StorePersistentTarget(const string &key,
                                  const PowerBIResolvedTarget &target) {
  if (MetadataCacheDisabled()) {
    return;
  }
  auto path = CacheFilePath("target", key);
  if (path.empty()) {
    return;
  }
  try {
    std::ofstream output(path, std::ios::trunc);
    if (!output.good()) {
      return;
    }
    output << TARGET_CACHE_VERSION << "\n";
    output << CacheLine(
        {CACHE_FORMAT_VERSION, std::to_string(CurrentUnixSeconds()), key});
    output << CacheLine(
        {target.workspace_name, target.workspace_id, target.workspace_type,
         target.capacity_object_id, target.capacity_uri, target.dataset_name,
         target.dataset_id, target.internal_catalog, target.aixl_url,
         target.fixed_cluster_uri, target.core_server_name});
  } catch (const Exception &ex) {
    if (DebugTimingsEnabled()) {
      std::fprintf(stderr,
                   "[pbi_scanner] metadata cache target store failed: %s\n",
                   ex.what());
    }
  } catch (const std::exception &ex) {
    if (DebugTimingsEnabled()) {
      std::fprintf(stderr,
                   "[pbi_scanner] metadata cache target store failed: %s\n",
                   ex.what());
    }
  }
}

static bool TryReadPersistentSchema(const string &key,
                                    std::vector<XmlaColumn> &columns) {
  if (MetadataCacheDisabled()) {
    return false;
  }
  auto path = CacheFilePath("schema", key);
  if (path.empty()) {
    return false;
  }
  try {
    std::ifstream input(path);
    if (!input.good()) {
      return false;
    }
    string line;
    if (!std::getline(input, line) || line != SCHEMA_CACHE_VERSION) {
      return false;
    }
    if (!std::getline(input, line)) {
      return false;
    }
    auto header = SplitCacheLine(line);
    if (header.size() != 3 || header[0] != CACHE_FORMAT_VERSION) {
      return false;
    }
    auto created_at = std::stoll(header[1]);
    if (CacheEntryExpired(
            created_at,
            ResolveCacheTtlSeconds("PBI_SCANNER_SCHEMA_CACHE_TTL_SECONDS"))) {
      return false;
    }
    if (!std::getline(input, line)) {
      return false;
    }
    auto count_fields = SplitCacheLine(line);
    if (count_fields.size() != 1) {
      return false;
    }
    auto count = static_cast<idx_t>(std::stoull(count_fields[0]));
    std::vector<XmlaColumn> cached;
    cached.reserve(count);
    for (idx_t i = 0; i < count; i++) {
      if (!std::getline(input, line)) {
        return false;
      }
      auto fields = SplitCacheLine(line);
      if (fields.size() != 7) {
        return false;
      }
      XmlaColumn column;
      column.name = fields[0];
      column.source_type = fields[1];
      column.duckdb_type = LogicalTypeFromCacheName(fields[2]);
      column.coercion_kind = CoercionKindFromCacheName(fields[3]);
      column.nullable = fields[4] == "1";
      column.nullable_known = fields[5] == "1";
      if (fields[6] != std::to_string(i) || column.name.empty()) {
        return false;
      }
      cached.push_back(std::move(column));
    }
    columns = std::move(cached);
    if (DebugTimingsEnabled()) {
      std::fprintf(stderr, "[pbi_scanner] metadata cache schema hit\n");
    }
    return true;
  } catch (const Exception &) {
    return false;
  } catch (const std::exception &) {
    return false;
  }
}

static void StorePersistentSchema(const string &key,
                                  const std::vector<XmlaColumn> &columns) {
  if (MetadataCacheDisabled() || columns.empty()) {
    return;
  }
  auto path = CacheFilePath("schema", key);
  if (path.empty()) {
    return;
  }
  try {
    std::ofstream output(path, std::ios::trunc);
    if (!output.good()) {
      return;
    }
    output << SCHEMA_CACHE_VERSION << "\n";
    output << CacheLine(
        {CACHE_FORMAT_VERSION, std::to_string(CurrentUnixSeconds()), key});
    output << CacheLine({std::to_string(columns.size())});
    for (idx_t i = 0; i < columns.size(); i++) {
      const auto &column = columns[i];
      output << CacheLine({column.name, column.source_type,
                           LogicalTypeCacheName(column.duckdb_type),
                           CoercionKindCacheName(column.coercion_kind),
                           column.nullable ? "1" : "0",
                           column.nullable_known ? "1" : "0",
                           std::to_string(i)});
    }
  } catch (const Exception &ex) {
    if (DebugTimingsEnabled()) {
      std::fprintf(stderr,
                   "[pbi_scanner] metadata cache schema store failed: %s\n",
                   ex.what());
    }
  } catch (const std::exception &ex) {
    if (DebugTimingsEnabled()) {
      std::fprintf(stderr,
                   "[pbi_scanner] metadata cache schema store failed: %s\n",
                   ex.what());
    }
  }
}

static void RemovePersistentCacheFile(const string &prefix, const string &key) {
  auto path = CacheFilePath(prefix, key);
  if (path.empty()) {
    return;
  }
  std::remove(path.c_str());
}

} // namespace

bool TryGetCachedTarget(const PowerBIConnectionConfig &config,
                        PowerBIResolvedTarget &target) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = TargetCacheKey(config);
  auto entry = resolved_target_cache.find(key);
  if (entry == resolved_target_cache.end()) {
    if (!TryReadPersistentTarget(key, target)) {
      return false;
    }
    resolved_target_cache[key] = target;
    return true;
  }
  target = entry->second;
  if (DebugTimingsEnabled()) {
    std::fprintf(stderr, "[pbi_scanner] metadata cache target memory hit\n");
  }
  return true;
}

void StoreCachedTarget(const PowerBIConnectionConfig &config,
                       const PowerBIResolvedTarget &target) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = TargetCacheKey(config);
  resolved_target_cache[key] = target;
  StorePersistentTarget(key, target);
}

void InvalidateCachedTarget(const PowerBIConnectionConfig &config) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = TargetCacheKey(config);
  resolved_target_cache.erase(key);
  RemovePersistentCacheFile("target", key);
}

bool TryGetCachedSchema(const PowerBIResolvedTarget &target,
                        const string &dax_text,
                        const string &effective_user_name,
                        std::vector<XmlaColumn> &columns) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = SchemaCacheKey(target, dax_text, effective_user_name);
  auto entry = schema_cache.find(key);
  if (entry == schema_cache.end()) {
    if (!TryReadPersistentSchema(key, columns)) {
      return false;
    }
    schema_cache[key] = columns;
    return true;
  }
  columns = entry->second;
  if (DebugTimingsEnabled()) {
    std::fprintf(stderr, "[pbi_scanner] metadata cache schema memory hit\n");
  }
  return true;
}

void StoreCachedSchema(const PowerBIResolvedTarget &target,
                       const string &dax_text,
                       const string &effective_user_name,
                       const std::vector<XmlaColumn> &columns) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = SchemaCacheKey(target, dax_text, effective_user_name);
  schema_cache[key] = columns;
  StorePersistentSchema(key, columns);
}

void InvalidateCachedSchema(const PowerBIResolvedTarget &target,
                            const string &dax_text,
                            const string &effective_user_name) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = SchemaCacheKey(target, dax_text, effective_user_name);
  schema_cache.erase(key);
  RemovePersistentCacheFile("schema", key);
}

bool TestMetadataCacheRoundTrip() {
  auto escaped = CacheLine({"ExampleTable[Example Key]", "xsd:long", "BIGINT",
                            "3", "1", "1", "contains\ttab\nnewline%percent"});
  if (!escaped.empty() && escaped.back() == '\n') {
    escaped.pop_back();
  }
  auto fields = SplitCacheLine(escaped);
  if (fields.size() != 7 || fields[0] != "ExampleTable[Example Key]" ||
      fields[6] != "contains\ttab\nnewline%percent") {
    return false;
  }
  auto type = LogicalTypeFromCacheName(fields[2]);
  auto coercion = CoercionKindFromCacheName(fields[3]);
  if (type.id() != LogicalTypeId::BIGINT ||
      coercion != XmlaCoercionKind::BIGINT) {
    return false;
  }
  auto now = CurrentUnixSeconds();
  if (CacheEntryExpired(now, DEFAULT_METADATA_CACHE_TTL_SECONDS)) {
    return false;
  }
  if (!CacheEntryExpired(now - 2, 1)) {
    return false;
  }
#ifdef _WIN32
  if (PathRootLength("C:\\Users\\pbi_scanner") != 3) {
    return false;
  }
  if (PathRootLength("\\\\server\\share\\pbi_scanner") != 15) {
    return false;
  }
#else
  if (PathRootLength("/tmp/pbi_scanner") != 1) {
    return false;
  }
#endif
  return true;
}

} // namespace duckdb
