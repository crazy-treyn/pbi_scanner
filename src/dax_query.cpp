#include "dax_query.hpp"

#include "auth.hpp"
#include "connection_string.hpp"
#include "pbi_scanner_util.hpp"
#include "powerbi_resolver.hpp"
#include "xmla.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <atomic>
#include <cctype>
#include <cerrno>
#include <condition_variable>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <string>
#include <deque>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
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
    "pbi_scanner_target_cache_v1";
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

static bool
HasNonNullNamedParameter(const named_parameter_map_t &named_parameters,
                         const string &name) {
  auto entry = named_parameters.find(name);
  return entry != named_parameters.end() && !entry->second.IsNull();
}

static string
GetOptionalNamedParameter(const named_parameter_map_t &named_parameters,
                          const string &name) {
  auto entry = named_parameters.find(name);
  if (entry == named_parameters.end() || entry->second.IsNull()) {
    return string();
  }
  return Trimmed(entry->second.GetValue<string>());
}

static int64_t CurrentUnixSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
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

static string JoinPath(const string &left, const string &right) {
  if (left.empty()) {
    return right;
  }
  auto last = left[left.size() - 1];
  if (last == '/' || last == '\\') {
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
  string current;
  for (idx_t i = 0; i < cache_dir.size(); i++) {
    current.push_back(cache_dir[i]);
    if (cache_dir[i] == '/' || cache_dir[i] == '\\') {
      if (current.size() > 1 && !CreateDirectoryIfMissing(current)) {
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
    if (fields.size() != 9) {
      return false;
    }
    cached.workspace_name = fields[0];
    cached.workspace_id = fields[1];
    cached.workspace_type = fields[2];
    cached.capacity_object_id = fields[3];
    cached.dataset_name = fields[4];
    cached.dataset_id = fields[5];
    cached.internal_catalog = fields[6];
    cached.aixl_url = fields[7];
    cached.fixed_cluster_uri = fields[8];
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
         target.capacity_object_id, target.dataset_name, target.dataset_id,
         target.internal_catalog, target.aixl_url, target.fixed_cluster_uri});
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

static bool TryGetCachedTarget(const PowerBIConnectionConfig &config,
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

static void StoreCachedTarget(const PowerBIConnectionConfig &config,
                              const PowerBIResolvedTarget &target) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = TargetCacheKey(config);
  resolved_target_cache[key] = target;
  StorePersistentTarget(key, target);
}

static void InvalidateCachedTarget(const PowerBIConnectionConfig &config) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = TargetCacheKey(config);
  resolved_target_cache.erase(key);
  RemovePersistentCacheFile("target", key);
}

static bool TryGetCachedSchema(const PowerBIResolvedTarget &target,
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

static void StoreCachedSchema(const PowerBIResolvedTarget &target,
                              const string &dax_text,
                              const string &effective_user_name,
                              const std::vector<XmlaColumn> &columns) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = SchemaCacheKey(target, dax_text, effective_user_name);
  schema_cache[key] = columns;
  StorePersistentSchema(key, columns);
}

static void InvalidateCachedSchema(const PowerBIResolvedTarget &target,
                                   const string &dax_text,
                                   const string &effective_user_name) {
  std::lock_guard<std::mutex> guard(query_cache_lock);
  auto key = SchemaCacheKey(target, dax_text, effective_user_name);
  schema_cache.erase(key);
  RemovePersistentCacheFile("schema", key);
}

} // namespace

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
  return true;
}

struct DaxQueryBindData : public TableFunctionData {
  DaxQueryBindData(PowerBIConnectionConfig config_p,
                   PowerBIResolvedTarget target_p,
                   std::vector<XmlaColumn> columns_p, string dax_text_p,
                   string access_token_p, string effective_user_name_p,
                   int64_t timeout_ms_p,
                   std::shared_ptr<HttpClient> xmla_http_client_p = nullptr)
      : config(std::move(config_p)), target(std::move(target_p)),
        columns(std::move(columns_p)), dax_text(std::move(dax_text_p)),
        access_token(std::move(access_token_p)),
        effective_user_name(std::move(effective_user_name_p)),
        timeout_ms(timeout_ms_p),
        xmla_http_client(std::move(xmla_http_client_p)) {}

  unique_ptr<FunctionData> Copy() const override {
    return make_uniq<DaxQueryBindData>(*this);
  }

  bool Equals(const FunctionData &other_p) const override {
    auto &other = other_p.Cast<DaxQueryBindData>();
    return config.raw == other.config.raw &&
           target.aixl_url == other.target.aixl_url &&
           dax_text == other.dax_text && access_token == other.access_token &&
           effective_user_name == other.effective_user_name &&
           timeout_ms == other.timeout_ms;
  }

  PowerBIConnectionConfig config;
  PowerBIResolvedTarget target;
  std::vector<XmlaColumn> columns;
  string dax_text;
  string access_token;
  string effective_user_name;
  int64_t timeout_ms;
  //! Populated when ProbeSchema ran; reused for execute on same TCP connection.
  std::shared_ptr<HttpClient> xmla_http_client;
};

struct DaxQueryGlobalState : public GlobalTableFunctionState {
  explicit DaxQueryGlobalState(const DaxQueryBindData &bind_data)
      : columns(bind_data.columns), bind_config(bind_data.config),
        bind_target(bind_data.target) {
    for (const auto &column : columns) {
      column_types.push_back(column.duckdb_type);
    }
    request.url = bind_data.target.aixl_url;
    request.catalog = bind_data.target.internal_catalog;
    request.access_token = bind_data.access_token;
    request.statement = bind_data.dax_text;
    request.effective_user_name = bind_data.effective_user_name;
    request.timeout_ms = bind_data.timeout_ms;
    if (bind_data.xmla_http_client) {
      executor = make_uniq<XmlaExecutor>(bind_data.timeout_ms,
                                         bind_data.xmla_http_client);
    } else {
      executor = make_uniq<XmlaExecutor>(bind_data.timeout_ms);
    }
    worker = std::thread([this]() { RunWorker(); });
  }

  ~DaxQueryGlobalState() override {
    stop_requested.store(true, std::memory_order_release);
    condition.notify_all();
    if (executor && !finished) {
      executor->Stop();
    }
    if (worker.joinable()) {
      worker.join();
    }
  }

  idx_t MaxThreads() const override { return 1; }

  bool PopChunk(unique_ptr<DataChunk> &chunk, ClientContext &context) {
    std::unique_lock<std::mutex> guard(lock);
    condition.wait(guard, [&]() {
      return stop_requested.load(std::memory_order_acquire) ||
             !chunks.empty() || finished || !error.empty();
    });
    if (context.IsInterrupted()) {
      stop_requested.store(true, std::memory_order_release);
      guard.unlock();
      if (executor) {
        executor->Stop();
      }
      throw InterruptException();
    }
    if (!error.empty()) {
      throw IOException("%s", error);
    }
    if (chunks.empty()) {
      return false;
    }
    chunk = std::move(chunks.front());
    chunks.pop_front();
    guard.unlock();
    condition.notify_all();
    return true;
  }

  std::vector<XmlaColumn> columns;
  vector<LogicalType> column_types;
  XmlaRequest request;
  std::unique_ptr<XmlaExecutor> executor;
  PowerBIConnectionConfig bind_config;
  PowerBIResolvedTarget bind_target;
  std::mutex lock;
  std::condition_variable condition;
  std::deque<unique_ptr<DataChunk>> chunks;
  std::thread worker;
  string error;
  bool finished = false;
  std::atomic<bool> stop_requested{false};
  idx_t max_buffered_chunks = 32;
  idx_t produced_rows = 0;
  std::chrono::steady_clock::time_point init_started_at =
      std::chrono::steady_clock::now();
  bool execute_started = false;

private:
  unique_ptr<DataChunk> CreateChunk() const {
    auto chunk = make_uniq<DataChunk>();
    chunk->Initialize(Allocator::DefaultAllocator(), column_types,
                      STANDARD_VECTOR_SIZE);
    return chunk;
  }

  bool EnqueueChunk(unique_ptr<DataChunk> chunk) {
    if (!chunk || chunk->size() == 0) {
      return true;
    }

    std::unique_lock<std::mutex> guard(lock);
    condition.wait(guard, [&]() {
      return stop_requested.load(std::memory_order_acquire) ||
             chunks.size() < max_buffered_chunks;
    });
    if (stop_requested.load(std::memory_order_acquire)) {
      return false;
    }
    chunks.push_back(std::move(chunk));
    guard.unlock();
    condition.notify_all();
    return true;
  }

  void RunWorker() {
    auto worker_start = std::chrono::steady_clock::now();
    bool first_row_logged = false;
    auto current_chunk = CreateChunk();
    idx_t current_chunk_size = 0;
    try {
      executor->ExecuteStreaming(
          request, nullptr,
          [&](const std::vector<Value> &row) {
            if (DebugTimingsEnabled() && !first_row_logged) {
              DebugTiming("ExecuteStreaming first row", worker_start);
              first_row_logged = true;
            }
            if (current_chunk_size >= STANDARD_VECTOR_SIZE) {
              current_chunk->SetCardinality(current_chunk_size);
              if (!EnqueueChunk(std::move(current_chunk))) {
                return false;
              }
              current_chunk = CreateChunk();
              current_chunk_size = 0;
            }

            for (idx_t column_idx = 0; column_idx < row.size(); column_idx++) {
              current_chunk->SetValue(column_idx, current_chunk_size,
                                      row[column_idx]);
            }
            current_chunk_size++;
            produced_rows++;
            return true;
          },
          [&]() { return stop_requested.load(std::memory_order_relaxed); });

      if (current_chunk_size > 0) {
        current_chunk->SetCardinality(current_chunk_size);
        if (!EnqueueChunk(std::move(current_chunk))) {
          return;
        }
      }
    } catch (const Exception &ex) {
      InvalidateCachedSchema(bind_target, request.statement,
                             request.effective_user_name);
      if (!bind_config.is_direct_xmla) {
        InvalidateCachedTarget(bind_config);
      }
      std::lock_guard<std::mutex> guard(lock);
      error = ex.what();
    } catch (const std::exception &ex) {
      InvalidateCachedSchema(bind_target, request.statement,
                             request.effective_user_name);
      if (!bind_config.is_direct_xmla) {
        InvalidateCachedTarget(bind_config);
      }
      std::lock_guard<std::mutex> guard(lock);
      error = ex.what();
    }
    if (DebugTimingsEnabled()) {
      DebugTiming("ExecuteStreaming total", worker_start);
      std::fprintf(stderr, "[pbi_scanner] dax_query rows: %llu\n",
                   static_cast<unsigned long long>(produced_rows));
    }
    {
      std::lock_guard<std::mutex> guard(lock);
      finished = true;
    }
    condition.notify_all();
  }
};

static string
ResolveEffectiveUserName(const PowerBIConnectionConfig &config,
                         const named_parameter_map_t &named_parameters) {
  auto value =
      GetOptionalNamedParameter(named_parameters, "effective_user_name");
  if (!value.empty()) {
    return value;
  }
  return Trimmed(config.effective_user_name);
}

static int64_t ResolveTimeoutMs(const PowerBIConnectionConfig &config,
                                const named_parameter_map_t &named_parameters) {
  if (HasNonNullNamedParameter(named_parameters, "timeout_ms")) {
    auto entry = named_parameters.find("timeout_ms");
    auto timeout_ms = entry->second.GetValue<int64_t>();
    if (timeout_ms <= 0) {
      throw InvalidInputException("timeout_ms must be greater than zero");
    }
    return timeout_ms;
  }
  if (config.has_timeout_ms) {
    return config.timeout_ms;
  }
  return 300000;
}

static void RegisterCommonDaxNamedParameters(TableFunction &function) {
  function.named_parameters["auth_mode"] = LogicalType::VARCHAR;
  function.named_parameters["access_token"] = LogicalType::VARCHAR;
  function.named_parameters["secret_name"] = LogicalType::VARCHAR;
  function.named_parameters["tenant_id"] = LogicalType::VARCHAR;
  function.named_parameters["client_id"] = LogicalType::VARCHAR;
  function.named_parameters["client_secret"] = LogicalType::VARCHAR;
  function.named_parameters["effective_user_name"] = LogicalType::VARCHAR;
  function.named_parameters["timeout_ms"] = LogicalType::BIGINT;
}

static unique_ptr<FunctionData> DaxQueryBind(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names) {
  auto connection_string = input.inputs[0].GetValue<string>();
  auto dax_text = input.table_function.extra_info;
  if (dax_text.empty()) {
    dax_text = input.inputs[1].GetValue<string>();
  }

  auto config = ParsePowerBIConnectionString(connection_string);
  auto trimmed_dax = dax_text;
  StringUtil::Trim(trimmed_dax);
  if (trimmed_dax.empty()) {
    throw InvalidInputException("dax_text is required");
  }

  auto bind_start = std::chrono::steady_clock::now();
  auto access_token_start = std::chrono::steady_clock::now();
  auto access_token =
      ResolvePowerBIAccessToken(context, config, input.named_parameters);
  DebugTiming("ResolvePowerBIAccessToken", access_token_start);
  auto effective_user_name =
      ResolveEffectiveUserName(config, input.named_parameters);
  auto timeout_ms = ResolveTimeoutMs(config, input.named_parameters);
  PowerBIResolvedTarget target;
  bool target_from_cache = false;
  if (config.is_direct_xmla) {
    target.aixl_url = config.data_source;
    target.internal_catalog = config.initial_catalog;
  } else {
    target_from_cache = TryGetCachedTarget(config, target);
    if (!target_from_cache) {
      auto resolver_start = std::chrono::steady_clock::now();
      target = ResolvePowerBITarget(config.endpoint, config.initial_catalog,
                                    access_token, timeout_ms);
      DebugTiming("ResolvePowerBITarget", resolver_start);
      StoreCachedTarget(config, target);
    }
  }

  std::shared_ptr<HttpClient> xmla_http_client;
  std::vector<XmlaColumn> columns;
  if (!TryGetCachedSchema(target, dax_text, effective_user_name, columns)) {
    auto probe_schema = [&]() {
      xmla_http_client = std::make_shared<HttpClient>(timeout_ms);
      XmlaExecutor executor(timeout_ms, xmla_http_client);
      XmlaRequest request;
      request.url = target.aixl_url;
      request.catalog = target.internal_catalog;
      request.access_token = access_token;
      request.statement = dax_text;
      request.effective_user_name = effective_user_name;
      request.timeout_ms = timeout_ms;
      auto probe_start = std::chrono::steady_clock::now();
      auto probed_columns = executor.ProbeSchema(request);
      DebugTiming("ProbeSchema", probe_start);
      return probed_columns;
    };
    try {
      columns = probe_schema();
    } catch (const Exception &) {
      if (config.is_direct_xmla || !target_from_cache) {
        throw;
      }
      InvalidateCachedTarget(config);
      target_from_cache = false;
      auto resolver_start = std::chrono::steady_clock::now();
      target = ResolvePowerBITarget(config.endpoint, config.initial_catalog,
                                    access_token, timeout_ms);
      DebugTiming("ResolvePowerBITarget retry", resolver_start);
      StoreCachedTarget(config, target);
      columns = probe_schema();
    } catch (const std::exception &) {
      if (config.is_direct_xmla || !target_from_cache) {
        throw;
      }
      InvalidateCachedTarget(config);
      target_from_cache = false;
      auto resolver_start = std::chrono::steady_clock::now();
      target = ResolvePowerBITarget(config.endpoint, config.initial_catalog,
                                    access_token, timeout_ms);
      DebugTiming("ResolvePowerBITarget retry", resolver_start);
      StoreCachedTarget(config, target);
      columns = probe_schema();
    }
    StoreCachedSchema(target, dax_text, effective_user_name, columns);
  }
  DebugTiming("DaxQueryBind total", bind_start);
  if (columns.empty()) {
    throw IOException("DAX query returned no columns");
  }

  for (const auto &column : columns) {
    names.push_back(column.name);
    return_types.push_back(column.duckdb_type);
  }

  return make_uniq<DaxQueryBindData>(
      std::move(config), std::move(target), std::move(columns),
      std::move(dax_text), std::move(access_token),
      std::move(effective_user_name), timeout_ms, std::move(xmla_http_client));
}

static unique_ptr<GlobalTableFunctionState>
DaxQueryInit(ClientContext &, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<DaxQueryBindData>();
  return make_uniq<DaxQueryGlobalState>(bind_data);
}

static void DaxQueryExecute(ClientContext &context, TableFunctionInput &input,
                            DataChunk &output) {
  auto &state = input.global_state->Cast<DaxQueryGlobalState>();

  if (DebugTimingsEnabled() && !state.execute_started) {
    state.execute_started = true;
    DebugTiming("DaxQueryExecute first call", state.init_started_at);
  }

  unique_ptr<DataChunk> chunk;
  if (state.PopChunk(chunk, context)) {
    output.Move(*chunk);
  } else {
    output.SetCardinality(0);
  }
}

TableFunction CreateDaxQueryFunction() {
  TableFunction function("dax_query",
                         {LogicalType::VARCHAR, LogicalType::VARCHAR},
                         DaxQueryExecute, DaxQueryBind, DaxQueryInit);
  RegisterCommonDaxNamedParameters(function);
  return function;
}

static TableFunction CreateFixedDaxFunction(const string &name,
                                            const string &dax_text) {
  TableFunction function(name, {LogicalType::VARCHAR}, DaxQueryExecute,
                         DaxQueryBind, DaxQueryInit);
  function.extra_info = dax_text;
  RegisterCommonDaxNamedParameters(function);
  return function;
}

TableFunction CreatePbiTablesFunction() {
  return CreateFixedDaxFunction("pbi_tables", "EVALUATE INFO.VIEW.TABLES()");
}

TableFunction CreatePbiColumnsFunction() {
  return CreateFixedDaxFunction("pbi_columns", "EVALUATE INFO.VIEW.COLUMNS()");
}

TableFunction CreatePbiMeasuresFunction() {
  return CreateFixedDaxFunction("pbi_measures",
                                "EVALUATE INFO.VIEW.MEASURES()");
}

TableFunction CreatePbiRelationshipsFunction() {
  return CreateFixedDaxFunction("pbi_relationships",
                                "EVALUATE INFO.VIEW.RELATIONSHIPS()");
}

} // namespace duckdb
