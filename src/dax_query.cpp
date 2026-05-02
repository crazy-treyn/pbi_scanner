#include "dax_query.hpp"

#include "auth.hpp"
#include "connection_string.hpp"
#include "pbi_scanner_util.hpp"
#include "powerbi_resolver.hpp"
#include "xmla.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#define PBI_SCANNER_HAS_FLAT_VECTOR_GET_DATA_MUTABLE 1
#else
#define PBI_SCANNER_HAS_FLAT_VECTOR_GET_DATA_MUTABLE 0
#endif
#include "duckdb/main/client_context.hpp"

#include <atomic>
#include <cctype>
#include <cerrno>
#include <condition_variable>
#include <chrono>
#include <cmath>
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
    "pbi_scanner_target_cache_v3";
static constexpr const char *SCHEMA_CACHE_VERSION =
    "pbi_scanner_schema_cache_v1";
static constexpr const char *CACHE_FORMAT_VERSION = "v1";
static constexpr int64_t DEFAULT_SCHEMA_PROBE_ROWS = 100;

static bool IsDaxIdentifierChar(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

static bool KeywordAt(const string &statement, idx_t position,
                      const string &keyword) {
  if (position + keyword.size() > statement.size()) {
    return false;
  }
  for (idx_t i = 0; i < keyword.size(); i++) {
    if (std::toupper(static_cast<unsigned char>(statement[position + i])) !=
        std::toupper(static_cast<unsigned char>(keyword[i]))) {
      return false;
    }
  }
  if (position > 0 && IsDaxIdentifierChar(statement[position - 1])) {
    return false;
  }
  auto end = position + keyword.size();
  return end >= statement.size() || !IsDaxIdentifierChar(statement[end]);
}

static idx_t FindDaxKeywordOutsideLiterals(const string &statement,
                                           const string &keyword,
                                           idx_t start_position,
                                           bool top_level_only) {
  bool in_string = false;
  bool in_double_string = false;
  bool in_bracket_identifier = false;
  idx_t paren_depth = 0;
  for (idx_t i = start_position; i < statement.size(); i++) {
    auto ch = statement[i];
    if (in_string) {
      if (ch == '\'' && i + 1 < statement.size() && statement[i + 1] == '\'') {
        i++;
      } else if (ch == '\'') {
        in_string = false;
      }
      continue;
    }
    if (in_double_string) {
      if (ch == '"' && i + 1 < statement.size() && statement[i + 1] == '"') {
        i++;
      } else if (ch == '"') {
        in_double_string = false;
      }
      continue;
    }
    if (in_bracket_identifier) {
      if (ch == ']') {
        in_bracket_identifier = false;
      }
      continue;
    }
    if (ch == '\'') {
      in_string = true;
      continue;
    }
    if (ch == '"') {
      in_double_string = true;
      continue;
    }
    if (ch == '[') {
      in_bracket_identifier = true;
      continue;
    }
    if (ch == '/' && i + 1 < statement.size() && statement[i + 1] == '/') {
      while (i < statement.size() && statement[i] != '\n') {
        i++;
      }
      continue;
    }
    if (ch == '-' && i + 1 < statement.size() && statement[i + 1] == '-') {
      while (i < statement.size() && statement[i] != '\n') {
        i++;
      }
      continue;
    }
    if (ch == '/' && i + 1 < statement.size() && statement[i + 1] == '*') {
      i += 2;
      while (i + 1 < statement.size() &&
             !(statement[i] == '*' && statement[i + 1] == '/')) {
        i++;
      }
      if (i + 1 < statement.size()) {
        i++;
      }
      continue;
    }
    if (ch == '(') {
      paren_depth++;
      continue;
    }
    if (ch == ')' && paren_depth > 0) {
      paren_depth--;
      continue;
    }
    if ((!top_level_only || paren_depth == 0) &&
        KeywordAt(statement, i, keyword)) {
      return i;
    }
  }
  return DConstants::INVALID_INDEX;
}

static string BuildLimitedDaxSchemaProbe(const string &statement,
                                         int64_t row_limit) {
  if (row_limit <= 0) {
    return statement;
  }
  auto evaluate_pos =
      FindDaxKeywordOutsideLiterals(statement, "EVALUATE", 0, false);
  if (evaluate_pos == DConstants::INVALID_INDEX) {
    return statement;
  }
  auto expression_start = evaluate_pos + string("EVALUATE").size();
  auto second_evaluate = FindDaxKeywordOutsideLiterals(statement, "EVALUATE",
                                                       expression_start, false);
  if (second_evaluate != DConstants::INVALID_INDEX) {
    return statement;
  }

  auto order_by_pos = FindDaxKeywordOutsideLiterals(statement, "ORDER BY",
                                                    expression_start, true);
  auto start_at_pos = FindDaxKeywordOutsideLiterals(statement, "START AT",
                                                    expression_start, true);
  auto expression_end = statement.size();
  if (order_by_pos != DConstants::INVALID_INDEX) {
    expression_end = MinValue<idx_t>(expression_end, order_by_pos);
  }
  if (start_at_pos != DConstants::INVALID_INDEX) {
    expression_end = MinValue<idx_t>(expression_end, start_at_pos);
  }

  auto table_expression =
      statement.substr(expression_start, expression_end - expression_start);
  StringUtil::Trim(table_expression);
  if (table_expression.empty() || KeywordAt(table_expression, 0, "VAR")) {
    return statement;
  }

  auto prefix = statement.substr(0, evaluate_pos);
  string probe;
  probe.reserve(statement.size() + 32);
  probe += prefix;
  if (!probe.empty() && probe.back() != '\n' && probe.back() != '\r') {
    probe += "\n";
  }
  probe += "EVALUATE TOPN(";
  probe += std::to_string(row_limit);
  probe += ", ";
  probe += table_expression;
  probe += ")";
  return probe;
}

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

template <class T> static T *GetFlatVectorDataMutable(Vector &vector) {
#if PBI_SCANNER_HAS_FLAT_VECTOR_GET_DATA_MUTABLE
  return FlatVector::GetDataMutable<T>(vector);
#else
  return FlatVector::GetData<T>(vector);
#endif
}

static bool TrySetFlatVectorValue(Vector &vector, idx_t row_idx,
                                  const Value &value,
                                  const LogicalType &target_type) {
  auto try_convert_double_to_date = [](double serial_value,
                                       date_t &out_date) -> bool {
    if (!std::isfinite(serial_value)) {
      return false;
    }
    static constexpr int64_t MICROS_PER_DAY = 24LL * 60LL * 60LL * 1000000LL;
    static const int32_t OLE_BASE_EPOCH_DAYS =
        Date::EpochDays(Date::FromString("1899-12-30", true));
    auto total_micros = static_cast<int64_t>(
        serial_value * static_cast<double>(MICROS_PER_DAY) +
        (serial_value >= 0 ? 0.5 : -0.5));
    auto day_delta = total_micros / MICROS_PER_DAY;
    auto micros_in_day = total_micros % MICROS_PER_DAY;
    if (micros_in_day < 0) {
      micros_in_day += MICROS_PER_DAY;
      day_delta--;
    }
    auto date_days = static_cast<int64_t>(OLE_BASE_EPOCH_DAYS) + day_delta;
    if (date_days < NumericLimits<int32_t>::Minimum() ||
        date_days > NumericLimits<int32_t>::Maximum()) {
      return false;
    }
    out_date = Date::EpochDaysToDate(static_cast<int32_t>(date_days));
    return true;
  };

  if (value.IsNull()) {
    FlatVector::SetNull(vector, row_idx, true);
    return true;
  }

  auto value_type = value.type().id();
  switch (target_type.id()) {
  case LogicalTypeId::BOOLEAN:
    if (value_type == LogicalTypeId::BOOLEAN) {
      GetFlatVectorDataMutable<bool>(vector)[row_idx] =
          value.GetValueUnsafe<bool>();
      return true;
    }
    break;
  case LogicalTypeId::BIGINT:
    if (value_type == LogicalTypeId::BIGINT) {
      GetFlatVectorDataMutable<int64_t>(vector)[row_idx] =
          value.GetValueUnsafe<int64_t>();
      return true;
    }
    break;
  case LogicalTypeId::UBIGINT:
    if (value_type == LogicalTypeId::UBIGINT) {
      GetFlatVectorDataMutable<uint64_t>(vector)[row_idx] =
          value.GetValueUnsafe<uint64_t>();
      return true;
    }
    break;
  case LogicalTypeId::DOUBLE:
    if (value_type == LogicalTypeId::DOUBLE) {
      GetFlatVectorDataMutable<double>(vector)[row_idx] =
          value.GetValueUnsafe<double>();
      return true;
    }
    break;
  case LogicalTypeId::DATE:
    if (value_type == LogicalTypeId::DATE) {
      GetFlatVectorDataMutable<date_t>(vector)[row_idx] =
          value.GetValueUnsafe<date_t>();
      return true;
    }
    if (value_type == LogicalTypeId::DOUBLE) {
      date_t converted_date;
      if (try_convert_double_to_date(value.GetValueUnsafe<double>(),
                                     converted_date)) {
        GetFlatVectorDataMutable<date_t>(vector)[row_idx] = converted_date;
        return true;
      }
    }
    break;
  case LogicalTypeId::TIME:
    if (value_type == LogicalTypeId::TIME) {
      GetFlatVectorDataMutable<dtime_t>(vector)[row_idx] =
          value.GetValueUnsafe<dtime_t>();
      return true;
    }
    break;
  case LogicalTypeId::TIMESTAMP:
    if (value_type == LogicalTypeId::TIMESTAMP) {
      GetFlatVectorDataMutable<timestamp_t>(vector)[row_idx] =
          value.GetValueUnsafe<timestamp_t>();
      return true;
    }
    break;
  case LogicalTypeId::TIMESTAMP_TZ:
    if (value_type == LogicalTypeId::TIMESTAMP_TZ) {
      GetFlatVectorDataMutable<timestamp_tz_t>(vector)[row_idx] =
          value.GetValueUnsafe<timestamp_tz_t>();
      return true;
    }
    break;
  case LogicalTypeId::VARCHAR:
    if (value_type == LogicalTypeId::VARCHAR) {
      auto &text = StringValue::Get(value);
      GetFlatVectorDataMutable<string_t>(vector)[row_idx] =
          StringVector::AddString(vector, text.data(), text.size());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
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

std::string BuildDaxSchemaProbeForTesting(const std::string &statement,
                                          int64_t row_limit) {
  return BuildLimitedDaxSchemaProbe(statement, row_limit);
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

struct DaxQueryBindData : public TableFunctionData {
  DaxQueryBindData(PowerBIConnectionConfig config_p,
                   PowerBIResolvedTarget target_p,
                   std::vector<XmlaColumn> columns_p, string dax_text_p,
                   string xmla_catalog_p, string xmla_auth_scheme_p,
                   string access_token_p, string effective_user_name_p,
                   int64_t timeout_ms_p,
                   std::shared_ptr<HttpClient> xmla_http_client_p = nullptr)
      : config(std::move(config_p)), target(std::move(target_p)),
        columns(std::move(columns_p)), dax_text(std::move(dax_text_p)),
        xmla_catalog(std::move(xmla_catalog_p)),
        xmla_auth_scheme(std::move(xmla_auth_scheme_p)),
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
           dax_text == other.dax_text && xmla_catalog == other.xmla_catalog &&
           xmla_auth_scheme == other.xmla_auth_scheme &&
           access_token == other.access_token &&
           effective_user_name == other.effective_user_name &&
           timeout_ms == other.timeout_ms;
  }

  PowerBIConnectionConfig config;
  PowerBIResolvedTarget target;
  std::vector<XmlaColumn> columns;
  string dax_text;
  string xmla_catalog;
  string xmla_auth_scheme;
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
    request.catalog = bind_data.xmla_catalog;
    request.auth_scheme = bind_data.xmla_auth_scheme;
    request.access_token = bind_data.access_token;
    request.xmla_server = bind_data.target.core_server_name;
    request.xmla_workspace_id = bind_data.target.workspace_id;
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
          request, &columns, nullptr,
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
              auto &vector = current_chunk->data[column_idx];
              if (!TrySetFlatVectorValue(vector, current_chunk_size,
                                         row[column_idx],
                                         column_types[column_idx])) {
                vector.SetValue(current_chunk_size, row[column_idx]);
              }
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

static int64_t
ResolveSchemaProbeRows(const named_parameter_map_t &named_parameters) {
  if (HasNonNullNamedParameter(named_parameters, "schema_probe_rows")) {
    auto entry = named_parameters.find("schema_probe_rows");
    auto row_limit = entry->second.GetValue<int64_t>();
    if (row_limit < 0) {
      throw InvalidInputException("schema_probe_rows must be zero or greater");
    }
    return row_limit;
  }
  auto *raw_value = std::getenv("PBI_SCANNER_SCHEMA_PROBE_ROWS");
  if (!raw_value || !*raw_value) {
    return DEFAULT_SCHEMA_PROBE_ROWS;
  }
  try {
    auto row_limit = std::stoll(raw_value);
    if (row_limit < 0) {
      return DEFAULT_SCHEMA_PROBE_ROWS;
    }
    return row_limit;
  } catch (const std::exception &) {
    return DEFAULT_SCHEMA_PROBE_ROWS;
  }
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
  function.named_parameters["schema_probe_rows"] = LogicalType::BIGINT;
}

static bool IsMwcXmlaUnauthorized(const string &xmla_auth_scheme,
                                  const string &message) {
  return xmla_auth_scheme == "MwcToken" &&
         StringUtil::Lower(message).find("http 401") != string::npos;
}

static void FallbackToLegacyBearerXmla(const PowerBIConnectionConfig &config,
                                       PowerBIResolvedTarget &target,
                                       const string &access_token,
                                       int64_t timeout_ms, string &xmla_catalog,
                                       string &xmla_auth_scheme,
                                       string &xmla_access_token) {
  target.aixl_url = ResolveLegacyPowerBIXmlaUrl(config.endpoint, target,
                                                access_token, timeout_ms);
  target.core_server_name.clear();
  xmla_catalog = target.internal_catalog;
  xmla_auth_scheme = "Bearer";
  xmla_access_token = access_token;
}

//! Sets catalog, auth scheme, and token for a resolved Power BI target (not
//! direct XMLA). Capacity-backed targets use dataset catalog + MWC token.
static void ApplyXmlaAuthForResolvedPowerBiTarget(
    const PowerBIConnectionConfig &config, const PowerBIResolvedTarget &target,
    const string &access_token, int64_t timeout_ms, string &xmla_catalog,
    string &xmla_auth_scheme, string &xmla_access_token) {
  if (!target.capacity_object_id.empty()) {
    xmla_catalog = target.dataset_name;
    auto xmla_token_start = std::chrono::steady_clock::now();
    xmla_access_token = GeneratePowerBIXmlaToken(config.endpoint, target,
                                                 access_token, timeout_ms);
    DebugTiming("GeneratePowerBIXmlaToken", xmla_token_start);
    xmla_auth_scheme = "MwcToken";
  } else {
    xmla_catalog = target.internal_catalog;
    xmla_auth_scheme = "Bearer";
    xmla_access_token = access_token;
  }
}

//! Full retry is only for SOAP fault-style probe failures (TOPN may be
//! rejected). Transport, HTTP, and interrupts must not trigger a second probe.
static bool LimitedSchemaProbeFailureAllowsFullRetry(const Exception &ex) {
  auto msg = StringUtil::Lower(string(ex.what()));
  if (msg.find("request failed") != string::npos) {
    return false;
  }
  if (msg.find("xmla schema probe http") != string::npos) {
    return false;
  }
  return msg.find("xmla schema probe failed") != string::npos;
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
  auto schema_probe_rows = ResolveSchemaProbeRows(input.named_parameters);
  PowerBIResolvedTarget target;
  bool target_from_cache = false;
  string xmla_catalog;
  string xmla_auth_scheme = "Bearer";
  string xmla_access_token = access_token;
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
    ApplyXmlaAuthForResolvedPowerBiTarget(config, target, access_token,
                                          timeout_ms, xmla_catalog,
                                          xmla_auth_scheme, xmla_access_token);
  }
  if (xmla_catalog.empty()) {
    xmla_catalog = target.internal_catalog;
  }

  std::shared_ptr<HttpClient> xmla_http_client;
  std::vector<XmlaColumn> columns;
  if (!TryGetCachedSchema(target, dax_text, effective_user_name, columns)) {
    auto full_probe_statement = dax_text;
    auto limited_probe_statement =
        BuildLimitedDaxSchemaProbe(dax_text, schema_probe_rows);
    auto limited_probe_available =
        limited_probe_statement != full_probe_statement;
    auto probe_schema = [&](const string &probe_statement) {
      xmla_http_client = std::make_shared<HttpClient>(timeout_ms);
      XmlaExecutor executor(timeout_ms, xmla_http_client);
      XmlaRequest request;
      request.url = target.aixl_url;
      request.catalog = xmla_catalog;
      request.auth_scheme = xmla_auth_scheme;
      request.access_token = xmla_access_token;
      request.xmla_server = target.core_server_name;
      request.xmla_workspace_id = target.workspace_id;
      request.statement = probe_statement;
      request.effective_user_name = effective_user_name;
      request.timeout_ms = timeout_ms;
      auto probe_start = std::chrono::steady_clock::now();
      auto probed_columns = executor.ProbeSchema(request);
      DebugTiming("ProbeSchema", probe_start);
      return probed_columns;
    };
    auto probe_schema_with_fallback = [&]() {
      if (!limited_probe_available) {
        return probe_schema(full_probe_statement);
      }
      try {
        if (DebugTimingsEnabled()) {
          std::fprintf(stderr,
                       "[pbi_scanner] limited schema probe rows: %lld\n",
                       static_cast<long long>(schema_probe_rows));
        }
        return probe_schema(limited_probe_statement);
      } catch (const InterruptException &) {
        throw;
      } catch (const Exception &ex) {
        if (!LimitedSchemaProbeFailureAllowsFullRetry(ex)) {
          throw;
        }
        if (DebugTimingsEnabled()) {
          std::fprintf(stderr,
                       "[pbi_scanner] limited schema probe failed, retrying "
                       "full probe: %s\n",
                       ex.what());
        }
        return probe_schema(full_probe_statement);
      }
    };
    try {
      columns = probe_schema_with_fallback();
    } catch (const Exception &ex) {
      if (IsMwcXmlaUnauthorized(xmla_auth_scheme, ex.what())) {
        FallbackToLegacyBearerXmla(config, target, access_token, timeout_ms,
                                   xmla_catalog, xmla_auth_scheme,
                                   xmla_access_token);
        columns = probe_schema_with_fallback();
      } else {
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
        ApplyXmlaAuthForResolvedPowerBiTarget(
            config, target, access_token, timeout_ms, xmla_catalog,
            xmla_auth_scheme, xmla_access_token);
        columns = probe_schema_with_fallback();
      }
    } catch (const std::exception &ex) {
      if (IsMwcXmlaUnauthorized(xmla_auth_scheme, ex.what())) {
        FallbackToLegacyBearerXmla(config, target, access_token, timeout_ms,
                                   xmla_catalog, xmla_auth_scheme,
                                   xmla_access_token);
        columns = probe_schema_with_fallback();
      } else {
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
        ApplyXmlaAuthForResolvedPowerBiTarget(
            config, target, access_token, timeout_ms, xmla_catalog,
            xmla_auth_scheme, xmla_access_token);
        columns = probe_schema_with_fallback();
      }
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
      std::move(dax_text), std::move(xmla_catalog), std::move(xmla_auth_scheme),
      std::move(xmla_access_token), std::move(effective_user_name), timeout_ms,
      std::move(xmla_http_client));
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
