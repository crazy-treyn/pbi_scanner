#pragma once

#include "pbi_duckdb_compat.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace duckdb {

inline string Trimmed(const string &value) {
  auto trimmed = value;
  StringUtil::Trim(trimmed);
  return trimmed;
}

inline bool
HasNonNullNamedParameter(const named_parameter_map_t &named_parameters,
                         const char *name) {
  auto entry = pbi_compat::FindNamedParameter(named_parameters, name);
  return entry != named_parameters.end() && !entry->second.IsNull();
}

inline string
GetOptionalNamedParameter(const named_parameter_map_t &named_parameters,
                          const char *name) {
  auto entry = pbi_compat::FindNamedParameter(named_parameters, name);
  if (entry == named_parameters.end() || entry->second.IsNull()) {
    return string();
  }
  return Trimmed(entry->second.ToString());
}

inline bool DebugTimingsEnabled() {
  auto *value = std::getenv("PBI_SCANNER_DEBUG_TIMINGS");
  return value && *value;
}

inline void DebugTiming(const char *label,
                        const std::chrono::steady_clock::time_point &start) {
  if (!DebugTimingsEnabled()) {
    return;
  }
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count();
  std::fprintf(stderr, "[pbi_scanner] %s: %lld ms\n", label,
               static_cast<long long>(elapsed_ms));
}

inline int64_t CurrentUnixSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline uint64_t Fnv1a64(const string &value) {
  uint64_t hash = 1469598103934665603ULL;
  for (auto ch : value) {
    hash ^= static_cast<uint8_t>(ch);
    hash *= 1099511628211ULL;
  }
  return hash;
}

inline string HashSensitiveValue(const string &value) {
  return std::to_string(Fnv1a64(value));
}

inline uint8_t DecodeHexDigit(char value, const char *error_message) {
  if (value >= '0' && value <= '9') {
    return static_cast<uint8_t>(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return static_cast<uint8_t>(value - 'a' + 10);
  }
  if (value >= 'A' && value <= 'F') {
    return static_cast<uint8_t>(value - 'A' + 10);
  }
  throw InvalidInputException("%s", error_message);
}

} // namespace duckdb
