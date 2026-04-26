#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

namespace duckdb {

inline string Trimmed(const string &value) {
  auto trimmed = value;
  StringUtil::Trim(trimmed);
  return trimmed;
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
