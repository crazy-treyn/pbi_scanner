#include "xmla_value.hpp"

#include "pbi_scanner_util.hpp"
#include "xmla_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/double_cast_operator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>

namespace duckdb {

namespace {

static bool TryParseInt64(const std::string &value, int64_t &result) {
  if (value.empty()) {
    return false;
  }
  constexpr size_t kStackMax = 128;
  char stack[kStackMax];
  const char *cstr;
  std::string heap;
  if (value.size() < kStackMax) {
    std::memcpy(stack, value.data(), value.size());
    stack[value.size()] = '\0';
    cstr = stack;
  } else {
    heap = value;
    cstr = heap.c_str();
  }
  errno = 0;
  char *end_ptr = nullptr;
  auto parsed = std::strtoll(cstr, &end_ptr, 10);
  if (errno != 0 || !end_ptr) {
    return false;
  }
  if (static_cast<size_t>(end_ptr - cstr) != value.size()) {
    return false;
  }
  result = parsed;
  return true;
}

static bool TryParseUInt64(const std::string &value, uint64_t &result) {
  if (value.empty()) {
    return false;
  }
  constexpr size_t kStackMax = 128;
  char stack[kStackMax];
  const char *cstr;
  std::string heap;
  if (value.size() < kStackMax) {
    std::memcpy(stack, value.data(), value.size());
    stack[value.size()] = '\0';
    cstr = stack;
  } else {
    heap = value;
    cstr = heap.c_str();
  }
  errno = 0;
  char *end_ptr = nullptr;
  auto parsed = std::strtoull(cstr, &end_ptr, 10);
  if (errno != 0 || !end_ptr) {
    return false;
  }
  if (static_cast<size_t>(end_ptr - cstr) != value.size()) {
    return false;
  }
  result = parsed;
  return true;
}

static bool TryParseDouble(const std::string &value, double &result) {
  return TryDoubleCast(value.data(), static_cast<idx_t>(value.size()), result,
                       true);
}

static bool HasTimezoneOffset(const std::string &value) {
  if (value.empty()) {
    return false;
  }
  auto last_char = value.back();
  if (last_char == 'Z' || last_char == 'z') {
    return true;
  }
  auto separator = value.find('T');
  if (separator == std::string::npos) {
    separator = value.find(' ');
  }
  if (separator == std::string::npos) {
    return false;
  }
  for (idx_t i = separator + 1; i < value.size(); i++) {
    if ((value[i] == '+' || value[i] == '-') && i + 2 < value.size()) {
      return true;
    }
  }
  return false;
}

static bool IsDigit(char value) { return value >= '0' && value <= '9'; }

static bool LooksLikeDateValue(const std::string &value) {
  return value.size() == 10 && IsDigit(value[0]) && IsDigit(value[1]) &&
         IsDigit(value[2]) && IsDigit(value[3]) && value[4] == '-' &&
         IsDigit(value[5]) && IsDigit(value[6]) && value[7] == '-' &&
         IsDigit(value[8]) && IsDigit(value[9]);
}

static bool LooksLikeTimeValue(const std::string &value) {
  return value.size() >= 8 && IsDigit(value[0]) && IsDigit(value[1]) &&
         value[2] == ':' && IsDigit(value[3]) && IsDigit(value[4]) &&
         value[5] == ':' && IsDigit(value[6]) && IsDigit(value[7]);
}

static bool LooksLikeTimestampValue(const std::string &value) {
  return value.size() >= 19 && LooksLikeDateValue(value.substr(0, 10)) &&
         (value[10] == 'T' || value[10] == ' ') &&
         LooksLikeTimeValue(value.substr(11));
}

static bool LooksLikeUnsignedIntegerValue(const std::string &value,
                                          idx_t start) {
  if (start >= value.size()) {
    return false;
  }
  for (idx_t i = start; i < value.size(); i++) {
    if (!IsDigit(value[i])) {
      return false;
    }
  }
  return true;
}

static bool LooksLikeSignedIntegerValue(const std::string &value) {
  if (value.empty()) {
    return false;
  }
  auto start = (value[0] == '-' || value[0] == '+') ? 1 : 0;
  return LooksLikeUnsignedIntegerValue(value, start);
}

static bool LooksLikeDoubleValue(const std::string &value) {
  if (value.empty()) {
    return false;
  }
  bool digit_seen = false;
  bool decimal_seen = false;
  bool exponent_seen = false;
  bool digit_after_exponent = true;
  for (idx_t i = 0; i < value.size(); i++) {
    auto ch = value[i];
    if (IsDigit(ch)) {
      digit_seen = true;
      if (exponent_seen) {
        digit_after_exponent = true;
      }
      continue;
    }
    if ((ch == '-' || ch == '+') &&
        (i == 0 || value[i - 1] == 'e' || value[i - 1] == 'E')) {
      continue;
    }
    if (ch == '.' && !decimal_seen && !exponent_seen) {
      decimal_seen = true;
      continue;
    }
    if ((ch == 'e' || ch == 'E') && !exponent_seen && digit_seen) {
      exponent_seen = true;
      digit_after_exponent = false;
      continue;
    }
    return false;
  }
  return digit_seen && digit_after_exponent &&
         (decimal_seen || exponent_seen || !LooksLikeSignedIntegerValue(value));
}

static bool TryParseDateValue(const std::string &value, Value &result) {
  if (!LooksLikeDateValue(value)) {
    return false;
  }
  if (value.find('T') != std::string::npos ||
      value.find(' ') != std::string::npos) {
    return false;
  }
  try {
    result = Value::DATE(Date::FromString(value, true));
    return true;
  } catch (const Exception &) {
    return false;
  } catch (const std::exception &) {
    return false;
  }
}

static bool TryParseTimeValue(const std::string &value, Value &result) {
  if (!LooksLikeTimeValue(value)) {
    return false;
  }
  if (value.find('T') != std::string::npos ||
      value.find(' ') != std::string::npos) {
    return false;
  }
  try {
    result = Value::TIME(Time::FromString(value, true));
    return true;
  } catch (const Exception &) {
    return false;
  } catch (const std::exception &) {
    return false;
  }
}

static bool TryParseTimestampValue(const std::string &value, Value &result) {
  if (!LooksLikeTimestampValue(value)) {
    return false;
  }
  try {
    auto has_offset = HasTimezoneOffset(value);
    auto timestamp = Timestamp::FromString(value, has_offset);
    if (has_offset) {
      result = Value::TIMESTAMPTZ(timestamp_tz_t(timestamp));
    } else {
      result = Value::TIMESTAMP(timestamp);
    }
    return true;
  } catch (const Exception &) {
    return false;
  } catch (const std::exception &) {
    return false;
  }
}

static bool TryDecodeSsasTemporalSerial(double serial_value, date_t &out_date,
                                        dtime_t &out_time) {
  if (!std::isfinite(serial_value)) {
    return false;
  }
  // SSAS commonly uses OLE Automation serial date semantics:
  // days since 1899-12-30, including fractional day for time.
  static constexpr int64_t MICROS_PER_DAY = 24LL * 60LL * 60LL * 1000000LL;
  static const int32_t OLE_BASE_EPOCH_DAYS =
      Date::EpochDays(Date::FromString("1899-12-30", true));

  auto total_micros =
      static_cast<int64_t>(serial_value * static_cast<double>(MICROS_PER_DAY) +
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
  out_time = dtime_t(micros_in_day);
  return true;
}

static bool TryParseTimeFromBaseDateTimestamp(const std::string &value,
                                              Value &result) {
  if (!LooksLikeTimestampValue(value)) {
    return false;
  }
  if (HasTimezoneOffset(value)) {
    return false;
  }
  try {
    auto timestamp = Timestamp::FromString(value, false);
    auto date = Timestamp::GetDate(timestamp);
    if (Date::ToString(date) != "1899-12-30") {
      return false;
    }
    result = Value::TIME(Timestamp::GetTime(timestamp));
    return true;
  } catch (const Exception &) {
    return false;
  } catch (const std::exception &) {
    return false;
  }
}

static bool TryParseDateFromZeroTimeTimestamp(const std::string &value,
                                              Value &result) {
  if (value.size() < 19 || (value[10] != 'T' && value[10] != ' ')) {
    return false;
  }
  if (HasTimezoneOffset(value)) {
    return false;
  }
  if (value.compare(11, 8, "00:00:00") != 0) {
    return false;
  }
  return TryParseDateValue(value.substr(0, 10), result);
}

static std::string NormalizeXmlType(const std::string &source_type) {
  return StringUtil::Lower(Trimmed(source_type));
}

static Value InferUntypedXmlValue(const std::string &body) {
  if (StringUtil::CIEquals(body, "true")) {
    return Value::BOOLEAN(true);
  }
  if (StringUtil::CIEquals(body, "false")) {
    return Value::BOOLEAN(false);
  }

  int64_t signed_value;
  if (LooksLikeSignedIntegerValue(body) && TryParseInt64(body, signed_value)) {
    return Value::BIGINT(signed_value);
  }

  uint64_t unsigned_value;
  if (LooksLikeUnsignedIntegerValue(body, 0) &&
      TryParseUInt64(body, unsigned_value)) {
    return Value::UBIGINT(unsigned_value);
  }

  double double_value;
  if (LooksLikeDoubleValue(body) && TryParseDouble(body, double_value)) {
    return Value::DOUBLE(double_value);
  }

  std::string temporal_source(body);
  Value temporal_value;
  if (TryParseDateValue(temporal_source, temporal_value)) {
    return temporal_value;
  }
  if (TryParseTimeValue(temporal_source, temporal_value)) {
    return temporal_value;
  }
  if (TryParseTimeFromBaseDateTimestamp(temporal_source, temporal_value)) {
    return temporal_value;
  }
  if (TryParseDateFromZeroTimeTimestamp(temporal_source, temporal_value)) {
    return temporal_value;
  }
  if (TryParseTimestampValue(temporal_source, temporal_value)) {
    return temporal_value;
  }

  return Value(body);
}

} // namespace

LogicalType MapXmlTypeToLogicalType(const std::string &source_type) {
  auto normalized = NormalizeXmlType(source_type);
  if (normalized == "xsd:boolean" || normalized == "boolean") {
    return LogicalType::BOOLEAN;
  }
  if (normalized == "xsd:byte" || normalized == "xsd:short" ||
      normalized == "xsd:int" || normalized == "xsd:integer" ||
      normalized == "xsd:long" || normalized == "byte" ||
      normalized == "short" || normalized == "int" || normalized == "integer" ||
      normalized == "long") {
    return LogicalType::BIGINT;
  }
  if (normalized == "xsd:unsignedbyte" || normalized == "xsd:unsignedshort" ||
      normalized == "xsd:unsignedint" || normalized == "xsd:unsignedlong" ||
      normalized == "unsignedbyte" || normalized == "unsignedshort" ||
      normalized == "unsignedint" || normalized == "unsignedlong") {
    return LogicalType::UBIGINT;
  }
  if (normalized == "xsd:decimal" || normalized == "xsd:double" ||
      normalized == "xsd:float" || normalized == "decimal" ||
      normalized == "double" || normalized == "float") {
    return LogicalType::DOUBLE;
  }
  if (normalized == "xsd:date" || normalized == "date") {
    return LogicalType::DATE;
  }
  if (normalized == "xsd:time" || normalized == "time") {
    return LogicalType::TIME;
  }
  if (normalized == "xsd:datetimeoffset" || normalized == "datetimeoffset") {
    return LogicalType::TIMESTAMP_TZ;
  }
  if (normalized == "xsd:datetime" || normalized == "datetime") {
    return LogicalType::TIMESTAMP;
  }
  return LogicalType::VARCHAR;
}

XmlaCoercionKind CoercionKindFromXmlType(const std::string &source_type) {
  return CoercionKindFromLogicalType(MapXmlTypeToLogicalType(source_type));
}

XmlaCoercionKind CoercionKindFromLogicalType(const LogicalType &type) {
  switch (type.id()) {
  case LogicalTypeId::BOOLEAN:
    return XmlaCoercionKind::BOOLEAN;
  case LogicalTypeId::BIGINT:
  case LogicalTypeId::HUGEINT:
    return XmlaCoercionKind::BIGINT;
  case LogicalTypeId::UBIGINT:
    return XmlaCoercionKind::UBIGINT;
  case LogicalTypeId::DOUBLE:
    return XmlaCoercionKind::DOUBLE;
  case LogicalTypeId::DATE:
    return XmlaCoercionKind::DATE;
  case LogicalTypeId::TIME:
    return XmlaCoercionKind::TIME;
  case LogicalTypeId::TIMESTAMP:
    return XmlaCoercionKind::TIMESTAMP;
  case LogicalTypeId::TIMESTAMP_TZ:
    return XmlaCoercionKind::TIMESTAMP_TZ;
  case LogicalTypeId::SQLNULL:
    return XmlaCoercionKind::INFER;
  default:
    return XmlaCoercionKind::VARCHAR;
  }
}

Value CoerceXmlValue(const std::string &raw_value,
                     XmlaCoercionKind coercion_kind) {
  std::string body = raw_value;
  if (raw_value.find('&') != std::string::npos) {
    body = UnescapeXML(raw_value);
  }
  TrimString(body);
  if (body.empty()) {
    if (coercion_kind == XmlaCoercionKind::VARCHAR) {
      return Value(std::string());
    }
    if (coercion_kind == XmlaCoercionKind::BOOLEAN) {
      // SSAS binary metadata frequently encodes false booleans as empty text.
      return Value::BOOLEAN(false);
    }
    return Value();
  }

  if (coercion_kind == XmlaCoercionKind::INFER) {
    return InferUntypedXmlValue(body);
  }
  if (coercion_kind == XmlaCoercionKind::VARCHAR) {
    return Value(body);
  }
  if (coercion_kind == XmlaCoercionKind::BOOLEAN) {
    if (StringUtil::CIEquals(body, "true") ||
        (body.size() == 1 && body[0] == '1')) {
      return Value::BOOLEAN(true);
    }
    if (StringUtil::CIEquals(body, "false") ||
        (body.size() == 1 && body[0] == '0')) {
      return Value::BOOLEAN(false);
    }
  }
  if (coercion_kind == XmlaCoercionKind::BIGINT) {
    int64_t parsed = 0;
    if (TryParseInt64(body, parsed)) {
      return Value::BIGINT(parsed);
    }
  }
  if (coercion_kind == XmlaCoercionKind::UBIGINT) {
    uint64_t parsed = 0;
    if (TryParseUInt64(body, parsed)) {
      return Value::UBIGINT(parsed);
    }
  }
  if (coercion_kind == XmlaCoercionKind::DOUBLE) {
    double parsed = 0;
    if (TryParseDouble(body, parsed)) {
      return Value::DOUBLE(parsed);
    }
  }

  if (coercion_kind == XmlaCoercionKind::DATE ||
      coercion_kind == XmlaCoercionKind::TIME ||
      coercion_kind == XmlaCoercionKind::TIMESTAMP ||
      coercion_kind == XmlaCoercionKind::TIMESTAMP_TZ) {
    double serial_value = 0;
    if (TryParseDouble(body, serial_value)) {
      date_t parsed_date;
      dtime_t parsed_time;
      if (TryDecodeSsasTemporalSerial(serial_value, parsed_date, parsed_time)) {
        if (coercion_kind == XmlaCoercionKind::DATE) {
          return Value::DATE(parsed_date);
        }
        if (coercion_kind == XmlaCoercionKind::TIME) {
          return Value::TIME(parsed_time);
        }
        auto parsed_timestamp =
            Timestamp::FromDatetime(parsed_date, parsed_time);
        if (coercion_kind == XmlaCoercionKind::TIMESTAMP_TZ) {
          return Value::TIMESTAMPTZ(timestamp_tz_t(parsed_timestamp));
        }
        return Value::TIMESTAMP(parsed_timestamp);
      }
    }
  }

  std::string temporal_source(body);
  Value temporal_value;
  if (coercion_kind == XmlaCoercionKind::DATE &&
      (TryParseDateValue(temporal_source, temporal_value) ||
       TryParseDateFromZeroTimeTimestamp(temporal_source, temporal_value))) {
    return temporal_value;
  }
  if (coercion_kind == XmlaCoercionKind::TIME &&
      (TryParseTimeValue(temporal_source, temporal_value) ||
       TryParseTimeFromBaseDateTimestamp(temporal_source, temporal_value))) {
    return temporal_value;
  }
  if ((coercion_kind == XmlaCoercionKind::TIMESTAMP ||
       coercion_kind == XmlaCoercionKind::TIMESTAMP_TZ) &&
      TryParseTimestampValue(temporal_source, temporal_value)) {
    return temporal_value;
  }

  return Value(std::string(body));
}

LogicalType InferLogicalType(const Value &value) {
  if (value.IsNull()) {
    return LogicalType::SQLNULL;
  }
  switch (value.type().id()) {
  case LogicalTypeId::BOOLEAN:
    return LogicalType::BOOLEAN;
  case LogicalTypeId::BIGINT:
    return LogicalType::BIGINT;
  case LogicalTypeId::UBIGINT:
    return LogicalType::UBIGINT;
  case LogicalTypeId::DOUBLE:
    return LogicalType::DOUBLE;
  case LogicalTypeId::DATE:
    return LogicalType::DATE;
  case LogicalTypeId::TIME:
    return LogicalType::TIME;
  case LogicalTypeId::TIMESTAMP:
    return LogicalType::TIMESTAMP;
  case LogicalTypeId::TIMESTAMP_TZ:
    return LogicalType::TIMESTAMP_TZ;
  default:
    return LogicalType::VARCHAR;
  }
}

LogicalType MergeLogicalTypes(const LogicalType &current,
                              const LogicalType &next) {
  if (current.id() == LogicalTypeId::SQLNULL) {
    return next;
  }
  if (next.id() == LogicalTypeId::SQLNULL) {
    return current;
  }
  if (current == next) {
    return current;
  }

  auto current_id = current.id();
  auto next_id = next.id();
  auto current_numeric = current_id == LogicalTypeId::BIGINT ||
                         current_id == LogicalTypeId::UBIGINT ||
                         current_id == LogicalTypeId::DOUBLE ||
                         current_id == LogicalTypeId::HUGEINT;
  auto next_numeric =
      next_id == LogicalTypeId::BIGINT || next_id == LogicalTypeId::UBIGINT ||
      next_id == LogicalTypeId::DOUBLE || next_id == LogicalTypeId::HUGEINT;
  if (current_numeric && next_numeric) {
    if (current_id == LogicalTypeId::DOUBLE ||
        next_id == LogicalTypeId::DOUBLE) {
      return LogicalType::DOUBLE;
    }
    if (current_id == LogicalTypeId::HUGEINT ||
        next_id == LogicalTypeId::HUGEINT) {
      return LogicalType::HUGEINT;
    }
    if ((current_id == LogicalTypeId::BIGINT &&
         next_id == LogicalTypeId::UBIGINT) ||
        (current_id == LogicalTypeId::UBIGINT &&
         next_id == LogicalTypeId::BIGINT)) {
      return LogicalType::HUGEINT;
    }
    if (current_id == LogicalTypeId::UBIGINT &&
        next_id == LogicalTypeId::UBIGINT) {
      return LogicalType::UBIGINT;
    }
    return LogicalType::BIGINT;
  }

  if ((current_id == LogicalTypeId::DATE &&
       next_id == LogicalTypeId::TIMESTAMP) ||
      (current_id == LogicalTypeId::TIMESTAMP &&
       next_id == LogicalTypeId::DATE)) {
    return LogicalType::TIMESTAMP;
  }
  if ((current_id == LogicalTypeId::DATE &&
       next_id == LogicalTypeId::TIMESTAMP_TZ) ||
      (current_id == LogicalTypeId::TIMESTAMP_TZ &&
       next_id == LogicalTypeId::DATE) ||
      (current_id == LogicalTypeId::TIMESTAMP &&
       next_id == LogicalTypeId::TIMESTAMP_TZ) ||
      (current_id == LogicalTypeId::TIMESTAMP_TZ &&
       next_id == LogicalTypeId::TIMESTAMP)) {
    return LogicalType::TIMESTAMP_TZ;
  }

  return LogicalType::VARCHAR;
}

} // namespace duckdb
