#include "xmla.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/double_cast_operator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <cerrno>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <unordered_map>

namespace duckdb {

namespace {

static std::string EscapeXML(const std::string &value) {
  std::string escaped;
  escaped.reserve(value.size() + 16);
  for (auto ch : value) {
    switch (ch) {
    case '&':
      escaped += "&amp;";
      break;
    case '<':
      escaped += "&lt;";
      break;
    case '>':
      escaped += "&gt;";
      break;
    case '"':
      escaped += "&quot;";
      break;
    case '\'':
      escaped += "&apos;";
      break;
    default:
      escaped.push_back(ch);
      break;
    }
  }
  return escaped;
}

enum class XmlaTransportMode : uint8_t { PLAIN, SX, XPRESS, SX_XPRESS };

static XmlaTransportMode ResolveXmlaTransportMode() {
  auto *raw_value = std::getenv("PBI_SCANNER_XMLA_TRANSPORT");
  if (!raw_value || !*raw_value) {
    return XmlaTransportMode::SX_XPRESS;
  }
  auto value = StringUtil::Lower(Trimmed(raw_value));
  if (value == "plain") {
    return XmlaTransportMode::PLAIN;
  }
  if (value == "sx") {
    return XmlaTransportMode::SX;
  }
  if (value == "xpress") {
    return XmlaTransportMode::XPRESS;
  }
  if (value == "sx_xpress" || value == "sxxpress" || value == "sx+xpress") {
    return XmlaTransportMode::SX_XPRESS;
  }
  throw InvalidInputException(
      "PBI_SCANNER_XMLA_TRANSPORT must be plain, sx, xpress, or sx_xpress");
}

static const char *XmlaTransportFlags(XmlaTransportMode mode) {
  switch (mode) {
  case XmlaTransportMode::PLAIN:
    return "0,0,0,0,0";
  case XmlaTransportMode::SX:
    return "1,0,0,1,0";
  case XmlaTransportMode::XPRESS:
    return "1,0,0,0,1";
  case XmlaTransportMode::SX_XPRESS:
    return "1,0,0,1,1";
  }
  return "0,0,0,0,0";
}

static XmlaTransportMode SchemaProbeTransportMode(XmlaTransportMode mode) {
  if (mode == XmlaTransportMode::SX || mode == XmlaTransportMode::SX_XPRESS) {
    return XmlaTransportMode::XPRESS;
  }
  return mode;
}

static bool XmlaTransportNeedsProtocolCapabilities(XmlaTransportMode mode) {
  return mode != XmlaTransportMode::PLAIN;
}

static void AppendProtocolCapabilities(std::string &envelope,
                                       XmlaTransportMode mode) {
  if (!XmlaTransportNeedsProtocolCapabilities(mode)) {
    return;
  }
  envelope += "<ProtocolCapabilities "
              "xmlns=\"http://schemas.microsoft.com/analysisservices/2003/"
              "engine\">";
  if (mode == XmlaTransportMode::SX || mode == XmlaTransportMode::SX_XPRESS) {
    envelope += "<Capability>sx</Capability>";
  }
  if (mode == XmlaTransportMode::XPRESS ||
      mode == XmlaTransportMode::SX_XPRESS) {
    envelope += "<Capability>xpress</Capability>";
  }
  envelope += "</ProtocolCapabilities>";
}

static std::string UnescapeXML(const std::string &value) {
  if (value.find('&') == std::string::npos) {
    return value;
  }
  std::string result;
  result.reserve(value.size());
  for (idx_t i = 0; i < value.size(); i++) {
    if (value[i] != '&') {
      result.push_back(value[i]);
      continue;
    }
    auto semi = value.find(';', i + 1);
    if (semi == std::string::npos) {
      result.push_back(value[i]);
      continue;
    }
    auto entity = value.substr(i + 1, semi - i - 1);
    if (entity == "amp") {
      result.push_back('&');
    } else if (entity == "lt") {
      result.push_back('<');
    } else if (entity == "gt") {
      result.push_back('>');
    } else if (entity == "quot") {
      result.push_back('"');
    } else if (entity == "apos") {
      result.push_back('\'');
    } else if (!entity.empty() && entity[0] == '#') {
      char *end_ptr = nullptr;
      long codepoint = 0;
      if (entity.size() > 2 && (entity[1] == 'x' || entity[1] == 'X')) {
        codepoint = std::strtol(entity.c_str() + 2, &end_ptr, 16);
      } else {
        codepoint = std::strtol(entity.c_str() + 1, &end_ptr, 10);
      }
      if (end_ptr && *end_ptr == '\0' && codepoint >= 0 && codepoint <= 0x7F) {
        result.push_back(static_cast<char>(codepoint));
      } else {
        result.append(value, i, semi - i + 1);
      }
    } else {
      result.append(value, i, semi - i + 1);
    }
    i = semi;
  }
  return result;
}

static std::string DecodeXMLName(const std::string &raw) {
  if (raw.find("_x") == std::string::npos) {
    return raw;
  }

  std::string result;
  result.reserve(raw.size());
  for (idx_t i = 0; i < raw.size();) {
    if (i + 7 <= raw.size() && raw[i] == '_' &&
        (raw[i + 1] == 'x' || raw[i + 1] == 'X') && raw[i + 6] == '_') {
      auto hex = raw.substr(i + 2, 4);
      char *end_ptr = nullptr;
      auto codepoint = std::strtol(hex.c_str(), &end_ptr, 16);
      if (end_ptr && *end_ptr == '\0' && codepoint >= 0 && codepoint <= 0x7F) {
        result.push_back(static_cast<char>(codepoint));
        i += 7;
        continue;
      }
    }
    result.push_back(raw[i++]);
  }
  return result;
}

static std::string ExtractLocalName(const std::string &name) {
  auto colon = name.find(':');
  return colon == std::string::npos ? name : name.substr(colon + 1);
}

static void TrimString(std::string &value) {
  idx_t begin = 0;
  while (
      begin < value.size() &&
      StringUtil::CharacterIsSpace(static_cast<unsigned char>(value[begin]))) {
    begin++;
  }
  idx_t end = value.size();
  while (end > begin && StringUtil::CharacterIsSpace(
                            static_cast<unsigned char>(value[end - 1]))) {
    end--;
  }
  if (begin == 0 && end == value.size()) {
    return;
  }
  value = value.substr(begin, end - begin);
}

static bool TagStartsWith(const std::string &tag, const char *prefix,
                          size_t prefix_len) {
  return tag.size() >= prefix_len &&
         std::memcmp(tag.data(), prefix, prefix_len) == 0;
}

static uint16_t ReadLittleEndian16(const_data_ptr_t data, idx_t size,
                                   idx_t offset) {
  if (offset + 2 > size) {
    throw IOException("XPRESS payload ended unexpectedly");
  }
  return static_cast<uint16_t>(data[offset]) |
         static_cast<uint16_t>(data[offset + 1] << 8);
}

static uint32_t ReadLittleEndian32(const_data_ptr_t data, idx_t size,
                                   idx_t offset) {
  if (offset + 4 > size) {
    throw IOException("XPRESS payload ended unexpectedly");
  }
  return static_cast<uint32_t>(data[offset]) |
         (static_cast<uint32_t>(data[offset + 1]) << 8) |
         (static_cast<uint32_t>(data[offset + 2]) << 16) |
         (static_cast<uint32_t>(data[offset + 3]) << 24);
}

static void DecompressXpressLz77Block(const_data_ptr_t input, idx_t input_size,
                                      std::string &output,
                                      idx_t expected_output_size) {
  idx_t input_pos = 0;
  idx_t output_start = output.size();
  idx_t last_length_half_byte = DConstants::INVALID_INDEX;

  while (output.size() - output_start < expected_output_size) {
    auto flags = ReadLittleEndian32(input, input_size, input_pos);
    input_pos += 4;

    for (int bit = 31;
         bit >= 0 && output.size() - output_start < expected_output_size;
         bit--) {
      if ((flags & (uint32_t(1) << bit)) == 0) {
        if (input_pos >= input_size) {
          throw IOException("XPRESS literal exceeded compressed block");
        }
        output.push_back(static_cast<char>(input[input_pos++]));
        continue;
      }

      auto match_bytes = ReadLittleEndian16(input, input_size, input_pos);
      input_pos += 2;
      uint32_t match_length = match_bytes & 0x7;
      auto match_offset = static_cast<idx_t>((match_bytes >> 3) + 1);
      if (match_length == 7) {
        if (last_length_half_byte == DConstants::INVALID_INDEX) {
          if (input_pos >= input_size) {
            throw IOException("XPRESS match length exceeded compressed block");
          }
          match_length = input[input_pos] & 0x0F;
          last_length_half_byte = input_pos++;
        } else {
          match_length = input[last_length_half_byte] >> 4;
          last_length_half_byte = DConstants::INVALID_INDEX;
        }
        if (match_length == 15) {
          if (input_pos >= input_size) {
            throw IOException("XPRESS extended match length missing");
          }
          match_length = input[input_pos++];
          if (match_length == 255) {
            match_length = ReadLittleEndian16(input, input_size, input_pos);
            input_pos += 2;
            if (match_length == 0) {
              match_length = ReadLittleEndian32(input, input_size, input_pos);
              input_pos += 4;
            }
            if (match_length < 22) {
              throw IOException("XPRESS extended match length was invalid");
            }
            match_length -= 22;
          }
          match_length += 15;
        }
        match_length += 7;
      }
      match_length += 3;

      if (match_offset == 0 || match_offset > output.size() - output_start) {
        throw IOException("XPRESS match offset was invalid");
      }
      for (uint32_t i = 0; i < match_length; i++) {
        output.push_back(output[output.size() - match_offset]);
        if (output.size() - output_start > expected_output_size) {
          throw IOException("XPRESS match exceeded expected block size");
        }
      }
    }
  }
}

static std::string DecompressXpressLz77Framed(const std::string &compressed) {
  std::string output;
  auto input = const_data_ptr_cast(compressed.data());
  auto input_size = static_cast<idx_t>(compressed.size());
  idx_t input_pos = 0;

  while (input_pos < input_size) {
    auto original_size = ReadLittleEndian32(input, input_size, input_pos);
    input_pos += 4;
    auto compressed_size = ReadLittleEndian32(input, input_size, input_pos);
    input_pos += 4;
    if (original_size > 65535 || compressed_size > 65535) {
      throw IOException("XPRESS block sizes must not exceed 65535 bytes");
    }
    if (input_pos + compressed_size > input_size) {
      throw IOException("XPRESS block exceeded payload size");
    }
    if (original_size == compressed_size) {
      output.append(reinterpret_cast<const char *>(input + input_pos),
                    compressed_size);
    } else {
      DecompressXpressLz77Block(input + input_pos, compressed_size, output,
                                original_size);
    }
    input_pos += compressed_size;
  }
  return output;
}

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

static bool TryParseDateValue(const std::string &value, Value &result) {
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

static LogicalType MapXmlTypeToLogicalType(const std::string &source_type) {
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

static XmlaCoercionKind CoercionKindFromLogicalType(const LogicalType &type) {
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

static XmlaCoercionKind
CoercionKindFromXmlType(const std::string &source_type) {
  return CoercionKindFromLogicalType(MapXmlTypeToLogicalType(source_type));
}

static Value InferUntypedXmlValue(const std::string &body) {
  if (StringUtil::CIEquals(body.data(), body.size(), "true", 4)) {
    return Value::BOOLEAN(true);
  }
  if (StringUtil::CIEquals(body.data(), body.size(), "false", 5)) {
    return Value::BOOLEAN(false);
  }

  int64_t signed_value;
  if (TryParseInt64(body, signed_value)) {
    return Value::BIGINT(signed_value);
  }

  uint64_t unsigned_value;
  if (TryParseUInt64(body, unsigned_value)) {
    return Value::UBIGINT(unsigned_value);
  }

  double double_value;
  if (TryParseDouble(body, double_value)) {
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

static Value CoerceXmlValue(const std::string &raw_value,
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
    if (StringUtil::CIEquals(body.data(), body.size(), "true", 4) ||
        (body.size() == 1 && body[0] == '1')) {
      return Value::BOOLEAN(true);
    }
    if (StringUtil::CIEquals(body.data(), body.size(), "false", 5) ||
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

static LogicalType InferLogicalType(const Value &value) {
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

static LogicalType MergeLogicalTypes(const LogicalType &current,
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

struct XmlTag {
  std::string name;
  case_insensitive_map_t<std::string> attributes;
  bool is_closing = false;
  bool is_self_closing = false;
};

struct XmlFrame {
  std::string name;
  case_insensitive_map_t<std::string> attributes;
  std::string text;
};

static idx_t FindTagEnd(const std::string &text, idx_t start) {
  if (text.compare(start, 4, "<!--") == 0) {
    auto end = text.find("-->", start + 4);
    return end == std::string::npos ? DConstants::INVALID_INDEX : end + 2;
  }
  if (text.compare(start, 9, "<![CDATA[") == 0) {
    auto end = text.find("]]>", start + 9);
    return end == std::string::npos ? DConstants::INVALID_INDEX : end + 2;
  }
  if (text.compare(start, 2, "<?") == 0) {
    auto end = text.find("?>", start + 2);
    return end == std::string::npos ? DConstants::INVALID_INDEX : end + 1;
  }

  char quote = '\0';
  for (idx_t i = start + 1; i < text.size(); i++) {
    auto ch = text[i];
    if (quote) {
      if (ch == quote) {
        quote = '\0';
      }
      continue;
    }
    if (ch == '\'' || ch == '"') {
      quote = ch;
      continue;
    }
    if (ch == '>') {
      return i;
    }
  }
  return DConstants::INVALID_INDEX;
}

static XmlTag ParseTag(const std::string &tag_text) {
  XmlTag tag;
  if (tag_text.size() < 2) {
    return tag;
  }
  auto inner = tag_text.substr(1, tag_text.size() - 2);
  TrimString(inner);
  if (!inner.empty() && inner.front() == '/') {
    tag.is_closing = true;
    inner.erase(0, 1);
    TrimString(inner);
    tag.name = ExtractLocalName(inner);
    return tag;
  }
  if (!inner.empty() && inner.back() == '/') {
    tag.is_self_closing = true;
    inner.pop_back();
    TrimString(inner);
  }

  idx_t pos = 0;
  while (pos < inner.size() && !StringUtil::CharacterIsSpace(
                                   static_cast<unsigned char>(inner[pos]))) {
    tag.name.push_back(inner[pos++]);
  }
  tag.name = ExtractLocalName(tag.name);

  while (pos < inner.size()) {
    while (pos < inner.size() && StringUtil::CharacterIsSpace(
                                     static_cast<unsigned char>(inner[pos]))) {
      pos++;
    }
    if (pos >= inner.size()) {
      break;
    }
    idx_t key_start = pos;
    while (
        pos < inner.size() && inner[pos] != '=' &&
        !StringUtil::CharacterIsSpace(static_cast<unsigned char>(inner[pos]))) {
      pos++;
    }
    auto key = inner.substr(key_start, pos - key_start);
    while (pos < inner.size() && StringUtil::CharacterIsSpace(
                                     static_cast<unsigned char>(inner[pos]))) {
      pos++;
    }
    if (pos >= inner.size() || inner[pos] != '=') {
      break;
    }
    pos++;
    while (pos < inner.size() && StringUtil::CharacterIsSpace(
                                     static_cast<unsigned char>(inner[pos]))) {
      pos++;
    }
    if (pos >= inner.size() || (inner[pos] != '\'' && inner[pos] != '"')) {
      break;
    }
    auto quote = inner[pos++];
    idx_t val_start = pos;
    while (pos < inner.size() && inner[pos] != quote) {
      pos++;
    }
    auto value = inner.substr(val_start, pos - val_start);
    if (pos < inner.size()) {
      pos++;
    }
    if (value.find('&') != std::string::npos) {
      tag.attributes[ExtractLocalName(key)] = UnescapeXML(std::string(value));
    } else {
      tag.attributes[ExtractLocalName(key)] = std::string(value);
    }
  }
  return tag;
}

class XmlaStreamParser {
public:
  static constexpr idx_t MAX_SCHEMA_INFERENCE_ROWS = 8;

  XmlaStreamParser(
      bool stop_after_schema_p,
      std::function<void(const std::vector<XmlaColumn> &columns)> on_schema_p,
      std::function<bool(const std::vector<Value> &row)> on_row_p,
      const std::vector<XmlaColumn> *known_columns_p = nullptr)
      : stop_after_schema(stop_after_schema_p),
        on_schema(std::move(on_schema_p)), on_row(std::move(on_row_p)) {
    if (known_columns_p) {
      for (const auto &column : *known_columns_p) {
        known_column_types[column.name] = column.duckdb_type;
        known_column_types_by_index.push_back(column.duckdb_type);
      }
    }
  }

  bool StartElement(std::string name,
                    case_insensitive_map_t<std::string> attributes) {
    if (stopped_early) {
      return false;
    }
    XmlTag tag;
    tag.name = ExtractLocalName(name);
    tag.attributes = std::move(attributes);
    HandleStartTag(tag);
    return !stopped_early;
  }

  bool EmptyElement(std::string name,
                    case_insensitive_map_t<std::string> attributes) {
    if (stopped_early) {
      return false;
    }
    XmlFrame frame{ExtractLocalName(name), std::move(attributes),
                   std::string()};
    ProcessCompletedElement(frame,
                            stack.empty() ? std::string() : stack.back().name);
    return !stopped_early;
  }

  bool Text(const std::string &text) {
    if (stopped_early) {
      return false;
    }
    if (!stack.empty()) {
      stack.back().text.append(text);
    }
    return !stopped_early;
  }

  bool EndElement() {
    if (stopped_early) {
      return false;
    }
    if (stack.empty()) {
      return !stopped_early;
    }
    auto frame = std::move(stack.back());
    stack.pop_back();
    ProcessCompletedElement(frame,
                            stack.empty() ? std::string() : stack.back().name);
    if (frame.name == "complexType" && inside_schema_row_complex_type > 0 &&
        StringUtil::CIEquals(GetAttribute(frame.attributes, "name"), "row")) {
      inside_schema_row_complex_type--;
    }
    if (frame.name == "schema" && inside_schema > 0) {
      inside_schema--;
    }
    return !stopped_early;
  }

  bool Feed(const_data_ptr_t data, idx_t data_length) {
    if (stopped_early) {
      return false;
    }
    AppendExcerpt(data, data_length);
    const auto append_len = static_cast<idx_t>(data_length);
    pending.reserve(pending.size() + append_len);
    pending.append(reinterpret_cast<const char *>(data), data_length);

    idx_t pos = pending_offset;
    while (pos < pending.size()) {
      auto next_tag = pending.find('<', pos);
      if (next_tag == std::string::npos) {
        Text(pending.substr(pos, pending.size() - pos));
        pos = pending.size();
        break;
      }
      if (next_tag > pos && !stack.empty()) {
        Text(pending.substr(pos, next_tag - pos));
      }

      auto tag_end = FindTagEnd(pending, next_tag);
      if (tag_end == DConstants::INVALID_INDEX) {
        pos = next_tag;
        break;
      }
      std::string tag_text = pending.substr(next_tag, tag_end - next_tag + 1);
      if (TagStartsWith(tag_text, "<![CDATA[", 9)) {
        if (!stack.empty() && tag_text.size() >= 12) {
          Text(tag_text.substr(9, tag_text.size() - 12));
        }
      } else if (TagStartsWith(tag_text, "<!--", 4) ||
                 TagStartsWith(tag_text, "<?", 2)) {
        // ignored
      } else {
        HandleTag(ParseTag(tag_text));
        if (stopped_early) {
          pos = tag_end + 1;
          pending_offset = pos;
          if (pending_offset > 0) {
            pending.erase(0, pending_offset);
            pending_offset = 0;
          }
          return false;
        }
      }
      pos = tag_end + 1;
    }
    pending_offset = pos;
    CompactPending();
    return !stopped_early;
  }

  void Finish() {
    if (!schema_emitted && !columns.empty()) {
      EmitSchemaIfNeeded();
    }
    pending.clear();
    pending_offset = 0;
  }

  bool HasSchema() const { return schema_emitted; }

  const std::vector<XmlaColumn> &Columns() const { return columns; }

  bool StoppedEarly() const { return stopped_early; }

  bool HasFault() const {
    return !fault_message.empty() || !fault_code.empty();
  }

  std::string GetFaultMessage() const {
    if (!fault_code.empty()) {
      return fault_code + ": " + fault_message;
    }
    return fault_message;
  }

  const std::string &Excerpt() const { return excerpt; }

private:
  void CompactPending() {
    constexpr idx_t COMPACT_THRESHOLD = 1024 * 1024;
    if (pending_offset == 0) {
      return;
    }
    if (pending_offset >= COMPACT_THRESHOLD ||
        pending_offset * 2 > pending.size()) {
      pending.erase(0, pending_offset);
      pending_offset = 0;
    }
  }

  void AppendExcerpt(const_data_ptr_t data, idx_t data_length) {
    constexpr idx_t MAX_EXCERPT_SIZE = 16384;
    if (excerpt.size() >= MAX_EXCERPT_SIZE) {
      return;
    }
    auto remaining = MAX_EXCERPT_SIZE - excerpt.size();
    auto append_count = MinValue<idx_t>(remaining, data_length);
    excerpt.append(reinterpret_cast<const char *>(data), append_count);
  }

  void EmitSchemaIfNeeded() {
    if (schema_emitted || columns.empty()) {
      return;
    }
    for (idx_t i = 0; i < columns.size(); i++) {
      if (columns[i].duckdb_type.id() == LogicalTypeId::SQLNULL) {
        columns[i].duckdb_type = LogicalType::VARCHAR;
      }
      column_indexes[columns[i].name] = i;
    }
    schema_emitted = true;
    if (on_schema) {
      on_schema(columns);
    }
    if (stop_after_schema) {
      stopped_early = true;
    }
  }

  std::string
  GetAttribute(const case_insensitive_map_t<std::string> &attributes,
               const std::string &name) const {
    auto entry = attributes.find(name);
    if (entry == attributes.end()) {
      return std::string();
    }
    return entry->second;
  }

  void UpdateInferredColumns() {
    for (const auto &entry : current_row) {
      auto inferred_type = InferLogicalType(entry.second);
      auto column_index = column_indexes.find(entry.first);
      if (column_index == column_indexes.end()) {
        XmlaColumn column;
        column.name = entry.first;
        column.duckdb_type = inferred_type;
        column.coercion_kind = CoercionKindFromLogicalType(inferred_type);
        column_indexes[column.name] = columns.size();
        columns.push_back(std::move(column));
        continue;
      }
      auto &column = columns[column_index->second];
      column.duckdb_type = MergeLogicalTypes(column.duckdb_type, inferred_type);
      column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
    }
    inferred_row_count++;
  }

  void RefineExplicitColumnsFromCurrentRow() {
    for (const auto &entry : current_row) {
      auto column_index = column_indexes.find(entry.first);
      if (column_index == column_indexes.end()) {
        continue;
      }
      auto inferred_type = InferLogicalType(entry.second);
      if (inferred_type.id() == LogicalTypeId::SQLNULL) {
        continue;
      }

      auto &column = columns[column_index->second];
      auto current_type = column.duckdb_type.id();
      auto inferred_type_id = inferred_type.id();

      if (current_type == LogicalTypeId::VARCHAR) {
        column.duckdb_type = inferred_type;
        column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
        continue;
      }

      if ((current_type == LogicalTypeId::TIMESTAMP ||
           current_type == LogicalTypeId::TIMESTAMP_TZ ||
           current_type == LogicalTypeId::DATE) &&
          inferred_type_id == LogicalTypeId::TIME) {
        column.duckdb_type = LogicalType::TIME;
        column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
        continue;
      }

      if (current_type == LogicalTypeId::DATE &&
          (inferred_type_id == LogicalTypeId::TIMESTAMP ||
           inferred_type_id == LogicalTypeId::TIMESTAMP_TZ)) {
        column.duckdb_type = inferred_type;
        column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
        continue;
      }

      if ((current_type == LogicalTypeId::BIGINT ||
           current_type == LogicalTypeId::UBIGINT ||
           current_type == LogicalTypeId::DOUBLE ||
           current_type == LogicalTypeId::HUGEINT) &&
          (inferred_type_id == LogicalTypeId::BIGINT ||
           inferred_type_id == LogicalTypeId::UBIGINT ||
           inferred_type_id == LogicalTypeId::DOUBLE ||
           inferred_type_id == LogicalTypeId::HUGEINT)) {
        column.duckdb_type =
            MergeLogicalTypes(column.duckdb_type, inferred_type);
        column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
      }
    }
  }

  void ProcessCompletedElement(const XmlFrame &frame,
                               const std::string &parent_name) {
    if (parent_name == "row" && inside_schema == 0 && frame.name != "row") {
      auto cell_name = DecodeXMLName(frame.name);
      idx_t column_index = DConstants::INVALID_INDEX;
      if (schema_emitted) {
        auto entry = column_indexes.find(cell_name);
        if (entry != column_indexes.end()) {
          column_index = entry->second;
        }
      }

      Value value;
      auto nil_value = GetAttribute(frame.attributes, "nil");
      if (StringUtil::CIEquals(nil_value, "true")) {
        value = Value(nullptr);
      } else {
        auto coercion_kind = column_index == DConstants::INVALID_INDEX
                                 ? XmlaCoercionKind::INFER
                                 : columns[column_index].coercion_kind;
        value = CoerceXmlValue(frame.text, coercion_kind);
      }

      if (schema_emitted && column_index != DConstants::INVALID_INDEX &&
          !current_row_dense.empty()) {
        current_row_dense[column_index] = std::move(value);
      } else {
        current_row.emplace_back(cell_name, std::move(value));
      }
      return;
    }

    if (frame.name == "element" && inside_schema > 0 &&
        inside_schema_row_complex_type > 0) {
      XmlaColumn column;
      column.name = DecodeXMLName(GetAttribute(frame.attributes, "name"));
      column.source_type = GetAttribute(frame.attributes, "type");
      column.duckdb_type = MapXmlTypeToLogicalType(column.source_type);
      column.coercion_kind = CoercionKindFromXmlType(column.source_type);
      auto known_position = columns.size();
      if (known_position < known_column_types_by_index.size()) {
        column.duckdb_type = known_column_types_by_index[known_position];
        column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
      } else {
        auto known_column = known_column_types.find(column.name);
        if (known_column != known_column_types.end()) {
          column.duckdb_type = known_column->second;
          column.coercion_kind =
              CoercionKindFromLogicalType(column.duckdb_type);
        }
      }
      if (column.name.empty()) {
        column.name = "column_" + std::to_string(columns.size());
        column.coercion_kind = CoercionKindFromLogicalType(column.duckdb_type);
      }
      auto nillable = GetAttribute(frame.attributes, "nillable");
      auto min_occurs = GetAttribute(frame.attributes, "minOccurs");
      if (StringUtil::CIEquals(nillable, "true")) {
        column.nullable = true;
        column.nullable_known = true;
      } else if (!min_occurs.empty()) {
        column.nullable = min_occurs == "0";
        column.nullable_known = true;
      }
      columns.push_back(std::move(column));
      return;
    }

    if (frame.name == "schema") {
      if (stop_after_schema && !columns.empty()) {
        EmitSchemaIfNeeded();
      } else if (!stop_after_schema) {
        EmitSchemaIfNeeded();
      }
      return;
    }

    if (frame.name == "row" && inside_schema == 0) {
      if (!schema_emitted) {
        if (!columns.empty()) {
          RefineExplicitColumnsFromCurrentRow();
          EmitSchemaIfNeeded();
        } else {
          column_indexes.clear();
          UpdateInferredColumns();
          if (!stop_after_schema ||
              inferred_row_count >= MAX_SCHEMA_INFERENCE_ROWS) {
            EmitSchemaIfNeeded();
          }
        }
      }
      if (stopped_early) {
        current_row.clear();
        current_row_dense.clear();
        return;
      }

      if (schema_emitted && !current_row_dense.empty()) {
        if (on_row && !on_row(current_row_dense)) {
          stopped_early = true;
        }
        current_row.clear();
        return;
      }

      std::vector<Value> row(columns.size(), Value(nullptr));
      for (auto &entry : current_row) {
        auto index = column_indexes.find(entry.first);
        if (index == column_indexes.end()) {
          continue;
        }
        row[index->second] = std::move(entry.second);
      }
      current_row.clear();
      if (on_row && !on_row(row)) {
        stopped_early = true;
      }
      return;
    }

    if (frame.name == "faultcode") {
      fault_code = Trimmed(UnescapeXML(frame.text));
      return;
    }
    if (frame.name == "faultstring") {
      fault_message = Trimmed(UnescapeXML(frame.text));
      return;
    }
  }

  void HandleStartTag(const XmlTag &tag) {
    if (tag.name == "schema") {
      inside_schema++;
    }
    if (tag.name == "complexType" && inside_schema > 0 &&
        StringUtil::CIEquals(GetAttribute(tag.attributes, "name"), "row")) {
      inside_schema_row_complex_type++;
    }
    if (tag.name == "row" && inside_schema == 0) {
      current_row.clear();
      if (schema_emitted && !columns.empty()) {
        current_row_dense.assign(columns.size(), Value(nullptr));
      } else {
        current_row_dense.clear();
      }
    }
    stack.push_back(XmlFrame{tag.name, tag.attributes, std::string()});
  }

  void HandleTag(const XmlTag &tag) {
    if (tag.is_closing) {
      EndElement();
      return;
    }

    if (tag.is_self_closing) {
      EmptyElement(tag.name, tag.attributes);
      return;
    }

    HandleStartTag(tag);
  }

  bool stop_after_schema;
  bool schema_emitted = false;
  bool stopped_early = false;
  idx_t inferred_row_count = 0;
  idx_t inside_schema = 0;
  idx_t inside_schema_row_complex_type = 0;
  std::string pending;
  idx_t pending_offset = 0;
  std::string excerpt;
  std::string fault_code;
  std::string fault_message;
  std::vector<XmlFrame> stack;
  std::vector<XmlaColumn> columns;
  case_insensitive_map_t<idx_t> column_indexes;
  case_insensitive_map_t<LogicalType> known_column_types;
  std::vector<LogicalType> known_column_types_by_index;
  std::vector<std::pair<std::string, Value>> current_row;
  std::vector<Value> current_row_dense;
  std::function<void(const std::vector<XmlaColumn> &columns)> on_schema;
  std::function<bool(const std::vector<Value> &row)> on_row;
};

struct BinXmlSubstitution {
  uint8_t type = 0;
  bool is_null = false;
  std::string text;
  std::string binxml;
};

class BinXmlParser {
public:
  BinXmlParser(const_data_ptr_t data_p, idx_t size_p, XmlaStreamParser &sink_p,
               const std::vector<BinXmlSubstitution> *substitutions_p = nullptr,
               bool template_definition_p = false)
      : data(data_p), size(size_p), sink(sink_p),
        substitutions(substitutions_p),
        template_definition(template_definition_p) {}

  void ParseDocument() {
    while (offset < size) {
      auto token = ReadByte();
      if (token == EOF_TOKEN) {
        return;
      }
      ParseToken(token);
      if (sink.StoppedEarly()) {
        return;
      }
    }
  }

private:
  static constexpr uint8_t EOF_TOKEN = 0x00;
  static constexpr uint8_t OPEN_START_ELEMENT_TOKEN = 0x01;
  static constexpr uint8_t CLOSE_START_ELEMENT_TOKEN = 0x02;
  static constexpr uint8_t CLOSE_EMPTY_ELEMENT_TOKEN = 0x03;
  static constexpr uint8_t END_ELEMENT_TOKEN = 0x04;
  static constexpr uint8_t VALUE_TEXT_TOKEN = 0x05;
  static constexpr uint8_t ATTRIBUTE_TOKEN = 0x06;
  static constexpr uint8_t CHAR_REF_TOKEN = 0x08;
  static constexpr uint8_t ENTITY_REF_TOKEN = 0x09;
  static constexpr uint8_t TEMPLATE_INSTANCE_TOKEN = 0x0C;
  static constexpr uint8_t NORMAL_SUBSTITUTION_TOKEN = 0x0D;
  static constexpr uint8_t OPTIONAL_SUBSTITUTION_TOKEN = 0x0E;
  static constexpr uint8_t FRAGMENT_HEADER_TOKEN = 0x0F;
  static constexpr uint8_t MORE_BIT = 0x40;
  static constexpr uint8_t TOKEN_MASK = 0x1F;

  static constexpr uint8_t NULL_TYPE = 0x00;
  static constexpr uint8_t STRING_TYPE = 0x01;
  static constexpr uint8_t ANSI_STRING_TYPE = 0x02;
  static constexpr uint8_t INT8_TYPE = 0x03;
  static constexpr uint8_t UINT8_TYPE = 0x04;
  static constexpr uint8_t INT16_TYPE = 0x05;
  static constexpr uint8_t UINT16_TYPE = 0x06;
  static constexpr uint8_t INT32_TYPE = 0x07;
  static constexpr uint8_t UINT32_TYPE = 0x08;
  static constexpr uint8_t INT64_TYPE = 0x09;
  static constexpr uint8_t UINT64_TYPE = 0x0A;
  static constexpr uint8_t REAL32_TYPE = 0x0B;
  static constexpr uint8_t REAL64_TYPE = 0x0C;
  static constexpr uint8_t BOOL_TYPE = 0x0D;
  static constexpr uint8_t BINXML_TYPE = 0x21;

  static bool IsSsasRecordFamilyToken(uint8_t token) {
    switch (token) {
    case 0xF0:
    case 0xFD:
    case 0xFE:
    case 0xEF:
    case 0xF8:
    case 0xF6:
    case 0xF5:
    case 0xF7:
      return true;
    default:
      return false;
    }
  }

  static uint8_t BaseToken(uint8_t token) { return token & TOKEN_MASK; }
  static bool HasMore(uint8_t token) { return (token & MORE_BIT) != 0; }
  static idx_t SkipNestedDfPreamble(const std::string &payload) {
    if (payload.empty() || static_cast<uint8_t>(payload[0]) != 0xDF) {
      return 0;
    }
    idx_t nested_offset = 1;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
      if (nested_offset >= payload.size()) {
        return 1;
      }
      auto byte = static_cast<uint8_t>(payload[nested_offset++]);
      if ((byte & 0x80) == 0) {
        return nested_offset;
      }
    }
    return 1;
  }

  [[noreturn]] void Fail(const char *message) const {
    throw IOException("BINXML %s at offset %llu", message,
                      static_cast<unsigned long long>(offset));
  }

  [[noreturn]] void FailUnsupported(uint8_t token, idx_t token_offset) const {
    throw IOException("BINXML unsupported token 0x%02x at offset %llu", token,
                      static_cast<unsigned long long>(token_offset));
  }

  void Ensure(idx_t bytes) const {
    if (offset + bytes > size) {
      Fail("payload ended unexpectedly");
    }
  }

  uint8_t ReadByte() {
    Ensure(1);
    return data[offset++];
  }

  uint16_t ReadUInt16() {
    Ensure(2);
    auto value = static_cast<uint16_t>(data[offset]) |
                 static_cast<uint16_t>(data[offset + 1] << 8);
    offset += 2;
    return value;
  }

  uint32_t ReadUInt32() {
    Ensure(4);
    auto value = static_cast<uint32_t>(data[offset]) |
                 (static_cast<uint32_t>(data[offset + 1]) << 8) |
                 (static_cast<uint32_t>(data[offset + 2]) << 16) |
                 (static_cast<uint32_t>(data[offset + 3]) << 24);
    offset += 4;
    return value;
  }

  uint64_t ReadUInt64() {
    auto lo = static_cast<uint64_t>(ReadUInt32());
    auto hi = static_cast<uint64_t>(ReadUInt32());
    return lo | (hi << 32);
  }

  std::string ReadBytesAsString(idx_t byte_length) {
    Ensure(byte_length);
    std::string result(reinterpret_cast<const char *>(data + offset),
                       byte_length);
    offset += byte_length;
    return result;
  }

  static void AppendUtf8(std::string &result, uint32_t codepoint) {
    if (codepoint <= 0x7F) {
      result.push_back(static_cast<char>(codepoint));
    } else if (codepoint <= 0x7FF) {
      result.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else if (codepoint <= 0xFFFF) {
      result.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else {
      result.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }
  }

  static std::string DecodeUtf16Le(const_data_ptr_t input, idx_t code_units) {
    std::string result;
    result.reserve(code_units);
    for (idx_t i = 0; i < code_units; i++) {
      auto unit = static_cast<uint16_t>(input[i * 2]) |
                  static_cast<uint16_t>(input[i * 2 + 1] << 8);
      if (unit == 0 && i + 1 == code_units) {
        break;
      }
      if (unit >= 0xD800 && unit <= 0xDBFF && i + 1 < code_units) {
        auto next = static_cast<uint16_t>(input[(i + 1) * 2]) |
                    static_cast<uint16_t>(input[(i + 1) * 2 + 1] << 8);
        if (next >= 0xDC00 && next <= 0xDFFF) {
          auto codepoint =
              0x10000 + (((unit - 0xD800) << 10) | (next - 0xDC00));
          AppendUtf8(result, codepoint);
          i++;
          continue;
        }
      }
      AppendUtf8(result, unit);
    }
    return result;
  }

  std::string ReadUtf16CodeUnits(idx_t code_units) {
    Ensure(code_units * 2);
    auto result = DecodeUtf16Le(data + offset, code_units);
    offset += code_units * 2;
    return result;
  }

  std::string ReadLengthPrefixedUnicodeString() {
    auto code_units = ReadUInt16();
    return ReadUtf16CodeUnits(code_units);
  }

  std::string ReadName() {
    ReadUInt16(); // NameHash
    auto code_units = ReadUInt16();
    auto name = ReadUtf16CodeUnits(code_units);
    auto terminator = ReadUInt16();
    if (terminator != 0) {
      Fail("name was not null terminated");
    }
    return ExtractLocalName(name);
  }

  std::string FormatDouble(double value) const {
    std::ostringstream stream;
    stream << std::setprecision(17) << value;
    return stream.str();
  }

  std::string ReadSqlDateTimeValueText() {
    static constexpr int64_t MICROS_PER_DAY = 24LL * 60LL * 60LL * 1000000LL;
    static const int32_t SQL_DATETIME_BASE_EPOCH_DAYS =
        Date::EpochDays(Date::FromDate(1900, 1, 1));
    auto day_part = static_cast<int32_t>(ReadUInt32());
    auto tick_part = ReadUInt32();
    auto total_micros = static_cast<int64_t>(tick_part) * 1000000LL;
    auto micros = (total_micros + 150) / 300;
    auto day_adjust = micros / MICROS_PER_DAY;
    micros %= MICROS_PER_DAY;
    auto epoch_days = static_cast<int64_t>(SQL_DATETIME_BASE_EPOCH_DAYS) +
                      static_cast<int64_t>(day_part) + day_adjust;
    auto date_value = Date::EpochDaysToDate(static_cast<int32_t>(epoch_days));
    auto timestamp = Timestamp::FromDatetime(date_value, dtime_t(micros));
    return Timestamp::ToString(timestamp);
  }

  std::string ReadScalarValue(uint8_t type, idx_t byte_length, bool &is_null) {
    is_null = false;
    auto value_start = offset;
    switch (type) {
    case NULL_TYPE:
      is_null = true;
      return std::string();
    case STRING_TYPE: {
      if (byte_length % 2 != 0) {
        Fail("string value length was not UTF-16 aligned");
      }
      return ReadUtf16CodeUnits(byte_length / 2);
    }
    case ANSI_STRING_TYPE:
      return ReadBytesAsString(byte_length);
    case INT8_TYPE:
      return std::to_string(static_cast<int8_t>(ReadByte()));
    case UINT8_TYPE:
      return std::to_string(ReadByte());
    case INT16_TYPE:
      return std::to_string(static_cast<int16_t>(ReadUInt16()));
    case UINT16_TYPE:
      return std::to_string(ReadUInt16());
    case INT32_TYPE:
      return std::to_string(static_cast<int32_t>(ReadUInt32()));
    case UINT32_TYPE:
      return std::to_string(ReadUInt32());
    case INT64_TYPE:
      return std::to_string(static_cast<int64_t>(ReadUInt64()));
    case UINT64_TYPE:
      return std::to_string(ReadUInt64());
    case REAL32_TYPE: {
      auto raw = ReadUInt32();
      float value;
      std::memcpy(&value, &raw, sizeof(value));
      return FormatDouble(value);
    }
    case REAL64_TYPE: {
      auto raw = ReadUInt64();
      double value;
      std::memcpy(&value, &raw, sizeof(value));
      return FormatDouble(value);
    }
    case BOOL_TYPE:
      return ReadByte() ? "true" : "false";
    case BINXML_TYPE:
      return ReadBytesAsString(byte_length);
    default:
      throw IOException("BINXML unsupported value type 0x%02x at offset %llu",
                        type, static_cast<unsigned long long>(value_start));
    }
  }

  std::string ReadInlineValueText(uint8_t token) {
    auto string_type = ReadByte();
    if (string_type != STRING_TYPE) {
      throw IOException(
          "BINXML unsupported ValueText type 0x%02x at offset %llu",
          string_type, static_cast<unsigned long long>(offset - 1));
    }
    return ReadLengthPrefixedUnicodeString();
  }

  bool ReadSubstitutionValue(uint8_t token, std::string &text,
                             bool allow_binxml) {
    auto substitution_offset = offset - 1;
    auto index = ReadUInt16();
    ReadByte(); // ValueType from the template definition.
    if (!substitutions || index >= substitutions->size()) {
      throw IOException(
          "BINXML substitution %llu was not available at offset %llu",
          static_cast<unsigned long long>(index),
          static_cast<unsigned long long>(substitution_offset));
    }
    auto &value = (*substitutions)[index];
    if (value.is_null) {
      return BaseToken(token) != OPTIONAL_SUBSTITUTION_TOKEN;
    }
    if (value.type == BINXML_TYPE) {
      if (!allow_binxml) {
        throw IOException("BINXML nested binary XML substitution is not valid "
                          "here at offset %llu",
                          static_cast<unsigned long long>(substitution_offset));
      }
      auto nested_offset = SkipNestedDfPreamble(value.binxml);
      if (nested_offset >= value.binxml.size()) {
        throw IOException("BINXML nested substitution payload was empty at "
                          "offset %llu",
                          static_cast<unsigned long long>(substitution_offset));
      }
      BinXmlParser nested(
          const_data_ptr_cast(value.binxml.data() + nested_offset),
          value.binxml.size() - nested_offset, sink);
      nested.ParseDocument();
      return true;
    }
    text += value.text;
    return true;
  }

  bool ReadCharDataStartingWith(uint8_t token, std::string &text,
                                bool allow_binxml) {
    bool include = true;
    while (true) {
      auto base = BaseToken(token);
      if (base == VALUE_TEXT_TOKEN) {
        text += ReadInlineValueText(token);
      } else if (base == NORMAL_SUBSTITUTION_TOKEN ||
                 base == OPTIONAL_SUBSTITUTION_TOKEN) {
        include = ReadSubstitutionValue(token, text, allow_binxml) && include;
      } else if (base == CHAR_REF_TOKEN) {
        auto codepoint = ReadUInt16();
        AppendUtf8(text, codepoint);
      } else if (base == ENTITY_REF_TOKEN) {
        text += "&";
        text += ReadName();
        text += ";";
      } else {
        FailUnsupported(token, offset - 1);
      }
      if (!HasMore(token)) {
        return include;
      }
      token = ReadByte();
    }
  }

  case_insensitive_map_t<std::string> ReadAttributes() {
    auto attr_list_start = offset;
    auto attr_list_length = ReadUInt32();
    auto attr_list_end = offset + attr_list_length;
    if (attr_list_end > size) {
      Fail("attribute list exceeded payload size");
    }
    case_insensitive_map_t<std::string> attributes;
    while (offset < attr_list_end) {
      auto token_offset = offset;
      auto token = ReadByte();
      if (BaseToken(token) != ATTRIBUTE_TOKEN) {
        FailUnsupported(token, token_offset);
      }
      auto name = ReadName();
      std::string value;
      bool include = true;
      if (offset < attr_list_end) {
        auto value_token = ReadByte();
        include = ReadCharDataStartingWith(value_token, value, false);
      }
      if (include) {
        attributes[ExtractLocalName(name)] = value;
      }
      if (!HasMore(token)) {
        break;
      }
    }
    if (offset != attr_list_end) {
      offset = attr_list_end;
    }
    (void)attr_list_start;
    return attributes;
  }

  void ParseStartElement(uint8_t token) {
    auto has_attributes = HasMore(token);
    idx_t dependency_id = 0xFFFF;
    if (template_definition) {
      dependency_id = ReadUInt16();
    }
    auto element_length = ReadUInt32();
    auto element_start = offset;
    if (template_definition && dependency_id != 0xFFFF && substitutions &&
        dependency_id < substitutions->size() &&
        (*substitutions)[dependency_id].is_null) {
      offset = element_start + element_length;
      if (offset > size) {
        Fail("dependent element exceeded payload size");
      }
      return;
    }

    auto name = ReadName();
    case_insensitive_map_t<std::string> attributes;
    if (has_attributes) {
      attributes = ReadAttributes();
    }
    auto close_token = ReadByte();
    if (close_token == CLOSE_EMPTY_ELEMENT_TOKEN) {
      sink.EmptyElement(name, std::move(attributes));
      return;
    }
    if (close_token != CLOSE_START_ELEMENT_TOKEN) {
      FailUnsupported(close_token, offset - 1);
    }
    sink.StartElement(name, std::move(attributes));
    while (offset < size && !sink.StoppedEarly()) {
      auto child_offset = offset;
      auto child_token = ReadByte();
      auto base = BaseToken(child_token);
      if (base == END_ELEMENT_TOKEN) {
        sink.EndElement();
        return;
      }
      if (base == OPEN_START_ELEMENT_TOKEN) {
        ParseStartElement(child_token);
        continue;
      }
      if (base == VALUE_TEXT_TOKEN || base == NORMAL_SUBSTITUTION_TOKEN ||
          base == OPTIONAL_SUBSTITUTION_TOKEN || base == CHAR_REF_TOKEN ||
          base == ENTITY_REF_TOKEN) {
        std::string text;
        auto include = ReadCharDataStartingWith(child_token, text, true);
        if (include) {
          sink.Text(text);
        }
        continue;
      }
      if (base == TEMPLATE_INSTANCE_TOKEN) {
        ParseTemplateInstance();
        continue;
      }
      FailUnsupported(child_token, child_offset);
    }
  }

  void ParseTemplateInstance() {
    auto marker = ReadByte();
    if (marker != 0xB0) {
      throw IOException(
          "BINXML expected template definition marker at offset %llu",
          static_cast<unsigned long long>(offset - 1));
    }
    Ensure(16);
    offset += 16; // TemplateId GUID
    auto template_length = ReadUInt32();
    auto template_start = offset;
    auto template_end = template_start + template_length;
    if (template_end > size) {
      Fail("template definition exceeded payload size");
    }
    offset = template_end;

    auto value_count = ReadUInt32();
    struct ValueSpecEntry {
      uint16_t byte_length;
      uint8_t type;
    };
    std::vector<ValueSpecEntry> specs;
    specs.reserve(value_count);
    for (uint32_t i = 0; i < value_count; i++) {
      ValueSpecEntry entry;
      entry.byte_length = ReadUInt16();
      entry.type = ReadByte();
      auto terminator = ReadByte();
      if (terminator != 0) {
        Fail("template value spec was not null terminated");
      }
      specs.push_back(entry);
    }

    std::vector<BinXmlSubstitution> values;
    values.reserve(value_count);
    for (auto &spec : specs) {
      BinXmlSubstitution value;
      value.type = spec.type;
      bool is_null = false;
      if (spec.type == BINXML_TYPE) {
        value.binxml = ReadBytesAsString(spec.byte_length);
      } else {
        value.text = ReadScalarValue(spec.type, spec.byte_length, is_null);
        value.is_null = is_null;
      }
      values.push_back(std::move(value));
    }

    BinXmlParser template_parser(data + template_start, template_length, sink,
                                 &values, true);
    template_parser.ParseDocument();
  }

  void ParseFragmentHeader() {
    auto major = ReadByte();
    auto minor = ReadByte();
    auto flags = ReadByte();
    if (major != 1 || minor != 1 || flags != 0) {
      throw IOException("BINXML unsupported fragment header %u.%u flags 0x%02x "
                        "at offset %llu",
                        major, minor, flags,
                        static_cast<unsigned long long>(offset - 3));
    }
  }

  void ParseToken(uint8_t token) {
    auto token_offset = offset - 1;
    if (IsSsasRecordFamilyToken(token)) {
      throw IOException(
          "BINXML encountered SSAS record token 0x%02x at offset %llu", token,
          static_cast<unsigned long long>(token_offset));
    }
    auto base = BaseToken(token);
    if (base == FRAGMENT_HEADER_TOKEN) {
      ParseFragmentHeader();
      return;
    }
    if (base == OPEN_START_ELEMENT_TOKEN) {
      ParseStartElement(token);
      return;
    }
    if (base == TEMPLATE_INSTANCE_TOKEN) {
      ParseTemplateInstance();
      return;
    }
    FailUnsupported(token, token_offset);
  }

  const_data_ptr_t data;
  idx_t size;
  idx_t offset = 0;
  XmlaStreamParser &sink;
  const std::vector<BinXmlSubstitution> *substitutions;
  bool template_definition;
};

class SsasBinaryXmlParser {
public:
  SsasBinaryXmlParser(const_data_ptr_t data_p, idx_t size_p,
                      XmlaStreamParser &sink_p)
      : data(data_p), size(size_p), sink(sink_p) {}

  void ParseDocument() {
    if (PeekByte() == 0xDF) {
      ReadByte();
      ReadVarUInt();
    }
    if (PeekByte() == 0xB0) {
      ReadByte();
      ReadByte();
    }
    while (offset < size && !sink.StoppedEarly()) {
      ParseRecord();
    }
    FlushPendingStart();
  }

private:
  static constexpr uint8_t EMPTY_TEXT_TOKEN = 0x86;
  static constexpr idx_t MEASURE_TRACE_LIMIT = 200;

  struct ExpandedName {
    std::string prefix;
    std::string local_name;
    std::string uri;
  };

  static bool DebugSsasMeasuresEnabled() {
    auto *value = std::getenv("PBI_SCANNER_DEBUG_SSAS_MEASURES");
    return value && *value;
  }

  [[noreturn]] void Fail(const char *message) const {
    throw IOException("SSAS binary XML %s at offset %llu", message,
                      static_cast<unsigned long long>(offset));
  }

  [[noreturn]] void FailUnsupported(uint8_t token, idx_t token_offset) const {
    throw IOException("SSAS binary XML unsupported token 0x%02x at offset %llu",
                      token, static_cast<unsigned long long>(token_offset));
  }

  void Ensure(idx_t bytes) const {
    if (offset + bytes > size) {
      Fail("payload ended unexpectedly");
    }
  }

  uint8_t PeekByte() const {
    if (offset >= size) {
      return 0;
    }
    return data[offset];
  }

  uint8_t ReadByte() {
    Ensure(1);
    return data[offset++];
  }

  uint32_t ReadVarUInt() {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
      auto byte = ReadByte();
      result |= static_cast<uint32_t>(byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) {
        return result;
      }
    }
    Fail("variable integer was too large");
  }

  static bool TryReadVarUIntAt(const_data_ptr_t input, idx_t input_size,
                               idx_t start, uint32_t &value_out,
                               idx_t &next_offset_out) {
    if (start >= input_size) {
      return false;
    }
    uint32_t value = 0;
    idx_t pos = start;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
      if (pos >= input_size) {
        return false;
      }
      auto byte = static_cast<uint8_t>(input[pos++]);
      value |= static_cast<uint32_t>(byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) {
        value_out = value;
        next_offset_out = pos;
        return true;
      }
    }
    return false;
  }

  uint64_t ReadUInt64() {
    Ensure(8);
    uint64_t result = 0;
    for (idx_t i = 0; i < 8; i++) {
      result |= static_cast<uint64_t>(data[offset + i]) << (i * 8);
    }
    offset += 8;
    return result;
  }

  uint32_t ReadUInt32() {
    Ensure(4);
    auto result = static_cast<uint32_t>(data[offset]) |
                  (static_cast<uint32_t>(data[offset + 1]) << 8) |
                  (static_cast<uint32_t>(data[offset + 2]) << 16) |
                  (static_cast<uint32_t>(data[offset + 3]) << 24);
    offset += 4;
    return result;
  }

  double ReadDouble() {
    auto raw = ReadUInt64();
    double value;
    std::memcpy(&value, &raw, sizeof(value));
    return value;
  }

  static void AppendUtf8(std::string &result, uint32_t codepoint) {
    if (codepoint <= 0x7F) {
      result.push_back(static_cast<char>(codepoint));
    } else if (codepoint <= 0x7FF) {
      result.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else if (codepoint <= 0xFFFF) {
      result.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else {
      result.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }
  }

  std::string FormatDouble(double value) const {
    std::ostringstream stream;
    stream << std::setprecision(17) << value;
    return stream.str();
  }

  std::string ReadSqlDateTimeValueText() {
    static constexpr int64_t MICROS_PER_DAY = 24LL * 60LL * 60LL * 1000000LL;
    static const int32_t SQL_DATETIME_BASE_EPOCH_DAYS =
        Date::EpochDays(Date::FromDate(1900, 1, 1));
    auto day_part = static_cast<int32_t>(ReadUInt32());
    auto tick_part = ReadUInt32();
    auto total_micros = static_cast<int64_t>(tick_part) * 1000000LL;
    auto micros = (total_micros + 150) / 300;
    auto day_adjust = micros / MICROS_PER_DAY;
    micros %= MICROS_PER_DAY;
    auto epoch_days = static_cast<int64_t>(SQL_DATETIME_BASE_EPOCH_DAYS) +
                      static_cast<int64_t>(day_part) + day_adjust;
    auto date_value = Date::EpochDaysToDate(static_cast<int32_t>(epoch_days));
    auto timestamp = Timestamp::FromDatetime(date_value, dtime_t(micros));
    return Timestamp::ToString(timestamp);
  }

  std::string ReadUtf16String(idx_t code_units, bool consume_null) {
    Ensure(code_units * 2);
    std::string result;
    result.reserve(code_units);
    for (idx_t i = 0; i < code_units; i++) {
      auto unit = static_cast<uint16_t>(data[offset + i * 2]) |
                  static_cast<uint16_t>(data[offset + i * 2 + 1] << 8);
      if (unit >= 0xD800 && unit <= 0xDBFF && i + 1 < code_units) {
        auto next = static_cast<uint16_t>(data[offset + (i + 1) * 2]) |
                    static_cast<uint16_t>(data[offset + (i + 1) * 2 + 1] << 8);
        if (next >= 0xDC00 && next <= 0xDFFF) {
          auto codepoint =
              0x10000 + (((unit - 0xD800) << 10) | (next - 0xDC00));
          AppendUtf8(result, codepoint);
          i++;
          continue;
        }
      }
      AppendUtf8(result, unit);
    }
    offset += code_units * 2;
    if (consume_null && offset + 1 < size && data[offset] == 0 &&
        data[offset + 1] == 0) {
      offset += 2;
    } else if (consume_null && offset < size && data[offset] == 0) {
      offset++;
    }
    return result;
  }

  std::string InternString(uint8_t token) {
    auto length = ReadVarUInt();
    auto value = ReadUtf16String(length, token == 0xFD);
    if (token == 0xF0) {
      strings.push_back(value);
    }
    return value;
  }

  const std::string &LookupString(uint32_t id) const {
    if (id == 0 || id > strings.size()) {
      throw IOException("SSAS binary XML string id %llu was not defined",
                        static_cast<unsigned long long>(id));
    }
    return strings[id - 1];
  }

  const ExpandedName &LookupName(uint32_t id) const {
    auto mapped = names_by_id.find(id);
    if (mapped != names_by_id.end()) {
      return mapped->second;
    }
    if (id == 0 || id > names.size()) {
      throw IOException("SSAS binary XML name id %llu was not defined",
                        static_cast<unsigned long long>(id));
    }
    return names[id - 1];
  }

  void DefineName() {
    ExpandedName name;
    auto first = ReadVarUInt();
    uint32_t name_id = static_cast<uint32_t>(names.size() + 1);
    uint32_t uri_id;
    uint32_t prefix_id;
    uint32_t local_id;
    if (first > strings.size() + 1) {
      // Some SSAS streams emit an explicit sparse NameId before
      // URI/Prefix/Local.
      name_id = first;
      uri_id = ReadVarUInt();
      prefix_id = ReadVarUInt();
      local_id = ReadVarUInt();
    } else {
      uri_id = first;
      prefix_id = ReadVarUInt();
      local_id = ReadVarUInt();
    }
    if (prefix_id != 0) {
      name.prefix = LookupString(prefix_id);
    }
    if (local_id != 0) {
      name.local_name = LookupString(local_id);
    } else if (prefix_id != 0) {
      name.local_name = LookupString(prefix_id);
    }
    if (uri_id != 0) {
      name.uri = LookupString(uri_id);
    }
    names.push_back(name);
    names_by_id[name_id] = std::move(name);
  }

  std::string NameText(uint32_t name_id) const {
    const auto &name = LookupName(name_id);
    return ExtractLocalName(name.local_name);
  }

  void FlushPendingStart() {
    if (!pending_start) {
      return;
    }
    sink.StartElement(std::move(pending_name), std::move(pending_attributes));
    pending_attributes.clear();
    pending_name.clear();
    pending_start = false;
  }

  std::string ReadInlineUtf16Value() {
    auto length = ReadVarUInt();
    return ReadUtf16String(length, false);
  }

  bool IsLikelyRecordToken(uint8_t token) const {
    switch (token) {
    case 0x00:
    case 0x01:
    case 0x04:
    case 0x08:
    case 0x0E:
    case 0x10:
    case 0x11:
    case 0x12:
    case 0x13:
    case 0x14:
    case 0x15:
    case 0x16:
    case 0x17:
    case 0x18:
    case 0x19:
    case 0x1A:
    case 0x1B:
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1:
    case 0xEF:
    case 0xF0:
    case 0xF8:
    case 0xF9:
    case 0xFA:
    case 0xFB:
    case 0xFD:
    case 0xFE:
    case 0xFF:
      return true;
    default:
      return false;
    }
  }

  bool IsLikelyUtf16FramedTokenValue() const {
    static constexpr uint32_t MAX_INLINE_UTF16_CODE_UNITS = 4096;
    uint32_t maybe_length = 0;
    idx_t next_offset = offset;
    if (!TryReadVarUIntAt(data, size, offset, maybe_length, next_offset)) {
      return false;
    }
    if (maybe_length > MAX_INLINE_UTF16_CODE_UNITS) {
      return false;
    }
    auto payload_end = next_offset + static_cast<idx_t>(maybe_length) * 2;
    if (payload_end > size) {
      return false;
    }
    if (payload_end == size) {
      return true;
    }
    return IsLikelyRecordToken(static_cast<uint8_t>(data[payload_end]));
  }

  std::string ReadTextValue(uint8_t token) {
    switch (token) {
    case 0x04:
      return FormatDouble(ReadDouble());
    case 0x08:
      return std::to_string(static_cast<int64_t>(ReadUInt64()));
    case 0x10:
    case 0x0E:
    case 0x11:
      return ReadInlineUtf16Value();
    case 0x12:
      return ReadSqlDateTimeValueText();
    case 0x13: {
      Ensure(2);
      auto value = static_cast<int16_t>(data[offset] | (data[offset + 1] << 8));
      offset += 2;
      return std::to_string(value);
    }
    case 0x14: {
      Ensure(4);
      auto value =
          static_cast<int32_t>(static_cast<uint32_t>(data[offset]) |
                               (static_cast<uint32_t>(data[offset + 1]) << 8) |
                               (static_cast<uint32_t>(data[offset + 2]) << 16) |
                               (static_cast<uint32_t>(data[offset + 3]) << 24));
      offset += 4;
      return std::to_string(value);
    }
    case 0x15:
      return FormatDouble(ReadDouble());
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1: {
      if (IsLikelyUtf16FramedTokenValue()) {
        return ReadInlineUtf16Value();
      }
      return FormatDouble(ReadDouble());
    }
    case 0x18:
      return std::to_string(ReadByte());
    case 0x19: {
      Ensure(2);
      auto value =
          static_cast<uint16_t>(data[offset] | (data[offset + 1] << 8));
      offset += 2;
      return std::to_string(value);
    }
    case 0x1A: {
      Ensure(4);
      auto value = static_cast<uint32_t>(data[offset]) |
                   (static_cast<uint32_t>(data[offset + 1]) << 8) |
                   (static_cast<uint32_t>(data[offset + 2]) << 16) |
                   (static_cast<uint32_t>(data[offset + 3]) << 24);
      offset += 4;
      return std::to_string(value);
    }
    case 0x1B:
      return std::to_string(ReadUInt64());
    case 0x16:
      return "true";
    case 0x17:
      return "false";
    case EMPTY_TEXT_TOKEN:
      return std::string();
    default:
      FailUnsupported(token, offset - 1);
    }
  }

  void ParseAttribute() {
    auto name_id = ReadVarUInt();
    auto value_token = ReadByte();
    pending_attributes[NameText(name_id)] = ReadTextValue(value_token);
  }

  void ParseStartElement() {
    FlushPendingStart();
    pending_name = NameText(ReadVarUInt());
    last_started_name = pending_name;
    pending_attributes.clear();
    pending_start = true;
  }

  void ParseRecord() {
    auto token_offset = offset;
    auto token = ReadByte();
    switch (token) {
    case 0x00:
      return;
    case 0xF0:
    case 0xFD:
    case 0xFE:
      InternString(token);
      return;
    case 0xEF:
      DefineName();
      return;
    case 0xF8:
      ParseStartElement();
      return;
    case 0x01:
      // 0x01 can appear both as a compact start-element token and as a control
      // marker in some strict SX metadata payloads. Disambiguate by probing the
      // following varuint as a valid known name id.
      {
        uint32_t maybe_name_id = 0;
        idx_t next_offset = offset;
        if (TryReadVarUIntAt(data, size, offset, maybe_name_id, next_offset) &&
            (names_by_id.find(maybe_name_id) != names_by_id.end() ||
             (maybe_name_id >= 1 && maybe_name_id <= names.size()))) {
          ParseStartElement();
          return;
        }
      }
      FlushPendingStart();
      return;
    case 0xF6:
      ParseAttribute();
      return;
    case 0xF5:
      FlushPendingStart();
      return;
    case 0xF7:
      FlushPendingStart();
      sink.EndElement();
      return;
    case 0x04:
    case 0x08:
    case 0x10:
    case 0x0E:
    case 0x11:
    case 0x12:
    case 0x13:
    case 0x14:
    case 0x15:
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1:
    case 0x18:
    case 0x19:
    case 0x1A:
    case 0x1B:
    case 0x16:
    case 0x17:
    case EMPTY_TEXT_TOKEN: {
      auto cell_name = pending_start ? pending_name : last_started_name;
      FlushPendingStart();
      auto text = ReadTextValue(token);
      if (DebugSsasMeasuresEnabled() && !cell_name.empty() &&
          measure_trace_count < MEASURE_TRACE_LIMIT) {
        std::fprintf(stderr,
                     "[pbi_scanner] SSAS row cell=%s token=0x%02x "
                     "text_len=%llu text=\"%s\"\n",
                     cell_name.c_str(), static_cast<unsigned int>(token),
                     static_cast<unsigned long long>(text.size()),
                     text.c_str());
        measure_trace_count++;
      }
      sink.Text(text);
    }
      return;
    default:
      FailUnsupported(token, token_offset);
    }
  }

  const_data_ptr_t data;
  idx_t size;
  idx_t offset = 0;
  XmlaStreamParser &sink;
  std::vector<std::string> strings;
  std::vector<ExpandedName> names;
  std::unordered_map<uint32_t, ExpandedName> names_by_id;
  idx_t measure_trace_count = 0;
  bool pending_start = false;
  std::string pending_name;
  std::string last_started_name;
  case_insensitive_map_t<std::string> pending_attributes;
};

class SsasFastRowParser {
public:
  SsasFastRowParser(
      const_data_ptr_t data_p, idx_t size_p,
      const std::vector<XmlaColumn> &columns_p,
      const std::function<bool(const std::vector<Value> &row)> &on_row_p,
      const std::function<bool()> &should_stop_p)
      : data(data_p), size(size_p), columns(columns_p), on_row(on_row_p),
        should_stop(should_stop_p) {
    auto *debug_value = std::getenv("PBI_SCANNER_DEBUG_SSAS_MEASURES");
    debug_trace_enabled = debug_value && *debug_value;
    for (idx_t i = 0; i < columns.size(); i++) {
      column_indexes[columns[i].name] = i;
      column_indexes_lower[StringUtil::Lower(columns[i].name)] = i;
    }
  }

  void ParseDocument() {
    if (PeekByte() == 0xDF) {
      ReadByte();
      ReadVarUInt();
    }
    if (PeekByte() == 0xB0) {
      ReadByte();
      ReadByte();
    }
    while (offset < size && !stopped_early) {
      if (should_stop && should_stop()) {
        stopped_early = true;
        return;
      }
      ParseRecord();
    }
    FlushPendingStart();
  }

  idx_t ProducedRows() const { return produced_rows; }

private:
  enum class ElementKind : uint8_t { OTHER, SCHEMA, ROW };
  static constexpr uint8_t EMPTY_TEXT_TOKEN = 0x86;
  static constexpr idx_t CELL_TRACE_LIMIT = 80;

  struct ExpandedName {
    std::string prefix;
    std::string local_name;
    std::string uri;
    idx_t column_index = DConstants::INVALID_INDEX;
  };

  struct StackEntry {
    uint32_t name_id;
    ElementKind kind;
  };

  [[noreturn]] void Fail(const char *message) const {
    throw IOException("SSAS fast row parser %s at offset %llu", message,
                      static_cast<unsigned long long>(offset));
  }

  [[noreturn]] void FailUnsupported(uint8_t token, idx_t token_offset) const {
    throw IOException(
        "SSAS fast row parser unsupported token 0x%02x at offset %llu", token,
        static_cast<unsigned long long>(token_offset));
  }

  void Ensure(idx_t bytes) const {
    if (offset + bytes > size) {
      Fail("payload ended unexpectedly");
    }
  }

  uint8_t PeekByte() const {
    if (offset >= size) {
      return 0;
    }
    return data[offset];
  }

  uint8_t ReadByte() {
    Ensure(1);
    return data[offset++];
  }

  uint32_t ReadVarUInt() {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
      auto byte = ReadByte();
      result |= static_cast<uint32_t>(byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) {
        return result;
      }
    }
    Fail("variable integer was too large");
  }

  static bool TryReadVarUIntAt(const_data_ptr_t input, idx_t input_size,
                               idx_t start, uint32_t &value_out,
                               idx_t &next_offset_out) {
    if (start >= input_size) {
      return false;
    }
    uint32_t value = 0;
    idx_t pos = start;
    for (uint32_t shift = 0; shift <= 28; shift += 7) {
      if (pos >= input_size) {
        return false;
      }
      auto byte = static_cast<uint8_t>(input[pos++]);
      value |= static_cast<uint32_t>(byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) {
        value_out = value;
        next_offset_out = pos;
        return true;
      }
    }
    return false;
  }

  uint16_t ReadUInt16() {
    Ensure(2);
    auto value = static_cast<uint16_t>(data[offset]) |
                 static_cast<uint16_t>(data[offset + 1] << 8);
    offset += 2;
    return value;
  }

  uint32_t ReadUInt32() {
    Ensure(4);
    auto value = static_cast<uint32_t>(data[offset]) |
                 (static_cast<uint32_t>(data[offset + 1]) << 8) |
                 (static_cast<uint32_t>(data[offset + 2]) << 16) |
                 (static_cast<uint32_t>(data[offset + 3]) << 24);
    offset += 4;
    return value;
  }

  uint64_t ReadUInt64() {
    Ensure(8);
    uint64_t result = 0;
    for (idx_t i = 0; i < 8; i++) {
      result |= static_cast<uint64_t>(data[offset + i]) << (i * 8);
    }
    offset += 8;
    return result;
  }

  double ReadDouble() {
    auto raw = ReadUInt64();
    double value;
    std::memcpy(&value, &raw, sizeof(value));
    return value;
  }

  static void AppendUtf8(std::string &result, uint32_t codepoint) {
    if (codepoint <= 0x7F) {
      result.push_back(static_cast<char>(codepoint));
    } else if (codepoint <= 0x7FF) {
      result.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else if (codepoint <= 0xFFFF) {
      result.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else {
      result.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
      result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }
  }

  std::string FormatDouble(double value) const {
    std::ostringstream stream;
    stream << std::setprecision(17) << value;
    return stream.str();
  }

  std::string ReadSqlDateTimeValueText() {
    static constexpr int64_t MICROS_PER_DAY = 24LL * 60LL * 60LL * 1000000LL;
    static const int32_t SQL_DATETIME_BASE_EPOCH_DAYS =
        Date::EpochDays(Date::FromDate(1900, 1, 1));
    auto day_part = static_cast<int32_t>(ReadUInt32());
    auto tick_part = ReadUInt32();
    auto total_micros = static_cast<int64_t>(tick_part) * 1000000LL;
    auto micros = (total_micros + 150) / 300;
    auto day_adjust = micros / MICROS_PER_DAY;
    micros %= MICROS_PER_DAY;
    auto epoch_days = static_cast<int64_t>(SQL_DATETIME_BASE_EPOCH_DAYS) +
                      static_cast<int64_t>(day_part) + day_adjust;
    auto date_value = Date::EpochDaysToDate(static_cast<int32_t>(epoch_days));
    auto timestamp = Timestamp::FromDatetime(date_value, dtime_t(micros));
    return Timestamp::ToString(timestamp);
  }

  std::string ReadUtf16String(idx_t code_units, bool consume_null) {
    Ensure(code_units * 2);
    std::string result;
    result.reserve(code_units);
    for (idx_t i = 0; i < code_units; i++) {
      auto unit = static_cast<uint16_t>(data[offset + i * 2]) |
                  static_cast<uint16_t>(data[offset + i * 2 + 1] << 8);
      if (unit >= 0xD800 && unit <= 0xDBFF && i + 1 < code_units) {
        auto next = static_cast<uint16_t>(data[offset + (i + 1) * 2]) |
                    static_cast<uint16_t>(data[offset + (i + 1) * 2 + 1] << 8);
        if (next >= 0xDC00 && next <= 0xDFFF) {
          auto codepoint =
              0x10000 + (((unit - 0xD800) << 10) | (next - 0xDC00));
          AppendUtf8(result, codepoint);
          i++;
          continue;
        }
      }
      AppendUtf8(result, unit);
    }
    offset += code_units * 2;
    if (consume_null && offset + 1 < size && data[offset] == 0 &&
        data[offset + 1] == 0) {
      offset += 2;
    } else if (consume_null && offset < size && data[offset] == 0) {
      offset++;
    }
    return result;
  }

  std::string InternString(uint8_t token) {
    auto length = ReadVarUInt();
    auto value = ReadUtf16String(length, token == 0xFD);
    if (token == 0xF0) {
      strings.push_back(value);
    }
    return value;
  }

  const std::string &LookupString(uint32_t id) const {
    if (id == 0 || id > strings.size()) {
      throw IOException("SSAS fast row parser string id %llu was not defined",
                        static_cast<unsigned long long>(id));
    }
    return strings[id - 1];
  }

  const ExpandedName &LookupName(uint32_t id) const {
    auto mapped = names_by_id.find(id);
    if (mapped != names_by_id.end()) {
      return mapped->second;
    }
    if (id == 0 || id > names.size()) {
      throw IOException("SSAS fast row parser name id %llu was not defined",
                        static_cast<unsigned long long>(id));
    }
    return names[id - 1];
  }

  ExpandedName &LookupNameMutable(uint32_t id) {
    auto mapped = names_by_id.find(id);
    if (mapped != names_by_id.end()) {
      return mapped->second;
    }
    if (id == 0 || id > names.size()) {
      throw IOException("SSAS fast row parser name id %llu was not defined",
                        static_cast<unsigned long long>(id));
    }
    return names[id - 1];
  }

  idx_t ResolveColumnIndex(const std::string &name) const {
    auto exact = column_indexes.find(name);
    if (exact != column_indexes.end()) {
      return exact->second;
    }
    auto lowered = column_indexes_lower.find(StringUtil::Lower(name));
    if (lowered != column_indexes_lower.end()) {
      return lowered->second;
    }
    return DConstants::INVALID_INDEX;
  }

  void DefineName() {
    ExpandedName name;
    auto first = ReadVarUInt();
    uint32_t name_id = static_cast<uint32_t>(names.size() + 1);
    uint32_t uri_id;
    uint32_t prefix_id;
    uint32_t local_id;
    if (first > strings.size() + 1) {
      name_id = first;
      uri_id = ReadVarUInt();
      prefix_id = ReadVarUInt();
      local_id = ReadVarUInt();
    } else {
      uri_id = first;
      prefix_id = ReadVarUInt();
      local_id = ReadVarUInt();
    }
    if (prefix_id != 0) {
      name.prefix = LookupString(prefix_id);
    }
    if (local_id != 0) {
      name.local_name = DecodeXMLName(ExtractLocalName(LookupString(local_id)));
    } else if (prefix_id != 0) {
      name.local_name =
          DecodeXMLName(ExtractLocalName(LookupString(prefix_id)));
    }
    if (uri_id != 0) {
      name.uri = LookupString(uri_id);
    }
    name.column_index = ResolveColumnIndex(name.local_name);
    names.push_back(name);
    names_by_id[name_id] = std::move(name);
  }

  idx_t ResolveCellColumnIndex(uint32_t name_id) const {
    const auto &name = LookupName(name_id);
    if (name.column_index != DConstants::INVALID_INDEX) {
      return name.column_index;
    }
    return DConstants::INVALID_INDEX;
  }

  static bool IsCompactCellAlias(const std::string &name) {
    if (name.size() < 2 || (name[0] != 'C' && name[0] != 'c')) {
      return false;
    }
    for (idx_t i = 1; i < name.size(); i++) {
      if (name[i] < '0' || name[i] > '9') {
        return false;
      }
    }
    return true;
  }

  idx_t ResolveRuntimeCellColumnIndex(uint32_t name_id) {
    auto resolved = ResolveCellColumnIndex(name_id);
    if (resolved != DConstants::INVALID_INDEX) {
      return resolved;
    }
    auto &name = LookupNameMutable(name_id);
    if (!IsCompactCellAlias(name.local_name)) {
      return DConstants::INVALID_INDEX;
    }
    auto entry = runtime_cell_indexes.find(name.local_name);
    if (entry != runtime_cell_indexes.end()) {
      name.column_index = entry->second;
      return entry->second;
    }
    if (runtime_column_count >= columns.size()) {
      return DConstants::INVALID_INDEX;
    }
    auto column_index = runtime_column_count++;
    runtime_cell_indexes[name.local_name] = column_index;
    name.column_index = column_index;
    return column_index;
  }

  void StartElement(uint32_t name_id) {
    const auto &name = LookupName(name_id);
    auto kind = ElementKind::OTHER;
    if (name.local_name == "schema") {
      kind = ElementKind::SCHEMA;
      inside_schema++;
    } else if (name.local_name == "row" && inside_schema == 0) {
      kind = ElementKind::ROW;
      current_row.assign(columns.size(), Value(nullptr));
    }
    stack.push_back(StackEntry{name_id, kind});
  }

  void FlushPendingStart() {
    if (!pending_start) {
      return;
    }
    StartElement(pending_name_id);
    pending_start = false;
    pending_name_id = 0;
  }

  void EndElement() {
    FlushPendingStart();
    if (stack.empty()) {
      return;
    }
    auto entry = stack.back();
    stack.pop_back();
    if (entry.kind == ElementKind::ROW) {
      if (on_row && !on_row(current_row)) {
        stopped_early = true;
      }
      produced_rows++;
      current_row.clear();
      return;
    }
    if (entry.kind == ElementKind::SCHEMA && inside_schema > 0) {
      inside_schema--;
      return;
    }
  }

  std::string ReadInlineUtf16Value() {
    auto length = ReadVarUInt();
    return ReadUtf16String(length, false);
  }

  bool IsLikelyRecordToken(uint8_t token) const {
    switch (token) {
    case 0x00:
    case 0x01:
    case 0x04:
    case 0x08:
    case 0x0E:
    case 0x10:
    case 0x11:
    case 0x12:
    case 0x13:
    case 0x14:
    case 0x15:
    case 0x16:
    case 0x17:
    case 0x18:
    case 0x19:
    case 0x1A:
    case 0x1B:
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1:
    case 0xEF:
    case 0xF0:
    case 0xF8:
    case 0xF9:
    case 0xFA:
    case 0xFB:
    case 0xFD:
    case 0xFE:
    case 0xFF:
      return true;
    default:
      return false;
    }
  }

  bool IsLikelyUtf16FramedTokenValue() const {
    static constexpr uint32_t MAX_INLINE_UTF16_CODE_UNITS = 4096;
    uint32_t maybe_length = 0;
    idx_t next_offset = offset;
    if (!TryReadVarUIntAt(data, size, offset, maybe_length, next_offset)) {
      return false;
    }
    if (maybe_length > MAX_INLINE_UTF16_CODE_UNITS) {
      return false;
    }
    auto payload_end = next_offset + static_cast<idx_t>(maybe_length) * 2;
    if (payload_end > size) {
      return false;
    }
    if (payload_end == size) {
      return true;
    }
    return IsLikelyRecordToken(static_cast<uint8_t>(data[payload_end]));
  }

  std::string ReadTextValue(uint8_t token) {
    switch (token) {
    case 0x04:
      return FormatDouble(ReadDouble());
    case 0x08:
      return std::to_string(static_cast<int64_t>(ReadUInt64()));
    case 0x10:
    case 0x0E:
    case 0x11:
      return ReadInlineUtf16Value();
    case 0x12:
      return ReadSqlDateTimeValueText();
    case 0x13:
      return std::to_string(static_cast<int16_t>(ReadUInt16()));
    case 0x14:
      return std::to_string(static_cast<int32_t>(ReadUInt32()));
    case 0x15:
      return FormatDouble(ReadDouble());
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1: {
      if (IsLikelyUtf16FramedTokenValue()) {
        return ReadInlineUtf16Value();
      }
      return FormatDouble(ReadDouble());
    }
    case 0x18:
      return std::to_string(ReadByte());
    case 0x19:
      return std::to_string(ReadUInt16());
    case 0x1A:
      return std::to_string(ReadUInt32());
    case 0x1B:
      return std::to_string(ReadUInt64());
    case 0x16:
      return "true";
    case 0x17:
      return "false";
    case EMPTY_TEXT_TOKEN:
      return std::string();
    default:
      FailUnsupported(token, offset - 1);
    }
  }

  Value ValueFromText(const std::string &text,
                      XmlaCoercionKind coercion_kind) const {
    return CoerceXmlValue(text, coercion_kind);
  }

  Value ReadCellValue(uint8_t token, XmlaCoercionKind coercion_kind) {
    switch (token) {
    case 0x04:
    case 0x15: {
      auto value = ReadDouble();
      if (coercion_kind == XmlaCoercionKind::DOUBLE ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::DOUBLE(value);
      }
      return ValueFromText(FormatDouble(value), coercion_kind);
    }
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1: {
      if (IsLikelyUtf16FramedTokenValue()) {
        return ValueFromText(ReadInlineUtf16Value(), coercion_kind);
      }
      auto numeric_value = ReadDouble();
      if (coercion_kind == XmlaCoercionKind::DOUBLE ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::DOUBLE(numeric_value);
      }
      return ValueFromText(FormatDouble(numeric_value), coercion_kind);
    }
    case 0x08: {
      auto value = static_cast<int64_t>(ReadUInt64());
      if (coercion_kind == XmlaCoercionKind::BIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x12: {
      auto text = ReadSqlDateTimeValueText();
      return ValueFromText(text, coercion_kind);
    }
    case 0x13: {
      auto value = static_cast<int16_t>(ReadUInt16());
      if (coercion_kind == XmlaCoercionKind::BIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x14: {
      auto value = static_cast<int32_t>(ReadUInt32());
      if (coercion_kind == XmlaCoercionKind::BIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x18: {
      auto value = static_cast<uint64_t>(ReadByte());
      if (coercion_kind == XmlaCoercionKind::UBIGINT) {
        return Value::UBIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::BIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BIGINT(static_cast<int64_t>(value));
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x19: {
      auto value = static_cast<uint64_t>(ReadUInt16());
      if (coercion_kind == XmlaCoercionKind::UBIGINT) {
        return Value::UBIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::BIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BIGINT(static_cast<int64_t>(value));
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x1A: {
      auto value = static_cast<uint64_t>(ReadUInt32());
      if (coercion_kind == XmlaCoercionKind::UBIGINT) {
        return Value::UBIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::BIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BIGINT(static_cast<int64_t>(value));
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x1B: {
      auto value = ReadUInt64();
      if (coercion_kind == XmlaCoercionKind::UBIGINT ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::UBIGINT(value);
      }
      if (coercion_kind == XmlaCoercionKind::DOUBLE) {
        return Value::DOUBLE(static_cast<double>(value));
      }
      return ValueFromText(std::to_string(value), coercion_kind);
    }
    case 0x16:
      if (coercion_kind == XmlaCoercionKind::BOOLEAN ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BOOLEAN(true);
      }
      return ValueFromText("true", coercion_kind);
    case 0x17:
      if (coercion_kind == XmlaCoercionKind::BOOLEAN ||
          coercion_kind == XmlaCoercionKind::INFER) {
        return Value::BOOLEAN(false);
      }
      return ValueFromText("false", coercion_kind);
    case 0x10:
    case 0x0E:
    case 0x11:
      return ValueFromText(ReadInlineUtf16Value(), coercion_kind);
    case EMPTY_TEXT_TOKEN:
      return ValueFromText(std::string(), coercion_kind);
    default:
      FailUnsupported(token, offset - 1);
    }
  }

  void ParseAttribute() {
    ReadVarUInt();
    auto value_token = ReadByte();
    ReadTextValue(value_token);
  }

  void ParseStartElement() {
    FlushPendingStart();
    pending_name_id = ReadVarUInt();
    pending_start = true;
  }

  bool ParentIsRow() const {
    return !stack.empty() && stack.back().kind == ElementKind::ROW;
  }

  idx_t ActiveColumnIndex() {
    if (pending_start && ParentIsRow()) {
      return ResolveRuntimeCellColumnIndex(pending_name_id);
    }
    if (stack.size() >= 2 && stack[stack.size() - 2].kind == ElementKind::ROW) {
      return ResolveRuntimeCellColumnIndex(stack.back().name_id);
    }
    return DConstants::INVALID_INDEX;
  }

  void ParseText(uint8_t token) {
    auto column_index = ActiveColumnIndex();
    if (debug_trace_enabled && cell_trace_count < CELL_TRACE_LIMIT) {
      std::string cell_name;
      if (pending_start) {
        cell_name = LookupName(pending_name_id).local_name;
      } else if (!stack.empty()) {
        cell_name = LookupName(stack.back().name_id).local_name;
      }
      std::fprintf(stderr,
                   "[pbi_scanner] SSAS fast cell=%s token=0x%02x "
                   "parent_is_row=%d column_index=%llu\n",
                   cell_name.c_str(), static_cast<unsigned int>(token),
                   ParentIsRow() ? 1 : 0,
                   static_cast<unsigned long long>(column_index));
      cell_trace_count++;
    }
    FlushPendingStart();
    if (column_index == DConstants::INVALID_INDEX || current_row.empty()) {
      ReadTextValue(token);
      return;
    }
    current_row[column_index] =
        ReadCellValue(token, columns[column_index].coercion_kind);
  }

  void ParseRecord() {
    auto token_offset = offset;
    auto token = ReadByte();
    switch (token) {
    case 0x00:
      return;
    case 0xF0:
    case 0xFD:
    case 0xFE:
      InternString(token);
      return;
    case 0xEF:
      DefineName();
      return;
    case 0xF8:
      ParseStartElement();
      return;
    case 0x01: {
      uint32_t maybe_name_id = 0;
      idx_t next_offset = offset;
      if (TryReadVarUIntAt(data, size, offset, maybe_name_id, next_offset) &&
          (names_by_id.find(maybe_name_id) != names_by_id.end() ||
           (maybe_name_id >= 1 && maybe_name_id <= names.size()))) {
        ParseStartElement();
        return;
      }
      FlushPendingStart();
      return;
    }
    case 0xF6:
      ParseAttribute();
      return;
    case 0xF5:
      FlushPendingStart();
      return;
    case 0xF7:
      EndElement();
      return;
    case 0x04:
    case 0x08:
    case 0x10:
    case 0x0E:
    case 0x11:
    case 0x12:
    case 0x13:
    case 0x14:
    case 0x15:
    case 0xAE:
    case 0xAF:
    case 0xB0:
    case 0xB1:
    case 0x18:
    case 0x19:
    case 0x1A:
    case 0x1B:
    case 0x16:
    case 0x17:
    case EMPTY_TEXT_TOKEN:
      ParseText(token);
      return;
    default:
      FailUnsupported(token, token_offset);
    }
  }

  const_data_ptr_t data;
  idx_t size;
  idx_t offset = 0;
  const std::vector<XmlaColumn> &columns;
  const std::function<bool(const std::vector<Value> &row)> &on_row;
  const std::function<bool()> &should_stop;
  std::vector<std::string> strings;
  std::vector<ExpandedName> names;
  std::unordered_map<uint32_t, ExpandedName> names_by_id;
  std::unordered_map<std::string, idx_t> column_indexes;
  std::unordered_map<std::string, idx_t> column_indexes_lower;
  std::unordered_map<std::string, idx_t> runtime_cell_indexes;
  std::vector<StackEntry> stack;
  std::vector<Value> current_row;
  idx_t inside_schema = 0;
  idx_t runtime_column_count = 0;
  idx_t produced_rows = 0;
  idx_t cell_trace_count = 0;
  uint32_t pending_name_id = 0;
  bool pending_start = false;
  bool stopped_early = false;
  bool debug_trace_enabled = false;
};

static HttpHeaders XmlaHeaders(const std::string &access_token,
                               XmlaTransportMode transport_mode) {
  // ADOMD.NET / DAX Studio also negotiate SOAP ProtocolCapabilities sx (binary
  // XML) and xpress (compression) per MS docs; that changes the response wire
  // format. Prefer sx+xpress by default because it has the smallest observed
  // payload; callers can set PBI_SCANNER_XMLA_TRANSPORT=plain for fallback.
  HttpHeaders headers{
      std::make_pair("Authorization", "Bearer " + access_token),
      std::make_pair("SOAPAction",
                     "\"urn:schemas-microsoft-com:xml-analysis:Execute\""),
      std::make_pair("User-Agent", "ASClient/.NET-Core"),
      std::make_pair("X-Transport-Caps-Negotiation-Flags",
                     XmlaTransportFlags(transport_mode)),
      std::make_pair("SspropInitAppName", "pbi_scanner")};
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
  headers.emplace_back("Accept-Encoding", "gzip, deflate");
#endif
  return headers;
}

static bool IsPlainXmlaResponse(const HttpResponse &response) {
  auto content_type = StringUtil::Lower(response.GetHeader("Content-Type"));
  auto content_encoding =
      StringUtil::Lower(response.GetHeader("Content-Encoding"));
  if (content_encoding.find("xpress") != std::string::npos) {
    return false;
  }
  return content_type.empty() ||
         StringUtil::StartsWith(content_type, "text/xml") ||
         content_type == "application/xml" ||
         StringUtil::StartsWith(content_type, "application/xml;");
}

static bool IsXpressXmlaResponse(const HttpResponse &response) {
  auto content_type = StringUtil::Lower(response.GetHeader("Content-Type"));
  return content_type.find("application/xml+xpress") != std::string::npos;
}

static bool IsSxXmlaResponse(const HttpResponse &response) {
  auto content_type = StringUtil::Lower(response.GetHeader("Content-Type"));
  return content_type.find("application/sx") != std::string::npos &&
         content_type.find("application/sx+xpress") == std::string::npos;
}

static bool IsSxXpressXmlaResponse(const HttpResponse &response) {
  auto content_type = StringUtil::Lower(response.GetHeader("Content-Type"));
  return content_type.find("application/sx+xpress") != std::string::npos;
}

static idx_t SkipSxDfPreamble(const std::string &payload, idx_t start = 0) {
  if (start >= payload.size() || static_cast<uint8_t>(payload[start]) != 0xDF) {
    return 0;
  }
  idx_t offset = start + 1;
  for (uint32_t shift = 0; shift <= 28; shift += 7) {
    if (offset >= payload.size()) {
      return 0;
    }
    auto byte = static_cast<uint8_t>(payload[offset++]);
    if ((byte & 0x80) == 0) {
      return offset - start;
    }
  }
  return 0;
}

static idx_t BinXmlPayloadOffset(const std::string &payload) {
  idx_t offset = 0;
  while (offset < payload.size() &&
         static_cast<uint8_t>(payload[offset]) == 0xDF) {
    auto preamble_size = SkipSxDfPreamble(payload, offset);
    if (preamble_size > 0) {
      offset += preamble_size;
      continue;
    }
    // Some payloads include a bare 0xDF marker before actual payload bytes.
    offset++;
  }
  return offset;
}

static idx_t SkipOptionalSsasFramingMarker(const std::string &payload,
                                           idx_t start) {
  if (start >= payload.size() || static_cast<uint8_t>(payload[start]) != 0xB0) {
    return start;
  }
  idx_t offset = start + 1;
  if (offset < payload.size() &&
      (static_cast<uint8_t>(payload[offset]) == 0x01 ||
       static_cast<uint8_t>(payload[offset]) == 0x04)) {
    offset++;
  }
  return offset;
}

static std::vector<idx_t>
BuildNormalizedPayloadCandidates(const std::string &payload) {
  std::vector<idx_t> candidates;
  candidates.push_back(0);

  auto df_offset = BinXmlPayloadOffset(payload);
  if (df_offset < payload.size()) {
    candidates.push_back(df_offset);
  }

  const auto initial_count = candidates.size();
  for (idx_t i = 0; i < initial_count; i++) {
    auto framed_offset = SkipOptionalSsasFramingMarker(payload, candidates[i]);
    if (framed_offset < payload.size()) {
      candidates.push_back(framed_offset);
    }
  }

  std::sort(candidates.begin(), candidates.end());
  candidates.erase(std::unique(candidates.begin(), candidates.end()),
                   candidates.end());
  return candidates;
}

static bool TryParseSsasRowsFast(
    const std::string &payload, const std::vector<XmlaColumn> &known_columns,
    const std::function<bool(const std::vector<Value> &row)> &on_row,
    const std::function<bool()> &should_stop) {
  auto candidates = BuildNormalizedPayloadCandidates(payload);
  for (auto offset : candidates) {
    auto candidate_size = payload.size() - offset;
    if (candidate_size == 0) {
      continue;
    }
    auto candidate_ptr = const_data_ptr_cast(payload.data() + offset);
    SsasFastRowParser parser(candidate_ptr, candidate_size, known_columns,
                             on_row, should_stop);
    try {
      parser.ParseDocument();
      return true;
    } catch (const Exception &) {
      if (parser.ProducedRows() > 0) {
        throw;
      }
      continue;
    }
  }
  return false;
}

static bool EnableSsasFastRowParser() {
  auto *raw_value = std::getenv("PBI_SCANNER_ENABLE_SSAS_FAST_ROWS");
  if (!raw_value || !*raw_value) {
    return false;
  }
  auto value = StringUtil::Lower(Trimmed(raw_value));
  return value == "1" || value == "true" || value == "yes" || value == "on";
}

static bool IsRecoverableEarlyFramingError(const Exception &ex,
                                           idx_t candidate_offset) {
  if (candidate_offset >= 8) {
    return false;
  }
  auto message = StringUtil::Lower(ex.what());
  auto offset_pos = message.find("offset ");
  if (offset_pos == std::string::npos) {
    return false;
  }
  offset_pos += 7;
  auto end_pos = offset_pos;
  while (end_pos < message.size() && message[end_pos] >= '0' &&
         message[end_pos] <= '9') {
    end_pos++;
  }
  if (end_pos == offset_pos) {
    return false;
  }
  idx_t parser_offset = 0;
  try {
    parser_offset = static_cast<idx_t>(
        std::stoull(message.substr(offset_pos, end_pos - offset_pos)));
  } catch (...) {
    return false;
  }
  if (parser_offset >= 8) {
    return false;
  }
  return message.find("unsupported token 0xdf at offset 0") !=
             std::string::npos ||
         message.find("unsupported token 0xfe at offset") !=
             std::string::npos ||
         message.find("encountered ssas record token") != std::string::npos ||
         message.find("ssas binary xml unsupported token") != std::string::npos;
}

static void ParseBinXmlResponse(const std::string &payload,
                                XmlaStreamParser &parser) {
  auto candidates = BuildNormalizedPayloadCandidates(payload);
  std::string last_error_message;
  bool have_last_error = false;

  for (auto offset : candidates) {
    auto candidate_size = payload.size() - offset;
    if (candidate_size == 0) {
      continue;
    }
    auto candidate_ptr = const_data_ptr_cast(payload.data() + offset);

    bool ssas_recoverable = false;
    try {
      SsasBinaryXmlParser ssas_binxml(candidate_ptr, candidate_size, parser);
      ssas_binxml.ParseDocument();
      return;
    } catch (const Exception &ex) {
      if (!IsRecoverableEarlyFramingError(ex, offset)) {
        throw;
      }
      ssas_recoverable = true;
      have_last_error = true;
      last_error_message = ex.what();
    }

    try {
      BinXmlParser binxml(candidate_ptr, candidate_size, parser);
      binxml.ParseDocument();
      return;
    } catch (const Exception &ex) {
      if (!IsRecoverableEarlyFramingError(ex, offset)) {
        throw;
      }
      have_last_error = true;
      last_error_message = ex.what();
      if (!ssas_recoverable) {
        continue;
      }
      // Try next normalized candidate only for early framing/token signatures.
      continue;
    }
  }

  if (have_last_error) {
    throw IOException("%s", last_error_message.c_str());
  }
  throw IOException("BINXML payload was empty after normalization");
}

static bool DecodeBufferedXmlaResponse(
    const HttpResponse &response, const std::string &buffered_response,
    XmlaStreamParser &parser, const char *operation_label,
    const std::vector<XmlaColumn> *known_columns = nullptr,
    const std::function<bool(const std::vector<Value> &row)> *on_row = nullptr,
    const std::function<bool()> *should_stop = nullptr) {
  auto stop_callback = should_stop ? *should_stop : std::function<bool()>();
  if (IsSxXpressXmlaResponse(response)) {
    auto decompress_start = std::chrono::steady_clock::now();
    auto decompressed = DecompressXpressLz77Framed(buffered_response);
    DebugTiming(
        (std::string("SX_XPRESS ") + operation_label + " decompress").c_str(),
        decompress_start);
    auto parse_start = std::chrono::steady_clock::now();
    if (EnableSsasFastRowParser() && known_columns && on_row &&
        TryParseSsasRowsFast(decompressed, *known_columns, *on_row,
                             stop_callback)) {
      DebugTiming(
          (std::string("SX_XPRESS ") + operation_label + " fast parse").c_str(),
          parse_start);
      return true;
    }
    ParseBinXmlResponse(decompressed, parser);
    DebugTiming(
        (std::string("SX_XPRESS ") + operation_label + " parse").c_str(),
        parse_start);
    return true;
  }
  if (IsSxXmlaResponse(response)) {
    auto parse_start = std::chrono::steady_clock::now();
    if (EnableSsasFastRowParser() && known_columns && on_row &&
        TryParseSsasRowsFast(buffered_response, *known_columns, *on_row,
                             stop_callback)) {
      DebugTiming(
          (std::string("SX ") + operation_label + " fast parse").c_str(),
          parse_start);
      return true;
    }
    ParseBinXmlResponse(buffered_response, parser);
    DebugTiming((std::string("SX ") + operation_label + " parse").c_str(),
                parse_start);
    return true;
  }
  if (IsXpressXmlaResponse(response)) {
    auto decompress_start = std::chrono::steady_clock::now();
    auto decompressed = DecompressXpressLz77Framed(buffered_response);
    DebugTiming(
        (std::string("XPRESS ") + operation_label + " decompress").c_str(),
        decompress_start);
    auto parse_start = std::chrono::steady_clock::now();
    parser.Feed(const_data_ptr_cast(decompressed.data()), decompressed.size());
    DebugTiming((std::string("XPRESS ") + operation_label + " parse").c_str(),
                parse_start);
    return true;
  }
  parser.Feed(const_data_ptr_cast(buffered_response.data()),
              buffered_response.size());
  return false;
}

static bool IsUnsupportedSsasTokenError(const Exception &ex) {
  auto message = StringUtil::Lower(ex.what());
  return message.find("ssas binary xml unsupported token") !=
             std::string::npos ||
         message.find("ssas fast row parser unsupported token") !=
             std::string::npos;
}

static void ValidateXmlaContentType(const HttpResponse &response,
                                    const std::string &operation,
                                    bool decoded_response) {
  if (response.HasRequestError() || response.status >= 400) {
    return;
  }
  if (decoded_response &&
      (IsXpressXmlaResponse(response) || IsSxXmlaResponse(response) ||
       IsSxXpressXmlaResponse(response))) {
    return;
  }
  if (IsPlainXmlaResponse(response)) {
    return;
  }
  throw IOException(
      "%s returned unsupported XMLA Content-Type \"%s\" "
      "(Content-Encoding \"%s\", %llu bytes in %llu chunks). "
      "Set PBI_SCANNER_XMLA_TRANSPORT=plain until this negotiated "
      "wire format has a decoder.",
      operation, response.GetHeader("Content-Type").c_str(),
      response.GetHeader("Content-Encoding").c_str(),
      static_cast<unsigned long long>(response.streamed_bytes),
      static_cast<unsigned long long>(response.streamed_chunks));
}

static std::string XmlaHttpErrorDetail(const HttpResponse &response,
                                       const XmlaStreamParser &parser) {
  if (!parser.Excerpt().empty()) {
    return parser.Excerpt();
  }
  if (!response.body.empty()) {
    return response.body;
  }
  return response.reason;
}

static bool LooksLikePausedCapacityError(const std::string &detail) {
  auto normalized = StringUtil::Lower(detail);
  return normalized.find("error in xmla in dedicated") != std::string::npos;
}

static void ValidateHttpResponse(const HttpResponse &response,
                                 const XmlaStreamParser &parser,
                                 const std::string &operation,
                                 bool allow_canceled_probe,
                                 bool decoded_response = false) {
  if (parser.HasFault()) {
    throw IOException("%s failed: %s", operation, parser.GetFaultMessage());
  }
  if (allow_canceled_probe && parser.StoppedEarly()) {
    return;
  }
  ValidateXmlaContentType(response, operation, decoded_response);
  if (response.HasRequestError()) {
    throw IOException("%s request failed: %s", operation,
                      response.request_error);
  }
  if (response.status >= 400) {
    auto detail = XmlaHttpErrorDetail(response, parser);
    if (response.status == 500 && LooksLikePausedCapacityError(detail)) {
      throw IOException(
          "%s http %d: Power BI/Fabric capacity appears to be paused or "
          "unavailable. Resume the capacity that hosts this semantic model, "
          "then retry the query. Original service message: %s",
          operation, response.status, detail.c_str());
    }
    throw IOException("%s http %d: %s", operation, response.status,
                      detail.c_str());
  }
}

static std::string BuildXmlaExecuteEnvelopeForTransport(
    const std::string &catalog, const std::string &statement,
    const std::string &effective_user_name, XmlaTransportMode transport_mode) {
  std::string envelope;
  envelope.reserve(catalog.size() + statement.size() +
                   effective_user_name.size() + 512);
  envelope += "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
  envelope += "<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">";
  envelope += "<Header><Version "
              "xmlns=\"http://schemas.microsoft.com/analysisservices/2003/"
              "engine/2\" Sequence=\"926\" />";
  AppendProtocolCapabilities(envelope, transport_mode);
  envelope += "</Header>";
  envelope +=
      "<Body><Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\">";
  envelope += "<Command><Statement>";
  envelope += EscapeXML(statement);
  envelope += "</Statement></Command>";
  envelope += "<Properties><PropertyList><Catalog>";
  envelope += EscapeXML(catalog);
  envelope += "</Catalog>";
  if (!Trimmed(effective_user_name).empty()) {
    envelope += "<EffectiveUserName>";
    envelope += EscapeXML(Trimmed(effective_user_name));
    envelope += "</EffectiveUserName>";
  }
  envelope += "<Format>Tabular</Format><Content>Data</Content></PropertyList></"
              "Properties>";
  envelope += "</Execute></Body></Envelope>";
  return envelope;
}

} // namespace

Value CoerceXmlValueForTesting(const std::string &raw_value,
                               XmlaCoercionKind coercion_kind) {
  return CoerceXmlValue(raw_value, coercion_kind);
}

std::string
EffectiveExecutionTransportForTesting(const std::string &statement) {
  (void)statement;
  auto mode = ResolveXmlaTransportMode();
  switch (mode) {
  case XmlaTransportMode::PLAIN:
    return "plain";
  case XmlaTransportMode::SX:
    return "sx";
  case XmlaTransportMode::XPRESS:
    return "xpress";
  case XmlaTransportMode::SX_XPRESS:
    return "sx_xpress";
  }
  return "plain";
}

std::string BuildXmlaExecuteEnvelope(const std::string &catalog,
                                     const std::string &statement,
                                     const std::string &effective_user_name) {
  return BuildXmlaExecuteEnvelopeForTransport(
      catalog, statement, effective_user_name, ResolveXmlaTransportMode());
}

XmlaParseTestResult
ParseXmlaChunksForTesting(const std::vector<std::string> &chunks,
                          bool stop_after_schema) {
  XmlaParseTestResult result;
  XmlaStreamParser parser(
      stop_after_schema,
      [&](const std::vector<XmlaColumn> &columns) { result.columns = columns; },
      [&](const std::vector<Value> &row) {
        result.rows.push_back(row);
        return true;
      });

  for (const auto &chunk : chunks) {
    if (!parser.Feed(const_data_ptr_cast(chunk.data()), chunk.size())) {
      break;
    }
  }
  parser.Finish();
  result.stopped_early = parser.StoppedEarly();
  if (result.columns.empty() && parser.HasSchema()) {
    result.columns = parser.Columns();
  }
  if (parser.HasFault()) {
    result.fault_message = parser.GetFaultMessage();
  }
  return result;
}

XmlaParseTestResult ParseBinXmlForTesting(const std::string &payload,
                                          bool stop_after_schema) {
  XmlaParseTestResult result;
  XmlaStreamParser parser(
      stop_after_schema,
      [&](const std::vector<XmlaColumn> &columns) { result.columns = columns; },
      [&](const std::vector<Value> &row) {
        result.rows.push_back(row);
        return true;
      });
  ParseBinXmlResponse(payload, parser);
  parser.Finish();
  result.stopped_early = parser.StoppedEarly();
  if (result.columns.empty() && parser.HasSchema()) {
    result.columns = parser.Columns();
  }
  if (parser.HasFault()) {
    result.fault_message = parser.GetFaultMessage();
  }
  return result;
}

XmlaExecutor::XmlaExecutor(int64_t timeout_ms_p)
    : XmlaExecutor(timeout_ms_p, nullptr) {}

XmlaExecutor::XmlaExecutor(int64_t timeout_ms_p,
                           std::shared_ptr<HttpClient> shared_http)
    : timeout_ms(timeout_ms_p > 0 ? timeout_ms_p : 300000),
      http(shared_http ? std::move(shared_http)
                       : std::make_shared<HttpClient>(timeout_ms)) {}

std::vector<XmlaColumn> XmlaExecutor::ProbeSchema(const XmlaRequest &request) {
  auto transport_mode = ResolveXmlaTransportMode();
  auto schema_transport_mode = SchemaProbeTransportMode(transport_mode);
  auto envelope = BuildXmlaExecuteEnvelopeForTransport(
      request.catalog, request.statement, request.effective_user_name,
      schema_transport_mode);
  auto buffer_response = schema_transport_mode != XmlaTransportMode::PLAIN;
  std::string buffered_response;
  std::vector<XmlaColumn> schema;
  XmlaStreamParser parser(
      true, [&](const std::vector<XmlaColumn> &columns) { schema = columns; },
      nullptr);
  auto response = http->PostStream(
      request.url, XmlaHeaders(request.access_token, schema_transport_mode),
      envelope, "text/xml",
      [&](const_data_ptr_t data, idx_t data_length) {
        if (buffer_response) {
          buffered_response.append(reinterpret_cast<const char *>(data),
                                   data_length);
          return true;
        }
        return parser.Feed(data, data_length);
      },
      false);
  bool decoded_response = false;
  if (buffer_response) {
    decoded_response = DecodeBufferedXmlaResponse(response, buffered_response,
                                                  parser, "schema");
  }
  parser.Finish();
  ValidateHttpResponse(response, parser, "XMLA schema probe", true,
                       decoded_response);
  if (schema.empty()) {
    throw IOException("XMLA schema probe did not discover any columns");
  }
  return schema;
}

void XmlaExecutor::ExecuteStreaming(
    const XmlaRequest &request, const std::vector<XmlaColumn> *known_columns,
    const std::function<void(const std::vector<XmlaColumn> &columns)>
        &on_schema,
    const std::function<bool(const std::vector<Value> &row)> &on_row,
    const std::function<bool()> &should_stop) {
  auto envelope = BuildXmlaExecuteEnvelope(request.catalog, request.statement,
                                           request.effective_user_name);
  auto transport_mode = ResolveXmlaTransportMode();
  auto buffer_response = transport_mode != XmlaTransportMode::PLAIN;
  std::string buffered_response;
  XmlaStreamParser parser(false, on_schema, on_row, known_columns);
  auto response = http->PostStream(
      request.url, XmlaHeaders(request.access_token, transport_mode), envelope,
      "text/xml",
      [&](const_data_ptr_t data, idx_t data_length) {
        if (should_stop && should_stop()) {
          return false;
        }
        if (buffer_response) {
          buffered_response.append(reinterpret_cast<const char *>(data),
                                   data_length);
          return true;
        }
        return parser.Feed(data, data_length);
      },
      true);
  bool decoded_response = false;
  if (buffer_response && !(should_stop && should_stop())) {
    try {
      decoded_response = DecodeBufferedXmlaResponse(
          response, buffered_response, parser, "execution", known_columns,
          &on_row, &should_stop);
    } catch (const Exception &ex) {
      if ((transport_mode == XmlaTransportMode::SX ||
           transport_mode == XmlaTransportMode::SX_XPRESS) &&
          IsUnsupportedSsasTokenError(ex)) {
        throw IOException(
            "%s. Retry with PBI_SCANNER_XMLA_TRANSPORT=xpress for this query "
            "shape while sx_xpress Date token coverage is expanded.",
            ex.what());
      }
      throw;
    }
  }
  parser.Finish();
  if (should_stop && should_stop() && response.HasRequestError()) {
    return;
  }
  ValidateHttpResponse(response, parser, "XMLA execution", false,
                       decoded_response);
}

void XmlaExecutor::Stop() { http->Stop(); }

} // namespace duckdb
