#include "xmla_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"

#include <cstdlib>

namespace duckdb {

std::string EscapeXML(const std::string &value) {
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

std::string UnescapeXML(const std::string &value) {
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

std::string DecodeXMLName(const std::string &raw) {
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

std::string ExtractLocalName(const std::string &name) {
  auto colon = name.find(':');
  return colon == std::string::npos ? name : name.substr(colon + 1);
}

void TrimString(std::string &value) {
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
  if (begin > 0 || end < value.size()) {
    value = value.substr(begin, end - begin);
  }
}

} // namespace duckdb
