#include "connection_string.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstdlib>

namespace duckdb {

static std::string UrlDecode(const std::string &value) {
  std::string decoded;
  decoded.reserve(value.size());

  for (size_t i = 0; i < value.size(); i++) {
    if (value[i] == '%' && i + 2 < value.size() &&
        StringUtil::CharacterIsHex(value[i + 1]) &&
        StringUtil::CharacterIsHex(value[i + 2])) {
      auto hex = value.substr(i + 1, 2);
      auto decoded_char =
          static_cast<char>(std::strtol(hex.c_str(), nullptr, 16));
      decoded.push_back(decoded_char);
      i += 2;
      continue;
    }
    decoded.push_back(value[i] == '+' ? ' ' : value[i]);
  }

  return decoded;
}

PowerBIEndpoint ParsePowerBIEndpoint(const std::string &raw) {
  auto trimmed = Trimmed(raw);
  if (trimmed.empty()) {
    throw InvalidInputException("Data Source is required");
  }

  auto scheme_pos = trimmed.find("://");
  if (scheme_pos == string::npos) {
    throw InvalidInputException("Data Source must use powerbi://");
  }

  auto scheme = trimmed.substr(0, scheme_pos);
  if (!StringUtil::CIEquals(scheme, "powerbi")) {
    throw InvalidInputException("Data Source must use powerbi://");
  }

  auto remainder = trimmed.substr(scheme_pos + 3);
  auto path_pos = remainder.find('/');
  auto host =
      path_pos == string::npos ? remainder : remainder.substr(0, path_pos);
  host = Trimmed(host);
  if (host.empty()) {
    throw InvalidInputException("Data Source host is required");
  }

  auto path = path_pos == std::string::npos ? std::string()
                                            : remainder.substr(path_pos + 1);
  while (!path.empty() && path.back() == '/') {
    path.pop_back();
  }
  auto last_slash = path.find_last_of('/');
  auto workspace_segment =
      last_slash == std::string::npos ? path : path.substr(last_slash + 1);
  workspace_segment = Trimmed(workspace_segment);
  if (workspace_segment.empty()) {
    throw InvalidInputException("Data Source workspace name is required");
  }

  return PowerBIEndpoint{trimmed, host, UrlDecode(workspace_segment)};
}

PowerBIConnectionConfig ParsePowerBIConnectionString(const std::string &raw) {
  auto trimmed = Trimmed(raw);
  if (trimmed.empty()) {
    throw InvalidInputException("connection_string is required");
  }

  PowerBIConnectionConfig result;
  result.raw = trimmed;

  auto segments = StringUtil::Split(trimmed, ';');
  for (const auto &segment : segments) {
    auto part = Trimmed(segment);
    if (part.empty()) {
      continue;
    }

    auto equals_pos = part.find('=');
    if (equals_pos == std::string::npos) {
      throw InvalidInputException("Invalid connection string segment \"%s\"",
                                  part.c_str());
    }

    auto key = Trimmed(part.substr(0, equals_pos));
    auto value = Trimmed(part.substr(equals_pos + 1));
    if (StringUtil::CIEquals(key, "Data Source")) {
      result.data_source = value;
    } else if (StringUtil::CIEquals(key, "Initial Catalog")) {
      result.initial_catalog = value;
    } else if (StringUtil::CIEquals(key, "Secret")) {
      result.secret_name = value;
    } else if (StringUtil::CIEquals(key, "EffectiveUserName")) {
      result.effective_user_name = value;
    } else if (StringUtil::CIEquals(key, "TimeoutMs")) {
      if (value.empty()) {
        throw InvalidInputException("TimeoutMs must not be empty");
      }
      try {
        result.timeout_ms = std::stoll(value);
      } catch (const std::exception &) {
        throw InvalidInputException("TimeoutMs must be an integer");
      }
      if (result.timeout_ms <= 0) {
        throw InvalidInputException("TimeoutMs must be greater than zero");
      }
      result.has_timeout_ms = true;
    }
  }

  if (Trimmed(result.data_source).empty()) {
    throw InvalidInputException("Data Source is required");
  }
  if (Trimmed(result.initial_catalog).empty()) {
    throw InvalidInputException("Initial Catalog is required");
  }

  if (StringUtil::StartsWith(StringUtil::Lower(result.data_source),
                             "https://") &&
      result.data_source.find("/xmla?") != std::string::npos) {
    result.is_direct_xmla = true;
  } else {
    result.endpoint = ParsePowerBIEndpoint(result.data_source);
  }
  return result;
}

} // namespace duckdb
