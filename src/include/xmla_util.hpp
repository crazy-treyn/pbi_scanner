#pragma once

#include <string>

namespace duckdb {

std::string EscapeXML(const std::string &value);
std::string UnescapeXML(const std::string &value);
std::string DecodeXMLName(const std::string &raw);
std::string ExtractLocalName(const std::string &name);
void TrimString(std::string &value);

} // namespace duckdb
