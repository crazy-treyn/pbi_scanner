#include "xmla_transport.hpp"

#include "pbi_scanner_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstdlib>

namespace duckdb {

namespace {

static bool ResolveBooleanEnv(const char *name, bool default_value) {
  auto *raw_value = std::getenv(name);
  if (!raw_value || !*raw_value) {
    return default_value;
  }
  auto value = StringUtil::Lower(Trimmed(raw_value));
  if (value == "1" || value == "true" || value == "yes" || value == "on") {
    return true;
  }
  if (value == "0" || value == "false" || value == "no" || value == "off") {
    return false;
  }
  return default_value;
}

} // namespace

XmlaTransportMode ResolveXmlaTransportMode() {
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

const char *XmlaTransportName(XmlaTransportMode mode) {
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
  return "sx_xpress";
}

const char *XmlaTransportFlags(XmlaTransportMode mode) {
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
  return "1,0,0,1,1";
}

XmlaTransportMode SchemaProbeTransportMode(XmlaTransportMode mode) {
  if (mode == XmlaTransportMode::SX || mode == XmlaTransportMode::SX_XPRESS) {
    return XmlaTransportMode::XPRESS;
  }
  return mode;
}

bool XmlaTransportNeedsProtocolCapabilities(XmlaTransportMode mode) {
  return mode != XmlaTransportMode::PLAIN;
}

bool XmlaTransportIsSx(XmlaTransportMode mode) {
  return mode == XmlaTransportMode::SX || mode == XmlaTransportMode::SX_XPRESS;
}

bool XmlaTransportIsSxXpress(XmlaTransportMode mode) {
  return mode == XmlaTransportMode::SX_XPRESS;
}

bool EnableSsasFastRowParser() {
  return ResolveBooleanEnv("PBI_SCANNER_ENABLE_SSAS_FAST_ROWS", false);
}

bool EnableStreamingSxParser() {
  return ResolveBooleanEnv("PBI_SCANNER_ENABLE_STREAMING_SX", false);
}

} // namespace duckdb
