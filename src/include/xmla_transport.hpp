#pragma once

#include <cstdint>

namespace duckdb {

enum class XmlaTransportMode : uint8_t { PLAIN, SX, XPRESS, SX_XPRESS };

XmlaTransportMode ResolveXmlaTransportMode();
const char *XmlaTransportName(XmlaTransportMode mode);
const char *XmlaTransportFlags(XmlaTransportMode mode);
XmlaTransportMode SchemaProbeTransportMode(XmlaTransportMode mode);
bool XmlaTransportNeedsProtocolCapabilities(XmlaTransportMode mode);
bool XmlaTransportIsSx(XmlaTransportMode mode);
bool XmlaTransportIsSxXpress(XmlaTransportMode mode);

bool EnableSsasFastRowParser();
bool EnableStreamingSxParser();

} // namespace duckdb
