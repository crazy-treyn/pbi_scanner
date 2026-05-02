#pragma once

#include "xmla.hpp"

#include "duckdb/common/types/value.hpp"

#include <string>

namespace duckdb {

LogicalType MapXmlTypeToLogicalType(const std::string &source_type);
XmlaCoercionKind CoercionKindFromLogicalType(const LogicalType &type);
XmlaCoercionKind CoercionKindFromXmlType(const std::string &source_type);
Value CoerceXmlValue(const std::string &raw_value,
                     XmlaCoercionKind coercion_kind);
LogicalType InferLogicalType(const Value &value);
LogicalType MergeLogicalTypes(const LogicalType &current,
                              const LogicalType &next);

} // namespace duckdb
