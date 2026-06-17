#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/string.hpp"

#if defined(__has_include)
#if __has_include("duckdb/common/identifier.hpp")
#include "duckdb/common/identifier.hpp"
#define PBI_HAS_DUCKDB_IDENTIFIER 1
#endif
#endif

namespace duckdb {

namespace pbi_compat {

inline named_parameter_map_t::const_iterator
FindNamedParameter(const named_parameter_map_t &named_parameters,
                   const char *name) {
  return named_parameters.find(name);
}

#ifdef PBI_HAS_DUCKDB_IDENTIFIER
inline string IdentifierName(const Identifier &identifier) {
  return identifier.GetIdentifierName();
}
#else
inline const string &IdentifierName(const string &value) { return value; }
#endif

} // namespace pbi_compat

} // namespace duckdb
