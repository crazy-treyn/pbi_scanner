#include "dax_column_names.hpp"

#include "pbi_scanner_util.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

static string NormalizeDaxColumnName(const string &raw_name) {
  auto name = Trimmed(raw_name);
  if (name.empty()) {
    return raw_name;
  }
  if (name.front() == '[' && name.back() == ']') {
    name = name.substr(1, name.size() - 2);
  } else {
    auto open_bracket = name.find('[');
    if (open_bracket != string::npos && open_bracket > 0 && name.back() == ']') {
      name = name.substr(open_bracket + 1, name.size() - open_bracket - 2);
    }
  }
  name = Trimmed(name);
  if (name.empty()) {
    return raw_name;
  }
  return name;
}

} // namespace

bool ResolveNormalizeDaxColumnNames(ClientContext &context) {
  Value setting;
  if (!context.TryGetCurrentSetting("pbi_scanner_normalize_dax_column_names",
                                    setting) ||
      setting.IsNull()) {
    return true;
  }
  return setting.GetValue<bool>();
}

string FormatDaxColumnNameForDuckDB(const string &raw_name, bool normalize) {
  if (!normalize) {
    return raw_name;
  }
  return NormalizeDaxColumnName(raw_name);
}

} // namespace duckdb
