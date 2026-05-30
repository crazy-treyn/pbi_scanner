#include "dax_column_names.hpp"

#include "pbi_scanner_util.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <unordered_map>

namespace duckdb {

namespace {

struct ParsedDaxColumnName {
  string table_part;
  string column_part;
};

static bool HasNonNullNamedParameter(const named_parameter_map_t &named_parameters,
                                     const string &name) {
  auto entry = named_parameters.find(name);
  return entry != named_parameters.end() && !entry->second.IsNull();
}

static ParsedDaxColumnName ParseDaxColumnNameParts(const string &raw_name) {
  ParsedDaxColumnName parsed;
  parsed.column_part = raw_name;
  auto name = Trimmed(raw_name);
  if (name.empty()) {
    return parsed;
  }
  if (name.front() == '[' && name.back() == ']') {
    parsed.column_part = Trimmed(name.substr(1, name.size() - 2));
  } else {
    auto open_bracket = name.find('[');
    if (open_bracket != string::npos && open_bracket > 0 && name.back() == ']') {
      parsed.table_part = Trimmed(name.substr(0, open_bracket));
      parsed.column_part =
          Trimmed(name.substr(open_bracket + 1, name.size() - open_bracket - 2));
    } else {
      parsed.column_part = name;
    }
  }
  if (parsed.column_part.empty()) {
    parsed.table_part.clear();
    parsed.column_part = raw_name;
  }
  return parsed;
}

static string FormatUniqueDaxColumnName(const ParsedDaxColumnName &parsed,
                                        idx_t bare_duplicate_index) {
  if (!parsed.table_part.empty()) {
    return parsed.table_part + "_" + parsed.column_part;
  }
  if (bare_duplicate_index == 0) {
    return parsed.column_part;
  }
  return parsed.column_part + "_" + std::to_string(bare_duplicate_index + 1);
}

} // namespace

bool ResolveNormalizeDaxColumnNames(
    ClientContext &context, const named_parameter_map_t &named_parameters) {
  if (HasNonNullNamedParameter(named_parameters, "normalize_column_names")) {
    return named_parameters.find("normalize_column_names")->second.GetValue<bool>();
  }
  Value setting;
  if (!context.TryGetCurrentSetting("pbi_scanner_normalize_dax_column_names",
                                    setting) ||
      setting.IsNull()) {
    return true;
  }
  return setting.GetValue<bool>();
}

std::vector<string>
FormatDaxColumnNamesForDuckDB(const std::vector<string> &raw_names,
                             bool normalize) {
  if (!normalize) {
    return raw_names;
  }
  if (raw_names.empty()) {
    return raw_names;
  }

  std::vector<ParsedDaxColumnName> parsed;
  parsed.reserve(raw_names.size());
  for (const auto &raw_name : raw_names) {
    parsed.push_back(ParseDaxColumnNameParts(raw_name));
  }

  std::unordered_map<string, idx_t> column_counts;
  for (const auto &entry : parsed) {
    column_counts[entry.column_part]++;
  }

  std::unordered_map<string, idx_t> bare_duplicate_indexes;
  std::vector<string> result;
  result.reserve(raw_names.size());
  for (const auto &entry : parsed) {
    if (column_counts[entry.column_part] <= 1) {
      result.push_back(entry.column_part);
      continue;
    }
    idx_t bare_index = 0;
    if (entry.table_part.empty()) {
      bare_index = bare_duplicate_indexes[entry.column_part]++;
    }
    result.push_back(FormatUniqueDaxColumnName(entry, bare_index));
  }
  return result;
}

string FormatDaxColumnNameForDuckDB(const string &raw_name, bool normalize) {
  return FormatDaxColumnNamesForDuckDB({raw_name}, normalize)[0];
}

} // namespace duckdb
