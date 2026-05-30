#include "dax_column_names.hpp"

#include "pbi_scanner_util.hpp"

#include "duckdb/main/client_context.hpp"

#include <unordered_map>
#include <unordered_set>

namespace duckdb {

namespace {

struct ParsedDaxColumnName {
  string table_part;
  string column_part;
};

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
    if (open_bracket != string::npos && open_bracket > 0 &&
        name.back() == ']') {
      parsed.table_part = Trimmed(name.substr(0, open_bracket));
      parsed.column_part = Trimmed(
          name.substr(open_bracket + 1, name.size() - open_bracket - 2));
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

static string MakeUniqueColumnName(string name,
                                   std::unordered_set<string> &used) {
  if (used.insert(name).second) {
    return name;
  }
  for (idx_t suffix = 2;; ++suffix) {
    auto candidate = name + "_" + std::to_string(suffix);
    if (used.insert(candidate).second) {
      return candidate;
    }
  }
}

} // namespace

bool ResolveNormalizeDaxColumnNames(
    ClientContext &context, const named_parameter_map_t &named_parameters) {
  auto entry = named_parameters.find("normalize_column_names");
  if (entry != named_parameters.end() && !entry->second.IsNull()) {
    return entry->second.GetValue<bool>();
  }
  Value setting;
  if (!context.TryGetCurrentSetting("pbi_scanner_normalize_dax_column_names",
                                    setting) ||
      setting.IsNull()) {
    return false;
  }
  return setting.GetValue<bool>();
}

std::vector<string>
FormatDaxColumnNamesForDuckDB(const std::vector<string> &raw_names,
                              bool normalize) {
  if (!normalize || raw_names.empty()) {
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

  std::unordered_set<string> used;
  std::vector<string> result;
  result.reserve(raw_names.size());
  for (const auto &entry : parsed) {
    string candidate;
    if (column_counts[entry.column_part] <= 1) {
      candidate = entry.column_part;
    } else if (!entry.table_part.empty()) {
      candidate = entry.table_part + "_" + entry.column_part;
    } else {
      candidate = entry.column_part;
    }
    result.push_back(MakeUniqueColumnName(std::move(candidate), used));
  }
  return result;
}

string FormatDaxColumnNameForDuckDB(const string &raw_name, bool normalize) {
  return FormatDaxColumnNamesForDuckDB({raw_name}, normalize)[0];
}

} // namespace duckdb
