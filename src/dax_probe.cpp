#include "dax_probe.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"

#include <cctype>

namespace duckdb {

namespace {

static bool IsDaxIdentifierChar(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

static bool KeywordAt(const string &statement, idx_t position,
                      const string &keyword) {
  if (position + keyword.size() > statement.size()) {
    return false;
  }
  for (idx_t i = 0; i < keyword.size(); i++) {
    if (std::toupper(static_cast<unsigned char>(statement[position + i])) !=
        std::toupper(static_cast<unsigned char>(keyword[i]))) {
      return false;
    }
  }
  if (position > 0 && IsDaxIdentifierChar(statement[position - 1])) {
    return false;
  }
  auto end = position + keyword.size();
  return end >= statement.size() || !IsDaxIdentifierChar(statement[end]);
}

static idx_t FindDaxKeywordOutsideLiterals(const string &statement,
                                           const string &keyword,
                                           idx_t start_position,
                                           bool top_level_only) {
  bool in_string = false;
  bool in_double_string = false;
  bool in_bracket_identifier = false;
  idx_t paren_depth = 0;
  for (idx_t i = start_position; i < statement.size(); i++) {
    auto ch = statement[i];
    if (in_string) {
      if (ch == '\'' && i + 1 < statement.size() && statement[i + 1] == '\'') {
        i++;
      } else if (ch == '\'') {
        in_string = false;
      }
      continue;
    }
    if (in_double_string) {
      if (ch == '"' && i + 1 < statement.size() && statement[i + 1] == '"') {
        i++;
      } else if (ch == '"') {
        in_double_string = false;
      }
      continue;
    }
    if (in_bracket_identifier) {
      if (ch == ']') {
        in_bracket_identifier = false;
      }
      continue;
    }
    if (ch == '\'') {
      in_string = true;
      continue;
    }
    if (ch == '"') {
      in_double_string = true;
      continue;
    }
    if (ch == '[') {
      in_bracket_identifier = true;
      continue;
    }
    if (ch == '/' && i + 1 < statement.size() && statement[i + 1] == '/') {
      while (i < statement.size() && statement[i] != '\n') {
        i++;
      }
      continue;
    }
    if (ch == '-' && i + 1 < statement.size() && statement[i + 1] == '-') {
      while (i < statement.size() && statement[i] != '\n') {
        i++;
      }
      continue;
    }
    if (ch == '/' && i + 1 < statement.size() && statement[i + 1] == '*') {
      i += 2;
      while (i + 1 < statement.size() &&
             !(statement[i] == '*' && statement[i + 1] == '/')) {
        i++;
      }
      if (i + 1 < statement.size()) {
        i++;
      }
      continue;
    }
    if (ch == '(') {
      paren_depth++;
      continue;
    }
    if (ch == ')' && paren_depth > 0) {
      paren_depth--;
      continue;
    }
    if ((!top_level_only || paren_depth == 0) &&
        KeywordAt(statement, i, keyword)) {
      return i;
    }
  }
  return DConstants::INVALID_INDEX;
}

} // namespace

std::string BuildLimitedDaxSchemaProbe(const std::string &statement,
                                       int64_t row_limit) {
  if (row_limit <= 0) {
    return statement;
  }
  auto evaluate_pos =
      FindDaxKeywordOutsideLiterals(statement, "EVALUATE", 0, false);
  if (evaluate_pos == DConstants::INVALID_INDEX) {
    return statement;
  }
  auto expression_start = evaluate_pos + string("EVALUATE").size();
  auto second_evaluate = FindDaxKeywordOutsideLiterals(statement, "EVALUATE",
                                                       expression_start, false);
  if (second_evaluate != DConstants::INVALID_INDEX) {
    return statement;
  }

  auto order_by_pos = FindDaxKeywordOutsideLiterals(statement, "ORDER BY",
                                                    expression_start, true);
  auto start_at_pos = FindDaxKeywordOutsideLiterals(statement, "START AT",
                                                    expression_start, true);
  auto expression_end = statement.size();
  if (order_by_pos != DConstants::INVALID_INDEX) {
    expression_end = MinValue<idx_t>(expression_end, order_by_pos);
  }
  if (start_at_pos != DConstants::INVALID_INDEX) {
    expression_end = MinValue<idx_t>(expression_end, start_at_pos);
  }

  auto table_expression =
      statement.substr(expression_start, expression_end - expression_start);
  StringUtil::Trim(table_expression);
  if (table_expression.empty() || KeywordAt(table_expression, 0, "VAR")) {
    return statement;
  }

  auto prefix = statement.substr(0, evaluate_pos);
  string probe;
  probe.reserve(statement.size() + 32);
  probe += prefix;
  if (!probe.empty() && probe.back() != '\n' && probe.back() != '\r') {
    probe += "\n";
  }
  probe += "EVALUATE TOPN(";
  probe += std::to_string(row_limit);
  probe += ", ";
  probe += table_expression;
  probe += ")";
  return probe;
}

std::string BuildDaxSchemaProbeForTesting(const std::string &statement,
                                          int64_t row_limit) {
  return BuildLimitedDaxSchemaProbe(statement, row_limit);
}

} // namespace duckdb
