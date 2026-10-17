#define DUCKDB_EXTENSION_MAIN

#include "pbi_scanner_extension.hpp"
#include "dax_query.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <string>

namespace duckdb {

static void SetPbiScannerAuthMode(ClientContext &context, SetScope scope,
                                  Value &parameter) {
  auto value = StringUtil::Lower(Trimmed(parameter.ToString()));
  if (value.empty()) {
    parameter = Value("");
    return;
  }
  if (value == "cli") {
    parameter = Value("azure_cli");
    return;
  }
  if (value == "azure_cli" || value == "access_token" ||
      value == "service_principal") {
    parameter = Value(value);
    return;
  }
  throw InvalidInputException("unsupported auth_mode \"%s\"", value);
}

static const string kExampleConnectionString =
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
    "Example%20Workspace;Initial Catalog=example_semantic_model;";

static vector<string> CatalogParameterNames(const TableFunction &function,
                                            vector<string> positional) {
  if (positional.size() != function.arguments.size()) {
    throw InternalException("pbi_scanner catalog positional names do not match "
                            "the function arguments");
  }
  // v1.5.6 duckdb_functions() writes parameter_types as the positional
  // arguments followed by named_parameters in unordered_map iteration order,
  // then applies FunctionDescription.parameter_names by index. Copy the map the
  // same way that catalog walk does so each name stays on its type. The
  // iteration order is not stable across standard libraries, so tests zip names
  // to types instead of expecting one list. DuckDB main ignores this list when
  // signature names already match the type count. There, parameters and
  // parameter_types both follow SetName plus WithTypedKwargs insertion order.
  TableFunction copied = function;
  for (auto &entry : copied.named_parameters) {
    positional.push_back(entry.first);
  }
  return positional;
}

static string ExampleSelect(const string &function_name,
                            const string &dax_literal) {
  string sql =
      "SELECT * FROM " + function_name + "('" + kExampleConnectionString + "'";
  if (!dax_literal.empty()) {
    sql += ", '" + dax_literal + "'";
  }
  sql += ");";
  return sql;
}

static void RegisterDocumentedTableFunction(ExtensionLoader &loader,
                                            TableFunction function,
                                            vector<string> parameter_names,
                                            const string &description_text,
                                            const string &example) {
  CreateTableFunctionInfo info(std::move(function));
  info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
  FunctionDescription description;
  description.parameter_names = std::move(parameter_names);
  description.description = description_text;
  description.examples.push_back(example);
  description.categories.push_back("powerbi");
  info.descriptions.push_back(std::move(description));
  loader.RegisterFunction(std::move(info));
}

static void LoadInternal(ExtensionLoader &loader) {
  auto &config = loader.GetDatabaseInstance().config;
  config.AddExtensionOption("pbi_scanner_auth_mode",
                            "Default auth mode for pbi_scanner table functions "
                            "(access_token, azure_cli, service_principal)",
                            LogicalType::VARCHAR, Value(""),
                            SetPbiScannerAuthMode);
  config.AddExtensionOption(
      "normalize_dax_column_names",
      "When true, strip DAX square brackets and table qualifiers from "
      "dax_query and pbi_* metadata table function column names (default "
      "off; opt in via SET or normalize_dax_column_names)",
      LogicalType::BOOLEAN, Value::BOOLEAN(false));

  auto dax_query = CreateDaxQueryFunction();
  RegisterDocumentedTableFunction(
      loader, dax_query,
      CatalogParameterNames(dax_query, {"connection_string", "dax_text"}),
      "Runs a DAX statement against a Power BI semantic model or a direct "
      "XMLA endpoint and returns the result rows as a table. "
      "connection_string and dax_text are positional; named options use :=.",
      ExampleSelect("dax_query", "EVALUATE ROW(\"probe_ok\", 1)"));
  auto pbi_tables = CreatePbiTablesFunction();
  RegisterDocumentedTableFunction(
      loader, pbi_tables,
      CatalogParameterNames(pbi_tables, {"connection_string"}),
      "Returns the tables in a Power BI semantic model by running EVALUATE "
      "INFO.VIEW.TABLES(). connection_string is positional; named options use "
      ":=.",
      ExampleSelect("pbi_tables", ""));
  auto pbi_columns = CreatePbiColumnsFunction();
  RegisterDocumentedTableFunction(
      loader, pbi_columns,
      CatalogParameterNames(pbi_columns, {"connection_string"}),
      "Returns the columns in a Power BI semantic model by running EVALUATE "
      "INFO.VIEW.COLUMNS(). connection_string is positional; named options use "
      ":=.",
      ExampleSelect("pbi_columns", ""));
  auto pbi_measures = CreatePbiMeasuresFunction();
  RegisterDocumentedTableFunction(
      loader, pbi_measures,
      CatalogParameterNames(pbi_measures, {"connection_string"}),
      "Returns the measures in a Power BI semantic model by running EVALUATE "
      "INFO.VIEW.MEASURES(). connection_string is positional; named options "
      "use :=.",
      ExampleSelect("pbi_measures", ""));
  auto pbi_relationships = CreatePbiRelationshipsFunction();
  RegisterDocumentedTableFunction(
      loader, pbi_relationships,
      CatalogParameterNames(pbi_relationships, {"connection_string"}),
      "Returns the relationships in a Power BI semantic model by running "
      "EVALUATE INFO.VIEW.RELATIONSHIPS(). "
      "connection_string is positional; named options use :=.",
      ExampleSelect("pbi_relationships", ""));
}

void PbiScannerExtension::Load(ExtensionLoader &loader) {
  LoadInternal(loader);
}

std::string PbiScannerExtension::Name() { return "pbi_scanner"; }

std::string PbiScannerExtension::Version() const {
#ifdef EXT_VERSION_PBI_SCANNER
  return EXT_VERSION_PBI_SCANNER;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(pbi_scanner, loader) {
  duckdb::LoadInternal(loader);
}
}
