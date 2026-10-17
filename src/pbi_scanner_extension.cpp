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

static string ExampleSelect(const string &function_name,
                            const string &dax_literal) {
  string sql = "SELECT * FROM " + function_name + "('" +
               kExampleConnectionString + "'";
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

  RegisterDocumentedTableFunction(
      loader, CreateDaxQueryFunction(), {"connection_string", "dax_text"},
      "Runs a DAX statement against a Power BI semantic model or a direct "
      "XMLA endpoint and returns the result rows as a table.",
      ExampleSelect("dax_query", "EVALUATE ROW(\"probe_ok\", 1)"));
  RegisterDocumentedTableFunction(
      loader, CreatePbiTablesFunction(), {"connection_string"},
      "Returns the tables in a Power BI semantic model by running EVALUATE "
      "INFO.VIEW.TABLES().",
      ExampleSelect("pbi_tables", ""));
  RegisterDocumentedTableFunction(
      loader, CreatePbiColumnsFunction(), {"connection_string"},
      "Returns the columns in a Power BI semantic model by running EVALUATE "
      "INFO.VIEW.COLUMNS().",
      ExampleSelect("pbi_columns", ""));
  RegisterDocumentedTableFunction(
      loader, CreatePbiMeasuresFunction(), {"connection_string"},
      "Returns the measures in a Power BI semantic model by running EVALUATE "
      "INFO.VIEW.MEASURES().",
      ExampleSelect("pbi_measures", ""));
  RegisterDocumentedTableFunction(
      loader, CreatePbiRelationshipsFunction(), {"connection_string"},
      "Returns the relationships in a Power BI semantic model by running "
      "EVALUATE INFO.VIEW.RELATIONSHIPS().",
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
