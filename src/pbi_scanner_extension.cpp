#define DUCKDB_EXTENSION_MAIN

#include "pbi_scanner_extension.hpp"
#include "dax_query.hpp"
#include "pbi_scanner_util.hpp"
#include "pbi_scanner_test_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

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

static void LoadInternal(ExtensionLoader &loader) {
  auto &config = loader.GetDatabaseInstance().config;
  config.AddExtensionOption("pbi_scanner_auth_mode",
                            "Default auth mode for pbi_scanner table functions "
                            "(access_token, azure_cli, service_principal)",
                            LogicalType::VARCHAR, Value(""),
                            SetPbiScannerAuthMode);

  loader.RegisterFunction(CreateDaxQueryFunction());
  loader.RegisterFunction(CreatePbiTablesFunction());
  loader.RegisterFunction(CreatePbiColumnsFunction());
  loader.RegisterFunction(CreatePbiMeasuresFunction());
  loader.RegisterFunction(CreatePbiRelationshipsFunction());
  RegisterPbiScannerTestFunctions(loader);
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
