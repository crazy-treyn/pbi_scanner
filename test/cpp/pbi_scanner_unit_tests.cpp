#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "auth.hpp"
#include "dax_column_names.hpp"
#include "dax_probe.hpp"
#include "metadata_cache.hpp"
#include "pbi_scanner_util.hpp"
#include "xmla.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <string>

using namespace duckdb;

namespace {

// MS-BINXML SQL-MONEY (token 0x05) — SSAS / sx can emit scalar typed values
// from the BINXML grammar, not only the compact tokens observed in early tests.
constexpr const char *kBinXmlLegacyRate112_5 =
    "0f01010001ff0000000000040072006f006f00740000000201b40000000000060073006300"
    "680065006d006100000002419b00000000000b0063006f006d0070006c0065007800540079"
    "007000650000001900000006000004006e0061006d00650000000501030072006f00770002"
    "415b0000000000070065006c0065006d0065006e00740000004200000046000004006e0061"
    "006d006500000005010400520061007400650006000004007400790070006500000005010a"
    "007800730064003a0064006f00750062006c00650003040401310000000000030072006f00"
    "7700000002011e000000000004005200610074006500000002050105003100310032002e00"
    "350004040400";
constexpr const char *kBinXmlCompactRate112_5 =
    "dfff01b004f00472006f006f007400ef000001f801f00372006f007700ef000002f802f004"
    "5200610074006500ef000003f803040000000000205c40f7f7f7";
constexpr const char *kBinXmlSqlMoneyRate112_5 =
    "dfff01b004f00472006f006f007400ef000001f801f00372006f007700ef000002f802f004"
    "5200610074006500ef000003f80305882a110000000000f7f7f7";
constexpr const char *kBinXmlShortFirstText =
    "dfff01b004f00472006f006f007400ef000001f801f00372006f007700ef000002f802f004"
    "5200610074006500ef000003f80386f7f7f7";

static std::string DecodeHex(const std::string &hex) {
  if (hex.size() % 2 != 0) {
    throw InvalidInputException(
        "hex input must contain an even number of digits");
  }
  std::string result;
  result.reserve(hex.size() / 2);
  for (idx_t i = 0; i < hex.size(); i += 2) {
    auto high =
        DecodeHexDigit(hex[i], "hex input contained a non-hex character");
    auto low =
        DecodeHexDigit(hex[i + 1], "hex input contained a non-hex character");
    result.push_back(static_cast<char>((high << 4) | low));
  }
  return result;
}

static XmlaCoercionKind ParseCoercionKind(const string &kind_name) {
  auto normalized = StringUtil::Upper(Trimmed(kind_name));
  if (normalized == "INFER") {
    return XmlaCoercionKind::INFER;
  }
  if (normalized == "VARCHAR") {
    return XmlaCoercionKind::VARCHAR;
  }
  if (normalized == "BOOLEAN") {
    return XmlaCoercionKind::BOOLEAN;
  }
  if (normalized == "BIGINT") {
    return XmlaCoercionKind::BIGINT;
  }
  if (normalized == "UBIGINT") {
    return XmlaCoercionKind::UBIGINT;
  }
  if (normalized == "DOUBLE") {
    return XmlaCoercionKind::DOUBLE;
  }
  if (normalized == "DATE") {
    return XmlaCoercionKind::DATE;
  }
  if (normalized == "TIME") {
    return XmlaCoercionKind::TIME;
  }
  if (normalized == "TIMESTAMP") {
    return XmlaCoercionKind::TIMESTAMP;
  }
  if (normalized == "TIMESTAMP_TZ") {
    return XmlaCoercionKind::TIMESTAMP_TZ;
  }
  throw InvalidInputException("unknown XMLA coercion kind: " + kind_name);
}

static double RequireSingleDouble(const XmlaParseTestResult &parsed) {
  REQUIRE(parsed.fault_message.empty());
  REQUIRE(parsed.rows.size() == 1);
  REQUIRE(parsed.rows[0].size() == 1);
  return parsed.rows[0][0].GetValueUnsafe<double>();
}

static Value RequireSingleValue(const XmlaParseTestResult &parsed) {
  REQUIRE(parsed.fault_message.empty());
  REQUIRE(parsed.rows.size() == 1);
  REQUIRE(parsed.rows[0].size() == 1);
  return parsed.rows[0][0];
}

static std::string CoerceXmlTypeString(const std::string &raw_value,
                                       const std::string &kind_name) {
  auto kind = ParseCoercionKind(kind_name);
  auto value = CoerceXmlValueForTesting(raw_value, kind);
  return value.type().ToString();
}

static std::string CoerceXmlTextString(const std::string &raw_value,
                                       const std::string &kind_name) {
  auto kind = ParseCoercionKind(kind_name);
  auto value = CoerceXmlValueForTesting(raw_value, kind);
  if (value.IsNull()) {
    return "<NULL>";
  }
  return value.ToString();
}

static std::string JoinPipeDelimited(const std::vector<string> &values) {
  string joined;
  for (idx_t i = 0; i < values.size(); i++) {
    if (i > 0) {
      joined.push_back('|');
    }
    joined += values[i];
  }
  return joined;
}

static std::vector<string> SplitPipeDelimited(const string &input) {
  std::vector<string> values;
  idx_t start = 0;
  for (idx_t i = 0; i <= input.size(); i++) {
    if (i == input.size() || input[i] == '|') {
      values.push_back(input.substr(start, i - start));
      start = i + 1;
    }
  }
  return values;
}

} // namespace

TEST_CASE("XMLA chunked parse", "[xmla][smoke]") {
  auto parsed = ParseXmlaChunksForTesting(
      {"<?xml version=\"1.0\" encoding=\"utf-8\"?><root><schema "
       "xmlns:xsd=\"http://www.w3.org/2001/"
       "XMLSchema\"><xsd:schema><xsd:complexType "
       "name=\"row\"><xsd:sequence><xsd:element name=\"Rate\" "
       "type=\"xsd:double\" "
       "/></xsd:sequence></xsd:complexType></xsd:schema></"
       "schema><row><Rate>1.125E2</Ra",
       "te></row></root>"});
  REQUIRE(RequireSingleDouble(parsed) == Approx(112.5));
}

TEST_CASE("BINXML double parse", "[xmla]") {
  REQUIRE(RequireSingleDouble(ParseBinXmlForTesting(
              DecodeHex(kBinXmlLegacyRate112_5))) == Approx(112.5));
  REQUIRE(RequireSingleDouble(ParseBinXmlForTesting(
              DecodeHex(kBinXmlCompactRate112_5))) == Approx(112.5));
  REQUIRE(RequireSingleDouble(ParseBinXmlForTesting(
              DecodeHex(kBinXmlSqlMoneyRate112_5))) == Approx(112.5));
}

TEST_CASE("BINXML first text", "[xmla]") {
  auto short_text = RequireSingleValue(
      ParseBinXmlForTesting(DecodeHex(kBinXmlShortFirstText)));
  REQUIRE(short_text.ToString().size() == 4);

  auto money_text = RequireSingleValue(
      ParseBinXmlForTesting(DecodeHex(kBinXmlSqlMoneyRate112_5)));
  REQUIRE(money_text.ToString() == "112.5");
}

TEST_CASE("Streaming SX double parse", "[xmla]") {
  auto decoded = DecodeHex(kBinXmlCompactRate112_5);
  auto decoded_money = DecodeHex(kBinXmlSqlMoneyRate112_5);

  REQUIRE(RequireSingleDouble(ParseStreamingSxRowsForTesting(decoded, 1)) ==
          Approx(112.5));
  REQUIRE(RequireSingleDouble(ParseStreamingSxRowsForTesting(decoded, 7)) ==
          Approx(112.5));
  REQUIRE(RequireSingleDouble(ParseStreamingSxRowsForTesting(
              decoded_money, 3)) == Approx(112.5));
}

TEST_CASE("Metadata cache roundtrip", "[metadata_cache]") {
  REQUIRE(TestMetadataCacheRoundTrip());
}

TEST_CASE("DAX schema probe", "[dax_probe]") {
  REQUIRE(BuildDaxSchemaProbeForTesting("EVALUATE 'Fact Allocation'", 100) ==
          "EVALUATE TOPN(100, 'Fact Allocation')");
  REQUIRE(BuildDaxSchemaProbeForTesting("EVALUATE ROW(\"x\", 1) ORDER BY [x]",
                                        10) ==
          "EVALUATE TOPN(10, ROW(\"x\", 1))");

  auto summarizing = StringUtil::Replace(
      BuildDaxSchemaProbeForTesting(
          "DEFINE MEASURE T[m] = 1 EVALUATE SUMMARIZECOLUMNS(T[c], \"m\", [m])",
          5),
      "\n", "|");
  REQUIRE(summarizing == "DEFINE MEASURE T[m] = 1 |EVALUATE TOPN(5, "
                         "SUMMARIZECOLUMNS(T[c], \"m\", [m]))");

  REQUIRE(BuildDaxSchemaProbeForTesting(
              "EVALUATE ROW(\"text\", \"ORDER BY ignored\")", 3) ==
          "EVALUATE TOPN(3, ROW(\"text\", \"ORDER BY ignored\"))");
  REQUIRE(BuildDaxSchemaProbeForTesting(
              "EVALUATE VAR X = ROW(\"x\", 1) RETURN X", 100) ==
          "EVALUATE VAR X = ROW(\"x\", 1) RETURN X");
  REQUIRE(BuildDaxSchemaProbeForTesting("EVALUATE ROW(\"x\", 1)", 0) ==
          "EVALUATE ROW(\"x\", 1)");
}

TEST_CASE("Service principal error messages", "[auth]") {
  REQUIRE(TestServicePrincipalAuthErrorMessage("request_error") ==
          "service principal token request failed: simulated_request_error");
  REQUIRE(TestServicePrincipalAuthErrorMessage("http_error") ==
          "service principal token request http 401: denied");
  REQUIRE(TestServicePrincipalAuthErrorMessage("invalid_json") ==
          "service principal token response was not valid JSON");
  REQUIRE(TestServicePrincipalAuthErrorMessage("missing_access_token") ==
          "service principal token response did not include access_token");
}

TEST_CASE("XML coercion type", "[xmla]") {
  REQUIRE(CoerceXmlTypeString("1899-12-30T12:34:56", "TIMESTAMP") ==
          "TIMESTAMP");
  REQUIRE(CoerceXmlTypeString("1899-12-30T12:34:56+00:00", "TIMESTAMP_TZ") ==
          "TIMESTAMP WITH TIME ZONE");
  REQUIRE(CoerceXmlTypeString("1899-12-30T12:34:56", "TIME") == "TIME");
  REQUIRE(CoerceXmlTypeString("9223372036854775807", "BIGINT") == "BIGINT");
  REQUIRE(CoerceXmlTypeString("1234.5678", "DOUBLE") == "DOUBLE");
  REQUIRE(CoerceXmlTypeString("true", "BOOLEAN") == "BOOLEAN");
}

TEST_CASE("XML coercion text", "[xmla]") {
  REQUIRE(CoerceXmlTextString("", "VARCHAR").size() == 0);
  REQUIRE(CoerceXmlTextString("", "INFER") == "<NULL>");
  REQUIRE(CoerceXmlTextString("1", "DATE") == "1899-12-31");
  REQUIRE(CoerceXmlTextString("0.5", "TIME") == "12:00:00");
  REQUIRE(CoerceXmlTextString("2.5", "TIMESTAMP") == "1900-01-01 12:00:00");
  REQUIRE(CoerceXmlTextString("0", "DATE") == "1899-12-30");
  REQUIRE(CoerceXmlTextString("-1", "DATE") == "1899-12-29");
  REQUIRE(CoerceXmlTextString("0.9999999", "TIME") == "23:59:59.99136");
  REQUIRE(CoerceXmlTextString("45292.25", "TIMESTAMP") ==
          "2024-01-01 06:00:00");
}

TEST_CASE("Effective execution transport", "[xmla]") {
  REQUIRE(EffectiveExecutionTransportForTesting(
              "EVALUATE INFO.VIEW.TABLES()") == "sx_xpress");
}

TEST_CASE("DAX column name normalization", "[dax_column_names]") {
  REQUIRE(FormatDaxColumnNameForDuckDB("[x]", true) == "x");
  REQUIRE(FormatDaxColumnNameForDuckDB("[Total Sales]", true) == "Total Sales");
  REQUIRE(FormatDaxColumnNameForDuckDB("Fact[Amount]", true) == "Amount");
  REQUIRE(FormatDaxColumnNameForDuckDB("[Total Sales]", false) ==
          "[Total Sales]");
  REQUIRE(FormatDaxColumnNameForDuckDB("Rate", true) == "Rate");

  REQUIRE(JoinPipeDelimited(FormatDaxColumnNamesForDuckDB(
              SplitPipeDelimited("TableA[Amount]|TableB[Amount]"), true)) ==
          "TableA_Amount|TableB_Amount");
  REQUIRE(JoinPipeDelimited(FormatDaxColumnNamesForDuckDB(
              SplitPipeDelimited("TableA[Amount]|[Amount]|TableB[Amount]"),
              true)) == "TableA_Amount|Amount|TableB_Amount");
  REQUIRE(JoinPipeDelimited(FormatDaxColumnNamesForDuckDB(
              SplitPipeDelimited("[Amount]|[Amount]"), true)) ==
          "Amount|Amount_2");
  REQUIRE(JoinPipeDelimited(FormatDaxColumnNamesForDuckDB(
              SplitPipeDelimited("TableA[Amount]|TableA[Amount]"), true)) ==
          "TableA_Amount|TableA_Amount_2");
  REQUIRE(
      JoinPipeDelimited(FormatDaxColumnNamesForDuckDB(
          SplitPipeDelimited("TableA[Amount]|TableB[Amount]|[TableA_Amount]"),
          true)) == "TableA_Amount|TableB_Amount|TableA_Amount_2");
  REQUIRE(JoinPipeDelimited(FormatDaxColumnNamesForDuckDB(
              SplitPipeDelimited("[Amount]|[Amount]|[Amount]"), true)) ==
          "Amount|Amount_2|Amount_3");
}

TEST_CASE("Resolve normalize dax column names setting", "[dax_column_names]") {
  DuckDB db(nullptr);
  ExtensionHelper::LoadExtension(db, "pbi_scanner");
  Connection con(db);
  auto &context = *con.context;

  named_parameter_map_t empty;
  REQUIRE(!ResolveNormalizeDaxColumnNames(context, empty));

  auto set_on_result =
      con.Query("SET pbi_scanner_normalize_dax_column_names = true");
  REQUIRE(!set_on_result->HasError());
  REQUIRE(ResolveNormalizeDaxColumnNames(context, empty));

  auto set_off_result =
      con.Query("SET pbi_scanner_normalize_dax_column_names = false");
  REQUIRE(!set_off_result->HasError());
  REQUIRE(!ResolveNormalizeDaxColumnNames(context, empty));

  named_parameter_map_t override_on;
  override_on["normalize_column_names"] = Value::BOOLEAN(true);
  REQUIRE(ResolveNormalizeDaxColumnNames(context, override_on));

  named_parameter_map_t override_off;
  override_off["normalize_column_names"] = Value::BOOLEAN(false);
  REQUIRE(!ResolveNormalizeDaxColumnNames(context, override_off));
}
