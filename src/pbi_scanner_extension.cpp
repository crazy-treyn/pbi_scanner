#define DUCKDB_EXTENSION_MAIN

#include "pbi_scanner_extension.hpp"
#include "auth.hpp"
#include "dax_query.hpp"
#include "pbi_scanner_util.hpp"
#include "xmla.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string>

namespace duckdb {

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

static void ParseChunkedDoubleTestFunction(DataChunk &args,
                                           ExpressionState &state,
                                           Vector &result) {
  BinaryExecutor::Execute<string_t, string_t, double>(
      args.data[0], args.data[1], result, args.size(),
      [&](string_t left, string_t right) {
        auto parsed =
            ParseXmlaChunksForTesting({left.GetString(), right.GetString()});
        if (!parsed.fault_message.empty()) {
          throw InvalidInputException(parsed.fault_message);
        }
        if (parsed.rows.size() != 1 || parsed.rows[0].size() != 1) {
          throw InvalidInputException(
              "expected exactly one parsed XMLA row with one column");
        }
        return parsed.rows[0][0].GetValueUnsafe<double>();
      });
}

static void ParseBinXmlDoubleTestFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  UnaryExecutor::Execute<string_t, double>(
      args.data[0], result, args.size(), [&](string_t input) {
        auto parsed = ParseBinXmlForTesting(DecodeHex(input.GetString()));
        if (!parsed.fault_message.empty()) {
          throw InvalidInputException(parsed.fault_message);
        }
        if (parsed.rows.size() != 1 || parsed.rows[0].size() != 1) {
          throw InvalidInputException(
              "expected exactly one parsed BINXML row with one column");
        }
        return parsed.rows[0][0].GetValueUnsafe<double>();
      });
}

static void ParseBinXmlFirstTextTestFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  UnaryExecutor::Execute<string_t, string_t>(
      args.data[0], result, args.size(), [&](string_t input) {
        auto parsed = ParseBinXmlForTesting(DecodeHex(input.GetString()));
        if (!parsed.fault_message.empty()) {
          throw InvalidInputException(parsed.fault_message);
        }
        if (parsed.rows.size() != 1 || parsed.rows[0].size() != 1) {
          throw InvalidInputException(
              "expected exactly one parsed BINXML row with one column");
        }
        return StringVector::AddString(result, parsed.rows[0][0].ToString());
      });
}

static void MetadataCacheRoundTripTestFunction(DataChunk &args,
                                               ExpressionState &state,
                                               Vector &result) {
  result.SetVectorType(VectorType::CONSTANT_VECTOR);
  auto data = ConstantVector::GetData<bool>(result);
  data[0] = TestMetadataCacheRoundTrip();
}

static void ServicePrincipalErrorMessageTestFunction(DataChunk &args,
                                                     ExpressionState &state,
                                                     Vector &result) {
  UnaryExecutor::Execute<string_t, string_t>(
      args.data[0], result, args.size(), [&](string_t test_case) {
        auto message =
            TestServicePrincipalAuthErrorMessage(test_case.GetString());
        return StringVector::AddString(result, message);
      });
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

static void CoerceXmlTypeTestFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  BinaryExecutor::Execute<string_t, string_t, string_t>(
      args.data[0], args.data[1], result, args.size(),
      [&](string_t raw_value, string_t coercion_kind) {
        auto kind = ParseCoercionKind(coercion_kind.GetString());
        auto value = CoerceXmlValueForTesting(raw_value.GetString(), kind);
        return StringVector::AddString(result, value.type().ToString());
      });
}

static void CoerceXmlTextTestFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  BinaryExecutor::Execute<string_t, string_t, string_t>(
      args.data[0], args.data[1], result, args.size(),
      [&](string_t raw_value, string_t coercion_kind) {
        auto kind = ParseCoercionKind(coercion_kind.GetString());
        auto value = CoerceXmlValueForTesting(raw_value.GetString(), kind);
        if (value.IsNull()) {
          return StringVector::AddString(result, "<NULL>");
        }
        return StringVector::AddString(result, value.ToString());
      });
}

static void EffectiveExecutionTransportTestFunction(DataChunk &args,
                                                    ExpressionState &state,
                                                    Vector &result) {
  UnaryExecutor::Execute<string_t, string_t>(
      args.data[0], result, args.size(), [&](string_t statement) {
        auto transport =
            EffectiveExecutionTransportForTesting(statement.GetString());
        return StringVector::AddString(result, transport);
      });
}

static void LoadInternal(ExtensionLoader &loader) {
  loader.RegisterFunction(CreateDaxQueryFunction());
  loader.RegisterFunction(CreatePbiTablesFunction());
  loader.RegisterFunction(CreatePbiColumnsFunction());
  loader.RegisterFunction(CreatePbiMeasuresFunction());
  loader.RegisterFunction(CreatePbiRelationshipsFunction());
  loader.RegisterFunction(
      ScalarFunction("__pbi_scanner_test_parse_chunked_double",
                     {LogicalType::VARCHAR, LogicalType::VARCHAR},
                     LogicalType::DOUBLE, ParseChunkedDoubleTestFunction));
  loader.RegisterFunction(ScalarFunction(
      "__pbi_scanner_test_parse_binxml_double", {LogicalType::VARCHAR},
      LogicalType::DOUBLE, ParseBinXmlDoubleTestFunction));
  loader.RegisterFunction(ScalarFunction(
      "__pbi_scanner_test_parse_binxml_first_text", {LogicalType::VARCHAR},
      LogicalType::VARCHAR, ParseBinXmlFirstTextTestFunction));
  loader.RegisterFunction(
      ScalarFunction("__pbi_scanner_test_metadata_cache_roundtrip", {},
                     LogicalType::BOOLEAN, MetadataCacheRoundTripTestFunction));
  loader.RegisterFunction(
      ScalarFunction("__pbi_scanner_test_service_principal_error_message",
                     {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     ServicePrincipalErrorMessageTestFunction));
  loader.RegisterFunction(
      ScalarFunction("__pbi_scanner_test_coerce_xml_type",
                     {LogicalType::VARCHAR, LogicalType::VARCHAR},
                     LogicalType::VARCHAR, CoerceXmlTypeTestFunction));
  loader.RegisterFunction(
      ScalarFunction("__pbi_scanner_test_coerce_xml_text",
                     {LogicalType::VARCHAR, LogicalType::VARCHAR},
                     LogicalType::VARCHAR, CoerceXmlTextTestFunction));
  loader.RegisterFunction(
      ScalarFunction("__pbi_scanner_test_effective_execution_transport",
                     {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     EffectiveExecutionTransportTestFunction));
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
