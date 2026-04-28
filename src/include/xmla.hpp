#pragma once

#include "http_client.hpp"

#include "duckdb/common/types/value.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

enum class XmlaCoercionKind : uint8_t {
  INFER,
  VARCHAR,
  BOOLEAN,
  BIGINT,
  UBIGINT,
  DOUBLE,
  DATE,
  TIME,
  TIMESTAMP,
  TIMESTAMP_TZ
};

struct XmlaColumn {
  std::string name;
  std::string source_type;
  LogicalType duckdb_type;
  XmlaCoercionKind coercion_kind = XmlaCoercionKind::INFER;
  bool nullable = true;
  bool nullable_known = false;
};

struct XmlaRequest {
  std::string url;
  std::string catalog;
  std::string access_token;
  std::string statement;
  std::string effective_user_name;
  int64_t timeout_ms = 300000;
};

struct XmlaParseTestResult {
  std::vector<XmlaColumn> columns;
  std::vector<std::vector<Value>> rows;
  std::string fault_message;
  bool stopped_early = false;
};

std::string BuildXmlaExecuteEnvelope(const std::string &catalog,
                                     const std::string &statement,
                                     const std::string &effective_user_name);
XmlaParseTestResult
ParseXmlaChunksForTesting(const std::vector<std::string> &chunks,
                          bool stop_after_schema = false);
XmlaParseTestResult ParseBinXmlForTesting(const std::string &payload,
                                          bool stop_after_schema = false);
Value CoerceXmlValueForTesting(const std::string &raw_value,
                               XmlaCoercionKind coercion_kind);
std::string EffectiveExecutionTransportForTesting(const std::string &statement);

class XmlaExecutor {
public:
  explicit XmlaExecutor(int64_t timeout_ms);
  //! Reuses the same HTTP connection (keep-alive) across ProbeSchema + execute.
  XmlaExecutor(int64_t timeout_ms, std::shared_ptr<HttpClient> shared_http);

  std::vector<XmlaColumn> ProbeSchema(const XmlaRequest &request);
  void ExecuteStreaming(
      const XmlaRequest &request, const std::vector<XmlaColumn> *known_columns,
      const std::function<void(const std::vector<XmlaColumn> &columns)>
          &on_schema,
      const std::function<bool(const std::vector<Value> &row)> &on_row,
      const std::function<bool()> &should_stop);
  void Stop();

private:
  int64_t timeout_ms;
  std::shared_ptr<HttpClient> http;
};

} // namespace duckdb
