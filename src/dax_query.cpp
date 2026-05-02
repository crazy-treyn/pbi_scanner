#include "dax_query.hpp"

#include "auth.hpp"
#include "connection_string.hpp"
#include "dax_probe.hpp"
#include "metadata_cache.hpp"
#include "pbi_scanner_util.hpp"
#include "powerbi_resolver.hpp"
#include "vector_writer.hpp"
#include "xmla.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

namespace duckdb {

namespace {

static constexpr int64_t DEFAULT_SCHEMA_PROBE_ROWS = 100;

static bool
HasNonNullNamedParameter(const named_parameter_map_t &named_parameters,
                         const string &name) {
  auto entry = named_parameters.find(name);
  return entry != named_parameters.end() && !entry->second.IsNull();
}

static string
GetOptionalNamedParameter(const named_parameter_map_t &named_parameters,
                          const string &name) {
  auto entry = named_parameters.find(name);
  if (entry == named_parameters.end() || entry->second.IsNull()) {
    return string();
  }
  return Trimmed(entry->second.GetValue<string>());
}

} // namespace

static bool IsMwcXmlaUnauthorized(const string &xmla_auth_scheme,
                                  const string &message);

struct DaxQueryBindData : public TableFunctionData {
  DaxQueryBindData(PowerBIConnectionConfig config_p,
                   PowerBIResolvedTarget target_p,
                   std::vector<XmlaColumn> columns_p, string dax_text_p,
                   string xmla_catalog_p, string xmla_auth_scheme_p,
                   string access_token_p, string power_bi_aad_token_p,
                   string effective_user_name_p, int64_t timeout_ms_p,
                   std::shared_ptr<HttpClient> xmla_http_client_p = nullptr)
      : config(std::move(config_p)), target(std::move(target_p)),
        columns(std::move(columns_p)), dax_text(std::move(dax_text_p)),
        xmla_catalog(std::move(xmla_catalog_p)),
        xmla_auth_scheme(std::move(xmla_auth_scheme_p)),
        access_token(std::move(access_token_p)),
        power_bi_aad_token(std::move(power_bi_aad_token_p)),
        effective_user_name(std::move(effective_user_name_p)),
        timeout_ms(timeout_ms_p),
        xmla_http_client(std::move(xmla_http_client_p)) {}

  unique_ptr<FunctionData> Copy() const override {
    return make_uniq<DaxQueryBindData>(*this);
  }

  bool Equals(const FunctionData &other_p) const override {
    auto &other = other_p.Cast<DaxQueryBindData>();
    return config.raw == other.config.raw &&
           target.aixl_url == other.target.aixl_url &&
           dax_text == other.dax_text && xmla_catalog == other.xmla_catalog &&
           xmla_auth_scheme == other.xmla_auth_scheme &&
           access_token == other.access_token &&
           power_bi_aad_token == other.power_bi_aad_token &&
           effective_user_name == other.effective_user_name &&
           timeout_ms == other.timeout_ms;
  }

  PowerBIConnectionConfig config;
  PowerBIResolvedTarget target;
  std::vector<XmlaColumn> columns;
  string dax_text;
  string xmla_catalog;
  string xmla_auth_scheme;
  //! XMLA Authorization token (MWC AS token or Azure AD Bearer).
  string access_token;
  //! Azure AD token used for Power BI REST + generateastoken (MWC refresh).
  string power_bi_aad_token;
  string effective_user_name;
  int64_t timeout_ms;
  //! Populated when ProbeSchema ran; reused for execute on same TCP connection.
  std::shared_ptr<HttpClient> xmla_http_client;
};

struct DaxQueryGlobalState : public GlobalTableFunctionState {
  explicit DaxQueryGlobalState(const DaxQueryBindData &bind_data)
      : columns(bind_data.columns), bind_config(bind_data.config),
        bind_target(bind_data.target),
        power_bi_aad_token(bind_data.power_bi_aad_token) {
    for (const auto &column : columns) {
      column_types.push_back(column.duckdb_type);
    }
    request.url = bind_data.target.aixl_url;
    request.catalog = bind_data.xmla_catalog;
    request.auth_scheme = bind_data.xmla_auth_scheme;
    request.access_token = bind_data.access_token;
    request.xmla_server = bind_data.target.core_server_name;
    request.xmla_workspace_id = bind_data.target.workspace_id;
    request.statement = bind_data.dax_text;
    request.effective_user_name = bind_data.effective_user_name;
    request.timeout_ms = bind_data.timeout_ms;
    if (bind_data.xmla_http_client) {
      executor = make_uniq<XmlaExecutor>(bind_data.timeout_ms,
                                         bind_data.xmla_http_client);
    } else {
      executor = make_uniq<XmlaExecutor>(bind_data.timeout_ms);
    }
    worker = std::thread([this]() { RunWorker(); });
  }

  ~DaxQueryGlobalState() override {
    stop_requested.store(true, std::memory_order_release);
    condition.notify_all();
    if (executor && !finished) {
      executor->Stop();
    }
    if (worker.joinable()) {
      worker.join();
    }
  }

  idx_t MaxThreads() const override { return 1; }

  bool PopChunk(unique_ptr<DataChunk> &chunk, ClientContext &context) {
    std::unique_lock<std::mutex> guard(lock);
    condition.wait(guard, [&]() {
      return stop_requested.load(std::memory_order_acquire) ||
             !chunks.empty() || finished || !error.empty();
    });
    if (context.IsInterrupted()) {
      stop_requested.store(true, std::memory_order_release);
      guard.unlock();
      if (executor) {
        executor->Stop();
      }
      throw InterruptException();
    }
    if (!error.empty()) {
      throw IOException("%s", error);
    }
    if (chunks.empty()) {
      return false;
    }
    chunk = std::move(chunks.front());
    chunks.pop_front();
    guard.unlock();
    condition.notify_all();
    return true;
  }

  std::vector<XmlaColumn> columns;
  vector<LogicalType> column_types;
  XmlaRequest request;
  std::unique_ptr<XmlaExecutor> executor;
  PowerBIConnectionConfig bind_config;
  PowerBIResolvedTarget bind_target;
  string power_bi_aad_token;
  std::mutex lock;
  std::condition_variable condition;
  std::deque<unique_ptr<DataChunk>> chunks;
  std::thread worker;
  string error;
  bool finished = false;
  std::atomic<bool> stop_requested{false};
  idx_t max_buffered_chunks = 32;
  idx_t produced_rows = 0;
  std::chrono::steady_clock::time_point init_started_at =
      std::chrono::steady_clock::now();
  bool execute_started = false;

private:
  unique_ptr<DataChunk> CreateChunk() const {
    auto chunk = make_uniq<DataChunk>();
    chunk->Initialize(Allocator::DefaultAllocator(), column_types,
                      STANDARD_VECTOR_SIZE);
    return chunk;
  }

  bool EnqueueChunk(unique_ptr<DataChunk> chunk) {
    if (!chunk || chunk->size() == 0) {
      return true;
    }

    std::unique_lock<std::mutex> guard(lock);
    condition.wait(guard, [&]() {
      return stop_requested.load(std::memory_order_acquire) ||
             chunks.size() < max_buffered_chunks;
    });
    if (stop_requested.load(std::memory_order_acquire)) {
      return false;
    }
    chunks.push_back(std::move(chunk));
    guard.unlock();
    condition.notify_all();
    return true;
  }

  void RunWorker() {
    auto worker_start = std::chrono::steady_clock::now();
    bool first_row_logged = false;
    auto current_chunk = CreateChunk();
    idx_t current_chunk_size = 0;
    bool retried_mwc_execute = false;
    bool retried_legacy_bearer_execute = false;
    bool finish_worker = false;
    while (!finish_worker) {
      try {
        executor->ExecuteStreaming(
            request, &columns, nullptr,
            [&](const std::vector<Value> &row) {
              if (DebugTimingsEnabled() && !first_row_logged) {
                DebugTiming("ExecuteStreaming first row", worker_start);
                first_row_logged = true;
              }
              if (current_chunk_size >= STANDARD_VECTOR_SIZE) {
                current_chunk->SetCardinality(current_chunk_size);
                if (!EnqueueChunk(std::move(current_chunk))) {
                  return false;
                }
                current_chunk = CreateChunk();
                current_chunk_size = 0;
              }

              for (idx_t column_idx = 0; column_idx < row.size();
                   column_idx++) {
                auto &vector = current_chunk->data[column_idx];
                if (!TrySetFlatVectorValue(vector, current_chunk_size,
                                           row[column_idx],
                                           column_types[column_idx])) {
                  vector.SetValue(current_chunk_size, row[column_idx]);
                }
              }
              current_chunk_size++;
              produced_rows++;
              return true;
            },
            [&]() { return stop_requested.load(std::memory_order_relaxed); });

        if (current_chunk_size > 0) {
          current_chunk->SetCardinality(current_chunk_size);
          if (!EnqueueChunk(std::move(current_chunk))) {
            return;
          }
        }
        finish_worker = true;
      } catch (const Exception &ex) {
        if (!retried_mwc_execute &&
            IsMwcXmlaUnauthorized(request.auth_scheme, ex.what()) &&
            !power_bi_aad_token.empty() &&
            !bind_target.capacity_object_id.empty()) {
          retried_mwc_execute = true;
          if (DebugTimingsEnabled()) {
            std::fprintf(stderr, "[pbi_scanner] MWC XMLA execute returned 401; "
                                 "refreshing AS token and retrying once\n");
          }
          request.access_token = GeneratePowerBIXmlaToken(
              bind_config.endpoint, bind_target, power_bi_aad_token,
              request.timeout_ms, true);
          executor = make_uniq<XmlaExecutor>(request.timeout_ms);
          current_chunk = CreateChunk();
          current_chunk_size = 0;
          continue;
        }
        if (!retried_legacy_bearer_execute &&
            IsMwcXmlaUnauthorized(request.auth_scheme, ex.what()) &&
            !power_bi_aad_token.empty() &&
            !bind_target.capacity_object_id.empty()) {
          retried_legacy_bearer_execute = true;
          try {
            if (DebugTimingsEnabled()) {
              std::fprintf(stderr,
                           "[pbi_scanner] MWC XMLA execute returned 401; "
                           "retrying legacy Bearer XMLA path\n");
            }
            request.url = ResolveLegacyPowerBIXmlaUrl(
                bind_config.endpoint, bind_target, power_bi_aad_token,
                request.timeout_ms);
            request.catalog = bind_target.internal_catalog;
            request.auth_scheme = "Bearer";
            request.access_token = power_bi_aad_token;
            request.xmla_server.clear();
            executor = make_uniq<XmlaExecutor>(request.timeout_ms);
            current_chunk = CreateChunk();
            current_chunk_size = 0;
            continue;
          } catch (const Exception &retry_ex) {
            std::lock_guard<std::mutex> guard(lock);
            error = retry_ex.what();
          } catch (const std::exception &retry_ex) {
            std::lock_guard<std::mutex> guard(lock);
            error = retry_ex.what();
          }
          InvalidateCachedSchema(bind_target, request.statement,
                                 request.effective_user_name);
          if (!bind_config.is_direct_xmla) {
            InvalidateCachedTarget(bind_config);
          }
          finish_worker = true;
          continue;
        }
        InvalidateCachedSchema(bind_target, request.statement,
                               request.effective_user_name);
        if (!bind_config.is_direct_xmla) {
          InvalidateCachedTarget(bind_config);
        }
        std::lock_guard<std::mutex> guard(lock);
        error = ex.what();
        finish_worker = true;
      } catch (const std::exception &ex) {
        InvalidateCachedSchema(bind_target, request.statement,
                               request.effective_user_name);
        if (!bind_config.is_direct_xmla) {
          InvalidateCachedTarget(bind_config);
        }
        std::lock_guard<std::mutex> guard(lock);
        error = ex.what();
        finish_worker = true;
      }
    }
    if (DebugTimingsEnabled()) {
      DebugTiming("ExecuteStreaming total", worker_start);
      std::fprintf(stderr, "[pbi_scanner] dax_query rows: %llu\n",
                   static_cast<unsigned long long>(produced_rows));
    }
    {
      std::lock_guard<std::mutex> guard(lock);
      finished = true;
    }
    condition.notify_all();
  }
};

static string
ResolveEffectiveUserName(const PowerBIConnectionConfig &config,
                         const named_parameter_map_t &named_parameters) {
  auto value =
      GetOptionalNamedParameter(named_parameters, "effective_user_name");
  if (!value.empty()) {
    return value;
  }
  return Trimmed(config.effective_user_name);
}

static int64_t ResolveTimeoutMs(const PowerBIConnectionConfig &config,
                                const named_parameter_map_t &named_parameters) {
  if (HasNonNullNamedParameter(named_parameters, "timeout_ms")) {
    auto entry = named_parameters.find("timeout_ms");
    auto timeout_ms = entry->second.GetValue<int64_t>();
    if (timeout_ms <= 0) {
      throw InvalidInputException("timeout_ms must be greater than zero");
    }
    return timeout_ms;
  }
  if (config.has_timeout_ms) {
    return config.timeout_ms;
  }
  return 300000;
}

static int64_t
ResolveSchemaProbeRows(const named_parameter_map_t &named_parameters) {
  if (HasNonNullNamedParameter(named_parameters, "schema_probe_rows")) {
    auto entry = named_parameters.find("schema_probe_rows");
    auto row_limit = entry->second.GetValue<int64_t>();
    if (row_limit < 0) {
      throw InvalidInputException("schema_probe_rows must be zero or greater");
    }
    return row_limit;
  }
  auto *raw_value = std::getenv("PBI_SCANNER_SCHEMA_PROBE_ROWS");
  if (!raw_value || !*raw_value) {
    return DEFAULT_SCHEMA_PROBE_ROWS;
  }
  try {
    auto row_limit = std::stoll(raw_value);
    if (row_limit < 0) {
      return DEFAULT_SCHEMA_PROBE_ROWS;
    }
    return row_limit;
  } catch (const std::exception &) {
    return DEFAULT_SCHEMA_PROBE_ROWS;
  }
}

static void RegisterCommonDaxNamedParameters(TableFunction &function) {
  function.named_parameters["auth_mode"] = LogicalType::VARCHAR;
  function.named_parameters["access_token"] = LogicalType::VARCHAR;
  function.named_parameters["secret_name"] = LogicalType::VARCHAR;
  function.named_parameters["tenant_id"] = LogicalType::VARCHAR;
  function.named_parameters["client_id"] = LogicalType::VARCHAR;
  function.named_parameters["client_secret"] = LogicalType::VARCHAR;
  function.named_parameters["effective_user_name"] = LogicalType::VARCHAR;
  function.named_parameters["timeout_ms"] = LogicalType::BIGINT;
  function.named_parameters["schema_probe_rows"] = LogicalType::BIGINT;
}

static bool IsMwcXmlaUnauthorized(const string &xmla_auth_scheme,
                                  const string &message) {
  return xmla_auth_scheme == "MwcToken" &&
         StringUtil::Lower(message).find("http 401") != string::npos;
}

static void FallbackToLegacyBearerXmla(const PowerBIConnectionConfig &config,
                                       PowerBIResolvedTarget &target,
                                       const string &access_token,
                                       int64_t timeout_ms, string &xmla_catalog,
                                       string &xmla_auth_scheme,
                                       string &xmla_access_token) {
  target.aixl_url = ResolveLegacyPowerBIXmlaUrl(config.endpoint, target,
                                                access_token, timeout_ms);
  target.core_server_name.clear();
  xmla_catalog = target.internal_catalog;
  xmla_auth_scheme = "Bearer";
  xmla_access_token = access_token;
}

//! Sets catalog, auth scheme, and token for a resolved Power BI target (not
//! direct XMLA). Capacity-backed targets use dataset catalog + MWC token.
static void ApplyXmlaAuthForResolvedPowerBiTarget(
    const PowerBIConnectionConfig &config, const PowerBIResolvedTarget &target,
    const string &access_token, int64_t timeout_ms, string &xmla_catalog,
    string &xmla_auth_scheme, string &xmla_access_token) {
  if (!target.capacity_object_id.empty()) {
    xmla_catalog = target.dataset_name;
    auto xmla_token_start = std::chrono::steady_clock::now();
    xmla_access_token = GeneratePowerBIXmlaToken(config.endpoint, target,
                                                 access_token, timeout_ms);
    DebugTiming("GeneratePowerBIXmlaToken", xmla_token_start);
    xmla_auth_scheme = "MwcToken";
  } else {
    xmla_catalog = target.internal_catalog;
    xmla_auth_scheme = "Bearer";
    xmla_access_token = access_token;
  }
}

//! Full retry is only for SOAP fault-style probe failures (TOPN may be
//! rejected). Transport, HTTP, and interrupts must not trigger a second probe.
static bool LimitedSchemaProbeFailureAllowsFullRetry(const Exception &ex) {
  auto msg = StringUtil::Lower(string(ex.what()));
  if (msg.find("request failed") != string::npos) {
    return false;
  }
  if (msg.find("xmla schema probe http") != string::npos) {
    return false;
  }
  return msg.find("xmla schema probe failed") != string::npos;
}

static unique_ptr<FunctionData> DaxQueryBind(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names) {
  auto connection_string = input.inputs[0].GetValue<string>();
  auto dax_text = input.table_function.extra_info;
  if (dax_text.empty()) {
    dax_text = input.inputs[1].GetValue<string>();
  }

  auto config = ParsePowerBIConnectionString(connection_string);
  auto trimmed_dax = dax_text;
  StringUtil::Trim(trimmed_dax);
  if (trimmed_dax.empty()) {
    throw InvalidInputException("dax_text is required");
  }

  auto bind_start = std::chrono::steady_clock::now();
  auto access_token_start = std::chrono::steady_clock::now();
  auto power_bi_aad_token =
      ResolvePowerBIAccessToken(context, config, input.named_parameters);
  DebugTiming("ResolvePowerBIAccessToken", access_token_start);
  auto effective_user_name =
      ResolveEffectiveUserName(config, input.named_parameters);
  auto timeout_ms = ResolveTimeoutMs(config, input.named_parameters);
  auto schema_probe_rows = ResolveSchemaProbeRows(input.named_parameters);
  PowerBIResolvedTarget target;
  bool target_from_cache = false;
  string xmla_catalog;
  string xmla_auth_scheme = "Bearer";
  string xmla_access_token = power_bi_aad_token;
  if (config.is_direct_xmla) {
    target.aixl_url = config.data_source;
    target.internal_catalog = config.initial_catalog;
  } else {
    target_from_cache = TryGetCachedTarget(config, target);
    if (!target_from_cache) {
      auto resolver_start = std::chrono::steady_clock::now();
      target = ResolvePowerBITarget(config.endpoint, config.initial_catalog,
                                    power_bi_aad_token, timeout_ms);
      DebugTiming("ResolvePowerBITarget", resolver_start);
      StoreCachedTarget(config, target);
    }
    ApplyXmlaAuthForResolvedPowerBiTarget(config, target, power_bi_aad_token,
                                          timeout_ms, xmla_catalog,
                                          xmla_auth_scheme, xmla_access_token);
  }
  if (xmla_catalog.empty()) {
    xmla_catalog = target.internal_catalog;
  }

  std::shared_ptr<HttpClient> xmla_http_client;
  std::vector<XmlaColumn> columns;
  if (!TryGetCachedSchema(target, dax_text, effective_user_name, columns)) {
    auto full_probe_statement = dax_text;
    auto limited_probe_statement =
        BuildLimitedDaxSchemaProbe(dax_text, schema_probe_rows);
    auto limited_probe_available =
        limited_probe_statement != full_probe_statement;
    auto probe_schema = [&](const string &probe_statement) {
      xmla_http_client = std::make_shared<HttpClient>(timeout_ms);
      XmlaExecutor executor(timeout_ms, xmla_http_client);
      XmlaRequest request;
      request.url = target.aixl_url;
      request.catalog = xmla_catalog;
      request.auth_scheme = xmla_auth_scheme;
      request.access_token = xmla_access_token;
      request.xmla_server = target.core_server_name;
      request.xmla_workspace_id = target.workspace_id;
      request.statement = probe_statement;
      request.effective_user_name = effective_user_name;
      request.timeout_ms = timeout_ms;
      auto probe_start = std::chrono::steady_clock::now();
      auto probed_columns = executor.ProbeSchema(request);
      DebugTiming("ProbeSchema", probe_start);
      return probed_columns;
    };
    auto probe_schema_with_fallback = [&]() {
      if (!limited_probe_available) {
        return probe_schema(full_probe_statement);
      }
      try {
        if (DebugTimingsEnabled()) {
          std::fprintf(stderr,
                       "[pbi_scanner] limited schema probe rows: %lld\n",
                       static_cast<long long>(schema_probe_rows));
        }
        return probe_schema(limited_probe_statement);
      } catch (const InterruptException &) {
        throw;
      } catch (const Exception &ex) {
        if (!LimitedSchemaProbeFailureAllowsFullRetry(ex)) {
          throw;
        }
        if (DebugTimingsEnabled()) {
          std::fprintf(stderr,
                       "[pbi_scanner] limited schema probe failed, retrying "
                       "full probe: %s\n",
                       ex.what());
        }
        return probe_schema(full_probe_statement);
      }
    };
    try {
      columns = probe_schema_with_fallback();
    } catch (const Exception &ex) {
      if (IsMwcXmlaUnauthorized(xmla_auth_scheme, ex.what())) {
        FallbackToLegacyBearerXmla(config, target, power_bi_aad_token,
                                   timeout_ms, xmla_catalog, xmla_auth_scheme,
                                   xmla_access_token);
        columns = probe_schema_with_fallback();
      } else {
        if (config.is_direct_xmla || !target_from_cache) {
          throw;
        }
        InvalidateCachedTarget(config);
        target_from_cache = false;
        auto resolver_start = std::chrono::steady_clock::now();
        target = ResolvePowerBITarget(config.endpoint, config.initial_catalog,
                                      power_bi_aad_token, timeout_ms);
        DebugTiming("ResolvePowerBITarget retry", resolver_start);
        StoreCachedTarget(config, target);
        ApplyXmlaAuthForResolvedPowerBiTarget(
            config, target, power_bi_aad_token, timeout_ms, xmla_catalog,
            xmla_auth_scheme, xmla_access_token);
        columns = probe_schema_with_fallback();
      }
    } catch (const std::exception &ex) {
      if (IsMwcXmlaUnauthorized(xmla_auth_scheme, ex.what())) {
        FallbackToLegacyBearerXmla(config, target, power_bi_aad_token,
                                   timeout_ms, xmla_catalog, xmla_auth_scheme,
                                   xmla_access_token);
        columns = probe_schema_with_fallback();
      } else {
        if (config.is_direct_xmla || !target_from_cache) {
          throw;
        }
        InvalidateCachedTarget(config);
        target_from_cache = false;
        auto resolver_start = std::chrono::steady_clock::now();
        target = ResolvePowerBITarget(config.endpoint, config.initial_catalog,
                                      power_bi_aad_token, timeout_ms);
        DebugTiming("ResolvePowerBITarget retry", resolver_start);
        StoreCachedTarget(config, target);
        ApplyXmlaAuthForResolvedPowerBiTarget(
            config, target, power_bi_aad_token, timeout_ms, xmla_catalog,
            xmla_auth_scheme, xmla_access_token);
        columns = probe_schema_with_fallback();
      }
    }
    StoreCachedSchema(target, dax_text, effective_user_name, columns);
  }
  DebugTiming("DaxQueryBind total", bind_start);
  if (columns.empty()) {
    throw IOException("DAX query returned no columns");
  }

  for (const auto &column : columns) {
    names.push_back(column.name);
    return_types.push_back(column.duckdb_type);
  }

  return make_uniq<DaxQueryBindData>(
      std::move(config), std::move(target), std::move(columns),
      std::move(dax_text), std::move(xmla_catalog), std::move(xmla_auth_scheme),
      std::move(xmla_access_token), std::move(power_bi_aad_token),
      std::move(effective_user_name), timeout_ms, std::move(xmla_http_client));
}

static unique_ptr<GlobalTableFunctionState>
DaxQueryInit(ClientContext &, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<DaxQueryBindData>();
  return make_uniq<DaxQueryGlobalState>(bind_data);
}

static void DaxQueryExecute(ClientContext &context, TableFunctionInput &input,
                            DataChunk &output) {
  auto &state = input.global_state->Cast<DaxQueryGlobalState>();

  if (DebugTimingsEnabled() && !state.execute_started) {
    state.execute_started = true;
    DebugTiming("DaxQueryExecute first call", state.init_started_at);
  }

  unique_ptr<DataChunk> chunk;
  if (state.PopChunk(chunk, context)) {
    output.Move(*chunk);
  } else {
    output.SetCardinality(0);
  }
}

TableFunction CreateDaxQueryFunction() {
  TableFunction function("dax_query",
                         {LogicalType::VARCHAR, LogicalType::VARCHAR},
                         DaxQueryExecute, DaxQueryBind, DaxQueryInit);
  RegisterCommonDaxNamedParameters(function);
  return function;
}

static TableFunction CreateFixedDaxFunction(const string &name,
                                            const string &dax_text) {
  TableFunction function(name, {LogicalType::VARCHAR}, DaxQueryExecute,
                         DaxQueryBind, DaxQueryInit);
  function.extra_info = dax_text;
  RegisterCommonDaxNamedParameters(function);
  return function;
}

TableFunction CreatePbiTablesFunction() {
  return CreateFixedDaxFunction("pbi_tables", "EVALUATE INFO.VIEW.TABLES()");
}

TableFunction CreatePbiColumnsFunction() {
  return CreateFixedDaxFunction("pbi_columns", "EVALUATE INFO.VIEW.COLUMNS()");
}

TableFunction CreatePbiMeasuresFunction() {
  return CreateFixedDaxFunction("pbi_measures",
                                "EVALUATE INFO.VIEW.MEASURES()");
}

TableFunction CreatePbiRelationshipsFunction() {
  return CreateFixedDaxFunction("pbi_relationships",
                                "EVALUATE INFO.VIEW.RELATIONSHIPS()");
}

} // namespace duckdb
