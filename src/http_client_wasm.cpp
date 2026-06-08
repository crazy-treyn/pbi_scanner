#include "http_client.hpp"
#include "http_client_common.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <emscripten.h>

namespace duckdb {

namespace {

struct WasmHttpResult {
  uint32_t status;
  uint32_t body_ptr;
  uint32_t body_len;
  uint32_t headers_ptr;
  uint32_t headers_len;
  uint32_t error_ptr;
  uint32_t error_len;
};

static void AbortWasmHttpClient(uintptr_t client_id) {
  // clang-format off
  EM_ASM(
      {
        var clientId = $0;
        if (!Module.pbi_xhr_by_client) {
          return;
        }
        var xhr = Module.pbi_xhr_by_client[clientId];
        if (xhr) {
          try {
            xhr.abort();
          } catch (e) {
          }
          delete Module.pbi_xhr_by_client[clientId];
        }
      },
      client_id);
  // clang-format on
}

static string HeaderLines(const HttpHeaders &headers,
                          const string &content_type) {
  string result;
  bool has_content_type = false;
  for (const auto &header : headers) {
    if (StringUtil::CIEquals(header.first, "Content-Type")) {
      has_content_type = true;
    }
    result += header.first;
    result.push_back('\t');
    result += header.second;
    result.push_back('\n');
  }
  if (!content_type.empty() && !has_content_type) {
    result += "Content-Type\t";
    result += content_type;
    result.push_back('\n');
  }
  return result;
}

static void FreeWasmHttpResult(WasmHttpResult *result) {
  if (!result) {
    return;
  }
  if (result->body_ptr) {
    std::free(reinterpret_cast<void *>(result->body_ptr));
  }
  if (result->headers_ptr) {
    std::free(reinterpret_cast<void *>(result->headers_ptr));
  }
  if (result->error_ptr) {
    std::free(reinterpret_cast<void *>(result->error_ptr));
  }
  std::free(result);
}

static string CopyWasmString(uint32_t ptr, uint32_t len) {
  if (!ptr || len == 0) {
    return string();
  }
  return string(reinterpret_cast<const char *>(ptr), len);
}

static void ParseResponseHeaders(const string &raw_headers,
                                 HttpHeaders &headers) {
  idx_t start = 0;
  while (start < raw_headers.size()) {
    auto end = raw_headers.find('\n', start);
    if (end == string::npos) {
      end = raw_headers.size();
    }
    auto line = raw_headers.substr(start, end - start);
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    auto colon = line.find(':');
    if (colon != string::npos) {
      auto name = Trimmed(line.substr(0, colon));
      auto value = Trimmed(line.substr(colon + 1));
      if (!name.empty()) {
        headers.emplace_back(std::move(name), std::move(value));
      }
    }
    start = end + 1;
  }
}

static HttpResponse
ExecuteWasmHttpRequest(HttpClient &client, const string &method,
                       const string &url, const HttpHeaders &headers,
                       const string &body, const string &content_type,
                       int64_t timeout_ms) {
  client.PrepareHttpRequest();
  auto header_lines = HeaderLines(headers, content_type);
  auto stop_flag_ptr = client.StopFlagPtr();
  auto client_id = reinterpret_cast<uintptr_t>(&client);
  // clang-format off
  auto *result = reinterpret_cast<WasmHttpResult *>(EM_ASM_PTR(
      {
        var method = UTF8ToString($0);
        var url = UTF8ToString($1);
        var headers = UTF8ToString($2);
        var bodyPtr = $3;
        var bodyLen = $4;
        var timeoutMs = $5;
        var stopPtr = $6;
        var clientId = $7;

        function isStopped() { return HEAPU8[stopPtr] !== 0; }

        function copyStringToHeap(value) {
          var text = value || "";
          var len = lengthBytesUTF8(text);
          var ptr = len ? _malloc(len + 1) : 0;
          if (len) {
            stringToUTF8Array(text, HEAPU8, ptr, len + 1);
          }
          var pair = new Array(2);
          pair[0] = ptr;
          pair[1] = len;
          return pair;
        }

        function copyBytesToHeap(bytes) {
          var len = bytes ? bytes.length : 0;
          var ptr = len ? _malloc(len) : 0;
          if (len) {
            HEAPU8.set(bytes, ptr);
          }
          var pair = new Array(2);
          pair[0] = ptr;
          pair[1] = len;
          return pair;
        }

        function setError(out, message) {
          var errorHeap = copyStringToHeap(message);
          HEAPU32[(out >> 2) + 5] = errorHeap[0];
          HEAPU32[(out >> 2) + 6] = errorHeap[1];
        }

        function clearClientXhr() {
          if (Module.pbi_xhr_by_client) {
            delete Module.pbi_xhr_by_client[clientId];
          }
        }

        var out = _malloc(7 * 4);
        HEAPU32[out >> 2] = 0;
        HEAPU32[(out >> 2) + 1] = 0;
        HEAPU32[(out >> 2) + 2] = 0;
        HEAPU32[(out >> 2) + 3] = 0;
        HEAPU32[(out >> 2) + 4] = 0;
        HEAPU32[(out >> 2) + 5] = 0;
        HEAPU32[(out >> 2) + 6] = 0;

        try {
          if (typeof XMLHttpRequest === "undefined") {
            throw new Error("XMLHttpRequest is not available in this WASM runtime");
          }
          if (isStopped()) {
            setError(out, "WASM browser HTTP request was cancelled");
            return out;
          }

          var xhr = new XMLHttpRequest();
          Module.pbi_xhr_by_client = Module.pbi_xhr_by_client || {};
          Module.pbi_xhr_by_client[clientId] = xhr;
          xhr.open(method, url, false);
          xhr.responseType = "arraybuffer";
          if (timeoutMs > 0) {
            xhr.timeout = timeoutMs;
          }
          var lines = headers.split("\n");
          for (var i = 0; i < lines.length; i++) {
            if (!lines[i]) {
              continue;
            }
            var tab = lines[i].indexOf("\t");
            if (tab <= 0) {
              continue;
            }
            var name = lines[i].substring(0, tab);
            var value = lines[i].substring(tab + 1);
            xhr.setRequestHeader(name, value);
          }
          if (isStopped()) {
            setError(out, "WASM browser HTTP request was cancelled");
            clearClientXhr();
            return out;
          }
          if (bodyLen > 0) {
            var payload = HEAPU8.slice(bodyPtr, bodyPtr + bodyLen);
            xhr.send(payload);
          } else {
            xhr.send(null);
          }

          if (isStopped() || xhr.status === 0) {
            var cancelMessage = isStopped()
                ? "WASM browser HTTP request was cancelled"
                : "WASM browser HTTP request failed; check CORS headers or route through a proxy";
            setError(out, cancelMessage);
            clearClientXhr();
            return out;
          }

          var bodyBytes;
          if (xhr.response) {
            bodyBytes = new Uint8Array(xhr.response);
          } else if (xhr.responseText) {
            bodyBytes = new TextEncoder().encode(xhr.responseText);
          } else {
            bodyBytes = new Uint8Array(0);
          }
          var bodyHeap = copyBytesToHeap(bodyBytes);
          var headerHeap = copyStringToHeap(xhr.getAllResponseHeaders() || "");
          HEAPU32[out >> 2] = xhr.status || 0;
          HEAPU32[(out >> 2) + 1] = bodyHeap[0];
          HEAPU32[(out >> 2) + 2] = bodyHeap[1];
          HEAPU32[(out >> 2) + 3] = headerHeap[0];
          HEAPU32[(out >> 2) + 4] = headerHeap[1];
        } catch (error) {
          var message = "WASM browser HTTP request failed; check CORS headers or route through a proxy";
          if (error && error.message) {
            message += ": " + error.message;
          }
          setError(out, message);
        } finally {
          clearClientXhr();
        }
        return out;
      },
      method.c_str(), url.c_str(), header_lines.c_str(), body.data(),
      body.size(), timeout_ms, stop_flag_ptr, client_id));
  // clang-format on

  HttpResponse response;
  if (!result) {
    response.request_error =
        "WASM browser HTTP request failed; no result returned";
    return response;
  }
  response.status = static_cast<int>(result->status);
  response.body = CopyWasmString(result->body_ptr, result->body_len);
  response.request_error = CopyWasmString(result->error_ptr, result->error_len);
  auto raw_headers = CopyWasmString(result->headers_ptr, result->headers_len);
  ParseResponseHeaders(raw_headers, response.headers);
  FreeWasmHttpResult(result);
  return response;
}

} // namespace

HttpClient::HttpClient(int64_t timeout_ms_p)
    : timeout_ms(timeout_ms_p > 0 ? timeout_ms_p : 300000) {}

HttpClient::~HttpClient() { ClearClient(); }

void HttpClient::ClearClient() {
  stop_requested.store(false, std::memory_order_release);
  AbortWasmHttpClient(reinterpret_cast<uintptr_t>(this));
}

HttpResponse HttpClient::Get(const string &url, const HttpHeaders &headers) {
  return ExecuteWasmHttpRequest(*this, "GET", url, headers, string(), string(),
                                timeout_ms);
}

HttpResponse HttpClient::Post(const string &url, const HttpHeaders &headers,
                              const string &body, const string &content_type) {
  return ExecuteWasmHttpRequest(*this, "POST", url, headers, body, content_type,
                                timeout_ms);
}

HttpResponse HttpClient::Put(const string &url, const HttpHeaders &headers,
                             const string &body, const string &content_type) {
  return ExecuteWasmHttpRequest(*this, "PUT", url, headers, body, content_type,
                                timeout_ms);
}

HttpResponse HttpClient::PostStream(
    const string &url, const HttpHeaders &headers, const string &body,
    const string &content_type,
    const std::function<bool(const_data_ptr_t data, idx_t data_length)>
        &receiver,
    bool disconnect_after_response) {
  (void)disconnect_after_response;
  auto started_at = std::chrono::steady_clock::now();
  auto response = Post(url, headers, body, content_type);
  if (!response.body.empty()) {
    response.first_byte_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    response.streamed_chunks = 1;
    response.streamed_bytes = response.body.size();
    (void)receiver(const_data_ptr_cast(response.body.data()),
                   response.body.size());
  }
  response.stream_elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started_at)
          .count();
  LogHttpPostStreamTimings(response, "WASM HTTP");
  return response;
}

void HttpClient::Stop() {
  stop_requested.store(true, std::memory_order_release);
  AbortWasmHttpClient(reinterpret_cast<uintptr_t>(this));
}

} // namespace duckdb
