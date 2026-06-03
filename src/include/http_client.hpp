#pragma once

#include "duckdb/common/types.hpp"
#include "pbi_platform.hpp"

#if PBI_USES_HTTPLIB_BACKEND
#include "httplib.hpp"
#include <mutex>
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
namespace pbi_httplib = duckdb_httplib_openssl;
#else
namespace pbi_httplib = duckdb_httplib;
#endif
#endif

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

using HttpHeaders = std::vector<std::pair<std::string, std::string>>;

struct HttpResponse {
  int status = 0;
  std::string reason;
  std::string body;
  std::string request_error;
  HttpHeaders headers;
  idx_t streamed_bytes = 0;
  idx_t streamed_chunks = 0;
  int64_t first_byte_ms = -1;
  int64_t stream_elapsed_ms = 0;

  bool HasRequestError() const { return !request_error.empty(); }
  std::string GetHeader(const std::string &name) const;
};

class HttpClient {
public:
  explicit HttpClient(int64_t timeout_ms);
  ~HttpClient();

  HttpResponse Get(const std::string &url, const HttpHeaders &headers);
  HttpResponse Post(const std::string &url, const HttpHeaders &headers,
                    const std::string &body, const std::string &content_type);
  HttpResponse Put(const std::string &url, const HttpHeaders &headers,
                   const std::string &body, const std::string &content_type);
  //! When disconnect_after_response is false, the TCP connection (if any) is
  //! left open for a follow-up request to the same host (keep-alive).
  HttpResponse
  PostStream(const std::string &url, const HttpHeaders &headers,
             const std::string &body, const std::string &content_type,
             const std::function<bool(const_data_ptr_t data, idx_t data_length)>
                 &receiver,
             bool disconnect_after_response = true);
  void Stop();

private:
#if PBI_USES_HTTPLIB_BACKEND
  pbi_httplib::Client &PrepareClient(const std::string &url,
                                     std::string &path_out);
  std::mutex mutex;
  std::unique_ptr<pbi_httplib::Client> active_client;
  std::string active_proto_host_port;
#endif
  void ClearClient();

  int64_t timeout_ms;
};

} // namespace duckdb
