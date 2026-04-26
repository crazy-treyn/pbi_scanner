#include "http_client.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#endif

namespace duckdb {

namespace {

static string ExtractHost(const string &proto_host_port) {
  auto scheme_pos = proto_host_port.find("://");
  auto host_start = scheme_pos == string::npos ? 0 : scheme_pos + 3;
  auto host_port = proto_host_port.substr(host_start);

  if (!host_port.empty() && host_port.front() == '[') {
    auto closing_bracket = host_port.find(']');
    if (closing_bracket == string::npos) {
      return string();
    }
    return host_port.substr(1, closing_bracket - 1);
  }

  auto colon = host_port.rfind(':');
  if (colon != string::npos) {
    return host_port.substr(0, colon);
  }
  return host_port;
}

static string TryResolveIPv4Address(const string &host) {
  if (host.empty()) {
    return string();
  }

  addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo *result = nullptr;
  if (getaddrinfo(host.c_str(), nullptr, &hints, &result) != 0 || !result) {
    return string();
  }

  std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> result_holder(
      result, freeaddrinfo);
  for (auto *entry = result; entry; entry = entry->ai_next) {
    if (entry->ai_family != AF_INET) {
      continue;
    }

    char buffer[INET_ADDRSTRLEN];
    auto *addr = &reinterpret_cast<sockaddr_in *>(entry->ai_addr)->sin_addr;
    if (inet_ntop(AF_INET, addr, buffer, sizeof(buffer))) {
      return buffer;
    }
  }
  return string();
}

static pbi_httplib::Headers ToHeaders(const HttpHeaders &headers) {
  pbi_httplib::Headers result;
  for (const auto &header : headers) {
    result.insert(header);
  }
  return result;
}

static HttpResponse TransformResult(pbi_httplib::Result &result) {
  HttpResponse response;
  if (result.error() == pbi_httplib::Error::Success) {
    auto &http_response = result.value();
    response.status = http_response.status;
    response.reason = http_response.reason;
    response.body = http_response.body;
    for (const auto &header : http_response.headers) {
      response.headers.emplace_back(header.first, header.second);
    }
  } else {
    response.request_error = std::to_string(static_cast<int>(result.error()));
  }
  return response;
}

} // namespace

std::string HttpResponse::GetHeader(const std::string &name) const {
  for (const auto &header : headers) {
    if (StringUtil::CIEquals(header.first, name)) {
      return header.second;
    }
  }
  return std::string();
}

HttpClient::HttpClient(int64_t timeout_ms_p)
    : timeout_ms(timeout_ms_p > 0 ? timeout_ms_p : 300000) {}

HttpClient::~HttpClient() { ClearClient(); }

pbi_httplib::Client &HttpClient::PrepareClient(const string &url,
                                               string &path_out) {
  string proto_host_port;
  HTTPUtil::DecomposeURL(url, path_out, proto_host_port);

  {
    std::lock_guard<std::mutex> guard(mutex);
    if (active_client && active_proto_host_port == proto_host_port) {
      return *active_client;
    }
  }

  auto client = make_uniq<pbi_httplib::Client>(proto_host_port);
  auto seconds = static_cast<time_t>(timeout_ms / 1000);
  auto micros = static_cast<time_t>((timeout_ms % 1000) * 1000);
  auto host = ExtractHost(proto_host_port);
  auto ipv4_address = TryResolveIPv4Address(host);
  if (!ipv4_address.empty()) {
    client->set_hostname_addr_map({{host, ipv4_address}});
  }
  client->set_address_family(AF_INET);
  client->set_follow_location(false);
  client->set_keep_alive(false);
  client->set_write_timeout(seconds, micros);
  client->set_read_timeout(seconds, micros);
  client->set_connection_timeout(seconds, micros);
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
  client->set_decompress(true);
#else
  client->set_decompress(false);
#endif

  std::lock_guard<std::mutex> guard(mutex);
  active_client = std::move(client);
  active_proto_host_port = proto_host_port;
  return *active_client;
}

void HttpClient::ClearClient() {
  std::lock_guard<std::mutex> guard(mutex);
  if (active_client) {
    active_client->stop();
  }
  active_client.reset();
  active_proto_host_port.clear();
}

HttpResponse HttpClient::Get(const string &url, const HttpHeaders &headers) {
  string path;
  auto &client_ref = PrepareClient(url, path);
  auto result = client_ref.Get(path, ::duckdb::ToHeaders(headers));
  auto response = TransformResult(result);
  if (response.HasRequestError() || response.status >= 400) {
    ClearClient();
  }
  return response;
}

HttpResponse HttpClient::Post(const string &url, const HttpHeaders &headers,
                              const string &body, const string &content_type) {
  string path;
  auto &client_ref = PrepareClient(url, path);
  auto result =
      client_ref.Post(path, ::duckdb::ToHeaders(headers), body, content_type);
  auto response = TransformResult(result);
  if (response.HasRequestError() || response.status >= 400) {
    ClearClient();
  }
  return response;
}

HttpResponse HttpClient::Put(const string &url, const HttpHeaders &headers,
                             const string &body, const string &content_type) {
  string path;
  auto &client_ref = PrepareClient(url, path);
  auto result =
      client_ref.Put(path, ::duckdb::ToHeaders(headers), body, content_type);
  auto response = TransformResult(result);
  if (response.HasRequestError() || response.status >= 400) {
    ClearClient();
  }
  return response;
}

HttpResponse HttpClient::PostStream(
    const string &url, const HttpHeaders &headers, const string &body,
    const string &content_type,
    const std::function<bool(const_data_ptr_t data, idx_t data_length)>
        &receiver,
    bool disconnect_after_response) {
  string path;
  auto &client_ref = PrepareClient(url, path);
  auto started_at = std::chrono::steady_clock::now();
  idx_t received_chunks = 0;
  idx_t received_bytes = 0;
  int64_t first_byte_ms = -1;
  auto result = client_ref.Post(
      path, ::duckdb::ToHeaders(headers), body, content_type,
      [&](const char *data, size_t data_length) {
        if (first_byte_ms < 0) {
          first_byte_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
        }
        received_chunks++;
        received_bytes += data_length;
        auto should_continue = receiver(const_data_ptr_cast(data), data_length);
        if (!should_continue) {
          client_ref.stop();
        }
        return should_continue;
      });
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - started_at)
                        .count();
  auto response = TransformResult(result);
  response.streamed_bytes = received_bytes;
  response.streamed_chunks = received_chunks;
  response.first_byte_ms = first_byte_ms;
  response.stream_elapsed_ms = elapsed_ms;
  if (DebugTimingsEnabled()) {
    auto content_type = response.GetHeader("Content-Type");
    auto content_encoding = response.GetHeader("Content-Encoding");
    auto transfer_encoding = response.GetHeader("Transfer-Encoding");
    auto negotiation_flags =
        response.GetHeader("X-Transport-Caps-Negotiation-Flags");
    std::fprintf(stderr,
                 "[pbi_scanner] HTTP PostStream: %llu bytes in %llu chunks "
                 "(first byte %lld ms, total %lld ms, content-type \"%s\", "
                 "content-encoding \"%s\", transfer-encoding \"%s\", "
                 "transport-flags \"%s\")\n",
                 static_cast<unsigned long long>(received_bytes),
                 static_cast<unsigned long long>(received_chunks),
                 static_cast<long long>(first_byte_ms),
                 static_cast<long long>(elapsed_ms), content_type.c_str(),
                 content_encoding.c_str(), transfer_encoding.c_str(),
                 negotiation_flags.c_str());
  }
  if (disconnect_after_response || response.HasRequestError() ||
      response.status >= 400) {
    ClearClient();
  }
  return response;
}

void HttpClient::Stop() {
  std::lock_guard<std::mutex> guard(mutex);
  if (active_client) {
    active_client->stop();
  }
}

} // namespace duckdb
