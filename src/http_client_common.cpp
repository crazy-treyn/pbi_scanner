#include "http_client_common.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/common/string_util.hpp"

#include <cstdio>

namespace duckdb {

std::string HttpResponse::GetHeader(const std::string &name) const {
	for (const auto &header : headers) {
		if (StringUtil::CIEquals(header.first, name)) {
			return header.second;
		}
	}
	return std::string();
}

void LogHttpPostStreamTimings(const HttpResponse &response, const char *label) {
	if (!DebugTimingsEnabled()) {
		return;
	}
	auto content_type_header = response.GetHeader("Content-Type");
	auto content_encoding = response.GetHeader("Content-Encoding");
	auto transfer_encoding = response.GetHeader("Transfer-Encoding");
	auto negotiation_flags = response.GetHeader("X-Transport-Caps-Negotiation-Flags");
	std::fprintf(stderr,
	             "[pbi_scanner] %s PostStream: %llu bytes in %llu chunks (first byte %lld ms, "
	             "total %lld ms, content-type \"%s\", content-encoding \"%s\", "
	             "transfer-encoding \"%s\", transport-flags \"%s\")\n",
	             label, static_cast<unsigned long long>(response.streamed_bytes),
	             static_cast<unsigned long long>(response.streamed_chunks),
	             static_cast<long long>(response.first_byte_ms),
	             static_cast<long long>(response.stream_elapsed_ms), content_type_header.c_str(),
	             content_encoding.c_str(), transfer_encoding.c_str(), negotiation_flags.c_str());
}

} // namespace duckdb
