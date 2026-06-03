#pragma once

#include "http_client.hpp"

namespace duckdb {

void LogHttpPostStreamTimings(const HttpResponse &response, const char *label);

} // namespace duckdb
