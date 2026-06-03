#include "pbi_platform.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

static void ThrowBrowserUnsupportedAuth(const char *message) {
	throw InvalidInputException("%s", message);
}

} // namespace

void RejectUnsupportedBrowserAuth(const char *message) {
	if (!PbiSupportsNativeAuth()) {
		ThrowBrowserUnsupportedAuth(message);
	}
}

void RejectUnsupportedBrowserAuthForTesting(const char *message) {
	RejectUnsupportedBrowserAuth(message);
}

} // namespace duckdb
