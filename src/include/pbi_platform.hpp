#pragma once

#if defined(__EMSCRIPTEN__)
#define PBI_USES_HTTPLIB_BACKEND 0
#else
#define PBI_USES_HTTPLIB_BACKEND 1
#endif

namespace duckdb {

inline constexpr bool PbiIsBrowserPlatform() {
#ifdef __EMSCRIPTEN__
	return true;
#else
	return false;
#endif
}

inline constexpr bool PbiSupportsNativeAuth() {
	return !PbiIsBrowserPlatform();
}

inline constexpr bool PbiSupportsFilesystemMetadataCache() {
	return !PbiIsBrowserPlatform();
}

inline constexpr bool PbiSupportsBackgroundThreads() {
	return !PbiIsBrowserPlatform();
}

inline constexpr bool PbiSupportsSxStreamingExecution() {
	return !PbiIsBrowserPlatform();
}

inline constexpr bool PbiUsesHttplibBackend() {
	return !PbiIsBrowserPlatform();
}

} // namespace duckdb
