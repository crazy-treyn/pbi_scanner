cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME pbi_scanner)

find_package(OpenSSL REQUIRED)
find_package(ZLIB)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(duckdb/third_party/httplib)
include_directories(duckdb/third_party/yyjson/include)

set(EXTENSION_SOURCES
    src/pbi_scanner_extension.cpp
    src/pbi_scanner_test_functions.cpp
    src/dax_probe.cpp
    src/dax_query.cpp
    src/metadata_cache.cpp
    src/vector_writer.cpp
    src/auth.cpp
    src/connection_string.cpp
    src/http_client.cpp
    src/powerbi_resolver.cpp
    src/xmla_transport.cpp
    src/xmla_util.cpp
    src/xmla_value.cpp
    src/xpress.cpp
    src/xmla.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_compile_definitions(${EXTENSION_NAME} PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
if(ZLIB_FOUND)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE CPPHTTPLIB_ZLIB_SUPPORT)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE
                             CPPHTTPLIB_ZLIB_SUPPORT)
  target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
