cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME pbi_scanner)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

if(NOT EMSCRIPTEN)
  if(WIN32)
    set(OPENSSL_USE_STATIC_LIBS TRUE)
  endif()
  find_package(OpenSSL REQUIRED)
  find_package(ZLIB)
endif()

include_directories(src/include)
include_directories(duckdb/third_party/yyjson/include)
if(NOT EMSCRIPTEN)
  include_directories(duckdb/third_party/httplib)
endif()

function(target_link_pbi_scanner_deps target)
  if(NOT EMSCRIPTEN)
    target_compile_definitions(${target} PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)
    target_link_libraries(${target} OpenSSL::SSL OpenSSL::Crypto)
    if(ZLIB_FOUND)
      target_compile_definitions(${target} PRIVATE CPPHTTPLIB_ZLIB_SUPPORT)
      target_link_libraries(${target} ZLIB::ZLIB)
    endif()
  endif()
endfunction()

set(EXTENSION_SOURCES
    src/pbi_platform.cpp
    src/pbi_scanner_extension.cpp
    src/dax_probe.cpp
    src/dax_column_names.cpp
    src/dax_query.cpp
    src/metadata_cache.cpp
    src/vector_writer.cpp
    src/auth.cpp
    src/connection_string.cpp
    src/powerbi_resolver.cpp
    src/xmla_transport.cpp
    src/xmla_util.cpp
    src/xmla_value.cpp
    src/xpress.cpp
    src/xmla.cpp
)

if(EMSCRIPTEN)
  list(APPEND EXTENSION_SOURCES src/http_client_wasm.cpp)
else()
  list(APPEND EXTENSION_SOURCES src/http_client.cpp)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_pbi_scanner_deps(${EXTENSION_NAME})
target_link_pbi_scanner_deps(${LOADABLE_EXTENSION_NAME})

if(NOT EMSCRIPTEN)
  add_executable(pbi_scanner_unit_tests test/cpp/pbi_scanner_unit_tests.cpp)
  set_target_properties(pbi_scanner_unit_tests PROPERTIES
                        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
  target_include_directories(pbi_scanner_unit_tests PRIVATE
                             ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/catch)
  # Match duckdb/test/CMakeLists.txt link order: duckdb_static before the generated
  # extension loader so Linux linkers resolve ExtensionHelper::LoadAllExtensions.
  target_link_libraries(pbi_scanner_unit_tests duckdb_static)
  link_extension_libraries(pbi_scanner_unit_tests "")
  target_link_pbi_scanner_deps(pbi_scanner_unit_tests)

  add_test(NAME pbi_scanner_unit_tests COMMAND pbi_scanner_unit_tests)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
