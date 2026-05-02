# This file is included by DuckDB's build system. It specifies which extension to load.

duckdb_extension_load(pbi_scanner
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    EXTENSION_VERSION 0.0.3
)
