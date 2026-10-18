# This file is included by DuckDB's build system. It specifies which extension to load.

duckdb_extension_load(pbi_scanner
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    EXTENSION_VERSION 0.0.6
)

# v1.5.6 links every duckdb_extension_load entry. Main links only names passed to
# duckdb_extension_statically_link, which is what pbi_scanner_unit_tests follows.
if(COMMAND duckdb_extension_statically_link)
  duckdb_extension_statically_link(pbi_scanner)
endif()
