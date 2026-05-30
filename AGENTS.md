# Repository Guide For Agents

## Scope

- This repo builds a DuckDB C++ extension named `pbi_scanner` for querying Power BI Semantic Models through DAX/XMLA.
- Normal edit surface: `src/`, `src/include/`, `test/sql/`, `README.md`, `CMakeLists.txt`, and `extension_config.cmake`.
- Treat `duckdb/` and `extension-ci-tools/` as pinned upstream/vendor trees; do not edit them unless the task explicitly requires it.
- Current stable target is DuckDB `v1.5.2`; CI also has a forward-compatibility build against DuckDB `main`.

## Architecture Pointers

- `src/pbi_scanner_extension.cpp` registers extension options and SQL table functions.
- `src/dax_query.cpp` owns DuckDB table-function binding/execution for DAX results and metadata table functions.
- `src/auth.cpp` handles Azure CLI, access-token, service-principal, and DuckDB `TYPE azure` secret auth.
- `src/powerbi_resolver.cpp` resolves Power BI locators to XMLA endpoints and MWC tokens.
- `src/xmla.cpp` handles XMLA request/response execution and XML/binary parsing.
- `src/xmla_transport.cpp` owns `PBI_SCANNER_XMLA_TRANSPORT`; default is `sx_xpress`.
- `src/metadata_cache.cpp` persists resolved targets/schemas only; never store tokens or secrets in the metadata cache.
- `test/cpp/pbi_scanner_unit_tests.cpp` exercises `*ForTesting` hooks via Catch (parser, coercion, cache, auth).

## Build And Verify

- First setup after clone: `git submodule update --init --recursive`.
- Default build: `make` or `make release`; artifacts go under `build/release/`.
- Focused offline tests: `make test-pbi-offline` (sqllogictest + Catch), or individually `./build/release/pbi_scanner_unit_tests` and `./build/release/test/unittest "test/sql/pbi_scanner.test"`.
- Full offline test suite: `make test`.
- Format check/fix: `make format-check` and `make format-fix`.
- Clang-tidy: `make tidy-check`.
- Format/tidy targets bootstrap tools with `uv` from `pyproject.toml`; run commands from repo root.
- macOS OpenSSL fallback when CMake cannot find it: `OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)" make release`.

## Native Windows Build Path

- On native Windows, use the repo wrapper from PowerShell. Do not invoke bare `cmake` directly for normal builds; the wrapper supplies DuckDB extension config, vcpkg manifest mode, static OpenSSL, and cache compatibility checks.
- First-time setup or missing toolchain: `powershell -ExecutionPolicy Bypass -File scripts\dev-win.ps1 bootstrap`.
- Standard reproducible build: `powershell -ExecutionPolicy Bypass -File scripts\dev-win.ps1 build`.
- Focused offline test: `powershell -ExecutionPolicy Bypass -File scripts\dev-win.ps1 test -R test/sql/pbi_scanner.test`.
- Wrapper defaults: `VCPKG_ROOT=local\vcpkg`, `VCPKG_TARGET_TRIPLET=x64-windows-static-release`, `VCPKG_BUILD=1`, `VCPKG_MANIFEST_MODE=ON`, and static OpenSSL. Override only with intent via environment variables.
- If a Windows executable exits immediately with `-1073741515`, suspect a runtime DLL dependency issue. Re-run the wrapper build so it can clear incompatible stale CMake cache state, then verify startup with `build\release\duckdb.exe -unsigned -c "SELECT 1;"`.
- To confirm OpenSSL is static on Windows, inspect dependencies from a VS developer shell: `dumpbin /dependents build\release\duckdb.exe` and `dumpbin /dependents build\release\unittest.exe`. Expected dependencies are normal Windows DLLs only; no `libssl*.dll` or `libcrypto*.dll`.

## Tests And Live Helpers

- Tests under `test/sql/` must be deterministic and offline; do not add live Power BI/Azure requirements there.
- Prefer sqllogictest coverage for extension behavior, especially user-visible validation errors.
- If an error message or parse rule changes, update exact `statement error` expectations in `test/sql/pbi_scanner.test`.
- Offline smoke helper: `uv run bench_native_http.py --smoke` (LOAD + `dax_query` registration, then Catch `[smoke]` tests).
- Live benchmark/helper: `uv run --group bench query_semantic_model_minimal.py`; use `PBI_BENCH_*` env vars and keep real workspace IDs/secrets in env, `.env` (gitignored), or `local/`.
- SQL CLI live smoke: `uv run query_semantic_model_sql_minimal.py`; set `PBI_SQL_USE_AZURE_SECRET=1` and `PBI_SQL_AZURE_PROVIDER=service_principal` to exercise DuckDB Azure service-principal secrets.

## XMLA Transport Notes

- `sx_xpress` is the default and fastest observed execution transport; do not change the default without fresh benchmark evidence.
- Supported debug/fallback transports are `plain`, `xpress`, `sx`, and `sx_xpress` via `PBI_SCANNER_XMLA_TRANSPORT`.
- Binary execution transports still use `application/xml+xpress` for schema probes to preserve real column names.
- Keep transport diagnostics useful: content type/encoding, byte and chunk counts, first byte, decompression, parse, first row, and total timings.
- Recent local benchmark shape for 176,754 rows: `sx_xpress` ~2.5s execution, `xpress` ~6.1s, `sx` ~8.8s, `plain` ~55s.

## Release And Versioning

- Extension version lives in `extension_config.cmake` as numeric semver; GitHub tags/releases use `v` prefix.
- For DuckDB version bumps, update together: `duckdb/` submodule, `extension-ci-tools/` submodule/ref, CI workflow refs and `duckdb_version`/`ci_tools_version`, Python `duckdb` pin in `pyproject.toml`, `uv.lock`, and README claims.
- CI stable build uses `.github/workflows/MainDistributionPipeline.yml` with DuckDB/CI tools `v1.5.2`; keep `duckdb-next-build` on `main` as an early warning job.
- Community publication is descriptor-only in `duckdb/community-extensions`; do not copy built binaries into this repo.
- Community descriptor must use a pushed, validated commit SHA for `repo.ref`, not a dirty local state or unpushed branch.
- Detailed release/community publication runbook: `docs/release_publication.md`.

## C++ Repo Conventions That Matter

- Follow DuckDB formatting (`duckdb/.clang-format`): tabs for indentation, 120-column target, includes are not auto-sorted.
- Production code lives in `namespace duckdb`; file-local helpers usually belong in an anonymous namespace.
- Use DuckDB exceptions consistently: `InvalidInputException` for user validation, `IOException` for transport/HTTP/parsing/external-process failures, `InterruptException` for cancellation.
- Register new public SQL functions through the extension load path; add offline coverage in `test/cpp/pbi_scanner_unit_tests.cpp` via existing `*ForTesting` hooks.
- Keep cross-platform shelling/socket behavior isolated to the existing auth/HTTP layers.
