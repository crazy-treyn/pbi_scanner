# Repository Guide For Agents

## Scope

- This repository builds a DuckDB C++ extension named `pbi_scanner`.
- The extension code you will usually edit lives in `src/`, `src/include/`, and `test/sql/`.
- `duckdb/` is a pinned submodule and `extension-ci-tools/` provides shared build logic.
- Treat `duckdb/` and `extension-ci-tools/` as upstream or vendored unless the task explicitly asks you to change them.
- The primary workflows are extension build, DuckDB sqllogictest execution, formatting, and clang-tidy checks.

## Repo Layout

- `src/`: extension implementation files.
- `src/include/`: extension headers.
- `test/sql/`: sqllogictest files; this is the normal test surface for extension behavior.
- `CMakeLists.txt`: extension source list and OpenSSL linkage.
- `Makefile`: delegates to `extension-ci-tools/makefiles/duckdb_extension.Makefile` and bootstraps local format/tidy tools via `uv`.
- `extension_config.cmake`: tells DuckDB to load `pbi_scanner` and tests.
- `README.md`: local build, test, runtime, and Windows notes.
- `pyproject.toml`: local Python tool metadata for **uv** (`uv run`, `uv run --group bench`, `uv run --group format`, `uv run --group tidy`); not part of the C++ build.
- `query_semantic_model_minimal.py`: **local-only** helper for real Power BI DAX runs and performance benchmarking (not CI). Prefer `PBI_BENCH_*` env vars; do not commit secrets. The repo ships only fictional placeholder defaults; real workspaces belong in env vars, `.env` (gitignored), or `local/`. Run with `uv run --group bench query_semantic_model_minimal.py` (or rely on the bundled `duckdb` CLI if Python `duckdb` is not used). Supports `PBI_BENCH_MODE=count|materialize`, `PBI_BENCH_ITERATIONS`, `PBI_BENCH_DIRECT_XMLA`, metadata probe matrix/strict modes (`PBI_BENCH_METADATA_PROBE`, `PBI_BENCH_METADATA_MATRIX`, `PBI_BENCH_METADATA_STRICT_SX`), and `PBI_SCANNER_XMLA_TRANSPORT=plain|xpress|sx|sx_xpress`; unset transport defaults to `sx_xpress`. Use `PBI_SCANNER_DISABLE_METADATA_CACHE=1` for cold target/schema benchmarks.
- `bench_duckdb_cli.py`: shared helpers for that CLI path (used by `bench_native_http.py --live` and the minimal script fallback).
- `bench_native_http.py`: optional helper—`uv run bench_native_http.py --smoke` (offline, CI-friendly manual check); `uv run bench_native_http.py --live` times `dax_query` via the bundled CLI and `PBI_BENCH_*`. For day-to-day real-query perf, prefer `query_semantic_model_minimal.py` (see README Performance section).
- `.github/workflows/MainDistributionPipeline.yml`: CI runs build distribution plus code quality checks.

## Editor / Agent Rules

- No repo-local Cursor or Copilot rules were found: there is no `.cursorrules`, no `.cursor/rules/`, and no `.github/copilot-instructions.md`.
- Do not assume hidden editor-specific instructions beyond this file and upstream DuckDB conventions.

## First Setup

- Clone with submodules or run `git submodule update --init --recursive` before building.
- For Python helper/benchmark scripts, install [uv](https://docs.astral.sh/uv/) and run commands from the repo root (see `pyproject.toml`).
- OpenSSL is required.
- On macOS, if CMake cannot find OpenSSL, use `OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`.
- The build is driven from the repository root.
- The default `make` target is `release`.

## Build Commands

- `make`: builds the default release configuration.
- `make release`: builds release artifacts under `build/release/`.
- `make debug`: builds debug artifacts under `build/debug/`.
- `make reldebug`: builds `RelWithDebInfo` under `build/reldebug/`.
- `make relassert`: builds `RelWithDebInfo` with forced asserts.
- `GEN=ninja make`: uses Ninja instead of the default generator.
- `CMAKE_BUILD_PARALLEL_LEVEL=4 GEN=ninja make`: bounded parallel Ninja build.
- `make clean`: removes local build outputs.

## Test Commands

- **CI expectation:** tests under `test/sql/` are deterministic and offline (no live Power BI). `make test` exercises those.
- **Local benchmarking:** use `uv run --group bench query_semantic_model_minimal.py` (or CLI fallback) for authenticated, real-query performance; use `uv run bench_native_http.py --smoke` as an extra offline smoke after `make release` if desired.
- `make test`: runs `make test_release`.
- `make test_release`: runs the release test runner against `test/*`.
- `make test_debug`: runs the debug test runner against `test/*`.
- `make test_reldebug`: runs the reldebug test runner against `test/*`.

## Run A Single Test

- Release single-file test: `./build/release/test/unittest "test/sql/pbi_scanner.test"`
- Debug single-file test: `./build/debug/test/unittest "test/sql/pbi_scanner.test"`
- General pattern: `./build/release/test/unittest "test/sql/<file>.test"`
- Windows example from `README.md`: `ctest --test-dir build\release --build-config Release --output-on-failure -R test/sql/pbi_scanner.test`

## Formatting / Lint / Quality

- `make format-check`: formatter check over `src` and `test`.
- `make format` or `make format-fix`: apply formatting fixes.
- `make tidy-check`: run clang-tidy over `src/`.
- These quality targets use `uv` to provide `black` and `clang-tidy` locally; update `pyproject.toml` and `uv.lock` if tool requirements change.
- CI runs `format` and `tidy` via `MainDistributionPipeline.yml`.

## C++ Formatting Rules

- Follow `duckdb/.clang-format` and DuckDB conventions.
- Use tabs for indentation and spaces only for alignment.
- Keep lines within 120 columns.
- Do not sort includes mechanically; `SortIncludes` is disabled.
- Prefer running `make format-fix` instead of hand-formatting large edits.

## Include Conventions

- Keep include groups separated by blank lines.
- Usual order is local project headers, then DuckDB or third-party headers, then standard library or platform headers.
- Keep platform-specific includes inside narrow `#ifdef _WIN32` blocks.
- Use `#pragma once` in headers.

## Naming Conventions

- File names: lowercase with underscores, e.g. `powerbi_resolver.cpp`.
- Types: `CamelCase`.
- Functions: `CamelCase`.
- Variables, fields, and parameters: `lower_case`.
- Macros: `UPPER_CASE`.
- Use descriptive names; avoid single-letter names except trivial loop indices.
- Prefer domain names like `PowerBIResolvedTarget`, `connection_config`, and `timeout_ms` over abbreviations.

## Type Conventions

- In headers, this repo commonly uses explicit `std::string` and fixed-width integers.
- In implementation files inside `namespace duckdb`, using DuckDB aliases like `string`, `vector`, `idx_t`, and `unique_ptr` is normal.
- Prefer `int64_t`, `uint64_t`, and `idx_t` over plain `int`, `long`, or `size_t` for DuckDB-facing counts, indices, and timeouts.
- Prefer `const` references for non-trivial inputs.
- Prefer `unique_ptr` and `make_uniq` for ownership.
- Avoid raw owning pointers, `new`, `delete`, and `malloc`.

## Namespace Rules

- Production C++ code should live in `namespace duckdb`.
- File-local helpers should usually live in an anonymous namespace.
- Do not add broad `using namespace ...` directives.
- A narrow alias or `using` for third-party namespaces in `.cpp` files is acceptable when already required by the API.

## Control Flow

- Prefer early returns over deep nesting.
- Validate inputs close to the boundary.
- Keep helper functions small and single-purpose.
- Use braces consistently around conditionals and loops.
- Avoid clever compact code when straightforward code is easier to audit.
- Avoid magic numbers; promote meaningful repeated values to named constants or `constexpr` values.

## Error Handling

- Follow DuckDB's exception style.
- Use `InvalidInputException` for user-facing validation failures.
- Use `IOException` for transport, HTTP, parsing, or external-process failures.
- Use `InterruptException` when propagating query interruption.
- Expected non-fatal parse probes should often return `bool`, empty strings, or optional-like state instead of throwing.
- Exception messages are part of the test surface; keep them precise and stable.
- For HTTP flows, check request-level failure first, then HTTP status, then JSON validity.

## Comments

- Keep comments sparse.
- Add comments when behavior is non-obvious, not to narrate obvious assignments.
- Preserve useful existing comments, especially around test wiring, CI, and platform-specific code.

## Testing Style

- Prefer sqllogictest files in `test/sql/` over new C++ unit tests.
- Add tests for both happy paths and failure paths.
- For validation errors, use `statement error` blocks and assert exact message text when practical.
- Keep automated tests deterministic and offline.
- Do not add tests that require live Power BI or Azure access to pass in CI.
- If a bug changes a user-visible error message or parse rule, update the sqllogictest coverage in the same change.

## DuckDB Extension Patterns

- Register new SQL functions in the extension load path in `src/pbi_scanner_extension.cpp`.
- Keep extension APIs aligned with DuckDB types and exception classes.
- Reuse DuckDB utilities like `StringUtil`, `Value`, `LogicalType`, and `DataChunk` instead of re-inventing equivalents.
- Cross-platform shelling or socket behavior should stay narrowly isolated, as in `auth.cpp` and `http_client.cpp`.
- XMLA execution defaults to `sx_xpress` for the fastest observed Power BI XMLA execution path. Binary transports use `application/xml+xpress` for schema probes to preserve real column names, then execute with the selected binary transport. `PBI_SCANNER_XMLA_TRANSPORT=plain` forces text XML fallback, `xpress` uses XPRESS plus the text XML parser for both schema and execution, and `sx` uses uncompressed SSAS binary XML for execution.
- XMLA binary parser implementation is independent, based on Microsoft Open Specifications plus observed wire behavior/interoperability testing, and is not copied from ADOMD.NET source.
- Resolved XMLA targets and schemas are persisted in a local metadata cache by default. Do not store tokens or secrets there. `PBI_SCANNER_CACHE_DIR` overrides the location, `PBI_SCANNER_DISABLE_METADATA_CACHE=1` disables it, and target/schema TTL env vars control expiry.
- Keep XMLA transport diagnostics precise: content type, content encoding, byte/chunk counts, first byte, decompression, parse, first row, and total timings are useful benchmark signals.

## Python Script Conventions

- Run repo Python helpers with **uv** from the repository root, e.g. `uv run bench_native_http.py --smoke` or `uv run --group bench query_semantic_model_minimal.py` (see `pyproject.toml` dependency groups). Do not assume bare `python3` for project scripts unless the user says otherwise.
- Match the existing lightweight helper-script style.
- Standard library imports first, then third-party imports.
- Use 4-space indentation.
- Add type hints to functions.
- Keep module constants in `UPPER_CASE`.
- Prefer `pathlib.Path` over manual path concatenation.
- Use `subprocess.run(..., check=True, capture_output=True, text=True)` for shell calls.
- Raise concrete built-in exceptions like `ValueError` or `FileNotFoundError` for script-level validation failures.
- Keep helper scripts explicit and self-contained; do not introduce a new Python framework without a clear repo-wide need.

## Change Boundaries

- Most feature work should stay within `src/`, `src/include/`, `test/sql/`, `README.md`, and this file.
- Avoid editing `duckdb/` unless the task explicitly requires an upstream change.
- Avoid editing `extension-ci-tools/` unless the task is specifically about build infrastructure.
- If you change build behavior, update both the relevant config file and the documented command here or in `README.md`.

## Good Defaults For Agents

- Start with the smallest correct change.
- Run `make format-fix` after non-trivial C++ edits.
- Run at least the most relevant single test before finishing.
- If behavior touches user-visible SQL errors, add or update a sqllogictest.
- If behavior touches registration, loading, or extension wiring, smoke-test with the bundled DuckDB shell when feasible.