# Repository Guide For Agents

## Scope

- This repository builds a DuckDB C++ extension named `pbi_scanner`.
- `pbi_scanner` queries Power BI Semantic Models through DAX/XMLA and exposes DuckDB table functions for data and metadata access.
- The extension code you will usually edit lives in `src/`, `src/include/`, and `test/sql/`.
- `duckdb/` is a pinned submodule and `extension-ci-tools/` provides shared build logic.
- Treat `duckdb/` and `extension-ci-tools/` as upstream or vendored unless the task explicitly asks you to change them.
- The primary workflows are extension build, DuckDB sqllogictest execution, formatting, and clang-tidy checks.
- The current stable build target is DuckDB 1.5.2. The CI workflow also has a DuckDB `main` forward-compatibility job.

## High-Level Architecture

- `src/pbi_scanner_extension.cpp`: extension load path and SQL function registration.
- `src/dax_query.cpp`: DuckDB table-function binding/execution for DAX query results.
- `src/auth.cpp`: Azure CLI, access-token, service-principal, and DuckDB secret auth plumbing.
- `src/connection_string.cpp`: Power BI/XMLA connection string parsing.
- `src/powerbi_resolver.cpp`: Power BI locator resolution to concrete XMLA endpoints.
- `src/http_client.cpp`: native HTTP transport wrappers.
- `src/xmla.cpp`: XMLA request/response handling, schema probes, XML/binary parsing, transport diagnostics, and metadata cache logic.
- `src/include/`: shared extension headers.
- `test/sql/`: deterministic offline sqllogictests; do not add live Power BI requirements here.

## Repo Layout

- `src/`: extension implementation files.
- `src/include/`: extension headers.
- `test/sql/`: sqllogictest files; this is the normal test surface for extension behavior.
- `CMakeLists.txt`: extension source list and OpenSSL linkage.
- `Makefile`: delegates to `extension-ci-tools/makefiles/duckdb_extension.Makefile` and bootstraps local format/tidy tools via `uv`.
- `extension_config.cmake`: tells DuckDB to load `pbi_scanner` and tests.
- `README.md`: local build, test, runtime, and Windows notes.
- `pyproject.toml`: local Python tool metadata for **uv** (`uv run`, `uv run --group bench`, `uv run --group format`, `uv run --group tidy`); not part of the C++ build.
- `query_semantic_model_minimal.py`: **local-only** helper for real Power BI DAX runs and performance benchmarking (not CI). Prefer `PBI_BENCH_`* env vars; do not commit secrets. The repo ships only fictional placeholder defaults; real workspaces belong in env vars, `.env` (gitignored), or `local/`. Run with `uv run --group bench query_semantic_model_minimal.py` (or rely on the bundled `duckdb` CLI if Python `duckdb` is not used). Supports `PBI_BENCH_MODE=count|materialize`, `PBI_BENCH_ITERATIONS`, `PBI_BENCH_DIRECT_XMLA`, metadata probe matrix/strict modes (`PBI_BENCH_METADATA_PROBE`, `PBI_BENCH_METADATA_MATRIX`, `PBI_BENCH_METADATA_STRICT_SX`), and `PBI_SCANNER_XMLA_TRANSPORT=plain|xpress|sx|sx_xpress`; unset transport defaults to `sx_xpress`. Use `PBI_SCANNER_DISABLE_METADATA_CACHE=1` for cold target/schema benchmarks.
- `query_semantic_model_sql_minimal.py`: **local-only** SQL smoke helper for the smallest practical live DuckDB CLI path. It loads the local extension, optionally installs/loads DuckDB `azure`, creates an Azure CLI credential-chain secret, runs `dax_query`, and prints CLI output. Use it to separate SQL/extension behavior from Python DuckDB/Polars benchmark harness overhead. Run with `uv run query_semantic_model_sql_minimal.py`; set `PBI_SQL_INSTALL_AZURE=0` after the first successful Azure extension install to skip repeated install overhead.
- `bench_duckdb_cli.py`: shared helpers for that CLI path (used by `bench_native_http.py --live` and the minimal script fallback).
- `bench_native_http.py`: optional helper—`uv run bench_native_http.py --smoke` (offline, CI-friendly manual check); `uv run bench_native_http.py --live` times `dax_query` via the bundled CLI and `PBI_BENCH_`*. For day-to-day real-query perf, prefer `query_semantic_model_minimal.py` (see README Performance section).
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

## DuckDB Version Bump Workflow

- Treat a DuckDB version bump as a coordinated change across submodules, CI, local helper tooling, docs, and validation evidence.
- For a stable/current bump, update `duckdb/` to the target DuckDB release tag and `extension-ci-tools/` to the matching release branch or commit.
- For DuckDB 1.5.2 specifically, the verified refs are:
  - `duckdb` tag `v1.5.2` at `8a5851971fae891f292c2714d86046ee018e9737`.
  - `extension-ci-tools` branch `v1.5.2` at `ec20f45aabeb9fcfcfa044dda249597f066d4826`.
- Update `.github/workflows/MainDistributionPipeline.yml` stable build and code-quality jobs so the reusable workflow ref, `duckdb_version`, and `ci_tools_version` all match the new stable release.
- If `pyproject.toml` pins Python `duckdb` for local helper/benchmark tooling, bump it to the same release and regenerate `uv.lock` with `uv lock`.
- Update `README.md` when the supported/current DuckDB release changes or when community publication instructions change.
- Prefer a clean/reconfigured build after submodule bumps because generated `build/` state may still contain paths or settings from the previous DuckDB release.
- Verify the bump before treating the commit as publishable:
  - `git -C duckdb describe --tags --exact-match`
  - `git -C duckdb rev-parse HEAD`
  - `git -C extension-ci-tools rev-parse HEAD`
  - release build
  - focused sqllogictest
  - format and tidy checks

## Forward Compatibility Testing

- Keep the stable CI job pinned to the latest validated DuckDB release.
- Add or maintain a separate `duckdb-next-build` job that uses DuckDB and extension-ci-tools `main`:
  - `uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main`
  - `duckdb_version: main`
  - `ci_tools_version: main`
  - `extension_name: pbi_scanner`
  - same `exclude_archs` as stable CI unless support intentionally differs.
- Treat `duckdb-next-build` as an early warning for upcoming DuckDB API/build changes. It is not the current release artifact.
- If stable passes but `main` fails near a DuckDB release, create a compatibility branch for the upcoming release and use `repo.ref_next` in the DuckDB Community Extensions descriptor.
- Once the new DuckDB release is out and validated, update the normal stable refs and community `repo.ref`; do not keep `repo.ref_next` as the current release pointer.

## Community Extension Publication

- DuckDB community publication is done through a descriptor PR to `duckdb/community-extensions`; do not copy built binaries into this repo.
- Add `extensions/pbi_scanner/description.yml` in the community repository. The directory name must match `extension.name` exactly.
- Use a pushed, validated commit SHA for `repo.ref`. Do not point `repo.ref` at a dirty local state, an unpushed branch, or an unvalidated commit.
- For the current DuckDB 1.5.2 release, `repo.ref` should point at the commit that contains the 1.5.2 submodule bump, CI pin bump, local tooling bump if applicable, docs updates, and passing validation.
- Descriptor fields to keep aligned with this repo:
  - `extension.name: pbi_scanner`
  - `extension.description: DuckDB extension for querying Power BI Semantic Models with DAX.`
  - `extension.language: C++`
  - `extension.build: cmake`
  - `extension.license: MIT`
  - `extension.maintainers`: confirm final GitHub handle(s) before publishing.
  - `extension.excluded_platforms: "wasm_mvp;wasm_eh;wasm_threads;windows_amd64_mingw;osx_amd64"`
  - `repo.github: crazy-treyn/pbi_scanner`
  - `repo.ref`: validated current release commit SHA.
- Use `repo.ref_next` only for future-release compatibility when DuckDB `main` needs a different commit than latest stable.
- Include validation evidence in the community PR description: stable CI, next CI if available, local build/test commands, and known platform exclusions.

## Extension Version Bump Runbook

- Treat extension version bumps as release-management changes that must keep local metadata, GitHub release tags, and community descriptor references in sync.
- Version format convention:
  - Use numeric semver for metadata fields (for example `0.0.1`).
  - Use `v`-prefixed tags/releases in GitHub (for example `v0.0.1`).
- Update these files/fields together for each bump:
  - `extension_config.cmake`: set `EXTENSION_VERSION` to the new numeric semver.
  - `duckdb/community-extensions` descriptor (`extensions/pbi_scanner/description.yml`): set `extension.version` to the same numeric semver.
  - `duckdb/community-extensions` descriptor: set `repo.ref` to the validated release commit SHA from this repository.
  - Optional: use `repo.ref_next` only when future DuckDB compatibility requires a different commit than stable.
- Preferred command sequence for a release bump (from this repo root):
  - `git status --short --branch`
  - `rg "EXTENSION_VERSION|0\\.0\\.1|v0\\.0\\.1" extension_config.cmake README.md .github/workflows/MainDistributionPipeline.yml AGENTS.md`
  - `make format-check`
  - `make release`
  - `./build/release/test/unittest "test/sql/pbi_scanner.test"`
  - `git add extension_config.cmake AGENTS.md README.md .github/workflows/MainDistributionPipeline.yml`
  - `git commit -m "<release prep message>"`
  - `git tag -a vX.Y.Z -m "Release vX.Y.Z"`
  - `git push origin HEAD --tags`
  - `gh release create vX.Y.Z --title "vX.Y.Z" --notes "<release notes>"`
- Verification checklist before publishing the community descriptor update:
  - Release tag exists remotely and points to the intended commit.
  - GitHub release exists for the same tag.
  - Community descriptor `extension.version` matches numeric semver.
  - Community descriptor `repo.ref` equals the exact release commit SHA.
  - Community PR body includes validation evidence (build/tests plus relevant refs).

## Test Commands

- **CI expectation:** tests under `test/sql/` are deterministic and offline (no live Power BI). `make test` exercises those.
- **Local benchmarking:** use `uv run --group bench query_semantic_model_minimal.py` (or CLI fallback) for authenticated, real-query performance; use `uv run bench_native_http.py --smoke` as an extra offline smoke after `make release` if desired.
- `make test`: runs `make test_release`.
- `make test_release`: runs the release test runner against `test/`*.
- `make test_debug`: runs the debug test runner against `test/`*.
- `make test_reldebug`: runs the reldebug test runner against `test/`*.

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
- On Windows shells without `make` or DuckDB's pinned formatter tools on `PATH`, run the formatter directly with `uv run --group format --with clang-format==11.0.1 --with cmake-format python duckdb/scripts/format.py --all --fix --noconfirm --directories src test`; replace `--fix --noconfirm` with `--check` for CI-equivalent validation.
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
- Before any `git push`, run `make format-fix` and `make format-check`; do not push while format-check fails.
- Run at least the most relevant single test before finishing.
- If behavior touches user-visible SQL errors, add or update a sqllogictest.
- If behavior touches registration, loading, or extension wiring, smoke-test with the bundled DuckDB shell when feasible.