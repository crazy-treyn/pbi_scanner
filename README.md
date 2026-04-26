# pbi_scanner

DuckDB extension that enables querying Power BI Semantic Models with DAX.

This repo is set up for local development first. The easiest way to try it on another machine is:

1. Clone the repo with submodules.
2. Build the bundled DuckDB shell.
3. Run `dax_query(...)` directly from that shell.

## What Works Today

- `dax_query(connection_string, dax_text)`
- `pbi_tables(connection_string)`
- `pbi_columns(connection_string)`
- `pbi_measures(connection_string)`
- `pbi_relationships(connection_string)`
- Direct auth:
  - `auth_mode := 'azure_cli'`
  - `auth_mode := 'access_token'`
  - `auth_mode := 'service_principal'`
- DuckDB `TYPE azure` secrets:
  - `PROVIDER credential_chain`
  - `PROVIDER service_principal`
- Connection string forms:
  - public Power BI locator: `Data Source=powerbi://...;Initial Catalog=<model>;`
  - direct XMLA fast path: `Data Source=https://.../xmla?...;Initial Catalog=sobe_wowvirtualserver-...;`

## Connection Strings

The normal user-facing form is the public Power BI locator:

```sql
Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;
```

`pbi_scanner` also accepts a direct XMLA endpoint plus the internal catalog name:

```sql
Data Source=https://example.analysis.windows.net/xmla?vs=sobe_wowvirtualserver&db=<resolved-database-id>;Initial Catalog=sobe_wowvirtualserver-<resolved-database-id>;
```

That second form is mainly useful for advanced or local tooling when you already resolved the XMLA target and want to bypass the Power BI control-plane lookup.

## Fastest Local Test

Clone with submodules:

```bash
git clone --recurse-submodules https://github.com/crazy-treyn/pbi_scanner.git
cd pbi_scanner
```

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

Build:

```bash
make
make test
```

Run the bundled DuckDB shell:

```bash
./build/release/duckdb
```

The bundled shell already has `pbi_scanner` linked in, so you can call `dax_query(...)` right away.

## Prerequisites

### macOS

Install the basics:

```bash
xcode-select --install
brew install cmake openssl
```

Build:

```bash
OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)" make
make test
```

If `brew --prefix openssl@3` does not exist on your machine, run `brew info openssl` and point `OPENSSL_ROOT_DIR` at that install path.

### Linux

Ubuntu/Debian:

```bash
sudo apt update
sudo apt install -y git build-essential cmake pkg-config libssl-dev
```

Build:

```bash
make
make test
```

Fedora/RHEL equivalents are usually:

```bash
sudo dnf install -y git gcc-c++ make cmake pkg-config openssl-devel
```

### Windows

Recommended for the easiest first run: use WSL2 and follow the Linux steps above.

If you want a native Windows build, install:

- Visual Studio 2022 with Desktop development with C++
- CMake
- Git
- OpenSSL for Windows

Then open `x64 Native Tools Command Prompt for VS 2022` and run:

```bat
git clone --recurse-submodules <your-private-github-url>
cd pbi_scanner
cmake -S duckdb -B build\release -DDUCKDB_EXTENSION_CONFIGS=%CD%\extension_config.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build\release --config Release
ctest --test-dir build\release --build-config Release --output-on-failure -R test/sql/pbi_scanner.test
```

If CMake cannot find OpenSSL, set `OPENSSL_ROOT_DIR` first:

```bat
set OPENSSL_ROOT_DIR=C:\path\to\OpenSSL
```

## Running Locally

### Option A: Use the bundled DuckDB shell

This is the simplest option and the one I recommend first.

macOS/Linux:

```bash
./build/release/duckdb
```

Windows:

```bat
build\release\duckdb.exe
```

### Option B: Load the built extension into another DuckDB shell

If you want to use the loadable extension artifact directly, use the unsigned flag and load the local file.

macOS/Linux:

```bash
duckdb -unsigned -c "LOAD './build/release/extension/pbi_scanner/pbi_scanner.duckdb_extension';"
```

Windows:

```bat
duckdb.exe -unsigned -c "LOAD './build/release/extension/pbi_scanner/pbi_scanner.duckdb_extension';"
```

## First Live Query

### Azure CLI auth

Log in on that machine:

```bash
az login --scope "https://analysis.windows.net/powerbi/api/.default"
```

Then run:

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)',
    auth_mode := 'azure_cli'
);
```

### DuckDB `TYPE azure` secret auth

Inside DuckDB:

```sql
INSTALL azure;
LOAD azure;

CREATE SECRET pbi_cli (
    TYPE azure,
    PROVIDER credential_chain,
    CHAIN 'cli'
);

SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;Secret=pbi_cli;',
    'EVALUATE ROW("probe_ok", 1)'
);
```

Or with a named parameter:

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)',
    secret_name := 'pbi_cli'
);
```

## Performance

`dax_query` uses native HTTP XMLA: a schema probe, then streaming execute with a SAX-style XML parser. For large result sets, most time is usually the service and the network; the extension can add measurable CPU for XML parsing and value conversion.

**CI vs local benchmarking**

- **CI and automated tests** stay offline and non-authenticating: `make test` / sqllogictest under `test/sql/` (e.g. `pbi_scanner.test`), plus optional `uv run bench_native_http.py --smoke` after a release build to sanity-check extension load and the XMLA parse self-test. Nothing in CI should require live Power BI or Azure credentials.
- **Local performance and real-query benchmarking** use your own workspace: set `PBI_BENCH_CONNECTION_STRING`, `PBI_BENCH_DAX`, and optionally `PBI_BENCH_SECRET_NAME` (see root `.env.example`). The committed script only ships **fictional** placeholders (`Example%20Workspace` / `example_semantic_model`); point env vars at a real workspace for authenticated runs. The `bench` dependency group installs the Python `duckdb` package pinned to the bundled DuckDB version, plus Polars/PyArrow for materialization. Without Python `duckdb`, the same script uses the bundled `./build/release/duckdb` CLI via `bench_duckdb_cli.py`. It enables `PBI_SCANNER_DEBUG_TIMINGS` and times materialization end-to-end.
- **Never commit real targets or DAX:** use a gitignored `.env` / `.envrc`, or drop ad-hoc helpers under `local/` (see `local/README.md`).
- **Benchmark modes:** set `PBI_BENCH_MODE=count` to time `SELECT count(*)` without converting to Polars, or `PBI_BENCH_MODE=materialize` to fetch into Polars. Set `PBI_BENCH_ITERATIONS=2` or higher to observe warm target/schema cache behavior in the same process. Set `PBI_BENCH_DIRECT_XMLA=1` to resolve a `powerbi://` locator once in Python and run the extension against the direct XMLA endpoint, bypassing the extension's public-locator resolver path.
- **XMLA transport / parser selection:** `PBI_SCANNER_XMLA_TRANSPORT=sx_xpress` is the default. It uses `application/xml+xpress` for schema probes so real column names are preserved, then negotiates `application/sx+xpress` for execution, decompresses XPRESS locally, and uses the SSAS binary XML rowset parser. Set `PBI_SCANNER_XMLA_TRANSPORT=xpress` to use XPRESS plus the text XML parser for both schema and execution. Set `PBI_SCANNER_XMLA_TRANSPORT=plain` to force text XML fallback. `sx` negotiates uncompressed `application/sx` for execution and uses the same binary XML parser path, but it is mostly useful for debugging because `sx_xpress` has the smaller payload.
- **Metadata cache:** resolved Power BI XMLA targets and real XMLA schema metadata are cached on disk by default so repeat runs can skip public-locator resolution and the cold `xpress` schema probe. No access tokens or secrets are stored. Set `PBI_SCANNER_DISABLE_METADATA_CACHE=1` for cold benchmarks, `PBI_SCANNER_CACHE_DIR=/path/to/cache` to override the cache location, or `PBI_SCANNER_TARGET_CACHE_TTL_SECONDS` / `PBI_SCANNER_SCHEMA_CACHE_TTL_SECONDS` to tune the default 24-hour TTL.
- `**bench_native_http.py`** remains a generic optional helper: `uv run bench_native_http.py --smoke` (offline), or `uv run bench_native_http.py --live` with the same `PBI_BENCH_*` env vars for row-count timing through the bundled CLI (no Python `duckdb` required for that path).

**Python tooling:** use **[uv](https://docs.astral.sh/uv/)** to run repo scripts (see `pyproject.toml`). Examples: `uv run bench_native_http.py --smoke`; `uv run --group bench query_semantic_model_minimal.py` for the optional Python + Polars path.

- **Timings:** set `PBI_SCANNER_DEBUG_TIMINGS=1` when running DuckDB. The extension logs phases such as token resolution, metadata cache target/schema hits, Power BI target-resolution HTTP calls, `ProbeSchema`, HTTP response headers/bytes/chunks, xpress decompression and parse time, first row, and `ExecuteStreaming` total (see stderr). The minimal script sets this by default.
- **Compression:** if CMake finds Zlib, the HTTP client may request `gzip, deflate` (`CPPHTTPLIB_ZLIB_SUPPORT`), which can reduce response size for verbose XMLA payloads.
- **Performance findings:** see `docs/xmla_transport_performance.md` for benchmark results, parser/transport details, and recommended next steps for hardening compressed transports.

## Useful Commands

Build everything:

```bash
make
```

Run tests:

```bash
make test
```

Run local quality checks:

```bash
make format-check
make tidy-check
```

These targets use `uv` to provide formatter/linter tools locally (`black` and `clang-tidy`) so they match the CI quality workflow without requiring global Python packages.

See the current DuckDB version this repo is pinned to:

```bash
git -C duckdb describe --tags --always
```

## Troubleshooting

### `OpenSSL` not found

Make sure the OpenSSL development package is installed and visible to CMake.

- macOS: set `OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Linux: install `libssl-dev` or `openssl-devel`
- Windows: install OpenSSL and set `OPENSSL_ROOT_DIR`

### Submodule errors

Run:

```bash
git submodule update --init --recursive
```

### Azure CLI auth fails

Re-auth on that machine:

```bash
az logout
az login --scope "https://analysis.windows.net/powerbi/api/.default"
```

### Unsigned extension load error

Use the local bundled shell, or start DuckDB with `-unsigned` when loading the built `.duckdb_extension` file.

## Notes

- This repo currently builds against the DuckDB version pinned in the `duckdb` submodule.
- Extension binaries should be rebuilt for every DuckDB version you want to support.
- The longer implementation plan lives in `duckdb-extension-plan.md`.

