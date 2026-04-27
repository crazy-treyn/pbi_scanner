# pbi_scanner

DuckDB extension for querying Power BI Semantic Models with DAX.

> **Experimental**: This extension is under active development. APIs, behavior, and performance characteristics may change between releases.

## Features

- Query semantic models with `dax_query(connection_string, dax_text, ...)`
- Discover model metadata with `pbi_tables`, `pbi_columns`, `pbi_measures`, and `pbi_relationships`
- Multiple auth paths: `azure_cli`, `access_token`, and `service_principal`
- DuckDB secret integration via `TYPE azure` secrets (`credential_chain` / `service_principal`)
- Power BI locator support (`powerbi://...`) plus direct XMLA fast path (`https://.../xmla?...`)
- XMLA transport controls (`plain`, `xpress`, `sx`, `sx_xpress`) with `sx_xpress` default
- Local metadata cache for resolved targets/schemas (no token/secret persistence)

## Quick Start

### Prerequisites

- DuckDB extension toolchain requirements (CMake, C++ build toolchain, OpenSSL)
- Git with submodule support
- Optional: Azure CLI for `auth_mode := 'azure_cli'`

### Step 1: Clone and Build

```bash
git clone --recurse-submodules https://github.com/crazy-treyn/pbi_scanner.git
cd pbi_scanner
make
make test
```

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

### Step 2: Start DuckDB

```bash
./build/release/duckdb
```

The bundled shell already has `pbi_scanner` linked in.

### Step 3: Run a First Query

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)',
    auth_mode := 'azure_cli'
);
```

## Connection Configuration

### Connection String Forms

#### Power BI Locator (Typical User Input)

```sql
Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;
```

#### Direct XMLA Endpoint (Advanced / Resolved Path)

```sql
Data Source=https://example.analysis.windows.net/xmla?vs=sobe_wowvirtualserver&db=<resolved-database-id>;Initial Catalog=sobe_wowvirtualserver-<resolved-database-id>;
```

Use the direct XMLA form when you already have a resolved target and want to bypass public Power BI locator resolution.

### Authentication Modes

`dax_query(...)` supports:

- `auth_mode := 'azure_cli'`
- `auth_mode := 'access_token'`
- `auth_mode := 'service_principal'`

### DuckDB Secret-Based Auth

Install and load DuckDB's Azure extension, then create a reusable secret:

```sql
INSTALL azure;
LOAD azure;

CREATE SECRET pbi_cli (
    TYPE azure,
    PROVIDER credential_chain,
    CHAIN 'cli'
);
```

Use it in the connection string:

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;Secret=pbi_cli;',
    'EVALUATE ROW("probe_ok", 1)'
);
```

Or by named argument:

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)',
    secret_name := 'pbi_cli'
);
```

## Catalog and Metadata Discovery

Use these helper functions to inspect semantic model structure:

```sql
SELECT * FROM pbi_tables('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
SELECT * FROM pbi_columns('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
SELECT * FROM pbi_measures('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
SELECT * FROM pbi_relationships('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
```

## Query Execution

### `dax_query(...)`

`dax_query` executes DAX over XMLA and streams rows back to DuckDB.

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE TOPN(10, VALUES(''DimDate''[Calendar Year]))',
    auth_mode := 'azure_cli'
);
```

### Local Extension Artifact Load (Optional)

If using another DuckDB shell, load the built extension explicitly:

```bash
duckdb -unsigned -c "LOAD './build/release/extension/pbi_scanner/pbi_scanner.duckdb_extension';"
```

Windows:

```bat
duckdb.exe -unsigned -c "LOAD './build/release/extension/pbi_scanner/pbi_scanner.duckdb_extension';"
```

## Performance

`dax_query` runs native HTTP XMLA with schema probe + streaming execution. For large results, end-to-end time is typically dominated by service and network; extension-side parse/conversion still matters.

### CI vs Local Benchmarking

- **CI/offline tests**: `make test` (sqllogictest under `test/sql/`) and optional `uv run bench_native_http.py --smoke`
- **Live benchmarking**: `uv run --group bench query_semantic_model_minimal.py`
- **Live bench inputs**: set `PBI_BENCH_CONNECTION_STRING`, `PBI_BENCH_DAX`, optional `PBI_BENCH_SECRET_NAME`
- **No real credentials in git**: use env vars, gitignored `.env`, or files under `local/`

### Benchmark and Transport Knobs

- `PBI_BENCH_MODE=count|materialize`
- `PBI_BENCH_ITERATIONS=<n>`
- `PBI_BENCH_DIRECT_XMLA=1` to resolve locator once in Python and run direct XMLA
- `PBI_SCANNER_XMLA_TRANSPORT=plain|xpress|sx|sx_xpress` (default: `sx_xpress`)
- `PBI_SCANNER_DISABLE_METADATA_CACHE=1` for cold-target/schema measurements
- `PBI_SCANNER_DEBUG_TIMINGS=1` for per-phase timings

### Metadata Cache

Resolved targets and schema metadata are cached on disk by default:

- No tokens or secrets are stored
- `PBI_SCANNER_CACHE_DIR` overrides location
- `PBI_SCANNER_DISABLE_METADATA_CACHE=1` disables cache
- `PBI_SCANNER_TARGET_CACHE_TTL_SECONDS` and `PBI_SCANNER_SCHEMA_CACHE_TTL_SECONDS` tune TTLs

For detailed transport benchmarks and parser notes, see [docs/xmla_transport_performance.md](docs/xmla_transport_performance.md).

## Development

### Build and Test

```bash
make          # release build
make test     # run tests
```

Other build variants:

```bash
make debug
make reldebug
make relassert
```

Single test file:

```bash
./build/release/test/unittest "test/sql/pbi_scanner.test"
```

### Quality Checks

```bash
make format-check
make tidy-check
```

Apply formatting fixes:

```bash
make format-fix
```

### Local Script Tooling

Run helper scripts with [`uv`](https://docs.astral.sh/uv/):

```bash
uv run bench_native_http.py --smoke
uv run --group bench query_semantic_model_minimal.py
```

## Platform Notes

### macOS

```bash
xcode-select --install
brew install cmake openssl
OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)" make
make test
```

### Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install -y git build-essential cmake pkg-config libssl-dev
make
make test
```

Fedora/RHEL equivalent:

```bash
sudo dnf install -y git gcc-c++ make cmake pkg-config openssl-devel
```

### Windows

Recommended: WSL2 + Linux steps.

For native Windows builds:

- Visual Studio 2022 (Desktop development with C++)
- CMake
- Git
- OpenSSL for Windows

Then from `x64 Native Tools Command Prompt for VS 2022`:

```bat
git clone --recurse-submodules https://github.com/crazy-treyn/pbi_scanner.git
cd pbi_scanner
cmake -S duckdb -B build\release -DDUCKDB_EXTENSION_CONFIGS=%CD%\extension_config.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build\release --config Release
ctest --test-dir build\release --build-config Release --output-on-failure -R test/sql/pbi_scanner.test
```

If OpenSSL is not found:

```bat
set OPENSSL_ROOT_DIR=C:\path\to\OpenSSL
```

## Troubleshooting

### OpenSSL Not Found

- macOS: `OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Linux: install `libssl-dev` or `openssl-devel`
- Windows: install OpenSSL and set `OPENSSL_ROOT_DIR`

### Submodule Errors

```bash
git submodule update --init --recursive
```

### Azure CLI Auth Fails

```bash
az logout
az login --scope "https://analysis.windows.net/powerbi/api/.default"
```

### Unsigned Extension Load Error

Use the bundled shell (`./build/release/duckdb`) or load local extension with DuckDB `-unsigned`.

## Limitations

- Live authenticated workloads are intentionally excluded from CI tests
- Power BI/Azure availability and network quality directly impact query latency
- Extension binaries should be rebuilt for each DuckDB version you target
- The project is experimental and APIs may evolve

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
