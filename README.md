# pbi_scanner

DuckDB extension for querying Power BI Semantic Models with DAX.

> **Experimental**: This extension is under active development. APIs, behavior, and performance characteristics may change between releases.

## Features

- Query semantic models with `dax_query(connection_string, dax_text, ...)`
- Discover model metadata with `pbi_tables`, `pbi_columns`, `pbi_measures`, and `pbi_relationships`
- Multiple auth paths: `azure_cli`, `access_token`, and `service_principal`
- DuckDB secret integration via `TYPE azure` secrets (`credential_chain` / `service_principal`)
- Power BI locator support (`powerbi://...`) plus direct XMLA fast path (`https://.../xmla?...`)
- Local metadata cache for resolved targets/schemas (no token/secret persistence)

## Quick Start

### Prerequisites

- DuckDB extension toolchain requirements (CMake, C++ build toolchain, OpenSSL)
- Git with submodule support
- Azure CLI (`az`) installed

### Step 1: Clone and Build

```bash
git clone --recurse-submodules https://github.com/<your-org>/pbi_scanner.git
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

In the examples below, we use Azure CLI login to authenticate.

```bash
az login
```

Then use one auth option below.

#### Option A: Azure CLI auth mode

```sql
INSTALL pbi_scanner FROM community;
LOAD pbi_scanner;
SET pbi_scanner_auth_mode = 'azure_cli';

SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)'
);
```

#### Option B: Azure secret + `secret_name`

If you already use another DuckDB Azure secret provider, you can use that secret instead of the `credential_chain` example shown here.

```sql
INSTALL azure;
LOAD azure;

CREATE SECRET pbi_cli (
    TYPE azure,
    PROVIDER credential_chain,
    CHAIN 'cli'
);

INSTALL pbi_scanner FROM community;
LOAD pbi_scanner;

SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)',
    secret_name := 'pbi_cli'
);
```

Replace the workspace and semantic model placeholders with values you can access.

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

Set a session default when using direct auth modes:

```sql
SET pbi_scanner_auth_mode = 'azure_cli';
```

When both are provided, per-call named `auth_mode := ...` overrides the session
`SET` value for that call.

Quick Start already shows the Azure CLI mode and the Azure secret mode end to end.
Use this section as reference for secret variants and other auth modes.

### DuckDB Secret-Based Auth

Create a reusable DuckDB Azure secret:

```sql
INSTALL azure;
LOAD azure;

CREATE SECRET pbi_cli (
    TYPE azure,
    PROVIDER credential_chain,
    CHAIN 'cli'
);
```

Use either pattern with `dax_query`:

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

#### Service principal secret (DuckDB `azure` extension)

For automation or when you use a service principal registered in Microsoft Entra ID,
create a `TYPE azure` secret with `PROVIDER service_principal`. The extension reads
`tenant_id`, `client_id`, and `client_secret` from that DuckDB secret (the same fields
you set in SQL as `TENANT_ID`, `CLIENT_ID`, and `CLIENT_SECRET`) and acquires a Power BI
API token the same way as `auth_mode := 'service_principal'` with named parameters.

When you pass `secret_name` (or `Secret=` in the connection string), the token comes
from the DuckDB secret; session `SET pbi_scanner_auth_mode` does not override that
for that call.

```sql
INSTALL azure;
LOAD azure;

CREATE SECRET pbi_sp (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID '<tenant-guid>',
    CLIENT_ID '<app-client-id>',
    CLIENT_SECRET '<client-secret>'
);

INSTALL pbi_scanner FROM community;
LOAD pbi_scanner;

SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ROW("probe_ok", 1)',
    secret_name := 'pbi_sp'
);
```

For a local CLI smoke test that builds this `CREATE SECRET` from environment variables,
see `query_semantic_model_sql_minimal.py` (`PBI_SQL_USE_AZURE_SECRET=1`,
`PBI_SQL_AZURE_PROVIDER=service_principal`, and `SP_TENANT_ID` / `SP_CLIENT_ID` /
`SP_CLIENT_SECRET` or the `AZURE_*` / `PBI_XMLA_*` fallbacks).

## Catalog and Metadata Discovery

Use these helper functions to inspect semantic model structure.

```sql
SELECT * FROM pbi_tables('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
SELECT * FROM pbi_columns('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
SELECT * FROM pbi_measures('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
SELECT * FROM pbi_relationships('Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;');
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

Run helper scripts with [uv](https://docs.astral.sh/uv/):

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

For native Windows builds, use the repo wrapper so Visual Studio and CMake
paths are resolved automatically:

```powershell
.\scripts\dev-win.ps1 build
.\scripts\dev-win.ps1 test -R test/sql/pbi_scanner.test
```

Command wrapper behavior:

- Uses `VsDevCmd.bat` from VS 2022 Build Tools first, then VS 2019 Build Tools.
- Uses `cmake.exe` on `PATH`, else Visual Studio CMake fallback paths.
- Configures with repo-safe defaults:
  - `-DCMAKE_IGNORE_PATH=C:/msys64`
  - OpenSSL/Zlib defaults: if `OPENSSL_ROOT_DIR`, `ZLIB_INCLUDE_DIR`, and `ZLIB_LIBRARY` are unset, the script tries `CONDA_PREFIX\Library` (when `opensslv.h` is present), then common installs under `%USERPROFILE%` (`miniconda3`, `miniconda`, `anaconda3`, `mambaforge`, `miniforge3`). Override any of these with the matching environment variable.
- Builds with serialized MSBuild (`-- /m:1`) to reduce Windows file-lock failures.

Optional launcher:

```bat
scripts\dev-win.cmd build
scripts\dev-win.cmd test -R test/sql/pbi_scanner.test
```

For live local benchmarking on native Windows, prefer the repo-local Python
environment:

```powershell
uv run --group bench python -u query_semantic_model_minimal.py
```

On Windows, this path uses Azure CLI access-token auth directly to avoid a
known crash observed with DuckDB Azure secret credential-chain resolution inside
the Python process. The bundled CLI fallback remains available with:

```powershell
$env:PBI_BENCH_USE_BUNDLED_CLI='1'
uv run --group bench python -u query_semantic_model_minimal.py
```

If `duckdb.exe` fails to start because runtime DLLs are missing, make sure the
OpenSSL `bin` directory is on `PATH`, or build with `OPENSSL_ROOT_DIR` pointing
at an OpenSSL install that includes runtime DLLs.

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