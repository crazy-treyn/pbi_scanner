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

### DAX column names

By default, `dax_query` and the `pbi_*` metadata table functions return XMLA
column names unchanged (including DAX table qualifiers and square brackets).

Opt in to normalization when you want DuckDB-friendly names: DAX square
brackets are removed and table qualifiers are dropped (`Fact[Amount]` becomes
`Amount`, `[Total Sales]` becomes `Total Sales`). When the same column name
appears more than once, qualified names are disambiguated with a table prefix
(`TableA[Amount]` and `TableB[Amount]` become `TableA_Amount` and
`TableB_Amount`); any remaining collisions get numeric suffixes (`Amount_2`,
`TableA_Amount_2`, etc.). DuckDB quotes identifiers with spaces when you
reference them in SQL.

Enable normalization for the session:

```sql
SET normalize_dax_column_names = true;
```

Per-call named parameter overrides the session default for that query:

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ''Fact Allocation''',
    normalize_dax_column_names := true
);
```

### Schema probe row limit

`dax_query` uses a limited schema probe (`TOPN`) during bind to discover column
types without scanning the full result. Control the row limit with a named
parameter or a process-wide environment variable:

```sql
SELECT *
FROM dax_query(
    'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example_semantic_model;',
    'EVALUATE ''Fact Allocation''',
    schema_probe_rows := 50
);
```

```bash
export PBI_SCANNER_SCHEMA_PROBE_ROWS=50
```

When unset, the default is 100 rows. Set `schema_probe_rows := 0` per query to
probe with the full DAX statement when a limited probe is not applicable.

Live check (requires workspace auth in `.env` or `PBI_BENCH_*`):

```bash
uv run --group bench verify_dax_column_names.py
```

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

Focused offline tests (sqllogictest + Catch unit tests):

```bash
make test-pbi-offline
# or individually:
./build/release/test/unittest "test/sql/pbi_scanner.test"
./build/release/pbi_scanner_unit_tests
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

### Cursor Worktree Setup

If you use Cursor worktrees (`/worktree` or `--worktree`), this repo includes
`.cursor/worktrees.json` to bootstrap each new worktree automatically:

- Copy `.env` from the root worktree when present (only if the new worktree
  does not already have one)
- Run `git submodule sync --recursive` and `git submodule update --init --recursive`
- Fail fast if `extension-ci-tools` did not populate (so submodule problems are not silent)

Bootstrapping submodules does **not** run `make`; build artifacts such as
`./build/release/test/unittest` appear only after you build in that worktree
(for example `make` / `make test`).

If setup did not run (or you need to repair a worktree manually), from the
worktree root you can run:

```bash
bash .cursor/setup-worktree-unix.sh
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

For native Windows builds, bootstrap once, then use the repo wrapper:

```powershell
.\scripts\dev-win.ps1 bootstrap
.\scripts\dev-win.ps1 build
.\scripts\dev-win.ps1 test -R test/sql/pbi_scanner.test
```

Command wrapper behavior:

- `bootstrap` installs missing prerequisites with `winget` (VS Build Tools +
  C++ workload, Git, CMake), then clones/bootstraps vcpkg into `local/vcpkg`
  (or `%VCPKG_ROOT%` when set).
- Uses `VsDevCmd.bat` from VS 2022 Build Tools first, then VS 2019 Build Tools.
- Uses `cmake.exe` on `PATH`, else Visual Studio CMake fallback paths.
- Configures with repo-safe defaults and vcpkg toolchain integration:
  - `-DCMAKE_IGNORE_PATH=C:/msys64`
  - `-DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake`
  - `-DVCPKG_TARGET_TRIPLET=x64-windows-static-release` by default (override
    with `%VCPKG_TARGET_TRIPLET%`)
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

If `.\scripts\dev-win.ps1 configure` reports a missing vcpkg toolchain file,
run `.\scripts\dev-win.ps1 bootstrap` or set `%VCPKG_ROOT%` to an existing
vcpkg checkout.

## Troubleshooting

### OpenSSL Not Found

- macOS: `OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Linux: install `libssl-dev` or `openssl-devel`
- Windows: run `.\scripts\dev-win.ps1 bootstrap` to install/bootstrap vcpkg

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