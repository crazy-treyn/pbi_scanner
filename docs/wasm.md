# DuckDB-Wasm Support

`pbi_scanner` can be built as a DuckDB-Wasm loadable extension for the non-threaded
`wasm_mvp` and `wasm_eh` platforms. The threaded `wasm_threads` platform remains
excluded until the browser test harness covers COOP/COEP and pthread-specific
deployment requirements.

## Build

Build a local WASM extension artifact with:

```bash
make wasm_eh
```

The expected local artifact is:

```text
build/wasm_eh/extension/pbi_scanner/pbi_scanner.duckdb_extension.wasm
```

## Smoke Test

Run the credential-free browser WASM smoke test with:

```bash
uv run test/wasm/run_pbi_wasm_smoke.py --build
```

The smoke test starts a local CORS-enabled extension server, starts a local
mock XMLA endpoint, loads `pbi_scanner` in `@duckdb/duckdb-wasm`, verifies the
SQL table functions are registered, and runs:

```sql
SELECT *
FROM dax_query(
    'Data Source=http://127.0.0.1:<mock_port>/xmla;Initial Catalog=mock;',
    'EVALUATE ROW("probe_ok", 1)',
    auth_mode := 'access_token',
    access_token := 'mock-token'
);
```

The test passes only if the query returns `probe_ok = 1`, the mock server sees
the expected `Authorization` header, and unsupported browser auth such as
`auth_mode := 'azure_cli'` fails with a clear error.

## Loading From DuckDB-Wasm

DuckDB-Wasm fetches extensions at `LOAD` time. If you serve this extension from
a custom repository, the server must allow browser CORS requests and expose the
WASM file at DuckDB-Wasm's repository layout:

```text
<repository>/duckdb-wasm/<duckdb_version_hash>/<duckdb_platform>/pbi_scanner.duckdb_extension.wasm
```

For local unsigned artifacts, instantiate DuckDB-Wasm with unsigned extensions
enabled:

```javascript
await db.open({ allowUnsignedExtensions: true });
```

Then load from a repository:

```sql
SET custom_extension_repository = 'https://example.com/extensions';
INSTALL pbi_scanner;
LOAD pbi_scanner;
```

## Browser Auth

Browser DuckDB-Wasm supports `access_token` as the primary auth path:

```sql
SELECT *
FROM dax_query(
    'Data Source=https://example.analysis.windows.net/xmla?vs=...;Initial Catalog=...',
    'EVALUATE ROW("probe_ok", 1)',
    auth_mode := 'access_token',
    access_token := '<power-bi-token>'
);
```

The following native auth paths are intentionally unsupported in browser WASM:

- `azure_cli`, because browser WASM cannot spawn `az`.
- `service_principal`, because browser-side client secrets are extractable.
- Azure `credential_chain` and `service_principal` secrets, for the same reasons.

Acquire tokens in the host application or a backend service and pass short-lived
tokens into the query.

## CORS And Proxying

The WASM HTTP backend uses browser-managed HTTP. That means all Power BI REST,
XMLA, and extension repository requests must obey browser CORS rules. If a
Microsoft endpoint or private XMLA endpoint does not return suitable CORS
headers for your application origin, route requests through a backend proxy that
adds the required CORS response headers and keeps credentials server-side.

## Current Limitations

- `wasm_mvp` and `wasm_eh` use a synchronous pull execution path rather than the
  native background producer thread.
- The first WASM HTTP implementation buffers each HTTP response before invoking
  the existing `PostStream` receiver.
- Persistent metadata cache files are disabled in WASM; in-memory cache remains
  available for the current DuckDB-Wasm session.
- `wasm_threads` is not distributed yet.
