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
uv run test/wasm/run_pbi_wasm_smoke.py --build --platform wasm_mvp
```

GitHub Actions runs the same smoke for both `wasm_eh` and `wasm_mvp` via
`.github/workflows/wasm-smoke.yml`.

The smoke test starts a local Playwright/Chromium browser, serves DuckDB-Wasm
assets and the extension artifact from a local unsigned extension repository,
serves a same-origin mock XMLA endpoint, loads `pbi_scanner` in
`@duckdb/duckdb-wasm`, verifies the SQL table functions are registered, and runs:

```sql
SELECT *
FROM dax_query(
    'Data Source=http://127.0.0.1:<smoke_port>/xmla;Initial Catalog=mock;',
    'EVALUATE ROW("probe_ok", 1)',
    auth_mode := 'access_token',
    access_token := 'mock-token'
);
```

The test passes only if the query returns `probe_ok = 1`, the mock server sees
the expected `Authorization` header, and unsupported browser paths fail with clear
errors on `wasm_eh`:

- `auth_mode := 'azure_cli'`
- `auth_mode := 'service_principal'` with explicit credentials
- `powerbi://` locators at bind time

The `wasm_mvp` smoke validates the same extension load, function registration,
`dax_query`, XMLA parsing, and access-token header path, but skips the negative
error-message assertions. In the current DuckDB-Wasm MVP runtime, deliberate
validation errors surface as runtime glue errors instead of the extension's
`InvalidInputException` messages.

Set `PBI_WASM_DUCKDB_PLATFORM` to `wasm_eh` or `wasm_mvp` when running smoke so
the DuckDB-Wasm core matches the extension artifact (CI sets this from the matrix
leg). When unset, smoke defaults to `wasm_eh`.

The mock XMLA endpoint is intentionally same-origin with the browser page. The
WASM HTTP client uses synchronous browser XHR; Chromium blocks synchronous
cross-origin loopback XHR before the mock server receives a request, even with
CORS headers. The smoke therefore validates the documented browser proxy shape:
host applications should either call same-origin/loopback XMLA URLs or route
cross-origin XMLA traffic through a backend/proxy with appropriate CORS policy.

## DuckDB-Wasm Version Alignment

This extension is built against DuckDB **v1.5.2** (see `extension_config.cmake`).
The smoke harness pins `@duckdb/duckdb-wasm` in `test/wasm/package.json`
(currently **1.33.1-dev55.0**, which embeds DuckDB **v1.5.3**). Use a
DuckDB-Wasm npm release whose embedded DuckDB version matches the extension you
load; version skew can cause ABI or runtime failures. For example,
`@duckdb/duckdb-wasm@1.29.0` embeds DuckDB `v1.1.1`, and
`@duckdb/duckdb-wasm@1.32.0` embeds DuckDB `v1.4.3`; neither is suitable for
testing this `v1.5.2` extension.

When publishing or integrating in a host app, record both:

- the DuckDB engine version used to build `pbi_scanner.duckdb_extension.wasm`
- the `@duckdb/duckdb-wasm` package version (or custom WASM build) in the browser

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

## Direct XMLA Connection Strings

Browser smoke tests use loopback HTTP, for example
`Data Source=http://127.0.0.1:<port>/xmla;Initial Catalog=mock;`.

Connection-string parsing allows:

- **HTTPS** direct XMLA with a Power BI-style path containing `/xmla?` (same as
  native CLI).
- **HTTP** direct XMLA only when the host is loopback (`127.0.0.1`, `localhost`,
  or `::1`) and the path contains `/xmla`.

Remote `http://` XMLA URLs are rejected (they do not match the `powerbi://`
locator pattern either).

## DuckDB-Wasm Workers And Cancellation

Run DuckDB-Wasm in a dedicated Web Worker when possible. Each `HttpClient`
registers its active `XMLHttpRequest` by client instance; `HttpClient::Stop()`
calls `abort()` only for that client. In-flight cancellation is still limited on
the single-threaded sync path while a request is blocked.

## Current Limitations

- `wasm_mvp` and `wasm_eh` use a synchronous pull execution path rather than the
  native background producer thread.
- `powerbi://` locators fail at bind time in the browser; use direct XMLA URLs or
  a host-provided proxy for Power BI REST resolution.
- The first WASM HTTP implementation buffers each HTTP response before invoking
  the existing `PostStream` receiver. Large XMLA payloads are held entirely in
  WASM heap memory; prefer smaller queries or a backend proxy for very large
  result sets until incremental streaming is available.
- Persistent metadata cache files are disabled in WASM; in-memory cache remains
  available for the current DuckDB-Wasm session.
- `wasm_threads` is not distributed yet.
