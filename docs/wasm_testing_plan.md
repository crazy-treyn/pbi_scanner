# WASM Validation Architecture

How `pbi_scanner` validates DuckDB-Wasm builds beyond compile-only CI. WASM
extensions are Emscripten side modules: a green link step does not prove the
artifact loads, resolves symbols, or runs usefully in a browser. See also
[Compiling Isn't Running: Functionally Testing DuckDB-WASM Extensions](https://rusty.today/blog/testing-duckdb-wasm-extensions/).

Operational commands and toolchain pins live in [wasm.md](wasm.md). Agent
entry points are in [AGENTS.md](../AGENTS.md).

## Two Validation Layers

| Layer | Harness | What it proves |
|-------|---------|----------------|
| **Browser smoke** | `test/wasm/smoke.mjs` | `INSTALL`/`LOAD`, function registration, same-origin mock XMLA HTTP, auth header propagation, XMLA parse/result materialization, and native-only path rejection |
| **Browser sqllogictest** | `test/wasm/run_sqllogictest.mjs` | Offline SQL assertions from `wasm_sql/pbi_scanner_wasm.test` — validation errors, WASM auth restrictions, `powerbi://` bind rejection — without live Power BI |

Both run DuckDB-Wasm inside Chromium via Playwright. Node only serves assets and
drives the browser; SQL executes in the same runtime shape production uses.

Shared asset serving (extension repo layout, DuckDB-Wasm bundles, mock XMLA,
CORS) lives in `test/wasm/browser_harness.mjs`.

## Native vs WASM Test Files

| File | Runtime | Scope |
|------|---------|-------|
| `test/sql/pbi_scanner.test` | Native `unittest` | Full offline validation including `powerbi://` resolver/auth paths |
| `wasm_sql/pbi_scanner_wasm.test` | Browser DuckDB-Wasm | WASM-portable cases: direct/loopback XMLA URLs, browser auth limits, earlier `powerbi://` bind failure |

WASM sqllogictest files are kept outside `test/` because DuckDB's native
`unittest` recursively registers every `test/**/*.test` file.

Do not run the native file unmodified against WASM — several `powerbi://`
records reach different validation stages on native vs browser.

When changing user-visible error text:

- Native paths → update `pbi_scanner.test`
- WASM/browser paths → update `pbi_scanner_wasm.test`

## CI

`.github/workflows/wasm-smoke.yml` on every push/PR:

1. Build `wasm_eh` with Emscripten 6.0.9
2. Build the matching DuckDB-Wasm host (`test/wasm/ensure_duckdb_wasm_host.sh`)
3. Run browser smoke
4. Run browser sqllogictest against `pbi_scanner_wasm.test`

There is no `wasm_mvp` browser leg. DuckDB-Wasm 2.0 ships only the EH `base`
bundle. Distribution CI still builds `wasm_eh` on the reduced matrix.

## Platform Notes

### `wasm_eh` (primary)

- Full smoke coverage including negative auth/locator checks
- All sqllogictest records run (queries + `statement error`)
- Browser host bundle is `duckdb-base.wasm` plus `duckdb-browser-base.worker.js`

### `wasm_mvp`

DuckDB-Wasm 2.0 dropped the MVP bundle (`-fwasm-exceptions` is always on). The
browser runners reject `--platform wasm_mvp`. `make wasm_mvp` can still compile
the extension; nothing in this harness can load it.

### `wasm_threads`

Excluded until the harness covers COOP/COEP and pthread deployment requirements.

## Local Commands

```bash
make wasm_eh
uv run test/wasm/run_pbi_wasm_smoke.py --build
uv run test/wasm/run_pbi_wasm_sqllogictest.py --build
make test-pbi-wasm    # smoke + sqllogictest on wasm_eh (expects artifact already built)
```

Prerequisites: Emscripten **6.0.9**, Node 20+, `ccache`, Yarn Classic 1.22,
`npm ci`, and Playwright Chromium under `test/wasm/`. The host is duckdb-wasm
`51ee2cc` (DuckDB v2.0.0). npm `@duckdb/duckdb-wasm@1.33.1-dev65.0` is DuckDB
v1.5.6 and cannot load this extension.

## Sqllogictest Subset

The browser runner supports: `query`, `statement ok|error`, `require`. Unsupported
directives (`loop`, `foreach`, hash results, multiple connections, etc.) are out
of scope.

Error matching uses substring containment (not full sqllogictest exact match) so
DuckDB-Wasm worker prefixes like `Invalid Input Error:` do not cause false
failures.

## Out of Scope

- Live Power BI / Azure tests in CI
- `wasm_threads`
- Large-payload streaming HTTP benchmarks
- Multi-browser matrix
- Published-catalog verification (pre-deploy CI uses local artifacts only)

## Related Docs

- [wasm.md](wasm.md) — build, load, auth, CORS, limitations
- [release_publication.md](release_publication.md) — release checklist including WASM validation
