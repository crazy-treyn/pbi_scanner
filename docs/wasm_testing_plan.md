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
| **Browser smoke** | `test/wasm/smoke.mjs` | `INSTALL`/`LOAD`, function registration, same-origin mock XMLA HTTP, auth header propagation, XMLA parse/result materialization, and (`wasm_eh` only) native-only path rejection |
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

1. Build `wasm_eh` or `wasm_mvp` (matrix leg)
2. Run browser smoke
3. Run browser sqllogictest against `pbi_scanner_wasm.test`

Distribution CI still builds and uploads both WASM artifacts via the main
extension pipeline.

## Platform Notes

### `wasm_eh` (primary)

- Full smoke coverage including negative auth/locator checks
- All sqllogictest records run (queries + `statement error`)

### `wasm_mvp`

- Smoke success path only; negative error-message checks skipped
- Sqllogictest skips `statement error` records — deliberate
  `InvalidInputException` messages currently surface as DuckDB-Wasm MVP runtime
  glue errors (`_setThrew is not defined`) instead of extension text

Use `wasm_eh` for functional proof; keep `wasm_mvp` in CI as a compile/load
regression leg.

### `wasm_threads`

Excluded until the harness covers COOP/COEP and pthread deployment requirements.

## Local Commands

```bash
make wasm_eh
uv run test/wasm/run_pbi_wasm_smoke.py --build
uv run test/wasm/run_pbi_wasm_sqllogictest.py --build
make test-pbi-wasm    # smoke + sqllogictest on wasm_eh (expects artifact already built)
```

Prerequisites: Emscripten **3.1.64**, Node 20+, `npm ci` and Playwright Chromium
under `test/wasm/`. Pin `@duckdb/duckdb-wasm` to a release whose embedded DuckDB
version matches the extension (currently **v1.5.3**).

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
