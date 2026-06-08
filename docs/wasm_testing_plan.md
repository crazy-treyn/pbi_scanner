# WASM Testing Plan

Plan to validate `feature/wasm-support` end-to-end: the extension builds, loads in DuckDB-Wasm in a real browser runtime, and runs a credential-free `dax_query` smoke against a local mock XMLA endpoint on CI and locally.

## Goal

Prove WASM support works end-to-end before merging the feature branch.

## Where Things Stand

The feature branch now has a working local and CI WASM validation path:

- WASM artifacts compile locally (`make wasm_eh` / `make wasm_mvp`)
- Native offline tests pass (including platform capability hooks)
- Dedicated WASM smoke CI passes for `wasm_eh` and `wasm_mvp`
- Distribution CI builds `wasm_eh` and `wasm_mvp` artifacts successfully
- The smoke harness runs DuckDB-Wasm in Chromium through Playwright; Node only starts local servers and drives the browser

### Local Checkpoint: 2026-06-06

- `make wasm_eh` passes locally and produces `build/wasm_eh/repository/v1.5.3/wasm_eh/pbi_scanner.duckdb_extension.wasm`
- `make wasm_mvp` passes locally after clearing stale Homebrew Emscripten CMake state and rebuilding with emsdk 3.1.64
- `uv run test/wasm/run_pbi_wasm_smoke.py --platform wasm_eh` passes locally with negative checks enabled
- `uv run test/wasm/run_pbi_wasm_smoke.py --platform wasm_mvp` passes locally with success-path checks enabled and negative checks skipped

### Browser Smoke Checkpoint: 2026-06-06

- Replaced the Node DuckDB-Wasm runtime with a Playwright/Chromium browser smoke
- `@duckdb/duckdb-wasm@1.29.0` embeds DuckDB `v1.1.1` and cannot load the `v1.5.3` extension
- `@duckdb/duckdb-wasm@1.32.0` embeds DuckDB `v1.4.3` and is still not compatible enough for this smoke
- `@duckdb/duckdb-wasm@1.33.1-dev55.0` embeds DuckDB `v1.5.3` and successfully loads the `wasm_eh` extension
- Cross-origin synchronous XHR to a separate loopback XMLA server is blocked by Chromium before the mock server receives a request
- Same-origin browser/proxy smoke passes for `wasm_eh`: `dax_query` returns `probe_ok = 1`, the mock endpoint sees `Authorization: Bearer mock-token`, and native-only auth/locator paths fail clearly
- `wasm_mvp` builds locally after clearing stale Homebrew Emscripten CMake state and rebuilding with emsdk 3.1.64
- Same-origin browser/proxy smoke passes for `wasm_mvp`: `dax_query` returns `probe_ok = 1` and the mock endpoint sees `Authorization: Bearer mock-token`
- `wasm_mvp` negative error-message checks are skipped because deliberate validation errors currently surface as DuckDB-Wasm MVP runtime glue errors (`_setThrew is not defined`) instead of the extension's `InvalidInputException` messages

### CI Checkpoint: 2026-06-06

- `WASM Smoke Test` passes for both `wasm_eh` and `wasm_mvp` on commit `697bc94`
- `Main Extension Distribution Pipeline` passes on commit `697bc94`
- Stable distribution CI builds and uploads both WASM artifacts (`wasm_eh` and `wasm_mvp`) on commit `697bc94`
- DuckDB-main forward-compatibility CI builds both WASM artifacts (`wasm_eh` and `wasm_mvp`) on commit `697bc94`

## Guiding Principle

Keep the validation small, but test the real runtime shape. Use Node only to start local servers and drive the test; run DuckDB-Wasm itself inside a browser page. Browser HTTP should be validated through a same-origin loopback/proxy path because Chromium blocks the extension's synchronous cross-origin XHR before a separate mock XMLA server receives the request.

## Plan

### 1. Get CI Building Both WASM Platforms

Push the fixes already on the branch (emsdk setup, formatting, HTTP stop-flag access) and confirm CI builds `wasm_eh` and `wasm_mvp` on every push.

**Outcome:** Reliable compile signal on Ubuntu with Emscripten 3.1.64 and Node 20 — without requiring HTTP to work yet.

### 2. Replace Node Runtime Smoke With Browser Smoke

Move the smoke runtime into a real browser page, driven by Playwright or an equivalent browser runner:

- Start a local server for the page, DuckDB-Wasm assets, extension artifact, and same-origin mock XMLA endpoint
- Open a browser page that instantiates `@duckdb/duckdb-wasm`
- Select the DuckDB-Wasm bundle matching the extension platform under test (`wasm_eh` or `wasm_mvp`)
- Run `INSTALL`, `LOAD pbi_scanner`, and the smoke SQL from inside the page

**Outcome:** The test validates the same browser APIs the extension will use in production, instead of validating a Node/worker/XHR shim combination.

### 3. Validate The End-To-End Success Path

Run the credential-free `dax_query` path against the same-origin mock XMLA endpoint:

- `dax_query` returns `probe_ok = 1`
- The mock XMLA server receives `Authorization: Bearer mock-token`
- The test passes for both `wasm_eh` and `wasm_mvp`

**Outcome:** Full browser smoke proves build, load, registration, HTTP, auth-header propagation, XMLA parsing, and result materialization.

### 4. Validate Browser-Specific Rejections

Keep the negative coverage narrow and user-visible:

- `auth_mode := 'azure_cli'` fails with a clear unsupported-in-WASM error
- `auth_mode := 'service_principal'` fails with a clear unsupported-in-WASM error
- `powerbi://` locators fail at bind time with the documented browser limitation

**Outcome:** CI catches accidental exposure of native-only auth/resolver paths in browser builds.

## Definition of Done

- [x] CI builds `wasm_eh` and `wasm_mvp` green on every push/PR
- [x] Browser smoke loads the extension in DuckDB-Wasm 1.5.x for `wasm_eh`
- [x] Browser smoke loads the extension in DuckDB-Wasm 1.5.x for `wasm_mvp`
- [x] Browser smoke runs `dax_query` successfully against a mock XMLA endpoint for `wasm_eh`
- [x] Browser smoke runs `dax_query` successfully against a mock XMLA endpoint for `wasm_mvp`
- [x] Browser smoke verifies the access-token auth header reaches the mock XMLA endpoint for `wasm_eh`
- [x] Browser smoke verifies the access-token auth header reaches the mock XMLA endpoint for `wasm_mvp`
- [x] Browser smoke verifies browser auth and locator restrictions for `wasm_eh`
- [x] `wasm_mvp` negative-message behavior is documented as a DuckDB-Wasm runtime limitation; success-path browser smoke still covers load, registration, HTTP, auth-header propagation, XMLA parsing, and result materialization
- [x] [docs/wasm.md](wasm.md) reflects the actual toolchain (Emscripten, Node, browser runner, and duckdb-wasm version)

## Suggested Order of Work

1. Push or finish CI/build fixes -> confirm `wasm_eh` and `wasm_mvp` compile
2. Add the browser smoke harness -> confirm extension `INSTALL` / `LOAD`
3. Add mock XMLA success assertions -> confirm HTTP and result parsing
4. Add narrow negative assertions -> confirm native-only paths are rejected
5. Commit remaining branch cleanup (e.g. `http_client_wasm.cpp` array-literal fix) and update docs

The recommended path is to avoid making Node emulate browser behavior:

| Approach | Tradeoff |
|----------|----------|
| **Browser smoke** | Smallest reliable end-to-end signal; matches production runtime |
| **Node smoke with XHR/worker shims** | Looks lightweight, but validates a hybrid runtime and adds brittle setup |
| **Async HTTP refactor** | Larger C++ change; useful later, not required to validate this branch |

## Out of Scope (for this plan)

- Live Power BI / Azure token tests in a browser
- `wasm_threads` platform
- Large-payload streaming HTTP
- Multiple-browser matrix
- Performance benchmarking
- Community extension publication

## Related Docs

- [wasm.md](wasm.md) — build, load, auth, and limitations
- [AGENTS.md](../AGENTS.md) — WASM build and smoke commands
