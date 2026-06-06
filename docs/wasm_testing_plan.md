# WASM Testing Plan

Plan to validate `feature/wasm-support` end-to-end: the extension builds, loads in DuckDB-Wasm in a real browser runtime, and runs a credential-free `dax_query` smoke against a mock XMLA server on CI and locally.

## Goal

Prove WASM support works end-to-end before merging the feature branch.

## Where Things Stand

The feature branch is **mostly built out** but **not fully validated**:

- WASM artifacts compile locally (`make wasm_eh` / `make wasm_mvp`)
- Native offline tests pass (including platform capability hooks)
- CI has **never completed** a full WASM smoke run (emsdk action typo; fix exists locally but is unpushed)
- The current smoke harness runs in **Node**, but the extension HTTP layer is designed for a **browser**
- The Node harness has tooling/runtime mismatches (`@duckdb/duckdb-wasm`, workers, and XHR shims)

## Guiding Principle

Keep the validation small, but test the real runtime shape. Use Node only to start local servers and drive the test; run DuckDB-Wasm itself inside a browser page.

## Plan

### 1. Get CI Building Both WASM Platforms

Push the fixes already on the branch (emsdk setup, formatting, HTTP stop-flag access) and confirm CI builds `wasm_eh` and `wasm_mvp` on every push.

**Outcome:** Reliable compile signal on Ubuntu with Emscripten 3.1.64 and Node 20 — without requiring HTTP to work yet.

### 2. Replace Node Runtime Smoke With Browser Smoke

Move the smoke runtime into a real browser page, driven by Playwright or an equivalent browser runner:

- Start a local CORS-enabled extension server
- Start a local CORS-enabled mock XMLA endpoint
- Open a browser page that instantiates `@duckdb/duckdb-wasm`
- Select the DuckDB-Wasm bundle matching the extension platform under test (`wasm_eh` or `wasm_mvp`)
- Run `INSTALL`, `LOAD pbi_scanner`, and the smoke SQL from inside the page

**Outcome:** The test validates the same browser APIs the extension will use in production, instead of validating a Node/worker/XHR shim combination.

### 3. Validate The End-To-End Success Path

Run the credential-free `dax_query` path against the mock XMLA endpoint:

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

- [ ] CI builds `wasm_eh` and `wasm_mvp` green on every push/PR
- [ ] Browser smoke loads the extension in DuckDB-Wasm 1.5.x for `wasm_eh`
- [ ] Browser smoke loads the extension in DuckDB-Wasm 1.5.x for `wasm_mvp`
- [ ] Browser smoke runs `dax_query` successfully against a mock XMLA server
- [ ] Browser smoke verifies the access-token auth header reaches the mock XMLA server
- [ ] Browser smoke verifies browser auth and locator restrictions
- [ ] [docs/wasm.md](wasm.md) reflects the actual toolchain (Emscripten, Node, browser runner, and duckdb-wasm version)

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
