# Windows validation plan — OpenSSL static link + vcpkg dev flow

This document is a handoff checklist for a Windows agent to confirm the OpenSSL/vcpkg changes work end to end. **Implementation is done in-repo**; **runtime verification must happen on Windows.**

## Background (short)

- CI shipping could previously pick up **runner OpenSSL DLLs** when `vcpkg.json` had no `openssl` dependency. Adding `openssl` to `vcpkg.json` makes manifest-mode vcpkg install OpenSSL for builds that use the vcpkg toolchain.
- `scripts/dev-win.ps1` now supports **`bootstrap`** (winget + vcpkg clone at pinned commit) and wires **`configure`** to the vcpkg toolchain + `x64-windows-static-release` by default.

---

## Completed (in this branch)

| Item | Status |
|------|--------|
| Add `openssl` to `vcpkg.json` | Done |
| Add `bootstrap` to `scripts/dev-win.ps1` (winget installs + vcpkg clone/checkout/bootstrap) | Done |
| Rewire `configure`/`build`/`test` to use vcpkg toolchain + manifest dir; remove Conda OpenSSL/Zlib defaults | Done |
| Document Windows flow in `README.md` and `AGENTS.md` | Done |
| Ensure `local/vcpkg` lives under `local/` (already gitignored via `/local/**`) | Done |

---

## Outstanding — Windows agent validation

Run these on a **Windows x64** machine (physical, VM, or CI job with MSVC). **Do not** rely on Python `import ssl` as part of the success criteria.

### 1) Fresh repo bootstrap + local build + tests

**Goal:** One-shot dev flow works without Conda and without preloaded OpenSSL DLLs.

1. Clone this branch and init submodules:
   ```bat
   git clone --recurse-submodules <repo-url>
   cd pbi_scanner
   git checkout <your-branch>
   git submodule update --init --recursive
   ```

2. Run bootstrap (may take a long time first run — vcpkg compiles dependencies):
   ```powershell
   .\scripts\dev-win.ps1 bootstrap
   ```

3. Build and run the focused offline test:
   ```powershell
   .\scripts\dev-win.ps1 build
   .\scripts\dev-win.ps1 test -R test/sql/pbi_scanner.test
   ```

**Pass criteria**

- `bootstrap` completes without errors; `local\vcpkg\vcpkg.exe` exists (or `%VCPKG_ROOT%` points at a valid vcpkg root).
- `build` finishes Release build under `build\release\`.
- `test` exits 0.

**If configure fails** with missing toolchain file, ensure bootstrap ran or set `VCPKG_ROOT` to an existing vcpkg checkout.

---

### 2) CI artifact — verify no OpenSSL DLL imports (distribution sanity)

**Goal:** The shipped `pbi_scanner.duckdb_extension` does not depend on `libssl*.dll` / `libcrypto*.dll` at load time.

1. Push the branch and wait for **Main Extension Distribution Pipeline** to finish.
2. Download the Windows extension artifact (name shape similar to `pbi_scanner-v1.5.2-extension-windows_amd64`).
3. From a **Developer Command Prompt for VS** (or any environment where `dumpbin` is on PATH):
   ```bat
   dumpbin /dependents path\to\pbi_scanner.duckdb_extension
   ```

**Pass criteria**

- Dependency list includes normal Windows/system DLLs (e.g. `KERNEL32.dll`, `WS2_32.dll`, `CRYPT32.dll`, etc.).
- **No** `libssl-*.dll`, `libcrypto-*.dll`, or similar OpenSSL DLL names.

**If OpenSSL DLLs still appear:** treat as failure of static linking for that artifact; follow up options (not done in this change):

- Confirm CI triplet / `OPENSSL_USE_STATIC_LIBS` behavior for the extension target, or
- Add explicit CMake preference for static OpenSSL before `find_package(OpenSSL)` if needed.

---

### 3) Clean runtime load (no `import ssl`)

**Goal:** Extension loads in DuckDB on a machine that has **not** preloaded OpenSSL via Python.

On a clean Windows environment (or after ensuring OpenSSL is **not** on `PATH`):

```bat
duckdb.exe -unsigned -c "LOAD '.\pbi_scanner.duckdb_extension'; SELECT 1;"
```

Use the artifact path from your download or the locally built extension under `build\release\extension\...\pbi_scanner.duckdb_extension` as appropriate.

**Pass criteria**

- Command succeeds **without** running Python or `import ssl` first.

---

## Optional overrides (for advanced verification)

| Variable | Purpose |
|----------|---------|
| `VCPKG_ROOT` | Point at an existing vcpkg checkout instead of `local\vcpkg` |
| `VCPKG_TARGET_TRIPLET` | Override triplet (default `x64-windows-static-release`) |
| `DEV_WIN_BUILD_RETRY_COUNT` | Retries for flaky MSBuild (existing behavior) |

---

## Sign-off

When all **Outstanding** sections pass, record:

- Git commit SHA validated
- `dumpbin /dependents` output snippet (or attached log)
- Result of `LOAD` test without Python SSL preload

This closes validation for the Windows OpenSSL + vcpkg dev-flow change.
