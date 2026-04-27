# sx_xpress Metadata Parser Debug Handoff

## 1) Executive Summary (Goal + Current Status)

- **Goal:** make `PBI_SCANNER_XMLA_TRANSPORT=sx_xpress` reliable for metadata rowsets (not only DAX row data), so strict metadata probe mode passes without compatibility fallback.
- **Current branch status:** parser hardening is in progress and several compatibility guardrails were removed/adjusted to keep true `sx_xpress` behavior visible rather than silently masking failures.
- **Most relevant current signal:** strict metadata mode now intentionally treats soft-fail compatibility outcomes as hard failures, so unresolved `sx_xpress` metadata issues fail loudly.
- **Risk if unresolved:** metadata functions (`INFO.VIEW.*` and `pbi_*` helpers) may still require `xpress` fallback in some live models/query shapes even if non-metadata DAX works.

## 2) Exact Reproducible Commands

Run from repo root (`/Users/trey/code/pbi_scanner`).

### A. Build + deterministic local SQL tests

```bash
make release
./build/release/test/unittest "test/sql/pbi_scanner.test"
```

### B. Live strict metadata repro (primary)

Requires valid live env vars (`PBI_BENCH_CONNECTION_STRING`, optional `PBI_BENCH_DAX`, auth prerequisites).

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_METADATA_PROBE=1 \
PBI_BENCH_METADATA_MATRIX=1 \
PBI_BENCH_METADATA_STRICT_SX=1 \
PBI_SCANNER_XMLA_TRANSPORT=sx_xpress \
PBI_SCANNER_DEBUG_SSAS_NAMES=1 \
uv run --group bench query_semantic_model_minimal.py
```

### C. Compatibility baseline (control path)

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_METADATA_PROBE=1 \
PBI_SCANNER_XMLA_TRANSPORT=xpress \
uv run --group bench query_semantic_model_minimal.py
```

### D. Useful targeted comparison (non-strict matrix)

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_METADATA_PROBE=1 \
PBI_BENCH_METADATA_MATRIX=1 \
PBI_SCANNER_XMLA_TRANSPORT=sx_xpress \
uv run --group bench query_semantic_model_minimal.py
```

## 3) Expected vs Actual Outcomes

### Expected

- Strict run (`STRICT_SX=1`) passes metadata probe summary with:
  - zero hard failures
  - no `sx_xpress` metadata function needing compatibility rescue
- Parser accepts normalized sx payload framing and resolves name/string tables consistently.

### Actual (current fix-loop state)

- Strict mode is wired to fail on previously soft compatibility cases (now promoted to hard fail).
- Metadata parser work is still vulnerable to early framing/token/name-table mismatches depending on live payload shape.
- Error signatures seen in this loop center around early token handling (`0xdf`, `0xfe`, record-family tokens) and SSAS name id lookup consistency.

## 4) Key Problem Areas in Code

Primary file: `src/xmla.cpp`

- **Transport resolution and execution path**
  - `ResolveXmlaTransportMode()`
  - `XmlaExecutor::ExecuteStreaming(...)`
  - Recent change removes metadata-specific fallback override and uses configured transport directly.

- **Payload normalization and parse dispatch**
  - `SkipSxDfPreamble(...)`
  - `BinXmlPayloadOffset(...)`
  - `SkipOptionalSsasFramingMarker(...)`
  - `BuildNormalizedPayloadCandidates(...)`
  - `IsRecoverableEarlyFramingError(...)`
  - `ParseBinXmlResponse(...)`

- **SSAS binary parser internals**
  - `SsasBinaryXmlParser::ParseToken(...)`
  - `SsasBinaryXmlParser::ReadVarUIntWithDebug(...)`
  - `SsasBinaryXmlParser::LookupName(...)`
  - `SsasBinaryXmlParser::DefineName(...)`
  - `SsasBinaryXmlParser::ParseDocument()` call chain

Secondary files:

- `src/include/xmla.hpp`
  - exposes `EffectiveExecutionTransportForTesting(...)`
- `src/pbi_scanner_extension.cpp`
  - test-only SQL helpers:
    - `__pbi_scanner_test_coerce_xml_text`
    - `__pbi_scanner_test_effective_execution_transport`
- `test/sql/pbi_scanner.test`
  - asserts execution transport remains `sx_xpress` for metadata-style query text.
- `query_semantic_model_minimal.py`
  - strict metadata summary behavior (`SOFT_FAIL` -> `HARD_FAIL` promotion in strict mode).

## 5) Error Signature Timeline (0x86 / 0x01 / 0xdf / 0xfe / name-id)

This timeline is organized by signature family and where it tends to surface:

1. **`0xdf` early-token framing issues**
   - Happens when decompressed payload begins with one or more DF framing bytes.
   - Triggered parser errors include unsupported-token at offset 0 before normalization catches up.
   - Work in this loop added multi-candidate normalization and retry logic.

2. **`0xfe` and SSAS record-family token confusion**
   - Some payload starts are interpreted as SSAS record-family markers before valid parse context.
   - Current code explicitly treats certain record-family tokens (`f0/fd/fe/ef/f8/f6/f5/f7`) as signatures that can drive recovery decisions.

3. **`0x01` framing marker ambiguity**
   - Optional `0xB0` framing + mode byte (`0x01`/`0x04`) handling can shift the effective payload start.
   - Candidate-offset parsing attempts were added to tolerate this.

4. **`0x86` empty-text handling in SSAS token stream**
   - `EMPTY_TEXT_TOKEN` is a known SSAS token constant in parser internals.
   - Not necessarily the top failure now, but part of the token-surface expansion that must remain consistent with name/string table state.

5. **name-id lookup failures**
   - Signature: `"SSAS binary XML name id <n> was not defined"`.
   - Strong indicator of incorrect `DefineName(...)` interpretation (sparse explicit name id vs positional), bad offset alignment, or earlier table corruption.
   - Current loop introduced sparse-name-id compatibility logic and extra debug output to inspect varints and lookup misses.

## 6) What Has Already Been Tried (Patches / Approaches)

1. **Strict-mode metadata accounting tightened**
   - `query_semantic_model_minimal.py` now promotes strict-mode soft fails into hard fails before summary totals and failure throw.
   - Purpose: avoid false confidence from compatibility pass paths.

2. **Removed metadata-specific execution fallback**
   - Old behavior downgraded `INFO.VIEW.*` execution from `sx_xpress` to `xpress`.
   - Current state executes with configured transport, and test asserts metadata-style statements remain `sx_xpress`.

3. **Payload normalization broadened**
   - Added repeated DF preamble skipping.
   - Added optional SSAS framing marker skip (`0xB0` then `0x01`/`0x04`).
   - Added candidate-offset parsing rather than single fixed offset.

4. **Early recoverable-error gate**
   - `IsRecoverableEarlyFramingError(...)` classifies specific early signatures (`0xdf`, `0xfe`, SSAS record token messages) as retryable across candidate offsets.

5. **SSAS parser guardrails and diagnostics**
   - Explicit SSAS record-family token detection in `ParseToken(...)`.
   - `ReadVarUIntWithDebug(...)` instrumentation (`PBI_SCANNER_DEBUG_SSAS_NAMES`).
   - Name lookup miss diagnostics.
   - `DefineName(...)` branch for sparse explicit name-id streams.

6. **Test surface updates**
   - Added SQL-level test hooks for text coercion and effective execution transport introspection.

## 7) Remaining Hypotheses + Suggested Next Experiments

### Hypothesis A: name table indexing rule still wrong for a subset of SSAS streams

**Experiment:**
- Keep strict sx run with `PBI_SCANNER_DEBUG_SSAS_NAMES=1`.
- Capture first failing metadata function and log sequence of:
  - `define_name.first/uri/prefix/local`
  - first lookup miss id
  - last successful name id
- Verify whether `name_id` rule should be:
  - explicit only above threshold, or
  - explicit under additional token-context conditions.

### Hypothesis B: candidate offset selection still admits false-positive parse starts

**Experiment:**
- Add temporary trace in `ParseBinXmlResponse(...)` for candidate list + first 16 bytes at each candidate.
- Correlate which candidate eventually fails with name-id mismatch.
- Restrict candidate generation heuristics to avoid misaligned starts that still pass early token checks.

### Hypothesis C: recoverable-error classifier is too broad or too narrow

**Experiment:**
- For each retry transition, record `(candidate_offset, parser_kind, message)`.
- Compare success rate when:
  - dropping one signature from recoverable list
  - adding one signature seen in strict metadata failure path
- Goal: converge on deterministic retry policy that does not hide structural parser bugs.

### Hypothesis D: metadata rowset token dialect diverges from DAX rowset token dialect

**Experiment:**
- Isolate one failing metadata function (for example one `INFO.VIEW.*` shape) and one passing DAX shape in the same session.
- Diff token progression and table-definition events from parser debug logs.
- Identify first dialect divergence and add targeted parser branch/fixture around it.

### Hypothesis E: regression from fallback removal is valid and parser coverage is simply incomplete

**Experiment:**
- Temporarily reintroduce guarded fallback behind explicit opt-in env var (debug-only) to validate functional gap, not transport/network instability.
- Keep strict mode default failing behavior in harness so unresolved parser gaps stay visible.

## 8) Validation Checklist + Pass Criteria

### Checklist

- [ ] `make release` succeeds.
- [ ] `./build/release/test/unittest "test/sql/pbi_scanner.test"` succeeds.
- [ ] `PBI_SCANNER_XMLA_TRANSPORT=sx_xpress` + strict metadata run succeeds (no hard-fail summary lines).
- [ ] Strict metadata summary has no promoted soft-fail lines.
- [ ] No early framing/token/name-id parser exceptions in successful strict run.
- [ ] `xpress` control run also succeeds (sanity/backstop).
- [ ] At least one repeated strict run remains green (basic stability).

### Pass criteria

- **Functional pass:** strict metadata probe is green under `sx_xpress` for the configured live target without compatibility fallback.
- **Parser correctness pass:** no hidden fallback path in execution transport for metadata statements.
- **Regression pass:** existing deterministic sqllogictest remains green, including transport introspection test expecting `sx_xpress`.

## Fast Orientation for Follow-on Agent

- Start in `src/xmla.cpp`, specifically `ParseBinXmlResponse(...)` and `SsasBinaryXmlParser::DefineName(...)`.
- Reproduce with strict metadata command first (Section 2B), not only local tests.
- Use `xpress` baseline (Section 2C) to separate parser dialect issues from auth/network problems.
- Treat name-id failures as high-signal for stream interpretation drift rather than isolated lookup bugs.
