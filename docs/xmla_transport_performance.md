# XMLA Transport Performance Findings

This note captures the local benchmark and tuning findings from the Power BI XMLA transport work. It is intended to help future agents and contributors understand what was slow, what improved, and where the next meaningful performance gains probably are.

## Summary

The original benchmark appeared to spend most of its time in DuckDB relation creation, but detailed timings showed several different costs:

- Public `powerbi://` target resolution was initially very expensive because of HTTP client lifecycle behavior.
- Plain XMLA execution returned a very large text XML payload: on the order of hundreds of MB for a wide fact-table projection (roughly 100k-scale rows and about a dozen columns).
- DAX Studio is faster largely because it uses ADOMD.NET, and modern Microsoft Analysis Services client libraries negotiate compressed binary XML transport with Power BI XMLA endpoints.

The current extension supports compressed transport modes:

```bash
PBI_SCANNER_XMLA_TRANSPORT=xpress \
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_MODE=count \
uv run --group bench query_semantic_model_minimal.py
```

`xpress` negotiates `application/xml+xpress`, decompresses XPRESS LZ77 locally, then reuses the existing text XML parser. `sx_xpress` uses `xpress` for schema probes so real column names are preserved, then negotiates `application/sx+xpress` for execution, uses the same XPRESS layer, and decodes the SSAS binary XML rowset format directly into the shared XMLA parser event path.

## Transport And Parser Selection

The extension selects the wire transport from `PBI_SCANNER_XMLA_TRANSPORT`.


| Value       | HTTP response target     | Parser path                                                   | Default? |
| ----------- | ------------------------ | ------------------------------------------------------------- | -------- |
| `plain`     | `text/xml`               | Streaming text XML parser                                     | No       |
| `xpress`    | `application/xml+xpress` | XPRESS LZ77, then text XML parser                             | No       |
| `sx`        | `application/sx`         | Xpress schema probe, then SSAS binary XML parser              | No       |
| `sx_xpress` | `application/sx+xpress`  | Xpress schema probe, then XPRESS LZ77 plus SSAS binary parser | Yes      |


If the environment variable is unset or empty, the extension uses `sx_xpress`. Set `PBI_SCANNER_XMLA_TRANSPORT=plain` to force the original text XML path if a model or query shape hits an unsupported binary XML record.

Binary XML rowsets use compact ordinal element names such as `C00`, `C01`, and `C11`. Because those names are not suitable as user-facing column names, binary transports currently probe schema with `xpress` and execute with the selected binary transport. Warm in-process schema cache avoids repeating the schema probe for the same target/query/effective-user combination.

Resolved `powerbi://` targets and XMLA schemas are also persisted on disk by default. The cache stores only metadata: direct XMLA endpoint, internal catalog, column names, source types, coercion kinds, and nullability. It never stores access tokens or secrets. Use these environment variables for benchmarking:

- `PBI_SCANNER_DISABLE_METADATA_CACHE=1`: force a cold target/schema path.
- `PBI_SCANNER_CACHE_DIR=/path/to/cache`: override the cache directory.
- `PBI_SCANNER_TARGET_CACHE_TTL_SECONDS`: override the 24-hour target TTL.
- `PBI_SCANNER_SCHEMA_CACHE_TTL_SECONDS`: override the 24-hour schema TTL.

## Benchmark Results

The benchmark query used a representative wide-table `EVALUATE` against a real semantic model (set via `PBI_BENCH_DAX` in the local harness; not checked into the repo).

It returned on the order of:

- Rows: hundreds of thousands
- Columns: about a dozen

Observed transport variants:


| Transport   | Content-Type             | Payload | Status                                                                 |
| ----------- | ------------------------ | ------- | ---------------------------------------------------------------------- |
| `plain`     | `text/xml`               | ~282 MB | Supported                                                              |
| `sx`        | `application/sx`         | ~84 MB  | Negotiates; decoder path is available but less useful than `sx_xpress` |
| `xpress`    | `application/xml+xpress` | ~10 MB  | Supported                                                              |
| `sx_xpress` | `application/sx+xpress`  | ~4.5 MB | Supported (default fast path)                                          |


Representative timings:


| Scenario                          | Relation creation            | Fetch/materialize | Notes                                           |
| --------------------------------- | ---------------------------- | ----------------- | ----------------------------------------------- |
| Plain direct XMLA count           | ~3.7s                        | ~28.0s            | ~282 MB text XML                                |
| Xpress direct XMLA count          | ~4.1s                        | ~5.3-5.6s         | ~10 MB compressed XML                           |
| Xpress direct XMLA materialize    | ~4.0s                        | ~5.5s             | Full Polars materialization                     |
| SX xpress direct XMLA count       | ~6.5s cold / ~0.3s warm bind | ~2.4s             | Xpress schema probe, ~4.45 MB execution payload |
| SX xpress direct XMLA materialize | ~6.5s cold / ~0.3s warm bind | ~2.3-2.7s         | Full Polars materialization                     |
| SX xpress repeat default run      | target/schema cache hit      | expected <5s      | Skips control-plane and schema probe            |


The xpress post-transport profile showed:

- Network stream: ~2.8s
- XPRESS decompression: ~0.7s
- XML parse and row production: ~1.7s

The latest `sx_xpress` execution profile showed:

- Network stream: ~1.4-1.6s
- XPRESS decompression: ~0.2s
- Binary XML parse and row production: ~0.5-0.6s

The cold bind path is higher than the earlier all-binary schema probe because it intentionally uses `xpress` for the schema probe to preserve real column names instead of compact `Cxx` rowset names.

## What We Learned

DAX Studio itself does not appear to contain a custom fast XMLA parser. It uses `Microsoft.AnalysisServices.AdomdClient` types such as `AdomdConnection`, `AdomdCommand`, and `AdomdDataReader`, then delegates the low-level wire protocol to Microsoft client libraries.

Microsoft has documented that modern ADOMD.NET/AMO libraries for Power BI XMLA switched from plain text XML to compressed binary XML by default. That aligns with our local negotiation results:

- `application/xml+xpress` dramatically reduces payload size and is enough to get count/materialization near the 5 second range.
- `application/sx+xpress` is even smaller and is the closest visible analogue to the compressed binary XML path ADOMD.NET likely uses.

## Current Implementation

Relevant files:

- `src/xmla.cpp`
  - Builds XMLA envelopes.
  - Sends optional `ProtocolCapabilities`.
  - Sets `X-Transport-Caps-Negotiation-Flags`.
  - Implements XPRESS LZ77 framed decompression for `application/xml+xpress`.
  - Contains binary XML event decoders for the earlier MS-EVEN6-style fixture and the SSAS `0xdf` rowset stream observed from Power BI.
  - Uses `application/xml+xpress` for schema probes when binary transport is selected, then routes `application/sx+xpress` execution through XPRESS LZ77 decompression and the SSAS binary XML event decoder.
- `src/http_client.cpp` and `src/include/http_client.hpp`
  - Preserve response headers.
  - Track stream byte count, chunk count, first byte, and total stream time.
- `query_semantic_model_minimal.py`
  - Prints benchmark mode and transport.
  - Supports `PBI_SCANNER_XMLA_TRANSPORT=plain|xpress|sx|sx_xpress`.

Default behavior is `sx_xpress` for the fastest observed Power BI XMLA path. Use `plain` or `xpress` explicitly as fallback transports while `sx_xpress` gains coverage across more models and query shapes.

## Recommended Next Steps

### 1. Harden the default compressed transport

`sx_xpress` is the default because it is materially faster on the sample query. `xpress` remains the safer fallback because it still produces text XML after decompression and reuses the existing parser. To keep the default robust:

- Run `xpress` and `sx_xpress` benchmarks across several models and queries.
- Confirm error handling and SOAP faults still decode correctly.
- Add deterministic parser/decompressor tests using captured or synthetic framed XPRESS data that does not contain secrets.

### 2. Broaden the Power BI `sx_xpress` binary XML dialect

`sx_xpress` reduced the sample payload from ~282 MB to ~4.5 MB and cut local count/materialization below 5 seconds in the benchmark harness.

The decoder now handles the observed rowset records:

- The `sx_xpress` response still uses the same framed plain XPRESS LZ77 layer as `application/xml+xpress`.
- After decompression, the payload starts with `0xdf`, then uses an SSAS binary XML string/name table with `f0`, `ef`, `f8`, `f6`, `f5`, and `f7` records.
- String, integer, double, boolean, and null-ish row values are decoded into the existing XMLA event path.

The remaining hardening path:

1. Add more deterministic fixtures for nulls, booleans, dates/times, faults, and nested or empty values.
2. Validate `sx_xpress` across multiple DAX shapes, including aliased columns, measures, single-row scalar results, and empty rowsets.
3. Confirm whether `application/sx` can share the same decoder without the XPRESS layer.
4. Keep `plain` and `xpress` as fallbacks.

`sx_xpress` remains the default transport. Keep validating it across additional live models and query shapes, and use `plain` or `xpress` as explicit fallbacks when needed.

### 3. Reduce schema probe duplication further

Current cold execution still does a schema probe and then a full execute. Persistent metadata caching avoids this on repeat runs, but the first run still pays the probe:

- Investigate whether `Content=SchemaData` can replace separate probe plus execute for some modes.
- Preserve correctness when DAX result schemas vary by query or model changes.

### 4. Optimize row conversion after transport

After xpress, execution time is roughly:

- Network: largest single component
- XML parse and row production: second largest
- XPRESS decompression: smaller but measurable

Potential row-path work:

- Avoid `Value` intermediates where a column type is known.
- Write primitive values directly into DuckDB vectors where practical.
- Keep null handling and type coercion behavior stable.

This should come after transport work because plain XML payload size was the dominant issue.

### 5. Keep benchmark modes explicit

Use separate benchmarks for:

- `PBI_BENCH_MODE=count`: measures extension streaming/parsing without Polars conversion noise.
- `PBI_BENCH_MODE=materialize`: measures the full Python/Polars path.
- `PBI_BENCH_ITERATIONS=2`: exposes warm in-process target/schema cache behavior.
- `PBI_BENCH_DIRECT_XMLA=1`: bypasses Power BI public locator resolution so transport can be measured directly.

## Useful Commands

Plain XML control:

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_MODE=count \
PBI_SCANNER_XMLA_TRANSPORT=plain \
uv run --group bench query_semantic_model_minimal.py
```

Xpress count benchmark:

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_MODE=count \
PBI_SCANNER_XMLA_TRANSPORT=xpress \
uv run --group bench query_semantic_model_minimal.py
```

Xpress materialization benchmark:

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_MODE=materialize \
PBI_SCANNER_XMLA_TRANSPORT=xpress \
uv run --group bench query_semantic_model_minimal.py
```

SX xpress materialization benchmark:

```bash
PBI_BENCH_DIRECT_XMLA=1 \
PBI_BENCH_MODE=materialize \
PBI_SCANNER_XMLA_TRANSPORT=sx_xpress \
uv run --group bench query_semantic_model_minimal.py
```

Cold default benchmark with metadata cache disabled:

```bash
PBI_SCANNER_DISABLE_METADATA_CACHE=1 \
PBI_BENCH_MODE=materialize \
uv run --group bench query_semantic_model_minimal.py
```

Repeat-run default benchmark with persistent cache enabled:

```bash
PBI_BENCH_MODE=materialize \
uv run --group bench query_semantic_model_minimal.py
```

