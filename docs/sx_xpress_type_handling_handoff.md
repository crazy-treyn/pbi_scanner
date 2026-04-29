# SX+XPRESS Data Type Handling Handoff

## Goal

Document current `sx+xpress` data-type handling in `pbi_scanner`, highlight likely defects (especially date/time), and provide a concrete checklist for a follow-up agent.

## Scope Reviewed

- `src/xmla.cpp`
- `src/http_client.cpp`
- `src/dax_query.cpp`
- `test/sql/pbi_scanner.test`

## End-to-End SX+XPRESS Flow

1. Transport mode selection defaults to `sx_xpress`.
2. HTTP streaming response bytes are buffered for non-plain transport.
3. `application/sx+xpress` payload is decompressed via XPRESS block logic.
4. Decompressed bytes are parsed as SSAS/BINXML (fast parser first, then fallback parser paths).
5. Parsed row values are coerced using schema type metadata (`xsd:*` strings) and textual/serial conversions.
6. Final values are materialized into DuckDB vectors.

## Key Functions (Current Behavior)

### Transport and decode

- `ResolveXmlaTransportMode`
- `DecodeBufferedXmlaResponse`
- `DecompressXpressLz77Framed`
- `DecompressXpressLz77Block`
- `TryParseSsasRowsFast`
- `ParseBinXmlResponse`

### BINXML / fast row parsing

- `SsasFastRowParser::ParseDocument`
- `SsasFastRowParser::ParseRecord`
- `SsasFastRowParser::ReadTextValue`
- `SsasFastRowParser::ReadCellValue`
- `SsasBinaryXmlParser::ParseRecord`
- `SsasBinaryXmlParser::ReadTextValue`

### Type mapping and coercion

- `MapXmlTypeToLogicalType`
- `CoercionKindFromXmlType`
- `CoerceXmlValue`
- `TryDecodeSsasTemporalSerial`
- `TryParseDateValue`
- `TryParseTimeValue`
- `TryParseTimestampValue`

## Current Type Mapping Coverage

Recognized schema strings include:

- `xsd:boolean`
- integer and unsigned integer xsd variants
- `xsd:decimal`, `xsd:double`, `xsd:float`
- `xsd:date`
- `xsd:time`
- `xsd:datetime`
- `xsd:datetimeoffset`

Anything else generally falls back to `VARCHAR`.

## Date/Time Conversion Semantics

Temporal serial handling uses OLE Automation style day serial math:

- epoch base: `1899-12-30`
- micros/day: `86400000000`
- conversion: `serial * micros_per_day` with nearest-microsecond rounding
- split into date-day component and time-of-day component for `DATE`/`TIME`/`TIMESTAMP`/`TIMESTAMP_TZ` coercion.

## High-Confidence Risks

1. There is an explicit message in `src/xmla.cpp` indicating `sx_xpress` date token coverage is not fully complete and suggesting fallback to `xpress`.
2. Several Microsoft BINXML temporal token families are not directly mapped by symbolic name in the current schema type mapping path and may degrade to `VARCHAR`.
3. Date-only values that include timezone suffix/offset can fail to coerce as `DATE` because parser helpers reject some offset-bearing forms.
4. `TIMESTAMP` coercion can produce timezone-aware values when offset text is present, risking type expectation drift.
5. Some binary token branches (notably `0xAE..0xB1`) are interpreted heuristically as text vs double, which can misclassify edge payloads.

## Test Coverage Gaps

`test/sql/pbi_scanner.test` includes basic temporal checks, but does not thoroughly cover:

- date-only with `Z` / `+HH:MM` / `-HH:MM`
- `TIMESTAMP` vs `TIMESTAMP_TZ` consistency when offsets appear
- negative serials and day-boundary rollover behavior
- microsecond rounding edge cases
- SX token family edge cases tied to temporal values

## Recommended Next Actions (for follow-up agent)

1. Add focused sqllogictests for date/time offset and serial edge cases before modifying parser logic.
2. Expand schema type mapping/coercion support for additional SSAS/BINXML temporal variants where applicable.
3. Make timestamp coercion behavior explicit when schema says non-offset datetime but payload includes an offset.
4. Reduce or guard ambiguous token heuristics (`0xAE..0xB1`) with stricter framing checks where feasible.
5. Validate fixes by running:
   - `./build/release/test/unittest "test/sql/pbi_scanner.test"`
   - plus any new focused sqllogictest file added for sx temporal behavior.

## Suggested Verification Matrix

- `DATE`: plain date, date with `Z`, date with offset, numeric serial integer, negative serial
- `TIME`: plain time string, datetime-at-base-date coercion, half-day serial
- `TIMESTAMP`: plain datetime, datetime with fractional seconds, negative serial
- `TIMESTAMP_TZ`: ISO datetime with `Z`, with positive offset, with negative offset
- Binary token edge samples passing through `sx+xpress` response decoding path

## Notes

- Keep tests deterministic and offline; do not add live Power BI dependencies.
- Preserve existing fallback behavior (`xpress` / `plain`) unless the task explicitly changes transport policy.
