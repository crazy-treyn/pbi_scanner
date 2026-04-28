#!/usr/bin/env python3
"""
Benchmark and smoke-test native HTTP XMLA execution via dax_query.

**CI / offline:** use `--smoke` only—no auth, no live Power BI. This complements
sqllogictest (`test/sql/pbi_scanner.test`) with a quick load + XMLA parse self-test.

**Local real-query performance:** prefer `query_semantic_model_minimal.py` (see
README) for benchmarking against your workspace with real DAX. This script is an
optional generic alternative (`--live` + PBI_BENCH_*): it uses the bundled
`./build/release/duckdb` CLI (no Python `duckdb.connect` required).

1) Offline smoke (no credentials): load the built extension and run the
   XMLA parse self-test (same as test/sql/pbi_scanner.test).

   uv run bench_native_http.py --smoke

2) Live Power BI: materialize rows with dax_query over HTTP XMLA. Requires
   the same setup as local DAX probes (azure extension + secret), plus
   PBI_BENCH_CONNECTION_STRING and PBI_BENCH_DAX.

   export PBI_BENCH_CONNECTION_STRING='Data Source=powerbi://...;Initial Catalog=...;'
   export PBI_BENCH_DAX='EVALUATE ...'
   uv run bench_native_http.py --live

   Set PBI_SCANNER_DEBUG_TIMINGS=1 in the environment to print phase timings
   to stderr from the extension (bind, probe, first row, execute total).

Optional: PBI_BENCH_SECRET_NAME (default pbi_cli), PBI_BENCH_ITERATIONS (default 2).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from time import perf_counter

from bench_duckdb_cli import (
    one_shot_count_sql,
    parse_last_c_column_count,
    require_release_artifacts,
    run_duckdb_cli,
)

REPO = Path(__file__).resolve().parent

PARSE_SQL = r"""
SELECT __pbi_scanner_test_parse_chunked_double(
  '<?xml version="1.0" encoding="utf-8"?><root><schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"><xsd:schema><xsd:complexType name="row"><xsd:sequence><xsd:element name="Rate" type="xsd:double" /></xsd:sequence></xsd:complexType></xsd:schema></schema><row><Rate>1.125E2</Ra',
  'te></row></root>'
);
""".strip()


def extension_sql_path(extension_path: Path) -> str:
    return str(extension_path).replace("'", "''")


def run_smoke() -> None:
    try:
        ext_path, _ = require_release_artifacts(REPO)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    load = extension_sql_path(ext_path)
    sql = f"LOAD '{load}'; " + PARSE_SQL
    t0 = perf_counter()
    proc = run_duckdb_cli(REPO, sql, forward_pbi_timings=False)
    if proc.returncode != 0:
        if proc.stderr:
            print(
                proc.stderr,
                end="" if proc.stderr.endswith("\n") else "\n",
                file=sys.stderr,
            )
        if proc.stdout:
            print(
                proc.stdout,
                end="" if proc.stdout.endswith("\n") else "\n",
                file=sys.stderr,
            )
        if not proc.stderr and not proc.stdout:
            print("(no output)", file=sys.stderr)
        sys.exit(proc.returncode or 1)
    elapsed = perf_counter() - t0
    print(f"[smoke] parse_chunked_double ok in {elapsed * 1000:.1f} ms")


def run_live() -> None:
    cs = os.environ.get("PBI_BENCH_CONNECTION_STRING", "").strip()
    dax = os.environ.get("PBI_BENCH_DAX", "").strip()
    secret = os.environ.get("PBI_BENCH_SECRET_NAME", "pbi_cli").strip()
    iterations = int(os.environ.get("PBI_BENCH_ITERATIONS", "2"))

    if not cs or not dax:
        print(
            "Live mode needs PBI_BENCH_CONNECTION_STRING and PBI_BENCH_DAX.",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        ext_path, _ = require_release_artifacts(REPO)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    bench_sql = one_shot_count_sql(ext_path, secret, cs, dax)
    bench_env = {k: os.environ[k] for k in os.environ if k.startswith("PBI_SCANNER")}

    print("Warm-up (HTTP XMLA via bundled duckdb CLI)...", flush=True)
    warm = run_duckdb_cli(REPO, bench_sql, env=bench_env)
    if warm.returncode != 0:
        if warm.stderr:
            print(
                warm.stderr,
                end="" if warm.stderr.endswith("\n") else "\n",
                file=sys.stderr,
            )
        if warm.stdout:
            print(
                warm.stdout,
                end="" if warm.stdout.endswith("\n") else "\n",
                file=sys.stderr,
            )
        if not warm.stderr and not warm.stdout:
            print("(no output)", file=sys.stderr)
        sys.exit(warm.returncode or 1)
    try:
        parse_last_c_column_count(warm.stdout)
    except ValueError:
        print(warm.stdout, file=sys.stderr)
        sys.exit(1)

    print(f"Timed runs ({iterations}x, each invocation loads extensions + runs count):")
    for i in range(iterations):
        t0 = perf_counter()
        proc = run_duckdb_cli(REPO, bench_sql, env=bench_env)
        elapsed = perf_counter() - t0
        if proc.returncode != 0:
            if proc.stderr:
                print(
                    proc.stderr,
                    end="" if proc.stderr.endswith("\n") else "\n",
                    file=sys.stderr,
                )
            if proc.stdout:
                print(
                    proc.stdout,
                    end="" if proc.stdout.endswith("\n") else "\n",
                    file=sys.stderr,
                )
            if not proc.stderr and not proc.stdout:
                print("(no output)", file=sys.stderr)
            sys.exit(proc.returncode or 1)
        try:
            n = parse_last_c_column_count(proc.stdout)
        except ValueError:
            print(proc.stdout, file=sys.stderr)
            sys.exit(1)
        rate = n / elapsed if elapsed > 0 and n else 0
        print(
            f"  [{i + 1}] rows={n} wall={elapsed * 1000:.1f} ms "
            f"({rate / 1e6:.2f} M rows/s)"
        )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Bench native HTTP XMLA (dax_query) and offline smoke"
    )
    p.add_argument(
        "--smoke",
        action="store_true",
        help="Offline: load extension + XMLA parse self-test (no network)",
    )
    p.add_argument(
        "--live",
        action="store_true",
        help="Live: time dax_query materialization (needs env + credentials)",
    )
    args = p.parse_args()
    if args.smoke and args.live:
        print("Use only one of --smoke or --live", file=sys.stderr)
        sys.exit(1)
    if not args.smoke and not args.live:
        args.smoke = True
    if args.smoke:
        run_smoke()
    else:
        run_live()


if __name__ == "__main__":
    main()
