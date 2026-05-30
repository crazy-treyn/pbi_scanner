#!/usr/bin/env python3
"""
Benchmark and smoke-test native HTTP XMLA execution via dax_query.

**CI / offline:** use `--smoke` only—no auth, no live Power BI. Dual-path check:
load the built extension and confirm `dax_query` is registered, plus Catch
`[smoke]` unit tests (former `__pbi_scanner_test_*` sqllogictest coverage).

**Local real-query performance:** prefer `query_semantic_model_minimal.py` (see
README) for benchmarking against your workspace with real DAX. This script is an
optional generic alternative (`--live` + PBI_BENCH_*): it uses the bundled
`./build/release/duckdb` CLI (no Python `duckdb.connect` required).

1) Offline smoke (no credentials): LOAD extension + `dax_query` registration check,
   then `pbi_scanner_unit_tests "[smoke]"`.

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
import subprocess
import sys
from pathlib import Path
from time import perf_counter

from bench_duckdb_cli import (
    escape_sql_literal,
    one_shot_count_sql,
    parse_last_c_column_count,
    require_release_artifacts,
    run_duckdb_cli,
)

REPO = Path(__file__).resolve().parent


def unit_tests_binary() -> Path:
    name = "pbi_scanner_unit_tests.exe" if sys.platform == "win32" else "pbi_scanner_unit_tests"
    return REPO / "build" / "release" / name


def load_check_sql(ext_path: Path) -> str:
    load = escape_sql_literal(str(ext_path))
    return (
        f"LOAD '{load}'; "
        "SELECT count(*) AS c FROM duckdb_functions() WHERE function_name='dax_query';"
    )


def run_unit_tests_smoke() -> None:
    binary = unit_tests_binary()
    if not binary.is_file():
        print(f"Missing {binary}. Run `make release` first.", file=sys.stderr)
        sys.exit(1)
    t0 = perf_counter()
    proc = subprocess.run(
        [str(binary), "[smoke]"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
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
    print(f"[smoke] pbi_scanner_unit_tests [smoke] ok in {elapsed * 1000:.1f} ms")


def run_load_check() -> None:
    try:
        ext_path, _ = require_release_artifacts(REPO)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    sql = load_check_sql(ext_path)
    t0 = perf_counter()
    proc = run_duckdb_cli(REPO, sql, forward_pbi_timings=False)
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
        count = parse_last_c_column_count(proc.stdout)
    except ValueError:
        print(proc.stdout, file=sys.stderr)
        sys.exit(1)
    if count != 1:
        print(
            f"[smoke] expected dax_query count=1 after LOAD, got {count}",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"[smoke] extension LOAD + dax_query ok in {elapsed * 1000:.1f} ms")


def run_smoke() -> None:
    run_load_check()
    run_unit_tests_smoke()


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
        help="Offline: LOAD extension + dax_query check and Catch [smoke] tests",
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
