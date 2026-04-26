#!/usr/bin/env python3
"""
Shared helpers for local benchmarks using the bundled DuckDB CLI (build/release/duckdb).

Use this when the Python `duckdb` package is missing or `duckdb.connect` is unavailable.
Not imported by the extension; local scripts only.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parent


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def require_release_artifacts(repo: Optional[Path] = None) -> tuple[Path, Path]:
    base = repo or REPO
    ext = base / "build" / "release" / "extension" / "pbi_scanner" / "pbi_scanner.duckdb_extension"
    cli = base / "build" / "release" / "duckdb"
    if not ext.is_file():
        raise FileNotFoundError(f"Missing extension at {ext}; run `make release` first.")
    if not cli.is_file():
        raise FileNotFoundError(f"Missing DuckDB CLI at {cli}; run `make release` first.")
    return ext, cli


def python_duckdb_connect_usable() -> bool:
    try:
        import duckdb  # noqa: PLC0415
    except ImportError:
        return False
    return callable(getattr(duckdb, "connect", None))


def live_session_sql(extension_path: Path, secret_name: str) -> str:
    """LOAD pbi_scanner, INSTALL/LOAD azure, CREATE SECRET. secret_name is a bare identifier."""
    sn = secret_name.strip()
    load = f"LOAD '{escape_sql_literal(str(extension_path))}'"
    secret = (
        f"CREATE OR REPLACE SECRET {sn} ( TYPE azure, PROVIDER credential_chain, CHAIN 'cli' )"
    )
    return "; ".join([load, "INSTALL azure", "LOAD azure", secret])


def dax_count_sql(connection_string: str, dax: str, secret_name: str) -> str:
    cs = escape_sql_literal(connection_string)
    dx = escape_sql_literal(dax)
    sn = escape_sql_literal(secret_name.strip())
    return f"SELECT count(*) AS c FROM dax_query('{cs}', '{dx}', secret_name := '{sn}')"


def one_shot_count_sql(
    extension_path: Path, secret_name: str, connection_string: str, dax: str
) -> str:
    return "; ".join(
        [live_session_sql(extension_path, secret_name), dax_count_sql(connection_string, dax, secret_name)]
    )


def materialize_and_summarize_sql(
    extension_path: Path,
    secret_name: str,
    connection_string: str,
    dax: str,
    sample_limit: int,
) -> str:
    cs = escape_sql_literal(connection_string)
    dx = escape_sql_literal(dax)
    sn = escape_sql_literal(secret_name.strip())
    id_sn = secret_name.strip()
    prefix = live_session_sql(extension_path, id_sn)
    dq = f"dax_query('{cs}', '{dx}', secret_name := '{sn}')"
    return "; ".join(
        [
            prefix,
            f"CREATE OR REPLACE TEMP TABLE _pbi_bench_rows AS SELECT * FROM {dq}",
            "SELECT count(*) FROM _pbi_bench_rows",
            f"SELECT * FROM _pbi_bench_rows LIMIT {int(sample_limit)}",
        ]
    )


def run_duckdb_cli(
    repo: Path,
    sql: str,
    *,
    env: Optional[dict[str, str]] = None,
    forward_pbi_timings: bool = True,
) -> subprocess.CompletedProcess[str]:
    _, cli = require_release_artifacts(repo)
    merged = os.environ.copy()
    if env:
        merged.update({k: v for k, v in env.items() if v is not None})
    proc = subprocess.run(
        [str(cli), "-unsigned", "-csv", "-c", sql],
        cwd=str(repo),
        check=False,
        capture_output=True,
        text=True,
        env=merged,
    )
    if forward_pbi_timings and proc.stderr:
        for line in proc.stderr.splitlines():
            if "[pbi_scanner]" in line:
                print(line, file=sys.stderr)
    return proc


def parse_last_c_column_count(stdout: str) -> int:
    """Parse stdout from `SELECT count(*) AS c` (one or more result sets in -csv mode)."""
    lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    last_n: Optional[int] = None
    i = 0
    while i < len(lines):
        if lines[i] == "c" and i + 1 < len(lines):
            try:
                last_n = int(lines[i + 1])
            except ValueError:
                pass
            i += 2
        else:
            i += 1
    if last_n is None:
        raise ValueError("Could not parse count from DuckDB CSV output")
    return last_n


def parse_count_star_then_sample_lines(stdout: str) -> tuple[int, list[str]]:
    """First result set is count(*) (CSV header count_star()); then sample table CSV lines."""
    lines = stdout.splitlines()
    if not lines:
        raise ValueError("empty stdout from DuckDB")
    idx = 0
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    if idx >= len(lines) or lines[idx].strip() != "count_star()":
        raise ValueError("expected count_star() header in DuckDB CSV output")
    idx += 1
    if idx >= len(lines):
        raise ValueError("missing count row")
    count = int(lines[idx].strip())
    idx += 1
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    sample_lines = lines[idx:]
    return count, sample_lines
