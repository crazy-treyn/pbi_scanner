#!/usr/bin/env python3
"""
Live-only check that DAX column name normalization works end-to-end via dax_query.

Uses the same PBI_BENCH_* env vars as query_semantic_model_minimal.py. Binds with
LIMIT 0 so only column names are fetched (no row materialization).

Checks:
  1. Default / normalize_column_names := true — no DAX square brackets in names
  2. normalize_column_names := false — bracketed XMLA names or a diff vs (1)
  3. SET pbi_scanner_normalize_dax_column_names = false — same as (2)
  4. Session false + normalize_column_names := true — matches (1) (override)
  5. pbi_columns LIMIT 0 — metadata helper shares bind path; names normalized

Run:
  uv run --group bench verify_dax_column_names.py

Optional hook in the minimal benchmark:
  PBI_BENCH_VERIFY_COLUMN_NAMES=1 uv run --group bench query_semantic_model_minimal.py
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from time import perf_counter

from bench_duckdb_cli import (
    _windows_runtime_path,
    dax_query_limit_zero_sql,
    python_duckdb_connect_usable,
)
from query_semantic_model_minimal import (
    REPO,
    _bench_config,
    _sql_escape,
    extension_path,
    log_timing,
    prepare_live_bench_connection,
    resolve_direct_xmla_connection_string,
)


@dataclass(frozen=True)
class ColumnNameCase:
    label: str
    session_normalize: bool | None = None
    param_normalize: bool | None = None


def _has_bracketed_dax_name(columns: list[str]) -> bool:
    return any("[" in column for column in columns)


def _fetch_dax_column_names(
    con,
    connection_string: str,
    dax: str,
    secret_name: str | None,
    access_token: str | None,
    *,
    session_normalize: bool | None,
    param_normalize: bool | None,
) -> list[str]:
    if session_normalize is None:
        con.sql("RESET pbi_scanner_normalize_dax_column_names")
    else:
        con.sql(
            "SET pbi_scanner_normalize_dax_column_names = "
            f"{'true' if session_normalize else 'false'}"
        )
    sql = dax_query_limit_zero_sql(
        connection_string,
        dax,
        secret_name=secret_name,
        access_token=access_token,
        normalize_column_names=param_normalize,
    )
    relation = con.sql(sql)
    columns = list(relation.columns)
    if not columns:
        raise RuntimeError(f"bind returned no columns for {sql}")
    return columns


def _fetch_pbi_columns_names(
    con,
    connection_string: str,
    secret_name: str | None,
    access_token: str | None,
) -> list[str]:
    con.sql("RESET pbi_scanner_normalize_dax_column_names")
    if access_token is not None:
        source = (
            f"pbi_columns('{_sql_escape(connection_string)}', "
            f"access_token := '{_sql_escape(access_token)}')"
        )
    elif secret_name:
        source = (
            f"pbi_columns('{_sql_escape(connection_string)}', "
            f"secret_name := '{_sql_escape(secret_name)}')"
        )
    else:
        source = f"pbi_columns('{_sql_escape(connection_string)}')"
    relation = con.sql(f"SELECT * FROM {source} LIMIT 0")
    columns = list(relation.columns)
    if not columns:
        raise RuntimeError("pbi_columns bind returned no columns")
    return columns


def verify_dax_column_normalization(
    con,
    connection_string: str,
    dax: str,
    secret_name: str | None,
    access_token: str | None = None,
) -> None:
    cases = [
        ColumnNameCase("session default (normalize on)"),
        ColumnNameCase("normalize_column_names := false", param_normalize=False),
        ColumnNameCase("session SET false", session_normalize=False),
        ColumnNameCase(
            "session false + param true override",
            session_normalize=False,
            param_normalize=True,
        ),
    ]

    results: dict[str, list[str]] = {}
    for case in cases:
        started = perf_counter()
        columns = _fetch_dax_column_names(
            con,
            connection_string,
            dax,
            secret_name,
            access_token,
            session_normalize=case.session_normalize,
            param_normalize=case.param_normalize,
        )
        log_timing(f"column names [{case.label}]", started)
        results[case.label] = columns
        preview = ", ".join(columns[:6])
        suffix = "..." if len(columns) > 6 else ""
        print(f"[verify] {case.label}: {len(columns)} columns — {preview}{suffix}")

    normalized = results["session default (normalize on)"]
    raw_param = results["normalize_column_names := false"]
    raw_session = results["session SET false"]
    override = results["session false + param true override"]

    if _has_bracketed_dax_name(normalized):
        raise AssertionError(
            "expected normalized columns without '['; got: "
            + ", ".join(normalized[:12])
        )

    if not _has_bracketed_dax_name(raw_param) and raw_param == normalized:
        raise AssertionError(
            "raw param mode matched normalized names; model may not expose "
            "bracketed XMLA names for this DAX query"
        )

    if raw_session != raw_param:
        raise AssertionError(
            "session SET false should match normalize_column_names := false; "
            f"session={raw_session[:5]} param={raw_param[:5]}"
        )

    if override != normalized:
        raise AssertionError(
            "named param true should override session false; "
            f"override={override[:5]} normalized={normalized[:5]}"
        )

    started = perf_counter()
    metadata_columns = _fetch_pbi_columns_names(
        con, connection_string, secret_name, access_token
    )
    log_timing("column names [pbi_columns default]", started)
    print(
        f"[verify] pbi_columns default: {len(metadata_columns)} columns — "
        f"{', '.join(metadata_columns[:6])}"
    )
    if _has_bracketed_dax_name(metadata_columns):
        raise AssertionError(
            "pbi_columns should normalize names by default; got: "
            + ", ".join(metadata_columns[:12])
        )

    print("[verify] PASS — column name normalization behaves as expected")


def main() -> None:
    if not python_duckdb_connect_usable():
        print(
            "Python duckdb.connect is required for column name verification.",
            file=sys.stderr,
        )
        sys.exit(1)

    connection_string, dax, secret_name, auth_mode = _bench_config()
    if os.environ.get("PBI_BENCH_DIRECT_XMLA", "").strip():
        connection_string = resolve_direct_xmla_connection_string(connection_string)
    import duckdb  # noqa: PLC0415

    runtime_path = _windows_runtime_path(REPO)
    if runtime_path:
        os.environ["PATH"] = runtime_path + os.pathsep + os.environ.get("PATH", "")

    print(f"[verify] dax_query probe: {dax[:80]}{'...' if len(dax) > 80 else ''}")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        ext_escaped = _sql_escape(str(extension_path))
        con.sql(f"LOAD '{ext_escaped}'")
        active_secret_name, access_token = prepare_live_bench_connection(
            con, secret_name, auth_mode
        )
        verify_dax_column_normalization(
            con,
            connection_string,
            dax,
            active_secret_name,
            access_token,
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
