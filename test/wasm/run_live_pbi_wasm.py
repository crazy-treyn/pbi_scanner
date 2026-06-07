#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from time import perf_counter
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
WASM_TEST_DIR = REPO_ROOT / "test" / "wasm"
sys.path.insert(0, str(REPO_ROOT))


def _load_local_env_file() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = raw_value.strip()
        if value and value[0] in {"'", '"'}:
            try:
                parsed = shlex.split(f"placeholder={value}", posix=True)
                if parsed and "=" in parsed[0]:
                    _, value = parsed[0].split("=", 1)
            except ValueError:
                pass
        os.environ.setdefault(key, value)


def default_extension(platform: str) -> Path:
    return (
        REPO_ROOT
        / "build"
        / platform
        / "extension"
        / "pbi_scanner"
        / "pbi_scanner.duckdb_extension.wasm"
    )


def run(command: list[str], cwd: Path = REPO_ROOT, env: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def timed(label: str, started_at: float) -> None:
    print(f"[wasm-live] {label}: {(perf_counter() - started_at) * 1000:.1f} ms")


def first_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def bench_config() -> tuple[str, str]:
    connection_string = first_env("PBI_WASM_CONNECTION_STRING", "PBI_BENCH_CONNECTION_STRING")
    dax = first_env("PBI_WASM_DAX", "PBI_BENCH_DAX") or 'EVALUATE ROW("x", 1)'
    if not connection_string:
        raise SystemExit(
            "PBI_WASM_CONNECTION_STRING or PBI_BENCH_CONNECTION_STRING is required"
        )
    return connection_string, dax


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if value == 0:
            return 0
        return value
    if isinstance(value, bytes | bytearray | memoryview):
        return list(bytes(value))
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, list | tuple):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): normalize_value(value[key]) for key in sorted(value)}
    return str(value)


def canonical_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {column: normalize_value(value) for column, value in zip(columns, row, strict=True)}
        for row in rows
    ]


def parse_connection_string(connection_string: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for segment in connection_string.split(";"):
        if not segment.strip() or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        result[key.strip().lower()] = value.strip()
    return result


def sql_escape(value: str) -> str:
    return value.replace("'", "''")


def get_access_token() -> str:
    token = first_env("PBI_WASM_ACCESS_TOKEN", "PBI_BENCH_ACCESS_TOKEN")
    if token:
        return token
    from query_semantic_model_minimal import _az_access_token  # noqa: PLC0415

    started_at = perf_counter()
    token = _az_access_token()
    timed("Azure CLI access token", started_at)
    return token


def direct_xmla_connection_string(connection_string: str) -> str:
    direct_override = first_env("PBI_WASM_DIRECT_CONNECTION_STRING")
    if direct_override:
        return direct_override

    target_xmla_url = first_env("PBI_WASM_TARGET_XMLA_URL")
    if target_xmla_url:
        original_parts = parse_connection_string(connection_string)
        catalog = first_env("PBI_WASM_INITIAL_CATALOG") or original_parts.get("initial catalog", "")
        if not catalog:
            raise SystemExit(
                "PBI_WASM_TARGET_XMLA_URL requires PBI_WASM_INITIAL_CATALOG when "
                "the base connection string has no Initial Catalog."
            )
        return f"Data Source={target_xmla_url};Initial Catalog={catalog};"

    parts = parse_connection_string(connection_string)
    data_source = parts.get("data source", "")
    if data_source.lower().startswith("http") and "/xmla" in data_source.lower():
        return connection_string

    from query_semantic_model_minimal import (  # noqa: PLC0415
        resolve_direct_xmla_connection_string,
    )

    started_at = perf_counter()
    try:
        direct = resolve_direct_xmla_connection_string(connection_string)
    except Exception as exc:
        raise SystemExit(
            "Failed to resolve powerbi:// to a direct XMLA URL. Browser WASM "
            "cannot use powerbi:// locators directly. Set "
            "PBI_WASM_DIRECT_CONNECTION_STRING, or set PBI_WASM_TARGET_XMLA_URL "
            "plus PBI_WASM_INITIAL_CATALOG, then rerun the live helper.\n"
            f"Resolver error: {exc}"
        ) from exc
    timed("resolve direct XMLA connection string", started_at)
    return direct


def native_result(
    connection_string: str,
    dax: str,
    access_token: str,
) -> dict[str, Any]:
    import duckdb  # noqa: PLC0415
    from bench_duckdb_cli import _windows_runtime_path  # noqa: PLC0415
    from query_semantic_model_minimal import extension_path  # noqa: PLC0415

    runtime_path = _windows_runtime_path(REPO_ROOT)
    if runtime_path:
        os.environ["PATH"] = runtime_path + os.pathsep + os.environ.get("PATH", "")

    if not extension_path.exists():
        raise SystemExit(
            f"Missing native extension artifact: {extension_path}\n"
            "Run `make release` before the live WASM comparison."
        )

    started_at = perf_counter()
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    timed("native duckdb.connect", started_at)

    try:
        started_at = perf_counter()
        con.sql(f"LOAD '{sql_escape(str(extension_path))}'")
        timed("native LOAD pbi_scanner", started_at)

        sql = f"""
            SELECT *
            FROM dax_query(
                '{sql_escape(connection_string)}',
                '{sql_escape(dax)}',
                auth_mode := 'access_token',
                access_token := '{sql_escape(access_token)}'
            )
        """
        started_at = perf_counter()
        relation = con.sql(sql)
        rows = relation.fetchall()
        timed("native full materialization", started_at)
        return {
            "columns": list(relation.columns),
            "rowCount": len(rows),
            "rows": canonical_rows(list(relation.columns), rows),
        }
    finally:
        con.close()


def wasm_result(
    platform: str,
    extension: Path,
    connection_string: str,
    dax: str,
    access_token: str,
) -> dict[str, Any]:
    if not (WASM_TEST_DIR / "node_modules").exists():
        run(["npm", "ci"], cwd=WASM_TEST_DIR)

    target_parts = parse_connection_string(connection_string)
    target_xmla_url = target_parts.get("data source", "")
    if not target_xmla_url.lower().startswith("http") or "/xmla" not in target_xmla_url.lower():
        raise SystemExit(
            "The WASM live helper requires a direct XMLA connection string after "
            "resolution; Data Source must be an http(s) URL containing /xmla."
        )

    env = os.environ.copy()
    env["PBI_WASM_EXTENSION_PATH"] = str(extension)
    env["PBI_WASM_DUCKDB_PLATFORM"] = platform
    env["PBI_WASM_CONNECTION_STRING"] = connection_string
    env["PBI_WASM_TARGET_XMLA_URL"] = target_xmla_url
    env["PBI_WASM_DAX"] = dax
    env["PBI_WASM_ACCESS_TOKEN"] = access_token

    command = ["npm", "run", "live:pbi"]
    print("+ " + " ".join(command), flush=True)
    proc = subprocess.run(
        command,
        cwd=WASM_TEST_DIR,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    if proc.stderr:
        print(proc.stderr, file=sys.stderr, end="")
    if proc.stdout:
        print(proc.stdout, end="")
    for line in reversed(proc.stdout.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        payload = json.loads(line)
        if payload.get("ok") is True:
            return payload["result"]
    raise RuntimeError("WASM live helper did not print a JSON result payload")


def compare_results(native: dict[str, Any], wasm: dict[str, Any]) -> None:
    if native["columns"] != wasm["columns"]:
        raise AssertionError(
            "column mismatch\n"
            f"native={native['columns']}\n"
            f"wasm={wasm['columns']}"
        )
    if native["rowCount"] != wasm["rowCount"]:
        raise AssertionError(
            f"row count mismatch: native={native['rowCount']} wasm={wasm['rowCount']}"
        )
    if native["rows"] != wasm["rows"]:
        for index, (native_row, wasm_row) in enumerate(
            zip(native["rows"], wasm["rows"], strict=True)
        ):
            if native_row != wasm_row:
                raise AssertionError(
                    "row mismatch at zero-based index "
                    f"{index}\nnative={native_row}\nwasm={wasm_row}"
                )
        raise AssertionError("row payload mismatch")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a live Power BI semantic model comparison through DuckDB-Wasm"
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="build the selected DuckDB WASM platform before running",
    )
    parser.add_argument(
        "--platform",
        choices=["wasm_eh", "wasm_mvp"],
        default="wasm_eh",
        help="DuckDB WASM platform directory under build/",
    )
    parser.add_argument(
        "--extension",
        type=Path,
        default=None,
        help="path to pbi_scanner.duckdb_extension.wasm",
    )
    args = parser.parse_args()

    _load_local_env_file()
    os.environ.setdefault("PBI_SCANNER_DEBUG_TIMINGS", "1")

    if args.build:
        run(["make", args.platform])

    extension = (args.extension or default_extension(args.platform)).resolve()
    if not extension.exists():
        raise SystemExit(
            f"Missing WASM extension artifact: {extension}\n"
            f"Run with --build, or build it with `make {args.platform}` first."
        )

    connection_string, dax = bench_config()
    direct_connection_string = direct_xmla_connection_string(connection_string)
    access_token = get_access_token()

    print("[wasm-live] running native baseline query")
    native = native_result(direct_connection_string, dax, access_token)
    print(f"[wasm-live] native rows={native['rowCount']} columns={native['columns']}")

    print("[wasm-live] running browser WASM query")
    wasm = wasm_result(
        args.platform,
        extension,
        direct_connection_string,
        dax,
        access_token,
    )
    print(f"[wasm-live] wasm rows={wasm['rowCount']} columns={wasm['columns']}")

    compare_results(native, wasm)
    print(
        "pbi_scanner live WASM semantic model comparison passed "
        f"(platform={args.platform}, rows={wasm['rowCount']})"
    )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode)
