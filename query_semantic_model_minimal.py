"""
Local-only: run real DAX against Power BI for development and performance
benchmarking. Not used in CI (see README Performance + AGENTS.md).

Configuration (prefer env vars so nothing secret is hardcoded):
  PBI_BENCH_CONNECTION_STRING — Power BI XMLA connection string
  PBI_BENCH_DAX — DAX to run (e.g. EVALUATE 'My Table')
  PBI_BENCH_SECRET_NAME — optional; default pbi_cli
  PBI_BENCH_MODE — count or materialize; default materialize
  PBI_BENCH_ITERATIONS — runs in the same process; default 1
  PBI_BENCH_METADATA_PROBE — set 1/true to run pbi_* metadata probes
  PBI_BENCH_METADATA_FAIL_FAST — optional; stop on first metadata failure
  PBI_BENCH_METADATA_MIN_ROWS — optional; minimum rows for tables/columns (default 1)
  PBI_BENCH_DIRECT_XMLA — resolve powerbi:// to direct XMLA before running
  PBI_SCANNER_XMLA_TRANSPORT — plain, sx, xpress, or sx_xpress; default sx_xpress
  PBI_SCANNER_DISABLE_METADATA_CACHE — set 1/true to force cold target/schema probes
  PBI_SCANNER_CACHE_DIR — override metadata cache directory

If `duckdb.connect` is missing from the Python `duckdb` package, this script
falls back to the bundled `./build/release/duckdb` CLI (after `make release`).

Run with uv (installs the `bench` group / Python duckdb when needed):
  uv run --group bench query_semantic_model_minimal.py
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from time import perf_counter
from urllib.error import HTTPError, URLError
from urllib.parse import unquote_plus
from urllib.request import Request, urlopen

REPO = Path(__file__).resolve().parent


def _load_local_env_file() -> None:
    env_path = REPO / ".env"
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
                # Match shell-style quoted values in .env.
                parsed = shlex.split(f"placeholder={value}", posix=True)
                if parsed and "=" in parsed[0]:
                    _, value = parsed[0].split("=", 1)
            except ValueError:
                pass
        os.environ.setdefault(key, value)


_load_local_env_file()
os.environ.setdefault("PBI_SCANNER_DEBUG_TIMINGS", "1")

# Fictional placeholders only; real runs must set PBI_BENCH_* (see README).
_DEFAULT_CONNECTION_STRING = (
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
    "Example%20Workspace;"
    "Initial Catalog=example_semantic_model;"
)
_DEFAULT_DAX = 'EVALUATE ROW("x", 1)'
_SAMPLE_ROWS = 100
_POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
_METADATA_SAMPLE_ROWS = 5

extension_path = (
    REPO
    / "build"
    / "release"
    / "extension"
    / "pbi_scanner"
    / "pbi_scanner.duckdb_extension"
)


def _bench_config() -> tuple[str, str, str]:
    cs = (
        os.environ.get("PBI_BENCH_CONNECTION_STRING", "").strip()
        or _DEFAULT_CONNECTION_STRING
    )
    dax = os.environ.get("PBI_BENCH_DAX", "").strip() or _DEFAULT_DAX
    secret = os.environ.get("PBI_BENCH_SECRET_NAME", "pbi_cli").strip() or "pbi_cli"
    return cs, dax, secret


def _parse_connection_string(connection_string: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for segment in connection_string.split(";"):
        if not segment.strip() or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        result[key.strip().lower()] = value.strip()
    return result


def _parse_powerbi_data_source(data_source: str) -> tuple[str, str]:
    prefix = "powerbi://"
    if not data_source.lower().startswith(prefix):
        raise ValueError("PBI_BENCH_DIRECT_XMLA requires a powerbi:// Data Source")
    remainder = data_source[len(prefix) :]
    host, _, path = remainder.partition("/")
    workspace_segment = path.rstrip("/").rsplit("/", 1)[-1]
    if not host or not workspace_segment:
        raise ValueError("powerbi:// Data Source must include host and workspace")
    return host, unquote_plus(workspace_segment)


def _az_access_token() -> str:
    proc = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--scope",
            _POWER_BI_SCOPE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def _json_request(
    method: str,
    url: str,
    access_token: str,
    body: dict[str, str] | None = None,
    timeout: int = 300,
) -> object:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {url} failed with HTTP {exc.code}: {detail}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc}") from exc


def resolve_direct_xmla_connection_string(connection_string: str) -> str:
    parts = _parse_connection_string(connection_string)
    data_source = parts.get("data source", "")
    initial_catalog = parts.get("initial catalog", "")
    if not initial_catalog:
        raise ValueError("Initial Catalog is required")
    if data_source.lower().startswith("https://") and "/xmla?" in data_source:
        return connection_string

    host, workspace_name = _parse_powerbi_data_source(data_source)
    access_token = _az_access_token()
    base_url = f"https://{host}"

    started_at = perf_counter()
    workspaces = _json_request(
        "GET",
        f"{base_url}/powerbi/databases/v201606/workspaces?includeMyWorkspace=true",
        access_token,
    )
    log_timing("resolve direct XMLA workspace list", started_at)
    workspace = next(
        (
            item
            for item in workspaces
            if str(item.get("name", "")).casefold() == workspace_name.casefold()
        ),
        None,
    )
    if not workspace:
        raise ValueError(f'workspace "{workspace_name}" was not found')

    started_at = perf_counter()
    datasets = _json_request(
        "POST",
        f"{base_url}/powerbi/databases/v201606/workspaces/"
        f"{workspace['id']}/getDatabaseName",
        access_token,
        {
            "datasetName": initial_catalog,
            "workspaceType": str(workspace["type"]),
        },
    )
    log_timing("resolve direct XMLA dataset lookup", started_at)
    dataset = next(
        (
            item
            for item in datasets
            if str(item.get("datasetName", "")).casefold()
            == initial_catalog.casefold()
        ),
        None,
    )
    if not dataset:
        raise ValueError(f'dataset "{initial_catalog}" was not found')

    started_at = perf_counter()
    cluster = _json_request(
        "PUT",
        f"{base_url}/spglobalservice/GetOrInsertClusterUrisByTenantLocation",
        access_token,
        {},
    )
    log_timing("resolve direct XMLA cluster lookup", started_at)

    database_name = str(dataset["databaseName"])
    fixed_cluster_uri = str(cluster["FixedClusterUri"]).rstrip("/") + "/"
    direct_source = f"{fixed_cluster_uri}xmla?vs=sobe_wowvirtualserver&db={database_name}"
    direct_catalog = f"sobe_wowvirtualserver-{database_name}"
    direct = f"Data Source={direct_source};Initial Catalog={direct_catalog};"
    print(f"[python] direct XMLA connection string: {direct}")
    return direct


def log_timing(label: str, started_at: float) -> None:
    elapsed_ms = (perf_counter() - started_at) * 1000
    print(f"[python] {label}: {elapsed_ms:.1f} ms")


def _dax_sql(
    connection_string: str, dax_query: str, secret_name: str, mode: str
) -> str:
    source_sql = f"""
        dax_query(
            '{connection_string.replace("'", "''")}',
            '{dax_query.replace("'", "''")}',
            secret_name := '{secret_name.replace("'", "''")}'
        )
    """
    if mode == "count":
        return f"SELECT count(*) AS total_rows FROM {source_sql}"
    if mode == "materialize":
        return f"SELECT * FROM {source_sql}"
    raise ValueError("PBI_BENCH_MODE must be count or materialize")


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _metadata_function_sql(
    function_name: str, connection_string: str, secret_name: str
) -> str:
    return (
        f"{function_name}("
        f"'{_sql_escape(connection_string)}', "
        f"secret_name := '{_sql_escape(secret_name)}'"
        f")"
    )


def _normalized_column_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _find_matching_columns(columns: list[str], token_options: set[str]) -> list[str]:
    matches: list[str] = []
    normalized_tokens = {_normalized_column_name(token) for token in token_options}
    normalized_columns = {
        column: _normalized_column_name(column) for column in columns
    }
    for column, normalized in normalized_columns.items():
        if any(token in normalized for token in normalized_tokens):
            matches.append(column)
    return matches


def _has_non_empty_value(rows: list[tuple[object, ...]], columns: list[str], target_column: str) -> bool:
    column_idx = columns.index(target_column)
    for row in rows:
        value = row[column_idx]
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        return True
    return False


def _run_metadata_probes(con: object, connection_string: str, secret_name: str) -> None:
    fail_fast = _truthy_env("PBI_BENCH_METADATA_FAIL_FAST")
    min_rows = int(os.environ.get("PBI_BENCH_METADATA_MIN_ROWS", "1"))
    checks = [
        {
            "name": "pbi_tables",
            "required_groups": [
                {"table"},
                {"name", "tablename", "tableid"},
            ],
            "non_empty_groups": [
                {"table", "name", "tablename"},
            ],
            "min_rows": min_rows,
        },
        {
            "name": "pbi_columns",
            "required_groups": [
                {"column"},
                {"table"},
                {"type", "datatype"},
            ],
            "non_empty_groups": [
                {"column", "columnname"},
                {"table", "tablename"},
            ],
            "min_rows": min_rows,
        },
        {
            "name": "pbi_measures",
            "required_groups": [
                {"measure"},
            ],
            "non_empty_groups": [
                {"measure", "measurename"},
                {"expression", "formula"},
            ],
            # Some models have no measures; default to informative pass at 0 rows.
            "min_rows": 0,
        },
        {
            "name": "pbi_relationships",
            "required_groups": [
                {"fromtable", "sourcetable", "from"},
                {"totable", "targettable", "to"},
            ],
            "non_empty_groups": [
                {"fromtable", "sourcetable", "from"},
                {"totable", "targettable", "to"},
            ],
            # Some models have no explicit relationships; default to informative pass at 0 rows.
            "min_rows": 0,
        },
    ]

    print(
        "[python] metadata probe enabled "
        f"(fail_fast={'on' if fail_fast else 'off'} "
        f"min_rows_tables_columns={min_rows})"
    )

    failures: list[str] = []
    for check in checks:
        name = str(check["name"])
        source_sql = _metadata_function_sql(name, connection_string, secret_name)

        count_started_at = perf_counter()
        try:
            total_rows = int(
                con.sql(f"SELECT count(*) AS total_rows FROM {source_sql}").fetchone()[0]
            )
        except Exception as exc:
            message = f"{name}: count query failed: {exc}"
            failures.append(message)
            print(f"[metadata][FAIL] {message}")
            if fail_fast:
                raise RuntimeError(message) from exc
            continue
        log_timing(f"metadata {name} count(*)", count_started_at)
        print(f"[metadata][PASS] {name}: row_count={total_rows}")

        if total_rows < int(check["min_rows"]):
            message = (
                f"{name}: expected at least {check['min_rows']} rows, got {total_rows}"
            )
            failures.append(message)
            print(f"[metadata][FAIL] {message}")
            if fail_fast:
                raise RuntimeError(message)
            continue

        shape_started_at = perf_counter()
        try:
            relation = con.sql(f"SELECT * FROM {source_sql} LIMIT {_METADATA_SAMPLE_ROWS}")
            columns = list(relation.columns)
            rows = relation.fetchall()
        except Exception as exc:
            message = f"{name}: sample/content query failed: {exc}"
            failures.append(message)
            print(f"[metadata][FAIL] {message}")
            if fail_fast:
                raise RuntimeError(message) from exc
            continue
        log_timing(f"metadata {name} content sample", shape_started_at)

        missing_groups: list[set[str]] = []
        for group in check["required_groups"]:
            if not _find_matching_columns(columns, set(group)):
                missing_groups.append(set(group))
        if missing_groups:
            message = (
                f"{name}: missing expected column groups={missing_groups}; "
                f"returned_columns={columns}"
            )
            failures.append(message)
            print(f"[metadata][FAIL] {message}")
            if fail_fast:
                raise RuntimeError(message)
            continue

        if total_rows == 0:
            print(f"[metadata][PASS] {name}: content checks skipped (0 rows)")
            continue

        non_empty_failures: list[set[str]] = []
        for group in check["non_empty_groups"]:
            candidate_columns = _find_matching_columns(columns, set(group))
            if not candidate_columns:
                continue
            if not any(
                _has_non_empty_value(rows, columns, candidate)
                for candidate in candidate_columns
            ):
                non_empty_failures.append(set(group))
        if non_empty_failures:
            message = (
                f"{name}: expected non-empty sample values for groups={non_empty_failures}; "
                f"sample_rows={len(rows)}"
            )
            failures.append(message)
            print(f"[metadata][FAIL] {message}")
            if fail_fast:
                raise RuntimeError(message)
            continue

        print(f"[metadata][PASS] {name}: content checks passed")

    if failures:
        joined = "\n - ".join(failures)
        raise RuntimeError(f"metadata probe failed:\n - {joined}")


def run_with_python_duckdb(connection_string: str, dax_query: str, secret_name: str) -> None:
    import duckdb  # noqa: PLC0415

    script_started_at = perf_counter()
    ext_escaped = str(extension_path).replace("'", "''")
    mode = (
        os.environ.get("PBI_BENCH_MODE", "materialize").strip().lower()
        or "materialize"
    )
    iterations = int(os.environ.get("PBI_BENCH_ITERATIONS", "1"))
    transport = (
        os.environ.get("PBI_SCANNER_XMLA_TRANSPORT", "sx_xpress").strip()
        or "sx_xpress"
    )
    metadata_cache_disabled = (
        os.environ.get("PBI_SCANNER_DISABLE_METADATA_CACHE", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    metadata_cache_dir = os.environ.get("PBI_SCANNER_CACHE_DIR", "").strip() or "default"
    metadata_probe = _truthy_env("PBI_BENCH_METADATA_PROBE")
    print(
        f"[python] benchmark mode={mode} iterations={iterations} "
        f"transport={transport} "
        f"metadata_cache={'disabled' if metadata_cache_disabled else 'enabled'} "
        f"cache_dir={metadata_cache_dir} "
        f"metadata_probe={'enabled' if metadata_probe else 'disabled'}"
    )
    if os.environ.get("PBI_BENCH_DIRECT_XMLA", "").strip():
        connection_string = resolve_direct_xmla_connection_string(connection_string)

    step_started_at = perf_counter()
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    log_timing("duckdb.connect", step_started_at)

    step_started_at = perf_counter()
    con.sql(f"LOAD '{ext_escaped}'")
    log_timing("LOAD pbi_scanner", step_started_at)

    step_started_at = perf_counter()
    con.install_extension("azure")
    log_timing("INSTALL azure", step_started_at)

    step_started_at = perf_counter()
    con.load_extension("azure")
    log_timing("LOAD azure", step_started_at)

    step_started_at = perf_counter()
    con.sql(
        f"""
        CREATE OR REPLACE SECRET {secret_name} (
            TYPE azure,
            PROVIDER credential_chain,
            CHAIN 'cli'
        )
        """
    )
    log_timing(f"CREATE SECRET {secret_name}", step_started_at)

    if metadata_probe:
        step_started_at = perf_counter()
        _run_metadata_probes(con, connection_string, secret_name)
        log_timing("metadata probe total", step_started_at)

    for iteration in range(1, iterations + 1):
        prefix = f"iteration {iteration}/{iterations} {mode}"
        step_started_at = perf_counter()
        relation = con.sql(
            _dax_sql(connection_string, dax_query, secret_name, mode)
        )
        log_timing(f"{prefix} relation creation", step_started_at)

        step_started_at = perf_counter()
        if mode == "count":
            total_rows = relation.fetchone()[0]
            log_timing(f"{prefix} fetch count", step_started_at)
            print(f"Total rows: {total_rows}")
        else:
            df = relation.pl()
            log_timing(f"{prefix} relation.pl() materialization", step_started_at)
            print(f"Total rows: {df.height}")
            if df.height > 0:
                print(df.sample(min(_SAMPLE_ROWS, df.height)))

    log_timing("script total", script_started_at)


def run_with_bundled_cli(connection_string: str, dax_query: str, secret_name: str) -> None:
    from bench_duckdb_cli import (  # noqa: PLC0415
        materialize_and_summarize_sql,
        parse_count_star_then_sample_lines,
        require_release_artifacts,
        run_duckdb_cli,
    )

    script_started_at = perf_counter()
    ext_path, _ = require_release_artifacts(REPO)
    sql = materialize_and_summarize_sql(
        ext_path, secret_name, connection_string, dax_query, _SAMPLE_ROWS
    )
    bench_env = {k: os.environ[k] for k in os.environ if k.startswith("PBI_SCANNER")}
    if _truthy_env("PBI_BENCH_METADATA_PROBE"):
        print(
            "[python] metadata probe requested, but bundled CLI fallback currently "
            "runs the dax_query materialize/count workflow only",
            file=sys.stderr,
        )

    step_started_at = perf_counter()
    proc = run_duckdb_cli(REPO, sql, env=bench_env)
    log_timing("bundled duckdb CLI (materialize + count + sample)", step_started_at)

    if proc.returncode != 0:
        print(proc.stderr or proc.stdout or "(no output)", file=sys.stderr)
        sys.exit(proc.returncode or 1)

    try:
        total, sample_lines = parse_count_star_then_sample_lines(proc.stdout)
    except ValueError:
        print(proc.stdout, file=sys.stderr)
        sys.exit(1)

    log_timing("script total", script_started_at)
    print(f"Total rows: {total}")
    if sample_lines:
        print("[sample csv]")
        print("\n".join(sample_lines))


def main() -> None:
    connection_string, dax_query, secret_name = _bench_config()

    from bench_duckdb_cli import python_duckdb_connect_usable  # noqa: PLC0415

    if python_duckdb_connect_usable():
        run_with_python_duckdb(connection_string, dax_query, secret_name)
    else:
        print(
            "[python] duckdb.connect not available; using bundled ./build/release/duckdb CLI",
            file=sys.stderr,
        )
        run_with_bundled_cli(connection_string, dax_query, secret_name)


if __name__ == "__main__":
    main()
