"""
Local-only: run real DAX against Power BI for development and performance
benchmarking. Not used in CI (see README Performance + AGENTS.md).

Configuration (prefer env vars so nothing secret is hardcoded):
  PBI_BENCH_CONNECTION_STRING
      Power BI XMLA connection string.
  PBI_BENCH_DAX
      DAX to run, for example EVALUATE 'My Table'.
  PBI_BENCH_SECRET_NAME
      Optional; defaults to pbi_cli.
  PBI_BENCH_AUTH_MODE
      Optional; session auth mode SET value: azure_cli, access_token, or
      service_principal. Defaults to azure_cli unless direct token mode is used.
  PBI_BENCH_MODE
      count or materialize; defaults to materialize.
  PBI_BENCH_ITERATIONS
      Runs in the same process; defaults to 1.
  PBI_BENCH_METADATA_PROBE
      Set 1/true to run pbi_* metadata probes.
  PBI_BENCH_METADATA_FAIL_FAST
      Optional; stop on first metadata failure.
  PBI_BENCH_METADATA_MIN_ROWS
      Optional; minimum rows for tables/columns; defaults to 1.
  PBI_BENCH_METADATA_PRINT_ROWS
      Optional; print sampled metadata rows; defaults on.
  PBI_BENCH_METADATA_SAMPLE_ROWS
      Optional; metadata display sample size; defaults to 100.
  PBI_BENCH_METADATA_MATRIX
      Optional; run metadata probes across transport profiles.
  PBI_BENCH_METADATA_INCLUDE_PLAIN
      Optional; include plain transport in matrix mode.
  PBI_BENCH_METADATA_STRICT_SX
      Optional; enforce sx_xpress primary pass.
  PBI_BENCH_DIRECT_XMLA
      Resolve powerbi:// to direct XMLA before running.
  PBI_BENCH_USE_BUNDLED_CLI
      Set 1/true to force bundled DuckDB CLI fallback.
  PBI_BENCH_USE_PYTHON_DUCKDB_ON_WINDOWS
      Optional; set 1/true to force Python duckdb path on Windows.
  PBI_BENCH_USE_DUCKDB_AZURE_SECRET
      Optional; set 1/true to use DuckDB Azure secret auth (installs/loads
      azure and creates secret). Default path is SET-based auth mode without
      azure extension setup overhead.
  PBI_SCANNER_XMLA_TRANSPORT
      plain, sx, xpress, or sx_xpress; defaults to sx_xpress.
  PBI_SCANNER_DISABLE_METADATA_CACHE
      Set 1/true to force cold target/schema probes.
  PBI_SCANNER_CACHE_DIR
      Override metadata cache directory.

If `duckdb.connect` is missing from the Python `duckdb` package, if
PBI_BENCH_USE_BUNDLED_CLI is truthy, or on Windows by default (unless
PBI_BENCH_USE_PYTHON_DUCKDB_ON_WINDOWS is truthy), this script falls back to
the bundled `./build/release/duckdb` CLI (after `make release`).

Run with uv (installs the `bench` group / Python duckdb when needed):
  uv run --group bench query_semantic_model_minimal.py

Metadata probe examples:
  PBI_BENCH_METADATA_PROBE=1 uv run --group bench query_semantic_model_minimal.py
  PBI_BENCH_METADATA_PROBE=1 PBI_BENCH_METADATA_MATRIX=1 uv run --group bench query_semantic_model_minimal.py
  PBI_BENCH_METADATA_PROBE=1 PBI_BENCH_METADATA_STRICT_SX=1 uv run --group bench query_semantic_model_minimal.py
  PBI_SCANNER_XMLA_TRANSPORT=xpress PBI_BENCH_METADATA_PROBE=1 uv run --group bench query_semantic_model_minimal.py
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
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
_METADATA_VALIDATION_SAMPLE_ROWS = 5
_METADATA_DISPLAY_SAMPLE_ROWS_DEFAULT = 100
_METADATA_FUNCTION_NAMES = (
    "pbi_tables",
    "pbi_columns",
    "pbi_measures",
    "pbi_relationships",
)

extension_path = (
    REPO
    / "build"
    / "release"
    / "extension"
    / "pbi_scanner"
    / "pbi_scanner.duckdb_extension"
)


def _normalize_auth_mode(value: str) -> str:
    normalized = value.strip().lower()
    if normalized == "cli":
        return "azure_cli"
    if normalized in {"azure_cli", "access_token", "service_principal"}:
        return normalized
    raise ValueError(
        "PBI_BENCH_AUTH_MODE must be one of azure_cli, access_token, "
        "service_principal, or cli"
    )


def _bench_config() -> tuple[str, str, str, str]:
    cs = (
        os.environ.get("PBI_BENCH_CONNECTION_STRING", "").strip()
        or _DEFAULT_CONNECTION_STRING
    )
    dax = os.environ.get("PBI_BENCH_DAX", "").strip() or _DEFAULT_DAX
    secret = os.environ.get("PBI_BENCH_SECRET_NAME", "pbi_cli").strip() or "pbi_cli"
    auth_mode = (
        os.environ.get("PBI_BENCH_AUTH_MODE", "").strip() or "azure_cli"
    )
    return cs, dax, secret, _normalize_auth_mode(auth_mode)


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
    az_command = shutil.which("az") or shutil.which("az.cmd") or "az"
    proc = subprocess.run(
        [
            az_command,
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
            if str(item.get("datasetName", "")).casefold() == initial_catalog.casefold()
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
    direct_source = (
        f"{fixed_cluster_uri}xmla?vs=sobe_wowvirtualserver&db={database_name}"
    )
    direct_catalog = f"sobe_wowvirtualserver-{database_name}"
    direct = f"Data Source={direct_source};Initial Catalog={direct_catalog};"
    print(f"[python] direct XMLA connection string: {direct}")
    return direct


def log_timing(label: str, started_at: float) -> None:
    elapsed_ms = (perf_counter() - started_at) * 1000
    print(f"[python] {label}: {elapsed_ms:.1f} ms")


def _dax_sql(
    connection_string: str,
    dax_query: str,
    secret_name: str | None,
    mode: str,
    access_token: str | None = None,
) -> str:
    if access_token is not None:
        source_sql = f"""
            dax_query(
                '{_sql_escape(connection_string)}',
                '{_sql_escape(dax_query)}',
                access_token := '{_sql_escape(access_token)}'
            )
        """
    elif secret_name:
        source_sql = f"""
            dax_query(
                '{_sql_escape(connection_string)}',
                '{_sql_escape(dax_query)}',
                secret_name := '{_sql_escape(secret_name)}'
            )
        """
    else:
        source_sql = f"""
            dax_query(
                '{_sql_escape(connection_string)}',
                '{_sql_escape(dax_query)}'
            )
        """
    if mode == "count":
        return f"SELECT count(*) AS total_rows FROM {source_sql}"
    if mode == "materialize":
        return f"SELECT * FROM {source_sql}"
    raise ValueError("PBI_BENCH_MODE must be count or materialize")


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _env_flag_default_on(name: str, default: str = "1") -> bool:
    value = os.environ.get(name, default).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _metadata_display_sample_rows() -> int:
    raw_value = os.environ.get(
        "PBI_BENCH_METADATA_SAMPLE_ROWS", str(_METADATA_DISPLAY_SAMPLE_ROWS_DEFAULT)
    ).strip()
    if not raw_value:
        return _METADATA_DISPLAY_SAMPLE_ROWS_DEFAULT
    try:
        parsed = int(raw_value)
    except ValueError:
        return _METADATA_DISPLAY_SAMPLE_ROWS_DEFAULT
    return max(1, parsed)


def _default_xmla_transport() -> str:
    return (
        os.environ.get("PBI_SCANNER_XMLA_TRANSPORT", "sx_xpress").strip() or "sx_xpress"
    )


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _metadata_function_sql(
    function_name: str,
    connection_string: str,
    secret_name: str | None,
    access_token: str | None = None,
) -> str:
    if access_token is not None:
        return (
            f"{function_name}('{_sql_escape(connection_string)}', "
            f"access_token := '{_sql_escape(access_token)}')"
        )
    if secret_name:
        return (
            f"{function_name}('{_sql_escape(connection_string)}', "
            f"secret_name := '{_sql_escape(secret_name)}')"
        )
    return f"{function_name}('{_sql_escape(connection_string)}')"


def _normalized_column_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _find_matching_columns(
    columns: list[str],
    token_options: set[str],
    column_norm: dict[str, str] | None = None,
) -> list[str]:
    matches: list[str] = []
    normalized_tokens = {_normalized_column_name(token) for token in token_options}
    if column_norm is None:
        column_norm = {column: _normalized_column_name(column) for column in columns}
    for column, normalized in column_norm.items():
        if any(token in normalized for token in normalized_tokens):
            matches.append(column)
    return matches


def _has_non_empty_value(
    rows: list[tuple[object, ...]], columns: list[str], target_column: str
) -> bool:
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


def _metadata_checks(min_rows: int) -> list[dict[str, object]]:
    return [
        {
            "name": "pbi_tables",
            # INFO.VIEW.TABLES uses [Name], [Model], [ID] (not [Table]).
            "required_groups": [
                {"name"},
                {"model"},
            ],
            "non_empty_groups": [
                {"name", "tablename"},
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
                {"name", "measure", "measurename"},
            ],
            "non_empty_groups": [
                {"name", "measure", "measurename"},
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


def _metadata_transport_profiles(
    matrix_mode: bool, include_plain: bool
) -> list[dict[str, str]]:
    configured_transport = _default_xmla_transport()
    profiles: list[dict[str, str]] = [
        {"name": "default", "transport": configured_transport, "kind": "primary"}
    ]
    if not matrix_mode:
        return profiles
    if configured_transport != "xpress":
        profiles.append(
            {"name": "xpress", "transport": "xpress", "kind": "compatibility"}
        )
    if include_plain and configured_transport != "plain":
        profiles.append({"name": "plain", "transport": "plain", "kind": "diagnostic"})
    return profiles


def _run_metadata_probe_profile(
    con: object,
    connection_string: str,
    secret_name: str | None,
    access_token: str | None,
    profile_name: str,
    transport: str,
    checks: list[dict[str, object]],
    fail_fast: bool,
    print_rows_enabled: bool,
    display_sample_rows: int,
) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    os.environ["PBI_SCANNER_XMLA_TRANSPORT"] = transport
    print(f"[metadata][PROFILE] {profile_name}: transport={transport}")
    for check in checks:
        name = str(check["name"])
        source_sql = _metadata_function_sql(
            name, connection_string, secret_name, access_token
        )
        results[name] = {
            "ok": False,
            "profile": profile_name,
            "transport": transport,
            "stage": "init",
            "message": "",
            "row_count": None,
        }

        count_started_at = perf_counter()
        try:
            total_rows = int(
                con.sql(f"SELECT count(*) AS total_rows FROM {source_sql}").fetchone()[
                    0
                ]
            )
        except Exception as exc:
            message = f"{name}: count query failed: {exc}"
            results[name]["stage"] = "count"
            results[name]["message"] = message
            print(f"[metadata][FAIL][{profile_name}][{name}][count] {exc}")
            if fail_fast:
                raise RuntimeError(message) from exc
            continue
        log_timing(f"metadata {profile_name} {name} count(*)", count_started_at)
        results[name]["row_count"] = total_rows
        print(f"[metadata][PASS][{profile_name}][{name}][count] row_count={total_rows}")

        if total_rows < int(check["min_rows"]):
            message = f"expected at least {check['min_rows']} rows, got {total_rows}"
            results[name]["stage"] = "count_threshold"
            results[name]["message"] = message
            print(
                f"[metadata][FAIL][{profile_name}][{name}][count_threshold] {message}"
            )
            if fail_fast:
                raise RuntimeError(f"{name}: {message}")
            continue

        shape_started_at = perf_counter()
        try:
            relation = con.sql(
                f"SELECT * FROM {source_sql} LIMIT {_METADATA_VALIDATION_SAMPLE_ROWS}"
            )
            columns = list(relation.columns)
            rows = relation.fetchall()
        except Exception as exc:
            message = f"{name}: sample/content query failed: {exc}"
            results[name]["stage"] = "content_fetch"
            results[name]["message"] = message
            print(f"[metadata][FAIL][{profile_name}][{name}][content_fetch] {exc}")
            if fail_fast:
                raise RuntimeError(message) from exc
            continue
        log_timing(f"metadata {profile_name} {name} content sample", shape_started_at)

        column_norm = {column: _normalized_column_name(column) for column in columns}
        missing_groups: list[set[str]] = []
        for group in check["required_groups"]:
            if not _find_matching_columns(columns, set(group), column_norm):
                missing_groups.append(set(group))
        if missing_groups:
            message = f"missing expected column groups={missing_groups}; returned_columns={columns}"
            results[name]["stage"] = "schema"
            results[name]["message"] = message
            print(f"[metadata][FAIL][{profile_name}][{name}][schema] {message}")
            if fail_fast:
                raise RuntimeError(f"{name}: {message}")
            continue

        if total_rows == 0:
            results[name]["ok"] = True
            results[name]["stage"] = "content"
            print(f"[metadata][PASS][{profile_name}][{name}][content] skipped (0 rows)")
            continue

        non_empty_failures: list[set[str]] = []
        for group in check["non_empty_groups"]:
            candidate_columns = _find_matching_columns(columns, set(group), column_norm)
            if not candidate_columns:
                continue
            if not any(
                _has_non_empty_value(rows, columns, candidate)
                for candidate in candidate_columns
            ):
                non_empty_failures.append(set(group))
        if non_empty_failures:
            message = (
                f"expected non-empty sample values for groups={non_empty_failures}; "
                f"sample_rows={len(rows)}"
            )
            results[name]["stage"] = "content"
            results[name]["message"] = message
            print(f"[metadata][FAIL][{profile_name}][{name}][content] {message}")
            if fail_fast:
                raise RuntimeError(f"{name}: {message}")
            continue

        results[name]["ok"] = True
        results[name]["stage"] = "content"
        print(f"[metadata][PASS][{profile_name}][{name}][content] checks passed")
        if print_rows_enabled:
            try:
                display_relation = con.sql(
                    f"SELECT * FROM {source_sql} LIMIT {display_sample_rows}"
                )
                display_df = display_relation.pl()
                print(
                    f"[metadata][ROWS][{profile_name}][{name}] "
                    f"showing top {display_df.height} rows (limit={display_sample_rows})"
                )
                if display_df.height > 0:
                    print(display_df)
            except Exception as exc:
                print(
                    f"[metadata][WARN][{profile_name}][{name}] "
                    f"display query failed (validation already passed): {exc}"
                )
    return results


def _classify_metadata_outcomes(
    profile_results: dict[str, dict[str, dict[str, object]]],
    profiles: list[dict[str, str]],
) -> tuple[list[str], list[str], list[str]]:
    function_names = list(_METADATA_FUNCTION_NAMES)
    default_profile_name = profiles[0]["name"]
    compatibility_profile_name = next(
        (profile["name"] for profile in profiles if profile["kind"] == "compatibility"),
        "",
    )
    pass_lines: list[str] = []
    soft_fail_lines: list[str] = []
    hard_fail_lines: list[str] = []

    for function_name in function_names:
        per_profile = profile_results.get(function_name, {})
        default_result = per_profile.get(default_profile_name)
        if default_result and bool(default_result["ok"]):
            pass_lines.append(
                f"{function_name}: PASS in primary profile ({default_profile_name})"
            )
            continue

        compatible_result = (
            per_profile.get(compatibility_profile_name)
            if compatibility_profile_name
            else None
        )
        if compatible_result and bool(compatible_result["ok"]):
            default_stage = (
                str(default_result["stage"]) if default_result else "unknown"
            )
            default_message = (
                str(default_result["message"]) if default_result else "unknown failure"
            )
            soft_fail_lines.append(
                f"{function_name}: SOFT_FAIL in primary ({default_profile_name}, stage={default_stage}) "
                f"but PASS in compatibility ({compatibility_profile_name}); cause={default_message}"
            )
            continue

        details: list[str] = []
        for profile in profiles:
            profile_name = profile["name"]
            result = per_profile.get(profile_name)
            if not result:
                details.append(f"{profile_name}: no result")
                continue
            details.append(
                f"{profile_name}: stage={result['stage']} message={result['message'] or 'n/a'}"
            )
        hard_fail_lines.append(f"{function_name}: HARD_FAIL ({'; '.join(details)})")

    return pass_lines, soft_fail_lines, hard_fail_lines


def _run_metadata_probes(
    con: object,
    connection_string: str,
    secret_name: str | None,
    access_token: str | None = None,
) -> None:
    fail_fast = _truthy_env("PBI_BENCH_METADATA_FAIL_FAST")
    min_rows = int(os.environ.get("PBI_BENCH_METADATA_MIN_ROWS", "1"))
    print_rows_enabled = _env_flag_default_on("PBI_BENCH_METADATA_PRINT_ROWS")
    matrix_mode = _truthy_env("PBI_BENCH_METADATA_MATRIX")
    include_plain = _truthy_env("PBI_BENCH_METADATA_INCLUDE_PLAIN")
    strict_sx_mode = _truthy_env("PBI_BENCH_METADATA_STRICT_SX")
    display_sample_rows = _metadata_display_sample_rows()
    configured_transport = _default_xmla_transport()
    if strict_sx_mode and configured_transport != "sx_xpress":
        raise RuntimeError(
            "PBI_BENCH_METADATA_STRICT_SX=1 requires "
            "PBI_SCANNER_XMLA_TRANSPORT=sx_xpress"
        )
    if strict_sx_mode:
        matrix_mode = True
    checks = _metadata_checks(min_rows)
    profiles = _metadata_transport_profiles(matrix_mode, include_plain)
    original_transport = os.environ.get("PBI_SCANNER_XMLA_TRANSPORT", "")

    print(
        "[python] metadata probe enabled "
        f"(fail_fast={'on' if fail_fast else 'off'} "
        f"min_rows_tables_columns={min_rows} "
        f"display_sample_rows={display_sample_rows} "
        f"strict_sx={'on' if strict_sx_mode else 'off'} "
        f"matrix_mode={'on' if matrix_mode else 'off'} "
        f"profiles={','.join(profile['name'] + ':' + profile['transport'] for profile in profiles)})"
    )
    per_function_results: dict[str, dict[str, dict[str, object]]] = {}
    try:
        for profile in profiles:
            profile_started_at = perf_counter()
            profile_name = profile["name"]
            profile_transport = profile["transport"]
            profile_results = _run_metadata_probe_profile(
                con,
                connection_string,
                secret_name,
                access_token,
                profile_name,
                profile_transport,
                checks,
                fail_fast,
                print_rows_enabled,
                display_sample_rows,
            )
            for function_name, result in profile_results.items():
                per_function_results.setdefault(function_name, {})[profile_name] = (
                    result
                )
            log_timing(f"metadata profile {profile_name} total", profile_started_at)
    finally:
        if original_transport:
            os.environ["PBI_SCANNER_XMLA_TRANSPORT"] = original_transport
        elif "PBI_SCANNER_XMLA_TRANSPORT" in os.environ:
            del os.environ["PBI_SCANNER_XMLA_TRANSPORT"]

    pass_lines, soft_fail_lines, hard_fail_lines = _classify_metadata_outcomes(
        per_function_results, profiles
    )
    effective_pass_lines = list(pass_lines)
    effective_soft_fail_lines = list(soft_fail_lines)
    effective_hard_fail_lines = list(hard_fail_lines)
    if strict_sx_mode and effective_soft_fail_lines:
        effective_hard_fail_lines.extend(
            [
                f"{line} [promoted by strict_sx mode]"
                for line in effective_soft_fail_lines
            ]
        )
        effective_soft_fail_lines = []

    print("[metadata][SUMMARY] ----------------------------------------")
    for line in effective_pass_lines:
        print(f"[metadata][SUMMARY][PASS] {line}")
    for line in effective_soft_fail_lines:
        print(f"[metadata][SUMMARY][SOFT_FAIL] {line}")
    for line in effective_hard_fail_lines:
        print(f"[metadata][SUMMARY][HARD_FAIL] {line}")
    print(
        "[metadata][SUMMARY] counts: "
        f"pass={len(effective_pass_lines)} soft_fail={len(effective_soft_fail_lines)} "
        f"hard_fail={len(effective_hard_fail_lines)}"
    )
    if effective_hard_fail_lines:
        print(
            "[metadata][SUMMARY] rerun hint: "
            "PBI_SCANNER_XMLA_TRANSPORT=xpress PBI_BENCH_METADATA_PROBE=1 "
            "uv run --group bench query_semantic_model_minimal.py"
        )
        raise RuntimeError(
            "metadata probe hard failures detected "
            f"(configured transport={configured_transport}); see summary above"
        )


def run_with_python_duckdb(
    connection_string: str, dax_query: str, secret_name: str, default_auth_mode: str
) -> None:
    import duckdb  # noqa: PLC0415
    from bench_duckdb_cli import _windows_runtime_path  # noqa: PLC0415

    script_started_at = perf_counter()
    ext_escaped = _sql_escape(str(extension_path))
    runtime_path = _windows_runtime_path(REPO)
    if runtime_path:
        os.environ["PATH"] = runtime_path + os.pathsep + os.environ.get("PATH", "")
    mode = (
        os.environ.get("PBI_BENCH_MODE", "materialize").strip().lower() or "materialize"
    )
    iterations = int(os.environ.get("PBI_BENCH_ITERATIONS", "1"))
    transport = _default_xmla_transport()
    metadata_cache_disabled = _truthy_env("PBI_SCANNER_DISABLE_METADATA_CACHE")
    metadata_cache_dir = (
        os.environ.get("PBI_SCANNER_CACHE_DIR", "").strip() or "default"
    )
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

    access_token = None
    use_secret_auth = _truthy_env("PBI_BENCH_USE_DUCKDB_AZURE_SECRET")
    use_direct_token = os.name == "nt" and not use_secret_auth
    session_auth_mode = default_auth_mode
    if use_direct_token:
        step_started_at = perf_counter()
        access_token = _az_access_token()
        log_timing("Azure CLI access token", step_started_at)
        session_auth_mode = "access_token"
        print(
            "[python] using direct Azure CLI access token auth "
            "for Python DuckDB API on Windows"
        )

    step_started_at = perf_counter()
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    log_timing("duckdb.connect", step_started_at)

    step_started_at = perf_counter()
    con.sql(f"LOAD '{ext_escaped}'")
    log_timing("LOAD pbi_scanner", step_started_at)

    step_started_at = perf_counter()
    con.sql(f"SET pbi_scanner_auth_mode = '{_sql_escape(session_auth_mode)}'")
    log_timing("SET pbi_scanner_auth_mode", step_started_at)

    active_secret_name: str | None = None
    if use_secret_auth:
        active_secret_name = secret_name
        step_started_at = perf_counter()
        con.install_extension("azure")
        log_timing("INSTALL azure", step_started_at)

        step_started_at = perf_counter()
        con.load_extension("azure")
        log_timing("LOAD azure", step_started_at)

        step_started_at = perf_counter()
        con.sql(f"""
            CREATE OR REPLACE SECRET {secret_name} (
                TYPE azure,
                PROVIDER credential_chain,
                CHAIN 'cli'
            )
            """)
        log_timing(f"CREATE SECRET {secret_name}", step_started_at)

    if metadata_probe:
        step_started_at = perf_counter()
        _run_metadata_probes(
            con, connection_string, active_secret_name, access_token
        )
        log_timing("metadata probe total", step_started_at)

    for iteration in range(1, iterations + 1):
        prefix = f"iteration {iteration}/{iterations} {mode}"
        step_started_at = perf_counter()
        relation = con.sql(
            _dax_sql(
                connection_string,
                dax_query,
                active_secret_name,
                mode,
                access_token,
            )
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


def run_with_bundled_cli(
    connection_string: str, dax_query: str, secret_name: str
) -> None:
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
    connection_string, dax_query, secret_name, auth_mode = _bench_config()

    from bench_duckdb_cli import python_duckdb_connect_usable  # noqa: PLC0415

    force_cli = _truthy_env("PBI_BENCH_USE_BUNDLED_CLI")
    # Python DuckDB extension load currently crashes on Windows in some local setups.
    # Prefer the bundled CLI there unless explicitly forced to use Python API.
    use_python_on_windows = _truthy_env("PBI_BENCH_USE_PYTHON_DUCKDB_ON_WINDOWS")
    if (
        python_duckdb_connect_usable()
        and not force_cli
        and (os.name != "nt" or use_python_on_windows)
    ):
        run_with_python_duckdb(connection_string, dax_query, secret_name, auth_mode)
    else:
        if os.name == "nt" and not force_cli and not use_python_on_windows:
            print(
                "[python] using bundled DuckDB CLI on Windows; set "
                "PBI_BENCH_USE_PYTHON_DUCKDB_ON_WINDOWS=1 to force Python duckdb path",
                file=sys.stderr,
            )
        print(
            "[python] using bundled DuckDB CLI "
            "(duckdb.connect unavailable or PBI_BENCH_USE_BUNDLED_CLI enabled)",
            file=sys.stderr,
        )
        run_with_bundled_cli(connection_string, dax_query, secret_name)


if __name__ == "__main__":
    main()
