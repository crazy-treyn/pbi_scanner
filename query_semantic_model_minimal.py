"""
Local-only: run real DAX against Power BI for development and performance
benchmarking. Not used in CI (see README Performance + AGENTS.md).

Configuration (prefer env vars so nothing secret is hardcoded):
  PBI_BENCH_CONNECTION_STRING — Power BI XMLA connection string
  PBI_BENCH_DAX — DAX to run (e.g. EVALUATE 'My Table')
  PBI_BENCH_SECRET_NAME — optional; default pbi_cli
  PBI_BENCH_MODE — count or materialize; default materialize
  PBI_BENCH_ITERATIONS — runs in the same process; default 1
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
import subprocess
import sys
from pathlib import Path
from time import perf_counter
from urllib.error import HTTPError, URLError
from urllib.parse import unquote_plus
from urllib.request import Request, urlopen

os.environ.setdefault("PBI_SCANNER_DEBUG_TIMINGS", "1")

REPO = Path(__file__).resolve().parent

# Fictional placeholders only; real runs must set PBI_BENCH_* (see README).
_DEFAULT_CONNECTION_STRING = (
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
    "Example%20Workspace;"
    "Initial Catalog=example_semantic_model;"
)
_DEFAULT_DAX = 'EVALUATE ROW("x", 1)'
_SAMPLE_ROWS = 100
_POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

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
    print(
        f"[python] benchmark mode={mode} iterations={iterations} "
        f"transport={transport} "
        f"metadata_cache={'disabled' if metadata_cache_disabled else 'enabled'} "
        f"cache_dir={metadata_cache_dir}"
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
