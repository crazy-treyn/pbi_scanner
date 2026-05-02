#!/usr/bin/env python3
"""
Local-only: run the smallest practical DuckDB SQL smoke for pbi_scanner.

This intentionally avoids the Python DuckDB API, Polars, metadata probes, and
Python-side direct XMLA resolution. It builds one DuckDB SQL script that loads
the local extension, installs/loads DuckDB's azure extension, creates an Azure
CLI credential-chain secret, runs dax_query, and prints the CLI result.

Configuration matches query_semantic_model_minimal.py where possible:
  PBI_BENCH_CONNECTION_STRING
  PBI_BENCH_DAX
  PBI_BENCH_SECRET_NAME
  PBI_SQL_AUTH_MODE              (default: azure_cli; also accepts cli)
  PBI_SQL_USE_AZURE_SECRET=0|1   (default: 0; set 1 to use secret_name auth)
  PBI_SQL_AZURE_PROVIDER         (default: credential_chain; use service_principal
                                  with tenant/client/secret env vars below)
  PBI_SQL_MODE=count|sample|all   (default: sample)
  PBI_SQL_LIMIT=<n>               (default: 100 for sample mode)
  PBI_SQL_INSTALL_AZURE=0|1      (default: 1; only used when secret mode is on)

Service principal env (first non-empty wins per field):
  SP_TENANT_ID, SP_CLIENT_ID, SP_CLIENT_SECRET
  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
  PBI_XMLA_TENANT_ID, PBI_XMLA_CLIENT_ID, PBI_XMLA_CLIENT_SECRET

When PBI_SQL_AZURE_PROVIDER=service_principal, all three values must resolve.
Session SET pbi_scanner_auth_mode is then service_principal (cosmetic; the token
still comes from the DuckDB secret).
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path
from time import perf_counter

from bench_duckdb_cli import (
    _windows_runtime_path,
    escape_sql_literal,
    require_release_artifacts,
)

REPO = Path(__file__).resolve().parent

_DEFAULT_CONNECTION_STRING = (
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
    "Example%20Workspace;"
    "Initial Catalog=example_semantic_model;"
)
_DEFAULT_DAX = 'EVALUATE ROW("x", 1)'


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
                parsed = shlex.split(f"placeholder={value}", posix=True)
                if parsed and "=" in parsed[0]:
                    _, value = parsed[0].split("=", 1)
            except ValueError:
                pass
        os.environ.setdefault(key, value)


def _bench_config() -> tuple[str, str, str]:
    connection_string = (
        os.environ.get("PBI_BENCH_CONNECTION_STRING", "").strip()
        or _DEFAULT_CONNECTION_STRING
    )
    dax = os.environ.get("PBI_BENCH_DAX", "").strip() or _DEFAULT_DAX
    secret_name = os.environ.get("PBI_BENCH_SECRET_NAME", "pbi_cli").strip()
    return connection_string, dax, secret_name or "pbi_cli"


def _auth_mode() -> str:
    configured = (
        os.environ.get("PBI_SQL_AUTH_MODE", "").strip()
        or os.environ.get("PBI_BENCH_AUTH_MODE", "").strip()
        or "azure_cli"
    )
    normalized = configured.lower()
    if normalized == "cli":
        return "azure_cli"
    if normalized in {"azure_cli", "access_token", "service_principal"}:
        return normalized
    raise ValueError(
        "PBI_SQL_AUTH_MODE must be one of azure_cli, access_token, "
        "service_principal, or cli"
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sample_limit() -> int:
    raw_value = os.environ.get("PBI_SQL_LIMIT", "100").strip() or "100"
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 100


def _truthy_env(name: str, default: str = "") -> bool:
    value = os.environ.get(name, default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _first_env(*keys: str) -> str:
    for key in keys:
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw
    return ""


def _resolve_service_principal_credentials() -> tuple[str, str, str]:
    tenant_id = _first_env(
        "SP_TENANT_ID",
        "AZURE_TENANT_ID",
        "PBI_XMLA_TENANT_ID",
    )
    client_id = _first_env(
        "SP_CLIENT_ID",
        "AZURE_CLIENT_ID",
        "PBI_XMLA_CLIENT_ID",
    )
    client_secret = _first_env(
        "SP_CLIENT_SECRET",
        "AZURE_CLIENT_SECRET",
        "PBI_XMLA_CLIENT_SECRET",
    )
    return tenant_id, client_id, client_secret


def _azure_provider() -> str:
    raw = os.environ.get("PBI_SQL_AZURE_PROVIDER", "").strip().lower()
    if not raw or raw == "credential_chain":
        return "credential_chain"
    if raw == "service_principal":
        return "service_principal"
    raise ValueError(
        "PBI_SQL_AZURE_PROVIDER must be credential_chain or service_principal"
    )


def _result_sql(
    connection_string: str, dax: str, secret_name: str, use_secret_auth: bool
) -> str:
    if use_secret_auth:
        source = (
            "dax_query("
            f"'{escape_sql_literal(connection_string)}', "
            f"'{escape_sql_literal(dax)}', "
            f"secret_name := '{escape_sql_literal(secret_name)}'"
            ")"
        )
    else:
        source = (
            "dax_query("
            f"'{escape_sql_literal(connection_string)}', "
            f"'{escape_sql_literal(dax)}'"
            ")"
        )
    mode = os.environ.get("PBI_SQL_MODE", "sample").strip().lower() or "sample"
    if mode == "count":
        return f"SELECT count(*) AS total_rows FROM {source}"
    if mode == "all":
        return f"SELECT * FROM {source}"
    if mode != "sample":
        raise ValueError("PBI_SQL_MODE must be count, sample, or all")
    return f"SELECT * FROM {source} LIMIT {_sample_limit()}"


def _build_sql(
    extension_path: Path, connection_string: str, dax: str, secret_name: str
) -> str:
    statements = [f"LOAD '{escape_sql_literal(str(extension_path))}'"]
    use_secret_auth = _truthy_env("PBI_SQL_USE_AZURE_SECRET", "0")
    provider = _azure_provider() if use_secret_auth else "credential_chain"
    if use_secret_auth:
        if _truthy_env("PBI_SQL_INSTALL_AZURE", "1"):
            statements.append("INSTALL azure")
        statements.append("LOAD azure")
        if provider == "credential_chain":
            statements.append(
                f"CREATE OR REPLACE SECRET {_quote_identifier(secret_name)} "
                "(TYPE azure, PROVIDER credential_chain, CHAIN 'cli')"
            )
        else:
            tenant_id, client_id, client_secret = (
                _resolve_service_principal_credentials()
            )
            missing = [
                label
                for label, val in (
                    ("tenant", tenant_id),
                    ("client_id", client_id),
                    ("client_secret", client_secret),
                )
                if not val
            ]
            if missing:
                raise ValueError(
                    "service_principal secret requires tenant, client_id, and "
                    "client_secret (set SP_* or AZURE_* or PBI_XMLA_* env vars): "
                    f"missing {', '.join(missing)}"
                )
            statements.append(
                f"CREATE OR REPLACE SECRET {_quote_identifier(secret_name)} "
                "(TYPE azure, PROVIDER service_principal, "
                f"TENANT_ID '{escape_sql_literal(tenant_id)}', "
                f"CLIENT_ID '{escape_sql_literal(client_id)}', "
                f"CLIENT_SECRET '{escape_sql_literal(client_secret)}')"
            )
    session_auth = (
        "service_principal"
        if use_secret_auth and provider == "service_principal"
        else _auth_mode()
    )
    statements.extend(
        [
            f"SET pbi_scanner_auth_mode = '{escape_sql_literal(session_auth)}'",
            _result_sql(connection_string, dax, secret_name, use_secret_auth),
        ]
    )
    return ";\n".join(statements) + ";\n"


def main() -> None:
    _load_local_env_file()
    os.environ.setdefault("PBI_SCANNER_DEBUG_TIMINGS", "1")
    connection_string, dax, secret_name = _bench_config()
    extension_path, duckdb_cli = require_release_artifacts(REPO)
    try:
        sql = _build_sql(extension_path, connection_string, dax, secret_name)
    except ValueError as exc:
        print(f"[sql-minimal] error: {exc}", file=sys.stderr)
        sys.exit(2)

    env = os.environ.copy()
    runtime_path = _windows_runtime_path(REPO)
    if runtime_path:
        env["PATH"] = runtime_path + os.pathsep + env.get("PATH", "")

    use_secret = _truthy_env("PBI_SQL_USE_AZURE_SECRET", "0")
    azure_prov = _azure_provider() if use_secret else "credential_chain"
    print(
        "[sql-minimal] running DuckDB CLI SQL smoke "
        f"mode={os.environ.get('PBI_SQL_MODE', 'sample') or 'sample'} "
        f"session_auth_note={_auth_mode()} "
        f"use_azure_secret={1 if use_secret else 0} "
        f"azure_provider={azure_prov} "
        f"install_azure={1 if _truthy_env('PBI_SQL_INSTALL_AZURE', '1') else 0} "
        f"secret={secret_name}"
    )
    started_at = perf_counter()
    proc = subprocess.run(
        [str(duckdb_cli), "-unsigned", "-csv"],
        cwd=str(REPO),
        input=sql,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    elapsed_ms = (perf_counter() - started_at) * 1000

    if proc.stderr:
        print(proc.stderr, file=sys.stderr, end="")
    if proc.stdout:
        print(proc.stdout, end="")
    print(f"[sql-minimal] duckdb cli total: {elapsed_ms:.1f} ms")
    if proc.returncode != 0:
        sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
