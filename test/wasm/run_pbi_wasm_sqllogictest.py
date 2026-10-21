#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WASM_TEST_DIR = REPO_ROOT / "test" / "wasm"
DEFAULT_TEST = REPO_ROOT / "wasm_sql" / "pbi_scanner_wasm.test"


def default_extension(platform: str) -> Path:
    return REPO_ROOT / "build" / platform / "extension" / "pbi_scanner" / "pbi_scanner.duckdb_extension.wasm"


def run(command, cwd=REPO_ROOT, env=None):
    print("+ " + " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def reject_unsupported_platform(platform: str) -> None:
    if platform == "wasm_mvp":
        raise SystemExit(
            "DuckDB-Wasm 2.0 has no MVP bundle, so browser tests run on wasm_eh. "
            "`make wasm_mvp` can still compile the extension."
        )


def ensure_duckdb_wasm_host(env: dict[str, str]) -> None:
    env.setdefault("OVERRIDE_GIT_DESCRIBE", "v2.0.0")
    run(["bash", str(WASM_TEST_DIR / "ensure_duckdb_wasm_host.sh")], env=env)
    env["PBI_DUCKDB_WASM_DIST"] = str(WASM_TEST_DIR / ".duckdb-wasm" / "packages" / "duckdb-wasm" / "dist")


def main():
    parser = argparse.ArgumentParser(description="Run pbi_scanner sqllogictest files against DuckDB-Wasm in a browser")
    parser.add_argument(
        "--build",
        action="store_true",
        help="build the selected DuckDB WASM platform before running sqllogictest",
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
    parser.add_argument(
        "--test",
        type=Path,
        action="append",
        default=None,
        help="sqllogictest file to run (default: wasm_sql/pbi_scanner_wasm.test)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print per-record sqllogictest results",
    )
    args = parser.parse_args()
    reject_unsupported_platform(args.platform)

    env = os.environ.copy()
    if args.build:
        env.setdefault("OVERRIDE_GIT_DESCRIBE", "v2.0.0")
        run(["make", args.platform], env=env)

    extension = (args.extension or default_extension(args.platform)).resolve()
    if not extension.exists():
        raise SystemExit(
            f"Missing WASM extension artifact: {extension}\n"
            f"Run this command again with --build, or build it with `make {args.platform}` first."
        )

    if not (WASM_TEST_DIR / "node_modules").exists():
        run(["npm", "ci"], cwd=WASM_TEST_DIR)

    env["PBI_WASM_EXTENSION_PATH"] = str(extension)
    env["PBI_WASM_DUCKDB_PLATFORM"] = args.platform
    ensure_duckdb_wasm_host(env)

    command = ["node", "run_sqllogictest.mjs"]
    for test_path in args.test or [DEFAULT_TEST]:
        command.extend(["--test", str(test_path.resolve())])
    if args.verbose:
        command.append("--verbose")
    run(command, cwd=WASM_TEST_DIR, env=env)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode)
