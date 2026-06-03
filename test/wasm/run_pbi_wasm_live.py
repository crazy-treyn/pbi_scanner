#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE = REPO_ROOT / "test" / "wasm" / "run_pbi_wasm_smoke.py"


def main():
    parser = argparse.ArgumentParser(description="Run optional live pbi_scanner DuckDB-Wasm validation")
    parser.add_argument("--build", action="store_true", help="build the wasm_eh extension before running")
    args = parser.parse_args()

    if os.environ.get("PBI_WASM_LIVE") != "1":
        print("Skipping live WASM validation because PBI_WASM_LIVE=1 is not set.")
        return
    missing = [name for name in ("PBI_WASM_ACCESS_TOKEN", "PBI_WASM_CONNECTION_STRING") if not os.environ.get(name)]
    if missing:
        raise SystemExit("Missing required live WASM environment variables: " + ", ".join(missing))

    command = [sys.executable, str(SMOKE)]
    if args.build:
        command.append("--build")
    subprocess.run(command, cwd=REPO_ROOT, check=True)

    print(
        "Live Power BI WASM validation is intentionally manual for now. "
        "The smoke harness is green; use PBI_WASM_PROXY_URL or a CORS-enabled endpoint "
        "for real browser XMLA calls."
    )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode)
