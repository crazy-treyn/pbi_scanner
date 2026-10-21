#!/usr/bin/env bash
# Build the DuckDB-Wasm browser host used by the smoke tests.
#
# npm has no @duckdb/duckdb-wasm package for DuckDB 2.0.0. This clones
# duckdb-wasm at the commit that pins DuckDB v2.0.0 and builds the EH "base"
# bundle (duckdb-base.wasm). That tree has no MVP bundle.
set -euo pipefail

PIN=51ee2cc4e22e1e4c9f6038cde755da5f51c9ee0a
DUCKDB_DESCRIBE="${OVERRIDE_GIT_DESCRIBE:-v2.0.0}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${PBI_DUCKDB_WASM_SRC:-$ROOT/.duckdb-wasm}"
DIST="$DEST/packages/duckdb-wasm/dist"
STAMP="$DEST/.pbi-host-stamp"
WASM_STAMP="$DEST/.pbi-wasm-stamp"

if [[ -f "$STAMP" && "$(cat "$STAMP")" == "$PIN" && -f "$DIST/duckdb-base.wasm" && -f "$DIST/duckdb-browser.mjs" && -f "$DIST/duckdb-browser-base.worker.js" ]]; then
	echo "DuckDB-Wasm host ready ($PIN)"
	exit 0
fi

need() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing required command: $1" >&2
		exit 1
	fi
}

need git
need emcc
need emcmake
need emmake
need python3
need ccache
need patch
need node
need npm
need make

if ! command -v yarn >/dev/null 2>&1 || [[ "$(yarn --version)" != 1.* ]]; then
	npm install --global yarn@1.22.22
fi
if ! command -v js-beautify >/dev/null 2>&1; then
	npm install --global js-beautify
fi

if [[ ! -d "$DEST/.git" ]]; then
	rm -rf "$DEST"
	git clone --filter=blob:none --no-checkout https://github.com/duckdb/duckdb-wasm.git "$DEST"
fi

# clone --no-checkout leaves the worktree empty even when HEAD is already the
# default-branch tip. That tip is this pin, so a HEAD check would skip checkout
# and make apply_patches would find no Makefile.
git -C "$DEST" fetch --depth 1 origin "$PIN"
git -C "$DEST" checkout --detach --force FETCH_HEAD
if [[ ! -f "$DEST/Makefile" ]]; then
	echo "duckdb-wasm checkout is missing Makefile" >&2
	exit 1
fi

bindings="$DEST/packages/duckdb-wasm/src/bindings"
if [[ "$(cat "$WASM_STAMP" 2>/dev/null || true)" != "$PIN" || ! -f "$bindings/duckdb-base.wasm" || ! -f "$bindings/duckdb-base.js" ]]; then
	if ! git -C "$DEST" submodule update --init --depth 1 --force; then
		git -C "$DEST" submodule update --init --force
	fi
	make -C "$DEST" apply_patches
	python3 - "$DEST/scripts/wasm_build_lib.sh" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
text = path.read_text()
old = "-j${CORES}"
new = "-j${CMAKE_BUILD_PARALLEL_LEVEL:-${CORES}}"
if old in text and new not in text:
    path.write_text(text.replace(old, new))
PY
	(
		cd "$DEST"
		export OVERRIDE_GIT_DESCRIBE="$DUCKDB_DESCRIBE"
		export USE_GENERATED_EXPORTED_LIST=no
		export DUCKDB_WASM_LOADABLE_EXTENSIONS=1
		./scripts/wasm_build_lib.sh relsize base
	)
	printf '%s\n' "$PIN" >"$WASM_STAMP"
fi

if [[ ! -f "$bindings/duckdb-threads.js" || ! -f "$bindings/duckdb-threads.wasm" ]]; then
	printf '%s\n' 'export default function duckdbThreadsNotBuilt(){throw new Error("duckdb-threads.wasm was not built");}' >"$bindings/duckdb-threads.js"
	: >"$bindings/duckdb-threads.wasm"
fi

(
	cd "$DEST"
	yarn install --frozen-lockfile
	# bundle.mjs is the part the browser loads. Skip tsc; the smoke test does not need declarations.
	cd packages/duckdb-wasm
	node bundle.mjs release
)

test -f "$DIST/duckdb-base.wasm"
test -f "$DIST/duckdb-browser.mjs"
test -f "$DIST/duckdb-browser-base.worker.js"
printf '%s\n' "$PIN" >"$STAMP"
echo "DuckDB-Wasm host dist: $DIST"
