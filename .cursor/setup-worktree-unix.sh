#!/usr/bin/env bash
set -euo pipefail

# Cursor injects ROOT_WORKTREE_PATH (main checkout) when running worktree setup.
if [[ -n "${ROOT_WORKTREE_PATH:-}" ]] && [[ -f "${ROOT_WORKTREE_PATH}/.env" ]] && [[ ! -f .env ]]; then
	cp "${ROOT_WORKTREE_PATH}/.env" .env
fi

git submodule sync --recursive
git submodule update --init --recursive

if [[ ! -f extension-ci-tools/makefiles/duckdb_extension.Makefile ]]; then
	echo "worktree bootstrap failed: extension-ci-tools submodule is missing expected files" >&2
	echo "Run: git submodule sync --recursive && git submodule update --init --recursive" >&2
	exit 1
fi
