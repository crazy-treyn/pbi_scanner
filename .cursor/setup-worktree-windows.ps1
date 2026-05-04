$ErrorActionPreference = 'Stop'

if ($env:ROOT_WORKTREE_PATH -and (Test-Path (Join-Path $env:ROOT_WORKTREE_PATH '.env')) -and -not (Test-Path '.env')) {
	Copy-Item (Join-Path $env:ROOT_WORKTREE_PATH '.env') '.env'
}

git submodule sync --recursive
git submodule update --init --recursive

if (-not (Test-Path 'extension-ci-tools\makefiles\duckdb_extension.Makefile')) {
	Write-Error 'worktree bootstrap failed: extension-ci-tools submodule is missing expected files. Run: git submodule sync --recursive; git submodule update --init --recursive'
}
