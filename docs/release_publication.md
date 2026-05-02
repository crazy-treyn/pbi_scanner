# Release And Community Publication

## DuckDB Version Bumps

- Treat a DuckDB version bump as a coordinated change across submodules, CI, local helper tooling, docs, and validation evidence.
- For a stable/current bump, update `duckdb/` to the target DuckDB release tag and `extension-ci-tools/` to the matching release branch or commit.
- Current verified stable target: DuckDB `v1.5.2` with CI tools `v1.5.2`.
- Update `.github/workflows/MainDistributionPipeline.yml` stable build and code-quality jobs so the reusable workflow ref, `duckdb_version`, and `ci_tools_version` all match the new stable release.
- If `pyproject.toml` pins Python `duckdb` for local helper/benchmark tooling, bump it to the same release and regenerate `uv.lock` with `uv lock`.
- Update `README.md` when the supported/current DuckDB release changes or when community publication instructions change.
- Keep `duckdb-next-build` on DuckDB/CI tools `main` as an early warning job, not as the current release artifact.

Verify a version bump before treating the commit as publishable:

```bash
git -C duckdb describe --tags --exact-match
git -C duckdb rev-parse HEAD
git -C extension-ci-tools rev-parse HEAD
make format-check
make release
./build/release/test/unittest "test/sql/pbi_scanner.test"
make tidy-check
```

## Extension Version Bumps

- Extension metadata uses numeric semver in `extension_config.cmake` (`EXTENSION_VERSION 0.0.3`).
- GitHub tags/releases use a `v` prefix, for example `v0.0.3`.
- Keep the community descriptor `extension.version` aligned with `extension_config.cmake`.
- The community descriptor `repo.ref` must point at the validated release commit SHA from this repo.

Preferred local release sequence:

```bash
git status --short --branch
make format-check
make release
./build/release/test/unittest "test/sql/pbi_scanner.test"
make tidy-check
git add extension_config.cmake README.md .github/workflows/MainDistributionPipeline.yml pyproject.toml uv.lock
git commit -m "Prepare pbi_scanner X.Y.Z release"
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin HEAD --tags
gh release create vX.Y.Z --title "vX.Y.Z" --notes "<release notes>"
```

Only include files in `git add` that actually changed for the release.

## DuckDB Community Extension Publication

- Community publication is descriptor-only in `duckdb/community-extensions`; do not copy built binaries into this repo.
- Add or update `extensions/pbi_scanner/description.yml` in the community repository. The directory name must match `extension.name` exactly.
- If an existing local fork clone exists, prefer it, e.g. `../community-extensions`.
- In that clone, keep `origin` as the fork and `upstream` as `duckdb/community-extensions`.
- Open community PRs from a fork feature branch into `duckdb/community-extensions:main`; do not open PRs from fork `main`.
- Use a pushed, validated commit SHA for `repo.ref`; never use a dirty local state, unpushed branch, or unvalidated commit.
- Use `repo.ref_next` only for future-release compatibility when DuckDB `main` needs a different commit than latest stable.

Descriptor fields to keep aligned:

```yaml
extension:
  name: pbi_scanner
  description: DuckDB extension for querying Power BI Semantic Models with DAX.
  language: C++
  build: cmake
  license: MIT
  version: X.Y.Z
  excluded_platforms: "wasm_mvp;wasm_eh;wasm_threads;windows_amd64_mingw;osx_amd64"
repo:
  github: <owner>/pbi_scanner
  ref: <validated-release-commit-sha>
```

Preferred existing-fork descriptor update sequence:

```bash
cd ../community-extensions
git fetch upstream
git checkout main
git rebase upstream/main
git push origin main
git checkout -b pbi_scanner-vX.Y.Z
# edit extensions/pbi_scanner/description.yml: extension.version and repo.ref
git add extensions/pbi_scanner/description.yml
git commit -m "Update pbi_scanner to X.Y.Z"
git push -u origin HEAD
gh pr create --repo duckdb/community-extensions --base main --head <fork-owner>:pbi_scanner-vX.Y.Z --title "Update pbi_scanner to X.Y.Z" --body "<validation evidence>"
```

Community PR validation evidence should include stable CI, next CI if available, local build/test commands, release tag, release commit SHA, and known platform exclusions.
