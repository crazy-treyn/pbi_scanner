PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=pbi_scanner
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
export PATH := ${PROJ_DIR}.venv/bin:${PATH}

# DuckDB v1.5.3 fmt uses stdext::checked_array_iterator, removed in VS 2026.
ifeq ($(DUCKDB_PLATFORM),windows_amd64)
.PHONY: patch-fmt-vs2026
patch-fmt-vs2026:
	python $(PROJ_DIR)scripts/patch_fmt_vs2026.py $(FMT_FORMAT_H)
else ifeq ($(DUCKDB_PLATFORM),windows_arm64)
.PHONY: patch-fmt-vs2026
patch-fmt-vs2026:
	python $(PROJ_DIR)scripts/patch_fmt_vs2026.py $(FMT_FORMAT_H)
endif

include extension-ci-tools/makefiles/duckdb_extension.Makefile

FMT_FORMAT_H := $(DUCKDB_SRCDIR)third_party/fmt/include/fmt/format.h

ifeq ($(DUCKDB_PLATFORM),windows_amd64)
release: patch-fmt-vs2026
debug: patch-fmt-vs2026
reldebug: patch-fmt-vs2026
relassert: patch-fmt-vs2026
else ifeq ($(DUCKDB_PLATFORM),windows_arm64)
release: patch-fmt-vs2026
debug: patch-fmt-vs2026
reldebug: patch-fmt-vs2026
relassert: patch-fmt-vs2026
endif

.PHONY: pbi_scanner_unit_tests test-pbi-offline test-pbi-wasm

# BUILD_DIR selects release/debug/reldebug output (default: build/release).
pbi_scanner_unit_tests:
	./$(or $(BUILD_DIR),build/release)/pbi_scanner_unit_tests

# CI runs `make test_release`, which resolves to test_release_internal from
# extension-ci-tools/makefiles/duckdb_extension.Makefile. We shadow those
# *_internal targets here to append Catch tests after the DuckDB unittest sweep.
# Keep the unittest invocation in sync with upstream when bumping extension-ci-tools.
define RUN_PBI_EXTENSION_UNIT_TESTS
$(MAKE) pbi_scanner_unit_tests BUILD_DIR=build/$(1)
endef

test_release_internal:
	./build/release/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"
	$(call RUN_PBI_EXTENSION_UNIT_TESTS,release)

test_debug_internal:
	./build/debug/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"
	$(call RUN_PBI_EXTENSION_UNIT_TESTS,debug)

test_reldebug_internal:
	./build/reldebug/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"
	$(call RUN_PBI_EXTENSION_UNIT_TESTS,reldebug)

test-pbi-offline: pbi_scanner_unit_tests
	./build/release/test/unittest "test/sql/pbi_scanner.test"

test-pbi-wasm:
	uv run test/wasm/run_pbi_wasm_smoke.py --platform wasm_eh
	uv run test/wasm/run_pbi_wasm_sqllogictest.py --platform wasm_eh

.PHONY: ensure-uv
ensure-uv:
	@if command -v uv >/dev/null 2>&1; then \
		UV_CMD=uv; \
	elif python3 -m uv --version >/dev/null 2>&1; then \
		UV_CMD="python3 -m uv"; \
	else \
		python3 -m pip install --user --break-system-packages uv >/dev/null 2>&1 || \
			python3 -m pip install --user uv >/dev/null; \
		UV_CMD="python3 -m uv"; \
	fi; \
	$$UV_CMD --version >/dev/null

.PHONY: ensure-format-tools
ensure-format-tools: ensure-uv
	@if command -v uv >/dev/null 2>&1; then \
		UV_CMD=uv; \
	elif python3 -m uv --version >/dev/null 2>&1; then \
		UV_CMD="python3 -m uv"; \
	else \
		python3 -m pip install --user --break-system-packages uv >/dev/null 2>&1 || \
			python3 -m pip install --user uv >/dev/null; \
		UV_CMD="python3 -m uv"; \
	fi; \
	$$UV_CMD run --group format black --version >/dev/null

format-check: ensure-format-tools
	python3 duckdb/scripts/format.py --all --check --directories src test wasm_sql

format-fix: ensure-format-tools
	python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test wasm_sql

format: format-fix

.PHONY: ensure-tidy-tools
ensure-tidy-tools: ensure-uv
	@if command -v uv >/dev/null 2>&1; then \
		UV_CMD=uv; \
	elif python3 -m uv --version >/dev/null 2>&1; then \
		UV_CMD="python3 -m uv"; \
	else \
		python3 -m pip install --user --break-system-packages uv >/dev/null 2>&1 || \
			python3 -m pip install --user uv >/dev/null; \
		UV_CMD="python3 -m uv"; \
	fi; \
	$$UV_CMD run --group tidy clang-tidy --version >/dev/null

tidy-check: ensure-tidy-tools
