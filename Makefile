PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=pbi_scanner
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
export PATH := ${PROJ_DIR}.venv/bin:${PATH}

include extension-ci-tools/makefiles/duckdb_extension.Makefile

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

format-check format format-fix: ensure-format-tools

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
