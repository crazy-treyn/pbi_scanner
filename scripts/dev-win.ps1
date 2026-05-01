[CmdletBinding()]
param(
	[Parameter(Position = 0)]
	[ValidateSet("configure", "build", "test")]
	[string]$Command = "build",

	[Parameter(ValueFromRemainingArguments = $true)]
	[string[]]$ExtraArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-VsDevCmd {
	$candidates = @(
		"${env:ProgramFiles(x86)}\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat",
		"${env:ProgramFiles(x86)}\Microsoft Visual Studio\2019\BuildTools\Common7\Tools\VsDevCmd.bat"
	)

	foreach ($candidate in $candidates) {
		if (Test-Path -LiteralPath $candidate) {
			return $candidate
		}
	}
	throw "Could not find VsDevCmd.bat. Install Visual Studio Build Tools 2022 or 2019 (Desktop development with C++)."
}

function Resolve-CMakePath {
	$on_path = Get-Command cmake -ErrorAction SilentlyContinue
	if ($on_path) {
		return $on_path.Source
	}

	$candidates = @(
		"${env:ProgramFiles(x86)}\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
		"${env:ProgramFiles(x86)}\Microsoft Visual Studio\2019\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
	)

	foreach ($candidate in $candidates) {
		if (Test-Path -LiteralPath $candidate) {
			return $candidate
		}
	}
	throw "Could not find cmake.exe on PATH or in known Visual Studio Build Tools locations."
}

function Resolve-CTestPath {
	param([string]$CMakePath)

	$on_path = Get-Command ctest -ErrorAction SilentlyContinue
	if ($on_path) {
		return $on_path.Source
	}

	$cmake_bin_dir = Split-Path -Parent $CMakePath
	$candidate = Join-Path $cmake_bin_dir "ctest.exe"
	if (Test-Path -LiteralPath $candidate) {
		return $candidate
	}

	throw "Could not find ctest.exe on PATH or next to resolved cmake.exe."
}

function Quote-ForCmd {
	param([string]$Value)
	return '"' + ($Value -replace '"', '\"') + '"'
}

function Convert-ToCMakePath {
	param([string]$Value)
	if (-not $Value) {
		return $Value
	}
	return $Value -replace "\\", "/"
}

function Invoke-InVsDevShell {
	param(
		[string]$VsDevCmdPath,
		[string[]]$CommandParts
	)

	$joined_command = ($CommandParts -join " ")
	$cmd_payload = ('call {0} -arch=x64 -host_arch=x64 && {1}' -f (Quote-ForCmd $VsDevCmdPath), $joined_command)
	$cmd_args = @("/d", "/s", "/c", $cmd_payload)
	& cmd.exe @cmd_args
	if ($LASTEXITCODE -ne 0) {
		throw "Command failed with exit code ${LASTEXITCODE}: $joined_command"
	}
}

$repo_root = Split-Path -Parent $PSScriptRoot
$build_dir = Join-Path $repo_root "build\release"
$build_dir_cmake = Convert-ToCMakePath $build_dir
$cmake_path = Resolve-CMakePath
$vsdevcmd_path = Resolve-VsDevCmd
$ctest_path = Resolve-CTestPath -CMakePath $cmake_path
$extension_config_path = Convert-ToCMakePath (Join-Path $repo_root "extension_config.cmake")

$openssl_root_dir = Convert-ToCMakePath $(if ($env:OPENSSL_ROOT_DIR) { $env:OPENSSL_ROOT_DIR } else { "C:/Users/TreyUdy/miniconda3/Library" })
$zlib_include_dir = Convert-ToCMakePath $(if ($env:ZLIB_INCLUDE_DIR) { $env:ZLIB_INCLUDE_DIR } else { "C:/Users/TreyUdy/miniconda3/Library/include" })
$zlib_library = Convert-ToCMakePath $(if ($env:ZLIB_LIBRARY) { $env:ZLIB_LIBRARY } else { "C:/Users/TreyUdy/miniconda3/Library/lib/zlib.lib" })

switch ($Command) {
	"configure" {
		$configure_parts = @(
			(Quote-ForCmd $cmake_path),
			"-S", (Convert-ToCMakePath (Join-Path $repo_root "duckdb")),
			"-B", $build_dir_cmake,
			"-DDUCKDB_EXTENSION_CONFIGS=$extension_config_path",
			"-DCMAKE_BUILD_TYPE=Release",
			"-DCMAKE_IGNORE_PATH=C:/msys64",
			"-DOPENSSL_ROOT_DIR=$openssl_root_dir",
			"-DZLIB_INCLUDE_DIR=$zlib_include_dir",
			"-DZLIB_LIBRARY=$zlib_library",
			"-DCMAKE_RUNTIME_OUTPUT_DIRECTORY=$build_dir_cmake",
			"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=$build_dir_cmake",
			"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY=$build_dir_cmake",
			"-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE=$build_dir_cmake",
			"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE=$build_dir_cmake",
			"-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE=$build_dir_cmake"
		)
		if ($ExtraArgs) {
			$configure_parts += $ExtraArgs
		}
		Invoke-InVsDevShell -VsDevCmdPath $vsdevcmd_path -CommandParts $configure_parts
	}
	"build" {
		& $PSCommandPath configure @ExtraArgs
		if ($LASTEXITCODE -ne 0) {
			throw "Configure step failed."
		}
		$build_parts = @(
			(Quote-ForCmd $cmake_path),
			"--build", (Quote-ForCmd $build_dir),
			"--config", "Release",
			"--", "/m:1"
		)
		$max_attempts = if ($env:DEV_WIN_BUILD_RETRY_COUNT) { [int]$env:DEV_WIN_BUILD_RETRY_COUNT } else { 3 }
		$attempt = 0
		while ($attempt -lt $max_attempts) {
			$attempt += 1
			try {
				Invoke-InVsDevShell -VsDevCmdPath $vsdevcmd_path -CommandParts $build_parts
				break
			} catch {
				if ($attempt -ge $max_attempts) {
					throw
				}
				Write-Warning "Build attempt $attempt failed; retrying after 3 seconds."
				Start-Sleep -Seconds 3
			}
		}
	}
	"test" {
		$ctest_parts = @(
			(Quote-ForCmd $ctest_path),
			"--test-dir", (Quote-ForCmd $build_dir),
			"--build-config", "Release",
			"--output-on-failure"
		)
		if ($ExtraArgs) {
			$ctest_parts += $ExtraArgs
		}
		Invoke-InVsDevShell -VsDevCmdPath $vsdevcmd_path -CommandParts $ctest_parts
	}
	default {
		throw "Unknown command: $Command"
	}
}
