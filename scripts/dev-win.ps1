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

function Resolve-UnittestPath {
	param([string]$BuildDir)

	$candidates = @(
		(Join-Path $BuildDir "unittest.exe"),
		(Join-Path $BuildDir "test\Release\unittest.exe"),
		(Join-Path $BuildDir "test\unittest.exe")
	)

	foreach ($candidate in $candidates) {
		if (Test-Path -LiteralPath $candidate) {
			return $candidate
		}
	}

	throw "Could not find unittest.exe under build/release. Run .\scripts\dev-win.ps1 build first."
}

function Resolve-UnittestArgs {
	param([string[]]$InputArgs)

	if (-not $InputArgs -or $InputArgs.Count -eq 0) {
		return @("*pbi_scanner.test*")
	}

	$resolved = New-Object System.Collections.Generic.List[string]

	$NormalizeFilter = {
		param([string]$Filter)
		if ((-not $Filter.Contains("*")) -and $Filter.EndsWith(".test")) {
			return "*$Filter*"
		}
		return $Filter
	}

	for ($i = 0; $i -lt $InputArgs.Count; $i++) {
		$current = $InputArgs[$i]
		if ($current -eq "-R") {
			if ($i + 1 -ge $InputArgs.Count) {
				throw "Expected a test filter after -R."
			}
			$resolved.Add((& $NormalizeFilter $InputArgs[$i + 1]))
			$i += 1
			continue
		}
		$resolved.Add((& $NormalizeFilter $current))
	}

	return $resolved.ToArray()
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

function Resolve-DefaultCondaLibraryRoot {
	if ($env:CONDA_PREFIX) {
		$candidate = Join-Path $env:CONDA_PREFIX "Library"
		$openssl_hdr = Join-Path $candidate "include\openssl\opensslv.h"
		if (Test-Path -LiteralPath $openssl_hdr) {
			return $candidate
		}
	}

	$profile_root = $env:USERPROFILE
	if (-not $profile_root) {
		throw "Could not resolve OpenSSL/Zlib defaults: USERPROFILE is unset. Set OPENSSL_ROOT_DIR, ZLIB_INCLUDE_DIR, and ZLIB_LIBRARY."
	}

	foreach ($conda_name in @("miniconda3", "miniconda", "anaconda3", "mambaforge", "miniforge3")) {
		$candidate = Join-Path $profile_root "$conda_name\Library"
		$openssl_hdr = Join-Path $candidate "include\openssl\opensslv.h"
		if (Test-Path -LiteralPath $openssl_hdr) {
			return $candidate
		}
	}

	throw "Could not locate a Conda-style Library folder with OpenSSL headers. Install openssl/zlib in Conda or set OPENSSL_ROOT_DIR, ZLIB_INCLUDE_DIR, and ZLIB_LIBRARY."
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
$extension_config_path = Convert-ToCMakePath (Join-Path $repo_root "extension_config.cmake")

$default_conda_library = $null
if (-not ($env:OPENSSL_ROOT_DIR -and $env:ZLIB_INCLUDE_DIR -and $env:ZLIB_LIBRARY)) {
	$default_conda_library = Resolve-DefaultCondaLibraryRoot
}

$openssl_root_dir = Convert-ToCMakePath $(if ($env:OPENSSL_ROOT_DIR) { $env:OPENSSL_ROOT_DIR } else { $default_conda_library })
$zlib_include_dir = Convert-ToCMakePath $(if ($env:ZLIB_INCLUDE_DIR) { $env:ZLIB_INCLUDE_DIR } else { (Join-Path $default_conda_library "include") })
$zlib_library = Convert-ToCMakePath $(if ($env:ZLIB_LIBRARY) { $env:ZLIB_LIBRARY } else { (Join-Path $default_conda_library "lib\zlib.lib") })
$openssl_bin_dir = if ($env:OPENSSL_ROOT_DIR) { Join-Path $env:OPENSSL_ROOT_DIR "bin" } else { Join-Path $default_conda_library "bin" }

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
		$unittest_path = Resolve-UnittestPath -BuildDir $build_dir
		$unittest_args = Resolve-UnittestArgs -InputArgs $ExtraArgs
		$runtime_path = """PATH=$build_dir;$openssl_bin_dir;%PATH%"""
		$test_parts = @(
			"set", $runtime_path, "&&",
			(Quote-ForCmd $unittest_path)
		)
		if ($unittest_args) {
			foreach ($test_arg in $unittest_args) {
				$test_parts += (Quote-ForCmd $test_arg)
			}
		}
		Invoke-InVsDevShell -VsDevCmdPath $vsdevcmd_path -CommandParts $test_parts
	}
	default {
		throw "Unknown command: $Command"
	}
}
