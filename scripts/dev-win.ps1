[CmdletBinding()]
param(
	[Parameter(Position = 0)]
	[ValidateSet("bootstrap", "configure", "build", "test")]
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

function Resolve-WingetPath {
	$winget = Get-Command winget -ErrorAction SilentlyContinue
	if ($winget) {
		return $winget.Source
	}
	throw "winget is required for bootstrap. Install App Installer from the Microsoft Store and rerun '.\scripts\dev-win.ps1 bootstrap'."
}

function Invoke-WingetInstall {
	param(
		[string]$WingetPath,
		[string]$PackageId,
		[string[]]$ExtraArgs = @()
	)

	$args = @(
		"install",
		"--id", $PackageId,
		"--accept-package-agreements",
		"--accept-source-agreements"
	) + $ExtraArgs

	Write-Host "Installing $PackageId via winget..."
	& $WingetPath @args
	if ($LASTEXITCODE -ne 0) {
		throw "winget install failed for $PackageId (exit code $LASTEXITCODE)."
	}
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

function Resolve-UnitTestsPath {
	param([string]$BuildDir)

	$candidates = @(
		(Join-Path $BuildDir "pbi_scanner_unit_tests.exe"),
		(Join-Path $BuildDir "Release\pbi_scanner_unit_tests.exe")
	)

	foreach ($candidate in $candidates) {
		if (Test-Path -LiteralPath $candidate) {
			return $candidate
		}
	}

	throw "Could not find pbi_scanner_unit_tests.exe under build/release. Run .\scripts\dev-win.ps1 build first."
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

function Resolve-VcpkgRoot {
	param([string]$RepoRoot)
	if ($env:VCPKG_ROOT) {
		return $env:VCPKG_ROOT
	}
	return (Join-Path $RepoRoot "local\vcpkg")
}

function Resolve-VcpkgTriplet {
	if ($env:VCPKG_TARGET_TRIPLET) {
		return $env:VCPKG_TARGET_TRIPLET
	}
	return "x64-windows-static-release"
}

function Resolve-VcpkgToolchainFile {
	param([string]$VcpkgRoot)
	return (Join-Path $VcpkgRoot "scripts\buildsystems\vcpkg.cmake")
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

function Get-CMakeCacheValue {
	param(
		[string]$CachePath,
		[string]$Name
	)

	if (-not (Test-Path -LiteralPath $CachePath)) {
		return $null
	}

	$escaped_name = [regex]::Escape($Name)
	$line = Select-String -LiteralPath $CachePath -Pattern "^$escaped_name(?::[^=]*)?=(.*)$" | Select-Object -First 1
	if (-not $line) {
		return $null
	}
	return $line.Matches[0].Groups[1].Value
}

function Reset-CMakeConfigureState {
	param(
		[string]$BuildDir,
		[string]$Reason
	)

	Write-Host "Resetting CMake configure state: $Reason"
	$cache_path = Join-Path $BuildDir "CMakeCache.txt"
	$files_path = Join-Path $BuildDir "CMakeFiles"
	if (Test-Path -LiteralPath $cache_path) {
		Remove-Item -LiteralPath $cache_path -Force
	}
	if (Test-Path -LiteralPath $files_path) {
		Remove-Item -LiteralPath $files_path -Recurse -Force
	}
}

function Ensure-CompatibleCMakeCache {
	param(
		[string]$BuildDir,
		[string]$ExpectedSourceDir,
		[string]$ExpectedToolchainFile,
		[string]$ExpectedTargetTriplet,
		[string]$ExpectedHostTriplet
	)

	$cache_path = Join-Path $BuildDir "CMakeCache.txt"
	if (-not (Test-Path -LiteralPath $cache_path)) {
		return
	}

	$expected_source = Convert-ToCMakePath $ExpectedSourceDir
	$actual_source = Get-CMakeCacheValue -CachePath $cache_path -Name "CMAKE_HOME_DIRECTORY"
	if ($actual_source -and ((Convert-ToCMakePath $actual_source) -ne $expected_source)) {
		Reset-CMakeConfigureState -BuildDir $BuildDir -Reason "existing cache was generated from '$actual_source', expected '$expected_source'"
		return
	}

	$expected_toolchain = Convert-ToCMakePath $ExpectedToolchainFile
	$actual_toolchain = Get-CMakeCacheValue -CachePath $cache_path -Name "CMAKE_TOOLCHAIN_FILE"
	if ((-not $actual_toolchain) -or ((Convert-ToCMakePath $actual_toolchain) -ne $expected_toolchain)) {
		Reset-CMakeConfigureState -BuildDir $BuildDir -Reason "existing cache was not configured with vcpkg toolchain '$expected_toolchain'"
		return
	}

	$actual_target_triplet = Get-CMakeCacheValue -CachePath $cache_path -Name "VCPKG_TARGET_TRIPLET"
	if ($actual_target_triplet -ne $ExpectedTargetTriplet) {
		Reset-CMakeConfigureState -BuildDir $BuildDir -Reason "existing cache used VCPKG_TARGET_TRIPLET='$actual_target_triplet', expected '$ExpectedTargetTriplet'"
		return
	}

	$actual_host_triplet = Get-CMakeCacheValue -CachePath $cache_path -Name "VCPKG_HOST_TRIPLET"
	if ($actual_host_triplet -ne $ExpectedHostTriplet) {
		Reset-CMakeConfigureState -BuildDir $BuildDir -Reason "existing cache used VCPKG_HOST_TRIPLET='$actual_host_triplet', expected '$ExpectedHostTriplet'"
		return
	}
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

function Invoke-Bootstrap {
	param([string]$RepoRoot)

	$winget_path = Resolve-WingetPath

	$vsdevcmd_exists = $false
	try {
		$null = Resolve-VsDevCmd
		$vsdevcmd_exists = $true
	} catch {
		$vsdevcmd_exists = $false
	}
	if (-not $vsdevcmd_exists) {
		Invoke-WingetInstall -WingetPath $winget_path -PackageId "Microsoft.VisualStudio.2022.BuildTools" -ExtraArgs @(
			"--override",
			"--quiet --wait --add Microsoft.VisualStudio.Workload.VCTools --add Microsoft.VisualStudio.Component.VC.CMake.Project --add Microsoft.VisualStudio.Component.Windows11SDK.22621"
		)
	} else {
		Write-Host "Visual Studio Build Tools already detected."
	}

	$git_exists = $null -ne (Get-Command git -ErrorAction SilentlyContinue)
	if (-not $git_exists) {
		Invoke-WingetInstall -WingetPath $winget_path -PackageId "Git.Git" -ExtraArgs @("--silent")
	} else {
		Write-Host "Git already detected."
	}

	$cmake_exists = $null -ne (Get-Command cmake -ErrorAction SilentlyContinue)
	if (-not $cmake_exists) {
		Invoke-WingetInstall -WingetPath $winget_path -PackageId "Kitware.CMake" -ExtraArgs @("--silent")
	} else {
		Write-Host "CMake already detected."
	}

	$vcpkg_root = Resolve-VcpkgRoot -RepoRoot $RepoRoot
	if (-not (Test-Path -LiteralPath $vcpkg_root)) {
		New-Item -ItemType Directory -Path $vcpkg_root -Force | Out-Null
	}

	$bootstrap_bat = Join-Path $vcpkg_root "bootstrap-vcpkg.bat"
	$vcpkg_exe = Join-Path $vcpkg_root "vcpkg.exe"
	$expected_commit = "84bab45d415d22042bd0b9081aea57f362da3f35"

	if (-not (Test-Path -LiteralPath $bootstrap_bat)) {
		Write-Host "Cloning vcpkg into $vcpkg_root..."
		& git clone https://github.com/microsoft/vcpkg $vcpkg_root
		if ($LASTEXITCODE -ne 0) {
			throw "Failed to clone vcpkg."
		}
		& git -C $vcpkg_root checkout $expected_commit
		if ($LASTEXITCODE -ne 0) {
			throw "Failed to checkout vcpkg commit $expected_commit."
		}
	} else {
		Write-Host "vcpkg checkout already detected at $vcpkg_root."
	}

	if (-not (Test-Path -LiteralPath $vcpkg_exe)) {
		Write-Host "Bootstrapping vcpkg..."
		& $bootstrap_bat -disableMetrics
		if ($LASTEXITCODE -ne 0) {
			throw "vcpkg bootstrap failed."
		}
	} else {
		Write-Host "vcpkg.exe already present."
	}

	Write-Host ""
	Write-Host "Bootstrap complete:"
	Write-Host ("- VsDevCmd: {0}" -f (Resolve-VsDevCmd))
	Write-Host ("- CMake: {0}" -f (Resolve-CMakePath))
	& git --version
	& $vcpkg_exe version
	Write-Host ("- VCPKG_ROOT: {0}" -f $vcpkg_root)
}

$repo_root = Split-Path -Parent $PSScriptRoot
$build_dir = Join-Path $repo_root "build\release"
$build_dir_cmake = Convert-ToCMakePath $build_dir
$repo_root_cmake = Convert-ToCMakePath $repo_root

switch ($Command) {
	"bootstrap" {
		Invoke-Bootstrap -RepoRoot $repo_root
	}
	"configure" {
		$cmake_path = Resolve-CMakePath
		$vsdevcmd_path = Resolve-VsDevCmd
		$extension_config_path = Convert-ToCMakePath (Join-Path $repo_root "extension_config.cmake")
		$vcpkg_root = Resolve-VcpkgRoot -RepoRoot $repo_root
		$vcpkg_toolchain = Resolve-VcpkgToolchainFile -VcpkgRoot $vcpkg_root
		$vcpkg_triplet = Resolve-VcpkgTriplet
		$vcpkg_toolchain_cmake = Convert-ToCMakePath $vcpkg_toolchain
		if (-not (Test-Path -LiteralPath $vcpkg_toolchain)) {
			throw "Could not find vcpkg toolchain file at '$vcpkg_toolchain'. Run '.\scripts\dev-win.ps1 bootstrap' first or set VCPKG_ROOT."
		}
		Ensure-CompatibleCMakeCache `
			-BuildDir $build_dir `
			-ExpectedSourceDir (Join-Path $repo_root "duckdb") `
			-ExpectedToolchainFile $vcpkg_toolchain `
			-ExpectedTargetTriplet $vcpkg_triplet `
			-ExpectedHostTriplet $vcpkg_triplet

		$configure_parts = @(
			(Quote-ForCmd $cmake_path),
			"-S", (Convert-ToCMakePath (Join-Path $repo_root "duckdb")),
			"-B", $build_dir_cmake,
			"-DDUCKDB_EXTENSION_CONFIGS=$extension_config_path",
			"-DCMAKE_BUILD_TYPE=Release",
			"-DCMAKE_IGNORE_PATH=C:/msys64",
			"-DCMAKE_TOOLCHAIN_FILE=$vcpkg_toolchain_cmake",
			"-DVCPKG_BUILD=1",
			"-DVCPKG_MANIFEST_MODE=ON",
			"-DVCPKG_TARGET_TRIPLET=$vcpkg_triplet",
			"-DVCPKG_HOST_TRIPLET=$vcpkg_triplet",
			"-DVCPKG_MANIFEST_DIR=$repo_root_cmake",
			"-DOPENSSL_USE_STATIC_LIBS=TRUE",
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
		$cmake_path = Resolve-CMakePath
		$vsdevcmd_path = Resolve-VsDevCmd
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
		$vsdevcmd_path = Resolve-VsDevCmd
		$unittest_path = Resolve-UnittestPath -BuildDir $build_dir
		$unit_tests_path = Resolve-UnitTestsPath -BuildDir $build_dir
		$unittest_args = Resolve-UnittestArgs -InputArgs $ExtraArgs
		$test_parts = @(
			(Quote-ForCmd $unittest_path)
		)
		if ($unittest_args) {
			foreach ($test_arg in $unittest_args) {
				$test_parts += (Quote-ForCmd $test_arg)
			}
		}
		Invoke-InVsDevShell -VsDevCmdPath $vsdevcmd_path -CommandParts $test_parts
		Invoke-InVsDevShell -VsDevCmdPath $vsdevcmd_path -CommandParts @(
			(Quote-ForCmd $unit_tests_path)
		)
	}
	default {
		throw "Unknown command: $Command"
	}
}
