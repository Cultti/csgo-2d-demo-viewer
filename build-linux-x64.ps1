[CmdletBinding()]
param(
    [string]$OutDir = (Join-Path $PSScriptRoot 'deploy/linux-x64')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-Checked {
    param([string]$Command, [string[]]$Arguments)
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE"
    }
}

$go = (Get-Command go -ErrorAction Stop).Source
$npm = (Get-Command npm.cmd -ErrorAction Stop).Source
$deployRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'deploy'))
if (![IO.Path]::IsPathRooted($OutDir)) { $OutDir = Join-Path $PSScriptRoot $OutDir }
$OutDir = [IO.Path]::GetFullPath($OutDir).TrimEnd('\', '/')
# Restrict cleanup to a release folder inside this repository's deploy directory.
if (!$OutDir.StartsWith($deployRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'OutDir must be a folder inside this repository''s deploy directory.'
}
$tarballPath = "$OutDir.tar.gz"
$ancestor = $OutDir
while ($ancestor.StartsWith($deployRoot, [StringComparison]::OrdinalIgnoreCase)) {
    if (Test-Path -LiteralPath $ancestor) {
        if ((Get-Item -LiteralPath $ancestor).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Output path contains a junction or symlink; choose another output folder.'
        }
    }
    $ancestor = [IO.Path]::GetDirectoryName($ancestor)
}
$savedEnv = @{}
foreach ($key in @('GOOS', 'GOARCH', 'CGO_ENABLED')) {
    $savedEnv[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
}

Push-Location $PSScriptRoot
try {
    Write-Host '[1/6] Cleaning output directory...'
    if (Test-Path -LiteralPath $OutDir) {
        # Reject junctions/symlinks before recursively removing output.
        $item = Get-Item -LiteralPath $OutDir
        $links = @(Get-ChildItem -LiteralPath $OutDir -Recurse -Force | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint })
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -or $links.Count) {
            throw 'Output contains a junction or symlink; choose another output folder.'
        }
        Remove-Item -LiteralPath $OutDir -Recurse -Force
    }
    if (Test-Path -LiteralPath $tarballPath) {
        if ((Get-Item -LiteralPath $tarballPath).PSIsContainer) { throw 'Archive path is a directory.' }
        Remove-Item -LiteralPath $tarballPath -Force
    }
    New-Item -ItemType Directory -Path (Join-Path $OutDir 'bin'), (Join-Path $OutDir 'web') -Force | Out-Null

    Write-Host '[2/6] Building parser WASM assets...'
    $env:GOOS = 'js'
    $env:GOARCH = 'wasm'
    $env:CGO_ENABLED = '0'
    $wasmDir = Join-Path $PSScriptRoot 'web/public/wasm'
    New-Item -ItemType Directory -Path $wasmDir -Force | Out-Null
    Push-Location parser
    try { Invoke-Checked $go @('build', '-ldflags=-s -w', '-o', (Join-Path $wasmDir 'csdemoparser.wasm'), './wasm.go') }
    finally { Pop-Location }
    $goRoot = & $go env GOROOT
    if ($LASTEXITCODE -ne 0) { throw 'Cannot determine GOROOT' }
    Copy-Item -LiteralPath (Join-Path $goRoot 'lib/wasm/wasm_exec.js') -Destination $wasmDir

    Write-Host '[3/6] Installing web dependencies...'
    Invoke-Checked $npm @('--prefix', 'web', 'ci')
    Write-Host '[4/6] Building web frontend...'
    Invoke-Checked $npm @('--prefix', 'web', 'run', 'build')

    Write-Host '[5/6] Building Linux x64 server binary...'
    $env:GOOS = 'linux'
    $env:GOARCH = 'amd64'
    Push-Location server
    try { Invoke-Checked $go @('build', '-ldflags=-s -w', '-o', (Join-Path $OutDir 'bin/csgo-demo-server'), '.') }
    finally { Pop-Location }

    Write-Host '[6/6] Packaging runtime files...'
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'web/dist') -Destination (Join-Path $OutDir 'web/dist') -Recurse
    $runScript = @'
#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PORT="${PORT:-8080}"
export HOST="${HOST:-127.0.0.1}"
export WEB_DIST_DIR="${WEB_DIST_DIR:-$APP_DIR/web/dist}"

# Optional webhook/admin settings can be provided by env:
# WEBHOOK_HEADER_NAME, WEBHOOK_HEADER_VALUE, ADMIN_REPROCESS_TOKEN, REPLAYS_DIR, ALLOWED_ORIGINS

exec "$APP_DIR/bin/csgo-demo-server" -port "$PORT" -host "$HOST" -web-dir "$WEB_DIST_DIR"
'@
    [IO.File]::WriteAllText((Join-Path $OutDir 'run.sh'), ($runScript.Replace("`r`n", "`n") + "`n"), [Text.UTF8Encoding]::new($false))
    # Run the archive helper on the Windows host, rather than the cross-build target.
    foreach ($key in $savedEnv.Keys) { [Environment]::SetEnvironmentVariable($key, $null, 'Process') }
    Invoke-Checked $go @('run', (Join-Path $PSScriptRoot 'scripts/package-linux.go'), $OutDir, $tarballPath)
    Write-Host "Build complete.`nDeploy folder: $OutDir`nTarball: $tarballPath"
    Write-Host "Run on server: cd $([IO.Path]::GetFileName($OutDir)) && ./run.sh"
}
finally {
    foreach ($key in $savedEnv.Keys) { [Environment]::SetEnvironmentVariable($key, $savedEnv[$key], 'Process') }
    Pop-Location
}
