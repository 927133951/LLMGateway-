# Auto-download a portable Node.js runtime into .runtime\node
# No system changes, no admin rights needed.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$dest = Join-Path $root ".runtime"
$nodeDir = Join-Path $dest "node"

if (Test-Path (Join-Path $nodeDir "node.exe")) {
    Write-Host "[ok] portable node already present"
    exit 0
}

# pick latest LTS from official dist index
Write-Host "[1/4] querying nodejs.org for latest LTS..."
$idx = Invoke-RestMethod -Uri "https://nodejs.org/dist/index.json" -UseBasicParsing
$lts = $idx | Where-Object { $_.lts } | Select-Object -First 1
if (-not $lts) { throw "could not resolve LTS version" }
$ver = $lts.version
Write-Host "[2/4] found $ver (LTS)"

$arch = "x64"
if ($env:PROCESSOR_ARCHITECTURE -match "ARM64") { $arch = "arm64" }
$zipName = "node-$ver-win-$arch.zip"
$url = "https://nodejs.org/dist/$ver/$zipName"
$zipPath = Join-Path $env:TEMP $zipName

Write-Host "[3/4] downloading $zipName (~30MB)..."
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing

Write-Host "[4/4] extracting..."
if (Test-Path $dest) { Remove-Item $dest -Recurse -Force }
New-Item -ItemType Directory -Path $dest | Out-Null
Expand-Archive -Path $zipPath -DestinationPath $dest -Force
$extracted = Get-ChildItem $dest -Directory | Where-Object { $_.Name -like "node-*" } | Select-Object -First 1
if (-not $extracted) { throw "unexpected zip layout" }
Move-Item $extracted.FullName $nodeDir
Remove-Item $zipPath -Force

if (Test-Path (Join-Path $nodeDir "node.exe")) {
    Write-Host "[done] portable node ready: .runtime\node\node.exe"
} else {
    throw "extract failed"
}
