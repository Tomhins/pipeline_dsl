# build_exe.ps1
# Builds a standalone Windows executable for the pipeline DSL runner using PyInstaller.
# Output: dist/ppl.exe

param(
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

Write-Host "=== Pipeline DSL - Build Executable ===" -ForegroundColor Cyan

# Ensure PyInstaller is available
python -c "import PyInstaller" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing PyInstaller..." -ForegroundColor Yellow
    pip install pyinstaller
}

# Optionally clean previous build artifacts
if ($Clean -or (Test-Path "dist\ppl.exe")) {
    Write-Host "Cleaning previous build artifacts..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue build, dist, ppl.spec
}

Write-Host "Building executable..." -ForegroundColor Green

pyinstaller `
    --onefile `
    --name ppl `
    --add-data "ast_nodes.py;." `
    --add-data "executor.py;." `
    --add-data "file_reader.py;." `
    --add-data "ppl_parser.py;." `
    --hidden-import polars `
    --hidden-import pyarrow `
    --hidden-import pyarrow.lib `
    --hidden-import pyarrow.vendored.version `
    --collect-all polars `
    main.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed." -ForegroundColor Red
    exit $LASTEXITCODE
}

$exePath = Resolve-Path "dist\ppl.exe"
$sizeMB  = [math]::Round((Get-Item $exePath).Length / 1MB, 1)

Write-Host ""
Write-Host "Build succeeded!" -ForegroundColor Green
Write-Host "  Executable : $exePath"
Write-Host "  Size       : $sizeMB MB"
Write-Host ""
Write-Host "Usage: dist\ppl.exe <pipeline.ppl>" -ForegroundColor Cyan
