# build_exe.ps1
# Builds a standalone ppl.exe using PyInstaller.
# The resulting exe requires no Python installation to run.
#
# Usage (run from the project root with the venv active):
#   .\build_exe.ps1

$ErrorActionPreference = "Stop"
$VenvPython = ".\.venv\Scripts\python.exe"

if (-not (Test-Path $VenvPython)) {
    Write-Error "Virtual environment not found at .venv\. Run 'python -m venv .venv' and 'pip install -e .' first."
    exit 1
}

Write-Host "Installing PyInstaller..." -ForegroundColor Cyan
& $VenvPython -m pip install pyinstaller -q

Write-Host "Building standalone ppl.exe..." -ForegroundColor Cyan
& $VenvPython -m PyInstaller `
    --onefile `
    --name ppl `
    --add-data "ast_nodes.py;." `
    --add-data "file_reader.py;." `
    --add-data "ppl_parser.py;." `
    --add-data "executor.py;." `
    main.py

if ($LASTEXITCODE -ne 0) {
    Write-Error "PyInstaller build failed."
    exit 1
}

Write-Host ""
Write-Host "Build complete!" -ForegroundColor Green
Write-Host "Executable: dist\ppl.exe" -ForegroundColor Green
Write-Host ""
Write-Host "To make it available system-wide, copy it somewhere on your PATH:"
Write-Host "  Copy-Item dist\ppl.exe C:\Users\$env:USERNAME\bin\ppl.exe"
Write-Host "  (create that folder and add it to PATH if needed)"
