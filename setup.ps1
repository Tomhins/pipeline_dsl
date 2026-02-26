# setup.ps1
# Sets up the development environment for pipeline_dsl.
#
# What it does:
#   1. Creates a Python virtual environment at .venv (if not already present)
#   2. Installs the package and its dependencies in editable mode
#   3. Optionally installs the VS Code extension locally
#
# Parameters
#   -InstallExtension   Install the vscode-ppl extension into VS Code
#   -Force              Re-create the virtual environment from scratch
#
# Usage (run from the project root):
#   .\setup.ps1                    # set up Python environment
#   .\setup.ps1 -InstallExtension  # also install the VS Code extension
#   .\setup.ps1 -Force             # wipe and recreate the venv first
param(
    [switch]$InstallExtension,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$VenvDir  = ".\.venv"
$VenvPython = "$VenvDir\Scripts\python.exe"

# ── 1. Locate Python ─────────────────────────────────────────────────────────
$Python = Get-Command python -ErrorAction SilentlyContinue
if (-not $Python) {
    Write-Error "Python not found. Install Python 3.9+ and ensure it is on your PATH."
    exit 1
}

$PythonVersion = & python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
Write-Host "Found Python $PythonVersion" -ForegroundColor Cyan

# ── 2. Create virtual environment ────────────────────────────────────────────
if ($Force -and (Test-Path $VenvDir)) {
    Write-Host "Removing existing virtual environment..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $VenvDir
}

if (-not (Test-Path $VenvPython)) {
    Write-Host "Creating virtual environment at $VenvDir ..." -ForegroundColor Cyan
    python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create virtual environment."
        exit 1
    }
    Write-Host "Virtual environment created." -ForegroundColor Green
} else {
    Write-Host "Virtual environment already exists — skipping creation." -ForegroundColor Green
}

# ── 3. Install dependencies ───────────────────────────────────────────────────
Write-Host ""
Write-Host "Installing pipeline_dsl (editable) with dev dependencies..." -ForegroundColor Cyan
& $VenvPython -m pip install --upgrade pip -q
& $VenvPython -m pip install -e ".[dev]"
if ($LASTEXITCODE -ne 0) {
    Write-Error "pip install failed."
    exit 1
}

Write-Host ""
Write-Host "Dependencies installed successfully." -ForegroundColor Green

# ── 4. VS Code extension (optional) ─────────────────────────────────────────
if ($InstallExtension) {
    $Code = Get-Command code -ErrorAction SilentlyContinue
    if (-not $Code) {
        Write-Warning "VS Code CLI ('code') not found — skipping extension install."
        Write-Warning "Install VS Code and ensure 'code' is on your PATH, then run:"
        Write-Warning "  code --install-extension vscode-ppl\ppl.vsix"
    } else {
        $ExtDir = Join-Path $PSScriptRoot "vscode-ppl"

        # Try to use a pre-built .vsix if one exists, otherwise build it
        $Vsix = Get-ChildItem "$ExtDir\*.vsix" -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime | Select-Object -Last 1

        if (-not $Vsix) {
            Write-Host "No .vsix found — building extension..." -ForegroundColor Cyan
            $Npx = Get-Command npx -ErrorAction SilentlyContinue
            if (-not $Npx) {
                Write-Warning "npx not found. Install Node.js to build the extension, or install it manually from vscode-ppl/."
            } else {
                Push-Location $ExtDir
                try {
                    npx @vscode/vsce package --no-git-tag-version 2>&1 | Write-Host
                    if ($LASTEXITCODE -ne 0) { throw "vsce package failed." }
                    $Vsix = Get-ChildItem "$ExtDir\*.vsix" | Sort-Object LastWriteTime | Select-Object -Last 1
                } finally {
                    Pop-Location
                }
            }
        }

        if ($Vsix) {
            Write-Host "Installing VS Code extension from $($Vsix.Name)..." -ForegroundColor Cyan
            code --install-extension $Vsix.FullName
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Extension install failed — try manually: code --install-extension `"$($Vsix.FullName)`""
            } else {
                Write-Host "VS Code extension installed." -ForegroundColor Green
            }
        }
    }
}

# ── 5. Done ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Activate the virtual environment with:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "Then run a pipeline with:"
Write-Host "  ppl pipelines\example.ppl"
