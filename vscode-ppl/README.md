# Pipeline DSL — VS Code Extension

[![GitHub](https://img.shields.io/badge/github-Tomhins%2Fpipeline__dsl-black?logo=github)](https://github.com/Tomhins/pipeline_dsl)
[![README](https://img.shields.io/badge/docs-README-blue)](https://github.com/Tomhins/pipeline_dsl/blob/main/README.md)

Adds language support for `.ppl` pipeline files inside VS Code:

- **Syntax highlighting** — keywords, operators, strings, numbers, and comments are all coloured
- **Run button** — a ▶ in the editor title bar runs the current pipeline instantly
- **Dedicated terminal** — output appears in a persistent "Pipeline DSL" terminal panel
- **Comment support** — `#` line comments work in the editor (toggle with `Ctrl+/`)

---

## Requirements

The `ppl` command must be on your PATH before using this extension.  
See the [main project README](https://github.com/Tomhins/pipeline_dsl/blob/main/README.md) for setup instructions.

---

## Usage

1. Open any `.ppl` file in VS Code
2. Click **▶ Run Pipeline** in the top-right of the editor  
   — or right-click and choose **Run Pipeline**  
   — or open the Command Palette (`Ctrl+Shift+P`) and type `Run Pipeline`

The pipeline runs in the integrated terminal and output is shown there.

---

## How to Update

### 1. Edit the extension

| File | What it controls |
|---|---|
| [extension.js](extension.js) | The Run Pipeline command and status bar logic |
| [syntaxes/ppl.tmLanguage.json](syntaxes/ppl.tmLanguage.json) | Syntax highlighting rules |
| [language-configuration.json](language-configuration.json) | Comment toggling, bracket pairs, auto-close quotes |
| [package.json](package.json) | Menus, commands, version number |

### 2. Bump the version (optional but recommended)

In [package.json](package.json), increment `"version"`:
```json
"version": "0.2.0"
```

### 3. Repackage

```powershell
cd vscode-ppl
"y" | vsce package --no-dependencies
```

This creates a new `vscode-ppl-<version>.vsix` file.

### 4. Reinstall

```powershell
code --install-extension vscode-ppl-<version>.vsix
```

Then reload VS Code (`Ctrl+Shift+P` → `Developer: Reload Window`).

---

## Project Structure

```
vscode-ppl/
├── extension.js                  # Activation, command, status bar
├── package.json                  # Extension manifest
├── language-configuration.json   # Editor behaviour for .ppl files
├── LICENSE
└── syntaxes/
    └── ppl.tmLanguage.json       # TextMate grammar (syntax highlighting)
```
