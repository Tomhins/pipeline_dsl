# Pipeline DSL

[![README](https://img.shields.io/badge/docs-README-blue)](https://github.com/Tomhins/pipeline_dsl/blob/main/README.md)
[![GitHub](https://img.shields.io/badge/github-Tomhins%2Fpipeline__dsl-black?logo=github)](https://github.com/Tomhins/pipeline_dsl)

A lightweight Python-based DSL (Domain-Specific Language) for writing declarative data pipelines. Define your data transformations in plain `.ppl` files and run them from the command line — no Python required to use.

---

## Example

```
# pipelines/example.ppl

source "data/people.csv"
filter age > 18
select name, age, country
group by country
count
save "output/adults_by_country.csv"
```

```
ppl pipelines/example.ppl
```

```
Loaded 6 command(s) from 'pipelines/example.ppl'.
Parsed 6 AST node(s).

Pipeline completed successfully.
Output: 3 row(s) × 2 column(s).

Preview (first 10 rows):
country  count
Germany      5
    USA      5
 France      2
```

---

## Installation

**Requirements:** Python 3.9+

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS / Linux

# 2. Install the package (registers the ppl command)
pip install -e .
```

---

## Usage

```bash
ppl <pipeline_file.ppl>
```

Pipeline files should be placed in the `pipelines/` folder by convention, but any path works.

---

## Commands

### `source`
Load a CSV file into the pipeline.
```
source "data/people.csv"
```

### `filter`
Filter rows by a column condition.  
Supported operators: `>`, `<`, `>=`, `<=`, `==`, `!=`
```
filter age > 18
filter country == "Germany"
filter salary != 0
```

### `select`
Keep only the specified columns (comma-separated).
```
select name, age, country
```

### `group by`
Group rows by one or more columns. Must be followed by `count`.
```
group by country
group by country, age
```

### `count`
Count rows. When preceded by `group by`, counts per group.  
Without grouping, returns the total row count.
```
count
```

### `save`
Write the current data to a CSV file. Output directories are created automatically.
```
save "output/results.csv"
```

---

## Comments

Lines beginning with `#` are ignored, so you can annotate your pipelines freely.

```
# Load raw data
source "data/sales.csv"

# Only keep completed orders
filter status == "completed"
```

---

## Project Structure

```
pipeline_dsl/
├── main.py           # CLI entry point
├── file_reader.py    # Reads and cleans .ppl files
├── ppl_parser.py     # Converts lines into AST nodes
├── ast_nodes.py      # Node classes (one per command)
├── executor.py       # Runs the pipeline node by node
├── pyproject.toml    # Package config — registers the ppl command
├── data/             # Input CSV files
├── pipelines/        # .ppl pipeline definitions
└── output/           # Generated output (auto-created)
```

### Architecture

```
.ppl file
   │
   ▼
file_reader.py   →   cleaned list of lines
   │
   ▼
ppl_parser.py    →   list of ASTNode objects
   │
   ▼
executor.py      →   runs each node against PipelineContext
   │
   ▼
output CSV
```

Each command maps to a node class in [ast_nodes.py](ast_nodes.py). Adding a new command means adding one class and one parser entry — nothing else changes.

---

## Error Handling

The DSL provides clear error messages for common mistakes:

| Problem | Example message |
|---|---|
| File not found | `[SourceNode] Source file not found: 'data/missing.csv'` |
| Unknown column | `[FilterNode] column 'agee' not found. Available: ['name', 'age', ...]` |
| Invalid command | `Line 3: unknown command 'sortby'. Supported commands: count, filter, ...` |
| Missing `.ppl` extension | `Expected a .ppl file, got: 'data.csv'` |
| Bad filter syntax | `Line 2: could not parse 'filter' condition 'age'. Expected: filter <column> <op> <value>` |

---

## VS Code Extension

The `vscode-ppl/` folder contains a VS Code extension that adds syntax highlighting and a one-click run button for `.ppl` files.

**Install:**
```powershell
cd vscode-ppl
"y" | vsce package --no-dependencies
code --install-extension vscode-ppl-0.5.0.vsix
```
Then reload VS Code (`Ctrl+Shift+P` → `Developer: Reload Window`).

See [vscode-ppl/README.md](vscode-ppl/README.md) for full details and update instructions.

---

## Updating the Extension

1. Edit files in `vscode-ppl/` (see table in [vscode-ppl/README.md](vscode-ppl/README.md))
2. Bump `"version"` in [vscode-ppl/package.json](vscode-ppl/package.json)
3. Repackage and reinstall:

```powershell
cd vscode-ppl
"y" | vsce package --no-dependencies
code --install-extension vscode-ppl-<version>.vsix
```

---

## Building a Standalone Exe

To produce a `ppl.exe` that works on any Windows machine with no Python installed:

```powershell
.\build_exe.ps1
# Output: dist\ppl.exe
```

Copy `dist\ppl.exe` to any folder on your PATH and the `ppl` command works everywhere.

---

## License

Copyright (c) 2026 Tom Kremser.

Free to use and modify for personal or commercial projects — you can use this
tool to build pipelines and data workflows for yourself or your company.
Commercial use requires crediting the author (Tom Kremser) in your product
documentation or about page.
You may **not** sell or redistribute the Software itself as a product or service.
See [LICENSE](LICENSE) for full details.
