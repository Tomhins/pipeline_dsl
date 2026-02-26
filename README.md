# Pipeline DSL

[![README](https://img.shields.io/badge/docs-README-blue)](https://github.com/Tomhins/pipeline_dsl/blob/main/README.md)
[![GitHub](https://img.shields.io/badge/github-Tomhins%2Fpipeline__dsl-black?logo=github)](https://github.com/Tomhins/pipeline_dsl)

A lightweight Python-based DSL (Domain-Specific Language) for writing declarative data pipelines. Define your data transformations in plain `.ppl` files and run them from the command line — no Python required to use.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Commands](#commands)
  - [Data Loading](#data-loading) — `source`, `foreach`, `include`
  - [Filtering](#filtering) — `filter`, `where`, `count if`
  - [Column Selection](#column-selection) — `select`, `drop`, `limit`, `distinct`, `sample`
  - [Sorting](#sorting) — `sort by`
  - [Transformation](#transformation) — `rename`, `add`, `cast`, `trim`, `uppercase`, `lowercase`, `replace`, `pivot`
  - [Aggregation](#aggregation) — `count`, `sum`, `avg`, `min`, `max`, `agg`
  - [Joining](#joining) — `join`, `merge`
  - [Output](#output) — `save`, `print`
  - [Inspection](#inspection) — `schema`, `inspect`, `head`
  - [Data Quality](#data-quality) — `assert`, `fill`
  - [Variables & Environment](#variables--environment) — `set`, `env`
  - [Error Recovery](#error-recovery) — `try` / `on_error`
  - [Timing & Logging](#timing--logging) — `timer`, `log`
  - [Comments](#comments)
- [Advanced Features](#advanced-features)
  - [Variable Substitution](#variable-substitution)
  - [Chunked Streaming](#chunked-streaming)
  - [Sandbox Mode](#sandbox-mode)
- [Architecture](#architecture)
- [Error Reference](#error-reference)
- [VS Code Extension](#vs-code-extension)
- [Building a Standalone Exe](#building-a-standalone-exe)
- [License](#license)

---

## Quick Start

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

Pipeline files can live anywhere. All relative paths *inside* a `.ppl` file are resolved relative to that file's directory — so `source "data/people.csv"` always refers to the `data/` folder next to the pipeline file regardless of where you invoke `ppl` from.

---

## Commands

### Data Loading

#### `source`
Load a CSV, JSON, or Parquet file into the pipeline.
```
source "data/people.csv"
source "data/snapshot.parquet"
source "data/records.json"
```

Supports `$variable` references in the file path:
```
set path = data/people.csv
source "$path"
```

**Chunked streaming** — for large files, read in fixed-size chunks to reduce peak memory usage. Row-safe operations (`filter`, `select`, `cast`, `rename`, etc.) are applied per chunk before the results are concatenated:
```
source "data/big.csv" chunk 100000
```

#### `foreach`
Load and concatenate all CSV files matching a glob pattern. The result is the row-wise union of all matched files.
```
foreach "data/monthly/*.csv"
```

#### `include`
Include and execute another `.ppl` file, sharing the current context. Useful for reusable pipeline fragments such as a common data-cleaning step.
```
include "pipelines/shared/clean.ppl"
```

---

### Filtering

#### `filter`
Filter rows by a column condition.
Supported operators: `>`, `<`, `>=`, `<=`, `==`, `!=`
```
filter age > 18
filter country == "Germany"
filter salary != 0
```

Supports `$variable` references in the value:
```
set threshold = 50000
filter salary > $threshold
```

Compound conditions using `and` / `or` on a single line:
```
filter age >= 18 and country == "Germany"
filter status == "active" or status == "pending"
```

#### `where`
Alias for `filter`. Useful for SQL-style readability.
```
where age > 18
where country == "Germany"
```

#### `count if`
Print the count of rows matching a condition **without** modifying pipeline data.
```
count if salary > 50000
count if country == "Germany"
```

---

### Column Selection

#### `select`
Keep only the specified columns (comma-separated).
```
select name, age, country
```

#### `drop`
Remove one or more columns (comma-separated).
```
drop salary, department
```

#### `limit`
Keep only the first *n* rows.
```
limit 100
```

#### `distinct`
Remove duplicate rows.
```
distinct
```

#### `sample`
Take a random sample of rows — either a fixed count or a percentage.
```
sample 100
sample 10%
```

---

### Sorting

#### `sort by`
Sort rows by one or more columns. Direction defaults to `asc`.
```
sort by age
sort by age desc
sort by country asc, age desc
```

---

### Transformation

#### `rename`
Rename a column.
```
rename old_name new_name
```

#### `add`
Add a computed column using an arithmetic expression. Column names are referenced directly.
```
add tax = price * 0.2
add full_salary = salary + bonus
```

Supports `$variable` substitution in the expression:
```
set rate = 0.2
add tax = price * $rate
```

Conditional form using `if / then / else`:
```
add tier = if salary > 80000 then "senior" else "junior"
add label = if score >= 50 then "pass" else "fail"
```

#### `cast`
Cast a column to a different data type.

| Type keyword | Result |
|---|---|
| `int` / `integer` | Nullable integer |
| `float` / `double` | Float |
| `str` / `string` / `text` | String |
| `datetime` / `date` | Datetime |
| `bool` / `boolean` | Boolean |

```
cast age int
cast score float
cast ts datetime
cast active bool
```

#### `trim`
Strip leading and trailing whitespace from a string column.
```
trim country
```

#### `uppercase`
Convert a string column to uppercase.
```
uppercase country
```

#### `lowercase`
Convert a string column to lowercase.
```
lowercase email
```

#### `replace`
Replace occurrences of a value in a column.
```
replace country "Germany" "DE"
replace status active completed
```

#### `pivot`
Reshape data from long to wide format.
```
pivot index=country column=year value=revenue
```

---

### Aggregation

Each aggregation command works standalone (returns a single-row result) or after `group by` (returns one row per group).

#### `group by`
Group rows by one or more columns. Must be followed by an aggregation command (`count`, `sum`, `avg`, `min`, `max`, or `agg`).
```
group by country
group by country, age
```

#### `count`
Count rows. When preceded by `group by`, counts per group. Without grouping, returns the total row count.
```
count

group by country
count
```

#### `sum`
```
sum salary

group by country
sum salary
```

#### `avg`
```
avg age

group by country
avg salary
```

#### `min` / `max`
```
min age
max salary

group by country
max salary
```

#### `agg`
Apply multiple aggregations at once after a `group by`.
```
group by country
agg sum salary, avg age, count
```

Supported verbs inside `agg`: `sum`, `avg`, `min`, `max`, `count`.

---

### Joining

#### `join`
Join with another CSV on a shared key column. The join type defaults to `inner`; use `left`, `right`, or `outer` to change it.
```
join "data/departments.csv" on dept_id
join "data/departments.csv" on dept_id left
join "data/departments.csv" on dept_id outer
```

| Type | Behaviour |
|---|---|
| `inner` (default) | Only rows with matching keys in both files |
| `left` | All rows from the left side; nulls for unmatched right rows |
| `right` | All rows from the right side; nulls for unmatched left rows |
| `outer` | All rows from both sides; nulls wherever a match is missing |

#### `merge`
Append rows from another CSV file (union/stack — columns are matched by name).
```
merge "data/extra_people.csv"
```

---

### Output

#### `save`
Write the current data to a CSV, JSON, or Parquet file. Output directories are created automatically.
```
save "output/results.csv"
save "output/results.json"
save "output/results.parquet"
```

Supports `$variable` references in the file path:
```
set out = output/report.csv
save "$out"
```

#### `print`
Print the current data to the terminal without saving.
```
print
```

---

### Inspection

#### `schema`
Print column names and data types.
```
schema
```

#### `inspect`
Print column names, types, null counts, and unique value counts.
```
inspect
```

#### `head`
Print the first *n* rows to the terminal **without modifying pipeline data**.
```
head 10
```

---

### Data Quality

#### `assert`
Fail the pipeline if any row violates a condition. Uses the same operators as `filter`.
```
assert age > 0
assert salary != 0
```

#### `fill`
Fill missing or empty values in a column.

| Strategy | Description |
|---|---|
| `mean` | Fill with column average (numeric) |
| `median` | Fill with column median (numeric) |
| `mode` | Fill with most frequent value |
| `forward` | Copy last non-null value downward |
| `backward` | Copy next non-null value upward |
| `drop` | Remove rows where this column is null |
| `<value>` | Fill with a literal number or string |

```
fill age mean
fill country "Unknown"
fill salary 0
fill score forward
fill status drop
```

---

## Timestamp Commands

Pipeline DSL includes a suite of commands for working with date and time data. Columns must be in Polars `Datetime` format — use `parse_date` to convert a string column first.

### `parse_date`
Parse a string column into a datetime type using a [strftime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) format string.

```
parse_date created_at "%Y-%m-%d"
parse_date event_time "%d/%m/%Y %H:%M:%S"
```

### `extract`
Extract a single date/time component from a datetime column into a new integer column.

| Part | Description |
|---|---|
| `year` | Calendar year (e.g. 2024) |
| `month` | Month number 1–12 |
| `day` | Day of month 1–31 |
| `hour` | Hour 0–23 |
| `minute` | Minute 0–59 |
| `second` | Second 0–59 |
| `weekday` | Day of week, 0 = Monday … 6 = Sunday |
| `quarter` | Calendar quarter 1–4 |

```
extract year from order_date as order_year
extract month from event_time as event_month
```

### `date_diff`
Compute the signed difference between two datetime columns and store the result in a new integer column.

```
date_diff end_date start_date as duration_days in days
date_diff checkout checkin as stay_hours in hours
```

Units: `days` · `hours` · `minutes` · `seconds`

### `filter_date`
Filter rows by comparing a datetime column to a literal ISO date (`YYYY-MM-DD`).

```
filter_date order_date >= 2024-01-01
filter_date event_time < 2025-06-01
```

Operators: `>` `<` `>=` `<=` `==`

### `truncate_date`
Truncate a datetime column to the given precision, zeroing out finer time components.

```
truncate_date order_date to month   # 2024-03-15 → 2024-03-01
truncate_date event_time to hour    # 2024-03-15 14:37:22 → 2024-03-15 14:00:00
```

Units: `year` · `month` · `week` · `day` · `hour` · `minute` · `second`

### `ts_sort`
Sort the pipeline by a datetime column in ascending (chronological) order.

```
ts_sort order_date
```

---

## Timestamp Commands

Pipeline DSL includes a suite of commands for working with date and time data. Columns must be in Polars `Datetime` format — use `parse_date` to convert a string column first.

### `parse_date`
Parse a string column into a datetime type using a [strftime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) format string.

```
parse_date created_at "%Y-%m-%d"
parse_date event_time "%d/%m/%Y %H:%M:%S"
```

### `extract`
Extract a single date/time component from a datetime column into a new integer column.

| Part | Description |
|---|---|
| `year` | Calendar year (e.g. 2024) |
| `month` | Month number 1–12 |
| `day` | Day of month 1–31 |
| `hour` | Hour 0–23 |
| `minute` | Minute 0–59 |
| `second` | Second 0–59 |
| `weekday` | Day of week, 0 = Monday … 6 = Sunday |
| `quarter` | Calendar quarter 1–4 |

```
extract year from order_date as order_year
extract month from event_time as event_month
```

### `date_diff`
Compute the signed difference between two datetime columns and store the result in a new integer column.

```
date_diff end_date start_date as duration_days in days
date_diff checkout checkin as stay_hours in hours
```

Units: `days` · `hours` · `minutes` · `seconds`

### `filter_date`
Filter rows by comparing a datetime column to a literal ISO date (`YYYY-MM-DD`).

```
filter_date order_date >= 2024-01-01
filter_date event_time < 2025-06-01
```

Operators: `>` `<` `>=` `<=` `==`

### `truncate_date`
Truncate a datetime column to the given precision, zeroing out finer time components.

```
truncate_date order_date to month   # 2024-03-15 → 2024-03-01
truncate_date event_time to hour    # 2024-03-15 14:37:22 → 2024-03-15 14:00:00
```

Units: `year` · `month` · `week` · `day` · `hour` · `minute` · `second`

### `ts_sort`
Sort the pipeline by a datetime column in ascending (chronological) order.

```
ts_sort order_date
```

---

### Variables & Environment

#### `set`
Set a named variable, referenceable as `$name` in other commands.
```
set threshold = 50000
set label = "Europe"
set output_dir = output/reports
```

Then use it in any command that supports variable references:
```
filter salary > $threshold
log "Processing $label data"
source "$input_path"
save "$output_dir/summary.csv"
```

**Sandbox mode** — restrict all file I/O to a specific directory tree. Any `source`, `save`, or `join` that tries to access a path outside this directory will fail with a `PermissionError`.
```
set sandbox = data/safe_zone
source "data/safe_zone/people.csv"   # allowed
source "data/other/secret.csv"       # blocked — outside sandbox
```

#### `env`
Load an OS environment variable into the pipeline variable store.
```
env DATA_PATH
```

Then use it:
```
env DATA_PATH
source $DATA_PATH
```

Environment variables are accessible with `$NAME` just like `set` variables:
```
env OUTPUT_DIR
save "$OUTPUT_DIR/results.csv"
```

---

### Error Recovery

#### `try` / `on_error`
Wrap one or more commands in a `try` block. If any command inside the block raises an error, execution jumps to the `on_error` handler instead of stopping the entire pipeline.

```
try
    assert age > 0
on_error skip
```

The `on_error` handler can be:

| Handler | Effect |
|---|---|
| `skip` | Silently swallow the error and continue |
| `log "message"` | Print a message and continue |
| any command | Execute that command (e.g. `fill age 0`) and continue |

```
# Log the error and carry on
try
    cast ts datetime
on_error log "timestamp parse failed — skipping column"

# Run a recovery command when the block fails
try
    assert salary > 0
on_error fill salary 0

# Chain multiple recovery actions (nest try blocks)
try
    source "$dynamic_path"
on_error log "could not load $dynamic_path — using fallback"
```

`try` blocks can be nested:
```
try
    try
        cast age int
    on_error fill age 0
    assert age > 0
on_error log "age validation failed after fill"
```

---

### Timing & Logging

#### `timer`
Measure elapsed time between pipeline steps. A timer is identified by a label (defaults to `default` when omitted). Use `start` to begin, `lap` for a checkpoint (keeps the timer running), and `stop` to end and print the final elapsed time.

```
timer start loading
source "data/big.csv" chunk 100000
timer stop loading          # prints: [TIMER] loading: 3.42s
```

```
timer start
filter age > 18
timer lap                   # prints: [LAP] default: 0.012s — timer still running
sort by salary desc
timer stop                  # prints: [TIMER] default: 0.028s
```

Multiple independent timers can run at the same time:
```
timer start total
timer start load_phase
source "data/sales.csv"
timer stop load_phase       # prints: [TIMER] load_phase: 1.23s

# ... transform steps ...

timer stop total            # prints: [TIMER] total: 4.56s
```

| Action | Effect |
|---|---|
| `timer start [label]` | Record the current time for *label* |
| `timer lap [label]` | Print elapsed since `start`, keep the timer running |
| `timer stop [label]` | Print elapsed since `start`, then clear the timer |

#### `log`
Print a message to the terminal during pipeline execution. Supports `$variable` substitution.
```
log "Processing complete"
log "Loaded data for $label"
log "Rows remaining: checking now"
```

---

### Comments

Lines beginning with `#` are ignored, so you can annotate your pipelines freely.

```
# Load raw data
source "data/sales.csv"

# Only keep completed orders
filter status == "completed"
```

Inline comments (after whitespace) are also stripped:
```
select name, age, country   # keep only these three columns
sort by age desc             # newest first
```

---

## Advanced Features

### Variable Substitution

Variables set with `set` or loaded with `env` are referenced as `$name` anywhere in a command that accepts a value or path.

```
set input  = data/sales_2024.csv
set output = output/summary.csv
set min_amount = 1000

source "$input"
filter amount > $min_amount
save "$output"
```

Variable substitution works in:
- File paths (`source`, `save`, `join`, `merge`, `foreach`, `include`)
- Filter / assert values (`filter salary > $threshold`)
- Computed expressions (`add tax = price * $rate`)
- Log messages (`log "Done processing $label"`)
- Conditional values (`add tier = if salary > $cutoff then "high" else "low"`)

---

### Chunked Streaming

For files too large to fit comfortably in memory, append `chunk <n>` to `source` to process the file in fixed-size batches.

```
source "data/transactions.csv" chunk 50000
filter amount > 0
cast amount float
select id, date, amount
save "output/cleaned.csv"
```

How it works:
1. The CSV is read in chunks of *n* rows at a time.
2. **Chunk-safe** operations (`filter`, `select`, `cast`, `rename`, `add`, `trim`, `uppercase`, `lowercase`, `replace`, `fill`, `timer`) are applied independently to each chunk, keeping peak memory low.
3. The chunks are concatenated into a single DataFrame.
4. **Post-concat** operations (`sort by`, `group by`, aggregations, `join`, etc.) run on the full result.

**Choosing a chunk size:** start with 50 000–200 000 rows. Smaller chunks use less memory but add more overhead; larger chunks are faster but require more RAM.

---

### Sandbox Mode

Sandbox mode restricts all file I/O to a specific directory tree, preventing pipelines from reading or writing files outside the allowed area. Activate it with `set sandbox = <directory>`:

```
set sandbox = data/trusted

source "data/trusted/people.csv"    # OK
save   "data/trusted/out.csv"       # OK
source "data/secrets/passwords.csv" # PermissionError — outside sandbox
```

Sandbox checks apply to `source`, `save`, `join`, `merge`, `foreach`, and `include`. Path traversal attempts (`../../etc/passwd`) are blocked even if the resolved path happens to exist.

---

## Architecture

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
output CSV / JSON / Parquet
```

Each command maps to a node class in [ast_nodes.py](ast_nodes.py). Adding a new command means adding one class and one parser entry — nothing else changes.

### Project Structure

```
pipeline_dsl/
├── main.py           # CLI entry point
├── file_reader.py    # Reads and cleans .ppl files
├── ppl_parser.py     # Converts lines into AST nodes
├── ast_nodes.py      # Node classes (one per command)
├── executor.py       # Runs the pipeline node by node
├── pyproject.toml    # Package config — registers the ppl command
├── data/             # Sample input CSV files
├── pipelines/        # .ppl pipeline definitions and examples
├── tutorials/        # Progressive tutorial .ppl files (01–09)
└── output/           # Generated output (auto-created on first save)
```

---

## Error Reference

The DSL provides clear error messages for common mistakes:

| Problem | Example message |
|---|---|
| File not found | `[SourceNode] Source file not found: 'data/missing.csv'` |
| Unknown column | `[FilterNode] filter: column 'agee' not found. Available: ['name', 'age', ...]` |
| Invalid command | `Line 3: unknown command 'sortby'. Supported commands: add, assert, avg, ...` |
| Missing `.ppl` extension | `Expected a .ppl file, got: 'data.csv'` |
| Bad filter syntax | `Line 2: could not parse 'filter' condition 'age'. Expected: filter <column> <op> <value>` |
| Assert failure | `[AssertNode] assert: 3 row(s) failed condition 'age > 0'` |
| Bad expression in `add` | `[AddNode] add: could not evaluate expression 'agee * 2': ...` |
| Join key not found | `[JoinNode] join: key 'id' not in current data. Available: [...]` |
| Undefined variable | `[SourceNode] variable '$path' is not defined. Use 'set path = <value>' first.` |
| No files matched glob | `[ForeachNode] foreach: no files matched pattern 'data/monthly/*.csv'` |
| Sandbox violation | `PermissionError: Access denied: 'data/other.csv' is outside the sandbox` |
| try / on_error block | Caught by `on_error` handler — pipeline continues |
| Timer not started | `[TimerNode] timer: no timer named 'load' is running. Use 'timer start load' first.` |
| Unknown cast type | `[CastNode] cast: unknown type 'numeric'. Supported: bool, boolean, date, datetime, ...` |

---

## VS Code Extension

The `vscode-ppl/` folder contains a VS Code extension that adds syntax highlighting and a one-click run button for `.ppl` files.

**Install:**
```powershell
cd vscode-ppl
"y" | vsce package --no-dependencies
code --install-extension vscode-ppl-1.1.0.vsix
```
Then reload VS Code (`Ctrl+Shift+P` → `Developer: Reload Window`).

See [vscode-ppl/README.md](vscode-ppl/README.md) for full details and update instructions.

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
Licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).

Free to use, share, and adapt with attribution — commercial use is not permitted.
See [LICENSE](LICENSE) for full details.
