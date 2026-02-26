# Pipeline DSL — Complete Tutorial

> Learn to write data pipelines in **Pipeline DSL** (`.ppl`) from scratch.
> Each section builds on the last. Exercise files are in `tutorials/`.

---

## Table of Contents

1. [What is Pipeline DSL?](#1-what-is-pipeline-dsl)
2. [Setup & Running Your First Pipeline](#2-setup--running-your-first-pipeline)
3. [Loading Data — `source`](#3-loading-data--source)
4. [Inspecting Data — `schema`, `inspect`, `head`, `print`](#4-inspecting-data--schema-inspect-head-print)
5. [Filtering Rows — `filter`](#5-filtering-rows--filter)
6. [Selecting Columns — `select`, `drop`, `distinct`, `limit`](#6-selecting-columns--select-drop-distinct-limit)
7. [Transforming Data — `rename`, `add`, `sort by`](#7-transforming-data--rename-add-sort-by)
8. [Missing Values — `fill`](#8-missing-values--fill)
9. [Grouping & Aggregation — `group by`, `count`, `sum`, `avg`, `min`, `max`](#9-grouping--aggregation--group-by-count-sum-avg-min-max)
10. [Joining Data — `join`, `merge`](#10-joining-data--join-merge)
11. [Data Validation — `assert`](#11-data-validation--assert)
12. [Saving Output — `save`](#12-saving-output--save)
13. [Comments & Best Practices](#13-comments--best-practices)
14. [Capstone Project](#14-capstone-project)
15. [Advanced Features](#15-advanced-features)
    - [Parquet files](#parquet-files)
    - [Chunked streaming](#chunked-streaming--source--chunk-n)
    - [Join types](#join-types--inner-left-right-outer)
    - [Sandbox mode](#sandbox-mode--set-sandbox--dir)
    - [Error recovery](#error-recovery--try--on_error)
    - [Timing pipelines](#timing-pipelines--timer)
16. [Quick Reference Card](#16-quick-reference-card)

---

## 1. What is Pipeline DSL?

**Pipeline DSL** is a tiny language for transforming data. Instead of writing Python
or SQL, you write simple English-like commands in a `.ppl` file, one command per line.

The language is **declarative** and **sequential** — you describe what you want done,
and each command runs in order on the current state of your data.

**A typical pipeline looks like this:**

```
source "data/sales.csv"
filter status == "completed"
group by region
sum revenue
sort by revenue desc
save "output/region_totals.csv"
```

Under the hood, Pipeline DSL uses pandas DataFrames, so it's fast and handles
files of millions of rows. You never have to write Python yourself.

---

## 2. Setup & Running Your First Pipeline

### Install

```bash
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

pip install -e .
```

### Run a pipeline

```bash
ppl <path/to/file.ppl>
```

### Exercise 1 — Hello Pipeline

**File:** `tutorials/01_hello_pipeline.ppl`

```
source "data/people.csv"
print
```

Run it:

```bash
ppl tutorials/01_hello_pipeline.ppl
```

You should see 15 rows of people data printed to your terminal. That's your first
pipeline!

---

## 3. Loading Data — `source`

`source` loads a CSV file and makes it the current working dataset. Everything
after this command operates on that data.

```
source "path/to/file.csv"
```

- The path is **relative to the `.ppl` file's location**.
- You can use `source` multiple times in one pipeline to reload or switch datasets.
- Only CSV files are supported as input (output can be CSV or JSON).

**Example:**

```
# Load the dataset
source "data/people.csv"
```

### Key point

`source` **replaces** any existing data in the pipeline. If you call it a second
time, the first dataset is gone. Use this to start a fresh analysis within the
same file.

---

## 4. Inspecting Data — `schema`, `inspect`, `head`, `print`

Before transforming data, understand what you're working with. These four commands
are all **read-only** — they display information without changing your data.

### `schema`

Shows column names and their data types.

```
source "data/people.csv"
schema
```

Output:

```
name      object
age        int64
country   object
salary     int64
```

### `inspect`

More detailed than `schema`. Shows null counts and unique value counts too.

```
source "data/people.csv"
inspect
```

Output:

```
Column     Type     Nulls   Unique
name       object   0       15
age        int64    0       15
country    object   0       3
salary     int64    0       10
```

Use `inspect` to spot data quality issues early — nulls, low uniqueness, etc.

### `head`

Print the first N rows without changing the pipeline data.

```
source "data/people.csv"
head 5
```

### `print`

Print **all** current rows to the terminal.

```
source "data/people.csv"
print
```

**Exercise 2 file:** `tutorials/02_inspecting_data.ppl`

---

## 5. Filtering Rows — `filter`

`filter` keeps only the rows where a condition is true. Rows that fail the
condition are removed from the pipeline.

```
filter <column> <operator> <value>
```

### Operators

| Operator | Meaning               |
|----------|-----------------------|
| `>`      | greater than          |
| `<`      | less than             |
| `>=`     | greater than or equal |
| `<=`     | less than or equal    |
| `==`     | equal to              |
| `!=`     | not equal to          |

### Examples

```
# Keep rows where age is at least 18
filter age >= 18

# Keep only people from Germany
filter country == "Germany"

# Exclude rows with zero salary
filter salary != 0

# Keep only high earners
filter salary > 80000
```

### String values

Wrap string values in double quotes:

```
filter country == "France"
filter name != "Bob"
```

### Chaining filters

You can apply multiple filters in sequence — each one narrows the data further:

```
source "data/people.csv"
filter age >= 18
filter country == "USA"
filter salary > 50000
print
```

**Exercise 3 file:** `tutorials/03_filtering.ppl`

---

## 6. Selecting Columns — `select`, `drop`, `distinct`, `limit`

### `select`

Keep **only** the listed columns, in the order you specify.

```
source "data/people.csv"
select name, country, salary
print
```

Use `select` to reduce noise and only carry forward what you need.

### `drop`

Remove specific columns, keeping everything else.

```
source "data/people.csv"
drop salary
print
```

### `distinct`

Remove duplicate rows. Two rows are duplicates if every column value is the same.

```
source "data/people.csv"
select country
distinct
print
# Result: one row per unique country
```

### `limit`

Keep only the first N rows. Useful for sampling large files or getting a "top N".

```
source "data/people.csv"
sort by salary desc
limit 3
print
# The 3 highest-paid people
```

**Exercise 4 file:** `tutorials/04_selecting_columns.ppl`

---

## 7. Transforming Data — `rename`, `add`, `sort by`

### `rename`

Give a column a new name.

```
rename old_column_name new_column_name
```

Example:

```
source "data/people.csv"
rename salary income
schema
```

### `add`

Create a new computed column using an arithmetic expression. Reference other
columns by name directly in the expression.

```
add new_column = expression
```

Supported arithmetic: `+`, `-`, `*`, `/`

```
source "data/people.csv"
rename salary income

# 20% tax on income
add tax = income * 0.2

# What's left after tax
add net_income = income - tax

# Years until retirement (assuming retirement at 65)
add years_to_retire = 65 - age

print
```

You can chain `add` commands — each new column is immediately available to
subsequent `add` commands.

### `sort by`

Sort rows by one or more columns. Direction defaults to ascending.

```
sort by column_name
sort by column_name asc
sort by column_name desc
sort by col1 asc, col2 desc
```

Examples:

```
# Youngest first
sort by age asc

# Highest salary first
sort by salary desc

# Sort by country A-Z, then by salary highest first within each country
sort by country asc, salary desc
```

**Exercise 5 file:** `tutorials/05_transforming.ppl`

---

## 8. Missing Values — `fill`

Real data always has gaps. `fill` handles missing or empty values in a column.

```
fill <column> <strategy_or_value>
```

### Strategies

| Strategy   | What it does                                         |
|------------|------------------------------------------------------|
| `mean`     | Replace with the column's average (numeric only)     |
| `median`   | Replace with the column's median (numeric only)      |
| `mode`     | Replace with the most frequently occurring value     |
| `forward`  | Copy the last non-null value downward                |
| `backward` | Copy the next non-null value upward                  |
| `drop`     | Remove the entire row where this column is null      |

### Literal values

You can also replace missing values with a fixed number or string:

```
fill salary 0
fill country "Unknown"
fill score 100
```

### Examples

```
source "data/people.csv"

# Fill missing ages with the average age
fill age mean

# Fill missing country with a placeholder
fill country "Unknown"

# Remove rows that still have no salary after everything else
fill salary drop
```

Always `inspect` before filling so you know which columns actually have nulls.

---

## 9. Grouping & Aggregation — `group by`, `count`, `sum`, `avg`, `min`, `max`

Aggregation collapses many rows into summary rows. You can aggregate the whole
dataset, or use `group by` to get one summary row per group.

### Without `group by` — whole dataset summary

```
source "data/people.csv"
count
```
Returns a single number: total rows.

```
source "data/people.csv"
avg salary
```
Returns the average salary across all rows.

### With `group by` — one row per group

`group by` must be immediately followed by an aggregation command.

```
source "data/people.csv"
group by country
count
```

Output:

```
country  count
France       5
Germany      5
USA          5
```

```
source "data/people.csv"
filter age >= 18
group by country
avg salary
sort by avg_salary desc
print
```

### All aggregation commands

| Command      | What it does                           |
|--------------|----------------------------------------|
| `count`      | Count rows (or group size)             |
| `sum <col>`  | Sum of all values in column            |
| `avg <col>`  | Average (mean) of values in column     |
| `min <col>`  | Minimum value in column                |
| `max <col>`  | Maximum value in column                |

### Example — Multiple groups

```
source "data/people.csv"
group by country
sum salary
sort by country asc
print
```

**Exercise 6 file:** `tutorials/06_aggregation.ppl`

---

## 10. Joining Data — `join`, `merge`

### `join`

Combine two datasets side-by-side on a shared key column. This is an **inner join**
— only rows with a matching key in both files are kept.

```
join "path/to/other.csv" on key_column
```

Example: say you have `departments.csv` with columns `dept_id, dept_name`, and
your main data has a `dept_id` column:

```
source "data/employees.csv"
join "data/departments.csv" on dept_id
print
```

Result: every employee row now also has the `dept_name` column.

### `merge`

Append rows from another CSV onto the current data (vertical stack / union).
Columns are matched by name — columns that don't exist get null values.

```
merge "data/more_people.csv"
```

Example:

```
source "data/people.csv"
merge "data/extra_people.csv"
count
```

Use `merge` to combine datasets from different time periods or sources that have
the same structure.

---

## 11. Data Validation — `assert`

`assert` is your data quality gate. It checks that **all rows** satisfy a
condition. If any row violates it, the **pipeline stops immediately** with an
error.

```
assert <column> <operator> <value>
```

Uses the same operators as `filter`.

```
source "data/people.csv"

# Every person must have a positive age
assert age > 0

# Every person must have a name
assert name != ""
```

If the assertion passes, the pipeline continues silently. If it fails:

```
[AssertNode] assert: 3 row(s) failed condition 'age > 0'
```

### When to use assert

- **After loading data** — catch corrupted or unexpected input early.
- **Before saving** — make sure your output meets quality standards.
- **After filtering** — confirm your filter actually produced valid data.

```
source "data/people.csv"
filter age >= 18

# After filtering, all remaining salaries MUST be positive
assert salary > 0

save "output/valid_adults.csv"
```

**Exercise 7 file:** `tutorials/07_fill_and_assert.ppl`

---

## 12. Saving Output — `save`

Write the current pipeline data to a file. Output directories are created
automatically if they don't exist.

```
save "path/to/output.csv"
save "path/to/output.json"
```

- **`.csv`** — standard comma-separated format, compatible with Excel and every
  data tool.
- **`.json`** — JSON array format, useful for APIs and web applications.

You can save **multiple times** in one pipeline:

```
source "data/people.csv"
filter age >= 18

# Save for downstream use
save "output/adults.csv"

# Also save JSON for the API team
save "output/adults.json"

# Continue transforming and save a summary too
group by country
count
save "output/adults_by_country.csv"
```

**Exercise 8 file:** `tutorials/08_saving_output.ppl`

---

## 13. Comments & Best Practices

### Comments

Any line starting with `#` is a comment and is completely ignored.

```
# This is a comment
source "data/people.csv"  # inline comments are NOT supported — # must be first
```

Use comments liberally to explain your intent:

```
# Load raw employee data
source "data/employees.csv"

# Remove test records (id < 0 are synthetic)
filter id > 0

# Compute annual bonus (10% of salary, capped later)
add bonus = salary * 0.1
```

### Best Practices

**1. Inspect before transforming**

Always start exploration with `inspect` to understand your data before changing it.

**2. Assert early**

Add `assert` statements right after `source` to validate assumptions about your
input data.

**3. Build incrementally**

Write your pipeline step by step. Add `print` or `head` after each major step
while you're developing, then remove them when you're done.

**4. Use meaningful column names**

Use `rename` to give columns clear names before doing calculations with `add`.

**5. One responsibility per pipeline**

Keep pipelines focused. Instead of one 50-line mega-pipeline, consider splitting
into multiple pipelines with clear output files that feed into each other.

---

## 14. Capstone Project

**File:** `tutorials/09_capstone.ppl`

This pipeline brings everything together. Goal: produce a country salary report
for adult employees.

```
# 1. Load raw data
source "data/people.csv"

# 2. Inspect before touching anything
inspect

# 3. Handle data quality issues
fill salary 0
fill country "Unknown"

# 4. Validate — all ages must be positive
assert age > 0

# 5. Filter to working-age adults only
filter age >= 18

# 6. Create derived columns
rename salary income
add tax = income * 0.2
add net_income = income - tax

# 7. Drop columns we no longer need
drop tax

# 8. Sort by net income — highest earners first
sort by net_income desc

# 9. Preview top 5
head 5

# 10. Group and summarise by country
source "data/people.csv"
fill salary 0
filter age >= 18
group by country
avg salary

sort by country asc

# 11. Save the summary
save "output/capstone_by_country.csv"
print
```

Run it:

```bash
ppl tutorials/09_capstone.ppl
```

---

## 15. Advanced Features

These features are fully implemented and available today.

---

### Parquet files

Pipeline DSL can read **and** write Parquet files in addition to CSV and JSON.
Parquet is a columnar format that is faster and more compact than CSV for large
datasets and preserves column data types exactly.

**Reading:**
```
source "data/snapshot.parquet"
```

**Writing:**
```
save "output/results.parquet"
```

You can freely mix formats in one pipeline — read a Parquet snapshot, filter
it, then save the result as both CSV and JSON.

```
source "data/snapshot.parquet"
filter age >= 18
save "output/adults.csv"
save "output/adults.json"
```

---

### Chunked streaming — `source ... chunk N`

For very large CSV files that might not fit in memory, add `chunk N` to the
`source` command. The pipeline reads the file N rows at a time.

Row-safe operations (`filter`, `select`, `cast`, `rename`, `add`, `uppercase`,
etc.) are applied to each chunk before the results are concatenated, keeping peak
memory well below the full file size.

```
source "data/big.csv" chunk 100000
filter status == "active"
select name, salary
```

All other commands (sort, group by, aggregations) run after the chunks have been
concatenated and work exactly as normal.

---

### Join types — `inner`, `left`, `right`, `outer`

`join` defaults to an inner join. Add a join type keyword as the last token to
choose a different behaviour:

| Keyword | Keeps |
|---------|-------|
| `inner` (default) | Only rows that match in both files |
| `left` | All rows from the left side; nulls for unmatched right rows |
| `right` | All rows from the right side; nulls for unmatched left rows |
| `outer` | All rows from both sides; nulls wherever a match is missing |

```
# left join: keep every employee even if they have no department record
source "data/employees.csv"
join "data/departments.csv" on dept_id left
print
```

---

### Sandbox mode — `set sandbox = <dir>`

When a pipeline will be distributed or run in a shared environment, you can
limit all file I/O to a specific directory tree by setting the `sandbox`
variable. Any `source`, `save`, or `join` that tries to access a path outside
that directory will raise a `PermissionError` immediately.

```
set sandbox = data/safe_zone
source "data/safe_zone/people.csv"   # allowed
source "data/other/secret.csv"       # blocked: outside sandbox
```

This prevents pipelines from accidentally (or intentionally) reading or writing
files that should be off-limits.

---

### Error recovery — `try` / `on_error`

By default, any error inside a pipeline stops everything. `try` lets you catch
errors and decide what to do: skip them, log a message, or run a recovery command.

**Syntax:**
```
try
    <one or more commands>
on_error <action>
```

**Available actions:**

| Action | Effect |
|--------|--------|
| `skip` | Silently continue |
| `log "message"` | Print the message, then continue |
| any command | Run that command (e.g. `fill salary 0`) then continue |

**Examples:**

```
# Assert might fail on messy input — skip and carry on
source "data/people.csv"
try
    assert salary > 0
on_error skip

# Log a warning when a cast fails (e.g. non-numeric value in column)
try
    cast ts datetime
on_error log "timestamp parse failed — defaulting to null"

# Run a fill command to recover from a failed assertion
try
    assert age > 0
on_error fill age 0

# Nested try blocks
try
    try
        assert salary > 0
    on_error fill salary 0
on_error skip
```

If the commands inside `try` succeed, the `on_error` handler is never called.

---

### Timing pipelines — `timer`

`timer` lets you measure how long parts of your pipeline take. Results are printed
to the terminal and never affect the data.

```
timer start <label>    # start a named stopwatch
timer stop <label>     # stop it and print elapsed time
timer lap <label>      # print elapsed time without stopping
```

The label is optional — leave it out to use the default label `default`.

**Basic example:**

```
timer start load
source "data/big.csv" chunk 50000
filter status == "active"
timer stop load
```

Output:
```
[TIMER] load: 3.217s
```

**Timing multiple phases independently:**

```
timer start total

timer start ingest
source "data/sales.csv" chunk 100000
timer stop ingest

timer start transform
filter amount > 0
group by region
sum amount
timer stop transform

timer stop total
```

Output:
```
[TIMER] ingest: 4.102s
[TIMER] transform: 0.341s
[TIMER] total: 4.451s
```

Use `timer lap` to mark intermediate progress without stopping the timer:

```
timer start etl
source "data/events.csv"
timer lap etl
filter event_type == "click"
timer lap etl
group by page
count
timer stop etl
```

Output:
```
[LAP] etl: 1.203s
[LAP] etl: 1.387s
[TIMER] etl: 1.412s
```

For durations over a minute the output is formatted as `Xm Y.ZZZs` (e.g. `1m 4.812s`).

You can run any number of independently-named timers at the same time — each
label is tracked separately.

---

## 16. Working with Timestamps

This section shows how to use Pipeline DSL's date and time commands.  All timestamp
operations work on Polars `Datetime` columns.  Use `parse_date` to convert string
columns first.

### Sample data

```
date_str,product,revenue
2024-01-15,Widget,1200
2024-02-03,Gadget,850
2024-03-21,Widget,2400
2024-04-10,Gadget,975
2024-05-05,Widget,1800
```

Save this as `data/sales.csv` and follow along.

### Parse a string column into a datetime

```
source "data/sales.csv"
parse_date date_str "%Y-%m-%d"
schema
```

`date_str` is now a `Datetime` column instead of a plain string.

### Extract a date part

```
source "data/sales.csv"
parse_date date_str "%Y-%m-%d"
extract month from date_str as sale_month
select sale_month, product, revenue
print
```

```
sale_month product  revenue
         1  Widget     1200
         2  Gadget      850
         3  Widget     2400
         4  Gadget      975
         5  Widget     1800
```

### Filter by date

Keep only rows on or after 1 March 2024:

```
source "data/sales.csv"
parse_date date_str "%Y-%m-%d"
filter_date date_str >= 2024-03-01
print
```

### Truncate to month

Useful for grouping by calendar month:

```
source "data/sales.csv"
parse_date date_str "%Y-%m-%d"
truncate_date date_str to month
group by date_str
sum revenue
sort by date_str asc
print
```

### Compute date differences

If your data has two date columns (e.g. `start_date` and `end_date`), you can
compute the gap between them:

```
source "data/projects.csv"
parse_date start_date "%Y-%m-%d"
parse_date end_date   "%Y-%m-%d"
date_diff end_date start_date as duration_days in days
print
```

### Sort chronologically

`ts_sort` is a convenience wrapper for sorting by a datetime column ascending:

```
source "data/sales.csv"
parse_date date_str "%Y-%m-%d"
ts_sort date_str
print
```

**Tip:** You can also combine several timestamp commands.  The tutorial file
`tutorials/10_timestamps.ppl` has a complete worked example.

---

## 17. Quick Reference Card

```
# ── LOADING ──────────────────────────────────────
source "file.csv"              # load CSV
source "file.parquet"          # load Parquet
source "file.csv" chunk N      # chunked streaming for large files
join "other.csv" on column     # inner join (default)
join "other.csv" on column left  # left / right / outer also supported
merge "other.csv"              # append rows (stack vertically)
foreach "data/monthly/*.csv"   # load and concatenate all matching files
include "file.ppl"             # execute another pipeline inline

# ── INSPECTING ───────────────────────────────────
schema                         # column names + types
inspect                        # names, types, nulls, unique counts
head N                         # print first N rows (non-destructive)
print                          # print all rows
timer start label              # start a named stopwatch
timer lap label                # print elapsed without stopping
timer stop label               # stop and print elapsed time

# ── FILTERING ────────────────────────────────────
filter col > value             # operators: > < >= <= == !=
filter col > v1 and col2 == v2 # compound conditions
where col > value              # SQL-friendly alias for filter
distinct                       # remove duplicate rows
limit N                        # keep first N rows
sample N                       # random N rows
sample N%                      # random N% of rows

# ── COLUMNS ──────────────────────────────────────
select col1, col2              # keep only these columns
drop col1, col2                # remove these columns
rename old new                 # rename a column

# ── TRANSFORMING ─────────────────────────────────
add col = expr                 # new computed column (+ - * /)
add col = if cond then v else v  # conditional column
sort by col asc                # sort (asc default, or desc)
sort by col1 asc, col2 desc    # multi-column sort
trim col                       # strip whitespace
uppercase col                  # to uppercase
lowercase col                  # to lowercase
cast col int                   # types: int float str datetime bool
replace col "old" "new"        # find-and-replace in a column
pivot index=c column=c value=c # long → wide reshape

# ── MISSING VALUES ───────────────────────────────
fill col mean                  # strategies: mean median mode
fill col forward               #             forward backward drop
fill col "literal"             # or a literal value

# ── AGGREGATION ──────────────────────────────────
group by col1, col2            # group (must be followed by agg)
count                          # count rows / group sizes
count if col > value           # count without filtering
sum col                        # sum of column
avg col                        # average of column
min col                        # minimum value
max col                        # maximum value
agg sum col, avg col, count    # multiple aggregations at once

# ── QUALITY & ERROR RECOVERY ─────────────────────
assert col > value             # fail pipeline if condition broken
try                            # catch errors from enclosed commands
    <commands>
on_error skip                  # skip | log "msg" | any command

# ── TIMESTAMPS ───────────────────────────────────
parse_date col "%Y-%m-%d"      # parse string → datetime
extract year from col as y     # parts: year month day hour minute second weekday quarter
date_diff end start as d in days  # units: days hours minutes seconds
filter_date col >= 2024-01-01  # operators: > < >= <= ==
truncate_date col to month     # units: year month week day hour minute second
ts_sort col                    # sort chronologically (ascending)

# ── OUTPUT ───────────────────────────────────────
save "file.csv"                # save as CSV
save "file.json"               # save as JSON
save "file.parquet"            # save as Parquet

# ── VARIABLES ────────────────────────────────────
set name = value               # define $name
set sandbox = /safe/dir        # restrict all file I/O to this tree
env VAR_NAME                   # load OS env var into $VAR_NAME
log "message with $var"        # print during execution

# ── COMMENTS ─────────────────────────────────────
# anything after # is ignored
```

---

*Pipeline DSL is built with Python + Polars. Source code: [github.com/Tomhins/pipeline_dsl](https://github.com/Tomhins/pipeline_dsl)*
