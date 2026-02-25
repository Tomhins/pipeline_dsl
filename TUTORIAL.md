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
15. [Feature Suggestions](#15-feature-suggestions)
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

## 15. Feature Suggestions

Here are ideas for features that would make Pipeline DSL significantly more powerful.
They're grouped from easiest to most ambitious.

---

### Tier 1 — Easy Wins

**`sample N` / `sample N%`**
Take a random sample of rows. Essential for working with large files.

```
source "data/huge_file.csv"
sample 1000        # random 1000 rows
sample 10%         # random 10% of rows
```

**`uppercase` / `lowercase` / `trim`**
String normalization commands for text columns. Dirty strings are extremely
common in real data.

```
trim country        # remove leading/trailing whitespace
uppercase country   # "germany" → "GERMANY"
lowercase name      # "ALICE" → "alice"
```

**`cast <column> <type>`**
Explicit type conversion. Useful when CSVs load numbers as strings.

```
cast age int
cast salary float
cast date datetime
```

**`count if <condition>`**
Count rows matching a condition without filtering the dataset.

```
count if salary > 50000
# Prints a number but doesn't remove any rows
```

---

### Tier 2 — Medium Impact

**`where` as an alias for `filter`**
Many users coming from SQL will instinctively write `where`. Supporting both
keywords reduces the learning curve.

```
where age >= 18    # same as: filter age >= 18
```

**Multi-condition `filter` with `and` / `or`**
Currently filters must be chained across multiple lines. Single-line compound
conditions would be much more expressive.

```
filter age >= 18 and country == "Germany"
filter salary > 50000 or country == "USA"
```

**`replace <column> <old_value> <new_value>`**
Find-and-replace for column values. Useful for fixing data inconsistencies.

```
replace country "Germ" "Germany"
replace status "active" "Active"
```

**`if <condition> then <column> = <value> else <value>`**
Conditional column assignment — similar to `CASE WHEN` in SQL or `np.where`.

```
add tier = if salary > 80000 then "senior" else "junior"
```

**`pivot`**
Reshape data from long to wide format. A very common data transformation.

```
pivot index=country column=year value=revenue
```

---

### Tier 3 — Power Features

**Variables / `set`**
Allow named values to be defined once and reused.

```
set threshold = 50000
filter salary > $threshold
add above_threshold = salary - $threshold
```

**Multiple aggregations in one `group by` block**
Right now each `group by` only supports one aggregation. Allow several at once:

```
group by country
  sum salary
  avg age
  count
```

**`foreach <file_pattern>` — batch processing**
Run the same pipeline over multiple files at once.

```
foreach "data/monthly/*.csv"
  filter status == "completed"
  save "output/{filename}_cleaned.csv"
```

**`log <message>`**
Print a custom message to the terminal during pipeline execution — great for
debugging long pipelines.

```
source "data/people.csv"
log "Loaded people data"
filter age >= 18
log "Filtered to adults"
```

**`env <VAR>`**
Read values from environment variables, so secrets (like file paths or
credentials) don't have to be hardcoded in `.ppl` files.

```
source env DATA_PATH
```

**Pipeline `import` / `include`**
Reuse common pipeline logic across multiple files — like a shared "clean data"
step.

```
include "pipelines/shared/clean_input.ppl"
filter salary > 50000
```

---

## 16. Quick Reference Card

```
# ── LOADING ──────────────────────────────────────
source "file.csv"              # load CSV
join "other.csv" on column     # inner join on key column
merge "other.csv"              # append rows (stack vertically)

# ── INSPECTING ───────────────────────────────────
schema                         # column names + types
inspect                        # names, types, nulls, unique counts
head N                         # print first N rows (non-destructive)
print                          # print all rows

# ── FILTERING ────────────────────────────────────
filter col > value             # operators: > < >= <= == !=
distinct                       # remove duplicate rows
limit N                        # keep first N rows

# ── COLUMNS ──────────────────────────────────────
select col1, col2              # keep only these columns
drop col1, col2                # remove these columns
rename old new                 # rename a column

# ── TRANSFORMING ─────────────────────────────────
add col = expr                 # new computed column (+ - * /)
sort by col asc                # sort (asc default, or desc)
sort by col1 asc, col2 desc    # multi-column sort

# ── MISSING VALUES ───────────────────────────────
fill col mean                  # strategies: mean median mode
fill col forward               #             forward backward drop
fill col "literal"             # or a literal value

# ── AGGREGATION ──────────────────────────────────
group by col1, col2            # group (must be followed by agg)
count                          # count rows / group sizes
sum col                        # sum of column
avg col                        # average of column
min col                        # minimum value
max col                        # maximum value

# ── QUALITY ──────────────────────────────────────
assert col > value             # fail pipeline if condition broken

# ── OUTPUT ───────────────────────────────────────
save "file.csv"                # save as CSV
save "file.json"               # save as JSON

# ── COMMENTS ─────────────────────────────────────
# anything after # is ignored
```

---

*Pipeline DSL is built with Python + pandas. Source code: [github.com/Tomhins/pipeline_dsl](https://github.com/Tomhins/pipeline_dsl)*
