"""AST node definitions for the pipeline DSL.

Each DSL command is represented by exactly one node class.  Every node is a
:mod:`dataclasses` dataclass with an ``execute(context)`` method that reads
and/or mutates the shared :class:`~executor.PipelineContext`.

Node categories
---------------

**Data loading**
    :class:`SourceNode`, :class:`ForeachNode`, :class:`IncludeNode`

**Filtering**
    :class:`FilterNode`, :class:`CompoundFilterNode`

**Column selection / projection**
    :class:`SelectNode`, :class:`DropNode`, :class:`LimitNode`,
    :class:`DistinctNode`, :class:`SampleNode`

**Transformation**
    :class:`SortNode`, :class:`RenameNode`, :class:`AddNode`,
    :class:`AddIfNode`, :class:`TrimNode`, :class:`UppercaseNode`,
    :class:`LowercaseNode`, :class:`CastNode`, :class:`ReplaceNode`,
    :class:`PivotNode`

**Grouping**
    :class:`GroupByNode`

**Aggregation**
    :class:`CountNode`, :class:`CountIfNode`,
    ``SumNode``, ``AvgNode``, ``MinNode``, ``MaxNode`` (built by
    :func:`_agg_node`), :class:`MultiAggNode`

**Multi-source / joining**
    :class:`JoinNode`, :class:`MergeNode`

**Output & inspection**
    :class:`SaveNode`, :class:`PrintNode`, :class:`SchemaNode`,
    :class:`InspectNode`, :class:`HeadNode`, :class:`LogNode`,
    :class:`TimerNode`

**Data quality**
    :class:`AssertNode`, :class:`FillNode`

**Variables & environment**
    :class:`SetNode`, :class:`EnvNode`

**Error recovery**
    :class:`TryNode`

Helper utilities
----------------
:func:`_apply_operator`
    Apply a comparison operator to a pandas Series.
:func:`_resolve_value`
    Resolve a ``$varname`` token to its value in the pipeline context.
:func:`_substitute_vars`
    Replace all ``$varname`` tokens in a string.
:func:`_coerce_rhs`
    Parse a raw string as a float, falling back to a plain string.
:func:`_check_path_sandbox`
    Raise :exc:`PermissionError` if a file path is outside the sandbox.
"""

from __future__ import annotations

import glob as glob_module
import os
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from executor import PipelineContext

# ---------------------------------------------------------------------------
# Supported filter / assert operators
# ---------------------------------------------------------------------------
_OPERATORS: dict[str, str] = {
    ">=": ">=",
    "<=": "<=",
    "!=": "!=",
    "==": "==",
    ">":  ">",
    "<":  "<",
}


def _apply_polars_filter(column: str, op: str, rhs: float | str) -> pl.Expr:
    """Return a Polars boolean filter expression for *column op rhs*."""
    col_expr = pl.col(column)
    if op == ">":
        return col_expr > rhs
    if op == "<":
        return col_expr < rhs
    if op == ">=":
        return col_expr >= rhs
    if op == "<=":
        return col_expr <= rhs
    if op == "==":
        return col_expr == rhs
    if op == "!=":
        return col_expr != rhs
    raise ValueError(f"unsupported operator '{op}'. Supported: {list(_OPERATORS)}")


def _apply_polars_filter_expr(col_expr: pl.Expr, op: str, rhs_expr: pl.Expr) -> pl.Expr:
    """Return a Polars boolean expression comparing two expressions with *op*."""
    if op == ">":
        return col_expr > rhs_expr
    if op == "<":
        return col_expr < rhs_expr
    if op == ">=":
        return col_expr >= rhs_expr
    if op == "<=":
        return col_expr <= rhs_expr
    if op == "==":
        return col_expr == rhs_expr
    if op == "!=":
        return col_expr != rhs_expr
    raise ValueError(f"unsupported operator '{op}'. Supported: {list(_OPERATORS)}")


def _resolve_value(value: str, context: "PipelineContext") -> str:
    """Resolve a simple $varname token from context.variables.

    If *value* starts with ``$``, look it up in ``context.variables``.
    Otherwise return *value* unchanged (still stripped of surrounding spaces).
    """
    stripped = value.strip()
    if stripped.startswith("$"):
        var_name = stripped[1:]
        if var_name not in context.variables:
            raise KeyError(
                f"variable '${var_name}' is not defined. "
                f"Use 'set {var_name} = <value>' first."
            )
        return context.variables[var_name]
    return stripped


def _substitute_vars(text: str, context: "PipelineContext") -> str:
    """Replace every ``$varname`` token in *text* with its value."""
    def _replace(m: re.Match) -> str:
        var_name = m.group(1)
        if var_name not in context.variables:
            raise KeyError(f"variable '${var_name}' is not defined.")
        return context.variables[var_name]

    return re.sub(r'\$(\w+)', _replace, text)


def _coerce_rhs(raw: str) -> float | str:
    """Try to parse *raw* as a float; fall back to a stripped string."""
    cleaned = raw.strip("\"'")
    try:
        return float(cleaned)
    except ValueError:
        return cleaned


def _check_path_sandbox(path: str, context: "PipelineContext") -> None:
    """Raise :exc:`PermissionError` if *path* is outside the sandbox directory.

    When ``context.sandbox_dir`` is ``None`` (the default) every path is
    allowed.  Set a sandbox via ``set sandbox = ./data`` in a pipeline.
    """
    if context.sandbox_dir is None:
        return
    abs_path = os.path.realpath(os.path.abspath(path))
    abs_sandbox = os.path.realpath(os.path.abspath(context.sandbox_dir))
    if not (abs_path == abs_sandbox or abs_path.startswith(abs_sandbox + os.sep)):
        raise PermissionError(
            f"Access denied: '{path}' is outside the sandbox "
            f"'{context.sandbox_dir}'. "
            "Use 'set sandbox = <dir>' to change the allowed directory."
        )


def _str_to_polars_expr(expr_str: str, schema: dict) -> pl.Expr:
    """Convert a simple arithmetic expression string to a Polars Expr.

    Column names in *schema* are substituted with ``pl.col("name")``.
    Example: ``"salary * 0.2"`` → ``pl.col("salary") * 0.2``
    """
    col_names = set(schema.keys())

    def _replace_col(m: re.Match) -> str:
        name = m.group(0)
        if name in col_names:
            return f'_c_("{name}")'
        return name

    modified = re.sub(r'\b([a-zA-Z_]\w*)\b', _replace_col, expr_str)
    try:
        return eval(modified, {"_c_": pl.col, "__builtins__": {}})  # noqa: S307
    except Exception as exc:
        raise ValueError(str(exc)) from exc


def _make_val_expr(v: str, context: "PipelineContext", schema: dict) -> pl.Expr:
    """Return a Polars literal or column expression from a value string."""
    resolved = _resolve_value(v, context).strip("\"'")
    if resolved in schema:
        return pl.col(resolved)
    try:
        return pl.lit(float(resolved))
    except ValueError:
        return pl.lit(resolved)


# ---------------------------------------------------------------------------
# Base node
# ---------------------------------------------------------------------------

class ASTNode:
    """Abstract base class for all pipeline AST nodes."""

    def execute(self, context: "PipelineContext") -> None:  # noqa: D102
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement execute()"
        )


# ---------------------------------------------------------------------------
# Data loading nodes
# ---------------------------------------------------------------------------

@dataclass
class SourceNode(ASTNode):
    """Load a CSV or Parquet file into the pipeline context (Polars lazy mode).

    Supports ``$variable`` references in the file path.

    When *chunk_size* is set, Polars' streaming engine is used at collect
    time to reduce peak memory for large files.

    Examples::

        source "data/people.csv"
        source "data/big.csv" chunk 100000
        source "data/snapshot.parquet"
    """

    file_path: str
    chunk_size: int | None = None

    def execute(self, context: "PipelineContext") -> None:
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Source file not found: '{path}'")
        ext = os.path.splitext(path)[1].lower()
        if ext == ".parquet":
            try:
                context.lf = pl.scan_parquet(path)
            except ImportError:
                raise RuntimeError(
                    "source: reading Parquet files requires 'pyarrow'. "
                    "Install it with: pip install pyarrow"
                )
        elif ext in (".json", ".ndjson"):
            context.lf = pl.scan_ndjson(path)
        else:
            context.lf = pl.scan_csv(path, infer_schema_length=10000)
        if self.chunk_size is not None:
            context.streaming = True
        context.group_by_cols = None


@dataclass
class ForeachNode(ASTNode):
    """Load and concatenate all CSV files matching a glob pattern.

    Example: ``foreach "data/monthly/*.csv"``
    The resulting DataFrame is the row-wise union of all matched files.
    Each matched file is checked against the sandbox when one is active.
    """

    pattern: str

    def execute(self, context: "PipelineContext") -> None:
        pattern = _substitute_vars(self.pattern, context)
        files = sorted(glob_module.glob(pattern))
        if not files:
            raise FileNotFoundError(
                f"foreach: no files matched pattern '{pattern}'"
            )
        for f in files:
            _check_path_sandbox(f, context)
        dfs = [pl.read_csv(f) for f in files]
        context.lf = pl.concat(dfs, how="diagonal").lazy()
        context.group_by_cols = None


@dataclass
class IncludeNode(ASTNode):
    """Include and execute another ``.ppl`` file, sharing the current context.

    Example: ``include "pipelines/shared/clean.ppl"``
    """

    file_path: str

    def execute(self, context: "PipelineContext") -> None:
        from file_reader import read_ppl_file  # avoid circular import at module level
        from ppl_parser import parse_lines

        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        if not os.path.exists(path):
            raise FileNotFoundError(f"include: file not found: '{path}'")
        lines = read_ppl_file(path)
        nodes = parse_lines(lines)
        for sub_node in nodes:
            node_name = sub_node.__class__.__name__
            try:
                sub_node.execute(context)
            except Exception as exc:
                raise RuntimeError(
                    f"include '{path}': [{node_name}] {exc}"
                ) from exc


# ---------------------------------------------------------------------------
# Filtering nodes
# ---------------------------------------------------------------------------

@dataclass
class FilterNode(ASTNode):
    """Filter rows by a single column condition.

    Supports ``$variable`` references in *value*.
    Operators: ``>``, ``<``, ``>=``, ``<=``, ``==``, ``!=``.
    """

    column: str
    operator: str
    value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("filter: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"filter: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        raw = _resolve_value(self.value, context)
        rhs = _coerce_rhs(raw)
        try:
            mask = _apply_polars_filter(self.column, self.operator, rhs)
        except ValueError as exc:
            raise ValueError(f"filter: {exc}") from exc
        context.lf = context.lf.filter(mask)
        context.group_by_cols = None


@dataclass
class CompoundFilterNode(ASTNode):
    """Filter rows using multiple AND / OR conditions on a single line.

    Example: ``filter age >= 18 and country == "Germany"``
    """

    # conditions: list of (column, operator, value) tuples
    # logic:      list of "and" / "or" strings (len = len(conditions) - 1)
    conditions: list
    logic: list

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("filter: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        mask: pl.Expr | None = None
        for i, (col, op, val) in enumerate(self.conditions):
            if col not in schema:
                raise KeyError(
                    f"filter: column '{col}' not found. "
                    f"Available: {list(schema)}"
                )
            raw = _resolve_value(val, context)
            rhs = _coerce_rhs(raw)
            cond_mask = _apply_polars_filter(col, op, rhs)
            if mask is None:
                mask = cond_mask
            else:
                lg = self.logic[i - 1]
                mask = (mask & cond_mask) if lg == "and" else (mask | cond_mask)
        if mask is not None:
            context.lf = context.lf.filter(mask)
        context.group_by_cols = None


# ---------------------------------------------------------------------------
# Column selection / projection nodes
# ---------------------------------------------------------------------------

@dataclass
class SelectNode(ASTNode):
    """Keep only the specified columns in the current DataFrame."""

    columns: list[str]

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("select: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        missing = [c for c in self.columns if c not in schema]
        if missing:
            raise KeyError(
                f"select: unknown column(s) {missing}. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.select(self.columns)
        context.group_by_cols = None


@dataclass
class DropNode(ASTNode):
    """Remove one or more columns from the current DataFrame."""

    columns: list[str]

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("drop: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        missing = [c for c in self.columns if c not in schema]
        if missing:
            raise KeyError(
                f"drop: unknown column(s) {missing}. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.drop(self.columns)
        context.group_by_cols = None


@dataclass
class LimitNode(ASTNode):
    """Keep only the first *n* rows."""

    n: int

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("limit: no data loaded — use 'source' first")
        context.lf = context.lf.limit(self.n)
        context.group_by_cols = None


@dataclass
class DistinctNode(ASTNode):
    """Remove duplicate rows from the current DataFrame."""

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("distinct: no data loaded — use 'source' first")
        context.lf = context.lf.unique()
        context.group_by_cols = None


@dataclass
class SampleNode(ASTNode):
    """Take a random sample of N rows or N% of the data.

    Examples: ``sample 100``  or  ``sample 10%``
    """

    n: int | None
    pct: float | None

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("sample: no data loaded — use 'source' first")
        df = context.lf.collect()
        if self.pct is not None:
            result = df.sample(fraction=self.pct / 100.0)
        else:
            n = min(self.n, len(df))
            result = df.sample(n=n)
        context.lf = result.lazy()
        context.group_by_cols = None


# ---------------------------------------------------------------------------
# Data transformation nodes
# ---------------------------------------------------------------------------

@dataclass
class SortNode(ASTNode):
    """Sort the current DataFrame by one or more columns."""

    columns: list[str]
    ascending: list[bool]

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("sort: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        missing = [c for c in self.columns if c not in schema]
        if missing:
            raise KeyError(
                f"sort: unknown column(s) {missing}. "
                f"Available: {list(schema)}"
            )
        descending = [not a for a in self.ascending]
        context.lf = context.lf.sort(self.columns, descending=descending)
        context.group_by_cols = None


@dataclass
class RenameNode(ASTNode):
    """Rename a single column."""

    old_name: str
    new_name: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("rename: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.old_name not in schema:
            raise KeyError(
                f"rename: column '{self.old_name}' not found. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.rename({self.old_name: self.new_name})
        context.group_by_cols = None


@dataclass
class AddNode(ASTNode):
    """Add a computed column using an arithmetic expression.

    Supports ``$variable`` substitution in the expression.
    Column names in the expression are resolved to ``pl.col("name")``.
    """

    column: str
    expression: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("add: no data loaded — use 'source' first")
        try:
            expr_str = _substitute_vars(self.expression, context)
            schema = dict(context.lf.collect_schema())
            polars_expr = _str_to_polars_expr(expr_str, schema)
            context.lf = context.lf.with_columns(polars_expr.alias(self.column))
        except Exception as exc:
            raise ValueError(
                f"add: could not evaluate expression '{self.expression}': {exc}"
            ) from exc
        context.group_by_cols = None


@dataclass
class AddIfNode(ASTNode):
    """Add a column whose value depends on a condition (if/then/else).

    Example: ``add tier = if salary > 80000 then "senior" else "junior"``
    """

    column: str
    cond_col: str
    cond_op: str
    cond_val: str
    true_val: str
    false_val: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("add: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.cond_col not in schema:
            raise KeyError(
                f"add: column '{self.cond_col}' not found. "
                f"Available: {list(schema)}"
            )
        raw = _resolve_value(self.cond_val, context)
        rhs = _coerce_rhs(raw)
        mask = _apply_polars_filter(self.cond_col, self.cond_op, rhs)
        true_expr = _make_val_expr(self.true_val, context, schema)
        false_expr = _make_val_expr(self.false_val, context, schema)
        context.lf = context.lf.with_columns(
            pl.when(mask).then(true_expr).otherwise(false_expr).alias(self.column)
        )
        context.group_by_cols = None


@dataclass
class TrimNode(ASTNode):
    """Strip leading/trailing whitespace from a string column."""

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("trim: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"trim: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.with_columns(
            pl.col(self.column).cast(pl.String).str.strip_chars()
        )
        context.group_by_cols = None


@dataclass
class UppercaseNode(ASTNode):
    """Convert a string column to uppercase."""

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("uppercase: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"uppercase: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.with_columns(
            pl.col(self.column).cast(pl.String).str.to_uppercase()
        )
        context.group_by_cols = None


@dataclass
class LowercaseNode(ASTNode):
    """Convert a string column to lowercase."""

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("lowercase: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"lowercase: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.with_columns(
            pl.col(self.column).cast(pl.String).str.to_lowercase()
        )
        context.group_by_cols = None


_POLARS_TYPE_MAP: dict[str, Any] = {
    "int":      pl.Int64,
    "integer":  pl.Int64,
    "float":    pl.Float64,
    "double":   pl.Float64,
    "str":      pl.String,
    "string":   pl.String,
    "text":     pl.String,
    "datetime": pl.Datetime,
    "date":     pl.Date,
    "bool":     pl.Boolean,
    "boolean":  pl.Boolean,
}


@dataclass
class CastNode(ASTNode):
    """Cast a column to a different data type.

    Example: ``cast age int``  |  ``cast score float``  |  ``cast ts datetime``
    """

    column: str
    type_name: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("cast: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"cast: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        t = self.type_name.lower()
        if t not in _POLARS_TYPE_MAP:
            raise ValueError(
                f"cast: unknown type '{self.type_name}'. "
                f"Supported: {', '.join(sorted(_POLARS_TYPE_MAP))}"
            )
        target_type = _POLARS_TYPE_MAP[t]
        context.lf = context.lf.with_columns(
            pl.col(self.column).cast(target_type, strict=False)
        )
        context.group_by_cols = None


@dataclass
class ReplaceNode(ASTNode):
    """Replace occurrences of a specific value in a column.

    Example: ``replace country "Germany" "DE"``
    """

    column: str
    old_value: str
    new_value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("replace: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"replace: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        old = _coerce_rhs(self.old_value)
        new = _coerce_rhs(self.new_value)
        context.lf = context.lf.with_columns(
            pl.when(pl.col(self.column) == old)
            .then(pl.lit(new))
            .otherwise(pl.col(self.column))
            .alias(self.column)
        )
        context.group_by_cols = None


@dataclass
class PivotNode(ASTNode):
    """Reshape data from long to wide format.

    Example: ``pivot index=country column=year value=revenue``
    """

    index: str
    column: str
    value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("pivot: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        for col in [self.index, self.column, self.value]:
            if col not in schema:
                raise KeyError(
                    f"pivot: column '{col}' not found. "
                    f"Available: {list(schema)}"
                )
        df = context.lf.collect()
        result = df.pivot(
            values=self.value,
            index=self.index,
            on=self.column,
            aggregate_function="sum",
        )
        context.lf = result.lazy()
        context.group_by_cols = None


# ---------------------------------------------------------------------------
# Grouping node
# ---------------------------------------------------------------------------

@dataclass
class GroupByNode(ASTNode):
    """Group the current DataFrame by one or more columns."""

    columns: list[str]

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("group by: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        missing = [c for c in self.columns if c not in schema]
        if missing:
            raise KeyError(
                f"group by: unknown column(s) {missing}. "
                f"Available: {list(schema)}"
            )
        context.group_by_cols = self.columns


# ---------------------------------------------------------------------------
# Aggregation nodes
# ---------------------------------------------------------------------------

@dataclass
class CountNode(ASTNode):
    """Count rows — per group if ``group by`` is active, otherwise total."""

    def execute(self, context: "PipelineContext") -> None:
        if context.group_by_cols is not None:
            context.lf = context.lf.group_by(context.group_by_cols).agg(
                pl.len().alias("count")
            )
            context.group_by_cols = None
        elif context.lf is not None:
            context.lf = context.lf.select(pl.len().alias("count"))
        else:
            raise RuntimeError("count: no data loaded — use 'source' first")


@dataclass
class CountIfNode(ASTNode):
    """Print the count of rows matching a condition without modifying the data.

    Example: ``count if salary > 50000``
    """

    column: str
    operator: str
    value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("count if: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"count if: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        raw = _resolve_value(self.value, context)
        rhs = _coerce_rhs(raw)
        mask = _apply_polars_filter(self.column, self.operator, rhs)
        count = context.lf.filter(mask).select(pl.len()).collect().item()
        print(f"count if {self.column} {self.operator} {self.value}: {count}")


def _agg_node(verb: str, agg_fn: str):
    """Factory that returns a dataclass-based single-column aggregation node."""

    @dataclass
    class _AggNode(ASTNode):
        column: str

        def execute(self, _context: "PipelineContext") -> None:
            if _context.lf is None:
                raise RuntimeError(
                    f"{verb}: no data loaded — use 'source' first"
                )
            schema = dict(_context.lf.collect_schema())
            if self.column not in schema:
                raise KeyError(
                    f"{verb}: column '{self.column}' not found. "
                    f"Available: {list(schema)}"
                )
            agg_expr = getattr(pl.col(self.column), agg_fn)()
            if _context.group_by_cols is not None:
                _context.lf = _context.lf.group_by(_context.group_by_cols).agg(
                    agg_expr
                )
                _context.group_by_cols = None
            else:
                _context.lf = _context.lf.select(agg_expr)

        def __class_getitem__(cls, item):  # keep dataclass introspection happy
            return cls

    _AggNode.__name__ = f"{verb.capitalize()}Node"
    _AggNode.__qualname__ = _AggNode.__name__
    return _AggNode


SumNode = _agg_node("sum", "sum")
AvgNode = _agg_node("avg", "mean")
MinNode = _agg_node("min", "min")
MaxNode = _agg_node("max", "max")


@dataclass
class MultiAggNode(ASTNode):
    """Apply multiple aggregations at once after a ``group by``.

    Example (after ``group by country``)::

        agg sum salary, avg age, count

    ``specs`` is a list of ``(verb, column_or_None)`` tuples where *verb* is
    one of ``sum``, ``avg``, ``min``, ``max``, or ``count``.
    """

    specs: list  # list of (verb: str, col: str | None)

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("agg: no data loaded — use 'source' first")
        if context.group_by_cols is None:
            raise RuntimeError(
                "agg: must follow 'group by'. "
                "Example:\n  group by country\n  agg sum salary, avg age, count"
            )

        group_cols = context.group_by_cols

        _FN_MAP = {"sum": "sum", "avg": "mean", "min": "min", "max": "max"}
        agg_exprs: list[pl.Expr] = []
        schema = dict(context.lf.collect_schema())

        for verb, col in self.specs:
            if verb == "count":
                agg_exprs.append(pl.len().alias("count"))
                continue
            if col is None:
                raise ValueError(f"agg: '{verb}' requires a column name")
            if col not in schema:
                raise KeyError(
                    f"agg: column '{col}' not found. "
                    f"Available: {list(schema)}"
                )
            agg_exprs.append(getattr(pl.col(col), _FN_MAP[verb])())

        if not agg_exprs:
            raise ValueError("agg: no valid aggregation specs provided")

        context.lf = context.lf.group_by(group_cols).agg(agg_exprs)
        context.group_by_cols = None


# ---------------------------------------------------------------------------
# Data joining / multi-source nodes
# ---------------------------------------------------------------------------

# Polars uses "full" instead of "outer" for full outer joins.
_JOIN_HOW_MAP = {"inner": "inner", "left": "left", "right": "right", "outer": "full"}


@dataclass
class JoinNode(ASTNode):
    """Join the current LazyFrame with another CSV on a key column.

    Supports all standard join types via the *how* parameter.

    Examples::

        join "lookup.csv" on id               # inner join (default)
        join "lookup.csv" on id left          # left join — keep all left rows
        join "lookup.csv" on id right         # right join
        join "lookup.csv" on id outer         # full outer join
    """

    file_path: str
    key: str
    how: str = "inner"

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("join: no data loaded — use 'source' first")
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        if not os.path.exists(path):
            raise FileNotFoundError(f"join: file not found: '{path}'")
        schema = dict(context.lf.collect_schema())
        if self.key not in schema:
            raise KeyError(
                f"join: key '{self.key}' not in current data. "
                f"Available: {list(schema)}"
            )
        right_lf = pl.scan_csv(path, infer_schema_length=10000)
        right_schema = dict(right_lf.collect_schema())
        if self.key not in right_schema:
            raise KeyError(
                f"join: key '{self.key}' not found in '{path}'. "
                f"Available: {list(right_schema)}"
            )
        pl_how = _JOIN_HOW_MAP.get(self.how, self.how)
        coalesce = pl_how in {"full", "right"}
        context.lf = context.lf.join(right_lf, on=self.key, how=pl_how, coalesce=coalesce)
        context.group_by_cols = None


@dataclass
class MergeNode(ASTNode):
    """Append rows from another CSV file (union / stack)."""

    file_path: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("merge: no data loaded — use 'source' first")
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        if not os.path.exists(path):
            raise FileNotFoundError(f"merge: file not found: '{path}'")
        other_lf = pl.scan_csv(path, infer_schema_length=10000)
        context.lf = pl.concat([context.lf, other_lf], how="diagonal")
        context.group_by_cols = None


# ---------------------------------------------------------------------------
# Output / inspection nodes
# ---------------------------------------------------------------------------

@dataclass
class SaveNode(ASTNode):
    """Write the current DataFrame to a CSV, JSON, or Parquet file.

    The output format is determined by the file extension:
    ``.csv`` → CSV, ``.json`` → JSON, ``.parquet`` → Parquet.
    """

    file_path: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("save: no data to save — pipeline produced no output")
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        out_dir = os.path.dirname(path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        df = context.lf.collect()
        ext = os.path.splitext(path)[1].lower()
        if ext == ".json":
            df.write_json(path)
        elif ext == ".parquet":
            try:
                df.write_parquet(path)
            except ImportError:
                raise RuntimeError(
                    "save: writing Parquet files requires 'pyarrow'. "
                    "Install it with: pip install pyarrow"
                )
        else:
            df.write_csv(path)


@dataclass
class PrintNode(ASTNode):
    """Print the current DataFrame to stdout without saving."""

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("print: no data loaded — use 'source' first")
        df = context.lf.collect()
        print(df.to_pandas().to_string(index=False))


@dataclass
class SchemaNode(ASTNode):
    """Print the column names and data types of the current DataFrame."""

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("schema: no data loaded — use 'source' first")
        schema = context.lf.collect_schema()
        col_names = list(schema.keys())
        col_types = list(schema.values())
        row_count = context.lf.select(pl.len()).collect().item()
        print(
            f"\nSchema  ({len(col_names)} column(s), {row_count} row(s)):"
        )
        print(f"  {'Column':<22} {'Type'}")
        print("  " + "-" * 34)
        for col, dtype in zip(col_names, col_types):
            print(f"  {col:<22} {dtype}")
        print()


@dataclass
class InspectNode(ASTNode):
    """Print column names, types, null counts, and unique value counts."""

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("inspect: no data loaded — use 'source' first")
        df = context.lf.collect()
        print(
            f"\nInspect  ({len(df.columns)} column(s), {len(df)} row(s)):"
        )
        print(f"  {'Column':<22} {'Type':<12} {'Nulls':<8} {'Unique'}")
        print("  " + "-" * 50)
        for col in df.columns:
            nulls = df[col].null_count()
            if df[col].dtype in (pl.String, pl.Utf8):
                nulls += int((df[col] == "").sum())
            unique = df[col].n_unique()
            print(
                f"  {col:<22} {str(df[col].dtype):<12} "
                f"{nulls:<8} {unique}"
            )
        print()


@dataclass
class HeadNode(ASTNode):
    """Print the first *n* rows to stdout without modifying pipeline data."""

    n: int

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("head: no data loaded — use 'source' first")
        df = context.lf.collect()
        print(f"\nHead ({self.n} row(s)):")
        print(df.head(self.n).to_pandas().to_string(index=False))
        print()


@dataclass
class LogNode(ASTNode):
    """Print a message to the terminal during pipeline execution.

    Supports ``$variable`` substitution in the message.
    Example: ``log "Processing $label data"``
    """

    message: str

    def execute(self, context: "PipelineContext") -> None:
        msg = _substitute_vars(self.message.strip("\"'"), context)
        print(f"[LOG] {msg}")


@dataclass
class TimerNode(ASTNode):
    """Start, stop, or lap a named timer to measure pipeline step durations.

    Timer state is stored in ``context.variables`` so it persists for the
    entire pipeline run.  The label is optional; omitting it uses ``default``.

    Examples::

        timer start loading
        source "big.csv" chunk 50000
        timer stop loading          # prints: [TIMER] loading: 3.42s

        timer start                 # label defaults to "default"
        filter age > 18
        timer lap                   # checkpoint — keeps the timer running
        sort by salary desc
        timer stop                  # prints total elapsed
    """

    action: str   # "start" | "stop" | "lap"
    label: str    # name for this timer; defaults to "default"

    def execute(self, context: "PipelineContext") -> None:
        import time
        key = f"__timer_{self.label}"
        if self.action == "start":
            context.variables[key] = time.perf_counter()
        elif self.action in ("stop", "lap"):
            t0 = context.variables.get(key)
            if t0 is None:
                raise RuntimeError(
                    f"timer: no timer named '{self.label}' is running. "
                    "Use 'timer start <label>' first."
                )
            elapsed = time.perf_counter() - t0
            mins = int(elapsed // 60)
            secs = elapsed % 60
            formatted = f"{mins}m {secs:.3f}s" if mins else f"{secs:.3f}s"
            tag = "LAP" if self.action == "lap" else "TIMER"
            print(f"[{tag}] {self.label}: {formatted}")
            if self.action == "stop":
                del context.variables[key]
        else:
            raise ValueError(
                f"timer: unknown action '{self.action}'. Use start, stop, or lap."
            )


# ---------------------------------------------------------------------------
# Quality / validation nodes
# ---------------------------------------------------------------------------

@dataclass
class AssertNode(ASTNode):
    """Fail the pipeline if any row violates a column condition."""

    column: str
    operator: str
    value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("assert: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"assert: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        raw = _resolve_value(self.value, context)
        rhs = _coerce_rhs(raw)
        try:
            pass_mask = _apply_polars_filter(self.column, self.operator, rhs)
        except ValueError as exc:
            raise ValueError(f"assert: {exc}") from exc
        failures = context.lf.filter(~pass_mask).select(pl.len()).collect().item()
        if failures:
            raise AssertionError(
                f"assert: {failures} row(s) failed condition "
                f"'{self.column} {self.operator} {self.value}'"
            )


@dataclass
class FillNode(ASTNode):
    """Fill missing / empty values in a column.

    *strategy* can be a fill strategy (``mean``, ``median``, ``mode``,
    ``forward``, ``backward``, ``drop``) or a literal value.
    """

    column: str
    strategy: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("fill: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"fill: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        col = self.column
        dtype = schema[col]
        s = self.strategy.strip().lower()

        # For string columns, treat empty string as null first
        if dtype in (pl.String, pl.Utf8):
            context.lf = context.lf.with_columns(
                pl.when(pl.col(col) == "").then(None).otherwise(pl.col(col)).alias(col)
            )

        if s == "mean":
            mean_val = context.lf.select(
                pl.col(col).cast(pl.Float64).mean()
            ).collect().item()
            context.lf = context.lf.with_columns(
                pl.col(col).cast(pl.Float64).fill_null(mean_val).alias(col)
            )
        elif s == "median":
            median_val = context.lf.select(
                pl.col(col).cast(pl.Float64).median()
            ).collect().item()
            context.lf = context.lf.with_columns(
                pl.col(col).cast(pl.Float64).fill_null(median_val).alias(col)
            )
        elif s == "mode":
            df_tmp = context.lf.collect()
            mode_vals = df_tmp[col].mode()
            if len(mode_vals) > 0:
                fill_val = mode_vals[0]
                context.lf = context.lf.with_columns(
                    pl.col(col).fill_null(pl.lit(fill_val)).alias(col)
                )
        elif s == "forward":
            context.lf = context.lf.with_columns(
                pl.col(col).fill_null(strategy="forward")
            )
        elif s == "backward":
            context.lf = context.lf.with_columns(
                pl.col(col).fill_null(strategy="backward")
            )
        elif s == "drop":
            context.lf = context.lf.filter(pl.col(col).is_not_null())
        else:
            raw = self.strategy.strip("\"'")
            try:
                fill_val: float | int | str = float(raw)
                if fill_val == int(fill_val):
                    fill_val = int(fill_val)
            except ValueError:
                fill_val = raw
            context.lf = context.lf.with_columns(
                pl.col(col).fill_null(fill_val).alias(col)
            )

        context.group_by_cols = None


# ---------------------------------------------------------------------------
# Variable / environment nodes
# ---------------------------------------------------------------------------

@dataclass
class SetNode(ASTNode):
    """Set a named variable, referenceable as ``$name`` in other commands.

    Example: ``set threshold = 50000``
    Then use it:  ``filter salary > $threshold``
    """

    name: str
    value: str

    def execute(self, context: "PipelineContext") -> None:
        value = self.value.strip("\"'")
        context.variables[self.name] = value
        # Special variable: 'sandbox' activates the filesystem sandbox.
        if self.name == "sandbox":
            context.sandbox_dir = value


@dataclass
class EnvNode(ASTNode):
    """Load an OS environment variable into the pipeline variable store.

    Example: ``env DATA_PATH``
    Then use it:  ``source $DATA_PATH``
    """

    var_name: str

    def execute(self, context: "PipelineContext") -> None:
        val = os.environ.get(self.var_name)
        if val is None:
            raise RuntimeError(
                f"env: environment variable '{self.var_name}' is not set"
            )
        context.variables[self.var_name] = val


# ---------------------------------------------------------------------------
# Error recovery node
# ---------------------------------------------------------------------------

@dataclass
class TryNode(ASTNode):
    """Execute a block of commands, running an error handler if any fail.

    The body commands are executed in order.  If any raises an exception:

    * ``on_error skip``  — silently swallow the error and continue.
    * ``on_error log "message"`` — print *message* (plus the error) and continue.
    * ``on_error <command>`` — execute *command* (e.g. ``fill age 0``) and
      continue.

    ``body`` holds pre-parsed :class:`ASTNode` objects for the try block.
    ``on_error_nodes`` holds pre-parsed nodes for the handler (empty for
    ``skip`` and ``log`` actions, which are handled inline).
    ``error_action`` is the raw string after ``on_error`` for display /
    ``log`` message extraction.
    """

    body: list          # list[ASTNode]
    on_error_nodes: list  # list[ASTNode], empty for skip/log actions
    error_action: str   # raw on_error argument string

    def execute(self, context: "PipelineContext") -> None:
        try:
            for node in self.body:
                node.execute(context)
        except Exception as exc:
            action_lower = self.error_action.strip().lower()
            if action_lower == "skip":
                pass  # silently continue
            elif action_lower.startswith("log "):
                msg = _substitute_vars(
                    self.error_action[4:].strip().strip("\"'"), context
                )
                print(f"[TRY] {msg}: {exc}")
            else:
                for node in self.on_error_nodes:
                    node.execute(context)


# ---------------------------------------------------------------------------
# Timestamp / date-time nodes  (Task 2)
# ---------------------------------------------------------------------------

_EXTRACT_PARTS: dict[str, Any] = {
    "year":    lambda col: pl.col(col).dt.year(),
    "month":   lambda col: pl.col(col).dt.month(),
    "day":     lambda col: pl.col(col).dt.day(),
    "hour":    lambda col: pl.col(col).dt.hour(),
    "minute":  lambda col: pl.col(col).dt.minute(),
    "second":  lambda col: pl.col(col).dt.second(),
    "weekday": lambda col: pl.col(col).dt.weekday(),
    "quarter": lambda col: pl.col(col).dt.quarter(),
}

_TRUNCATE_UNIT_MAP: dict[str, str] = {
    "year":  "1y",
    "month": "1mo",
    "week":  "1w",
    "day":   "1d",
    "hour":  "1h",
}

_DATE_DIFF_UNITS: dict[str, Any] = {
    "days":    lambda expr: expr.dt.total_days(),
    "hours":   lambda expr: expr.dt.total_hours(),
    "minutes": lambda expr: expr.dt.total_minutes(),
    "seconds": lambda expr: expr.dt.total_seconds(),
}


@dataclass
class ParseDateNode(ASTNode):
    """Parse a string column into a Polars Datetime type.

    Example::

        parse_date order_date "%Y-%m-%d"
        parse_date timestamp "%d/%m/%Y %H:%M:%S"
    """

    column: str
    format: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("parse_date: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"parse_date: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.with_columns(
            pl.col(self.column).str.to_datetime(self.format, strict=False).alias(self.column)
        )
        context.group_by_cols = None


@dataclass
class ExtractNode(ASTNode):
    """Extract a date/time component from a datetime column into a new integer column.

    Supported parts: ``year``, ``month``, ``day``, ``hour``, ``minute``,
    ``second``, ``weekday``, ``quarter``.

    Example::

        extract year from order_date as order_year
        extract weekday from order_date as day_of_week
    """

    part: str
    column: str
    new_column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("extract: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"extract: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        if self.part not in _EXTRACT_PARTS:
            raise ValueError(
                f"extract: unsupported part '{self.part}'. "
                f"Supported: {', '.join(sorted(_EXTRACT_PARTS))}"
            )
        context.lf = context.lf.with_columns(
            _EXTRACT_PARTS[self.part](self.column).alias(self.new_column)
        )
        context.group_by_cols = None


@dataclass
class DateDiffNode(ASTNode):
    """Compute the difference between two datetime columns.

    Units: ``days``, ``hours``, ``minutes``, ``seconds``.

    Example::

        date_diff shipped_date order_date as days_to_ship in days
    """

    col1: str
    col2: str
    new_column: str
    unit: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("date_diff: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        for col in [self.col1, self.col2]:
            if col not in schema:
                raise KeyError(
                    f"date_diff: column '{col}' not found. "
                    f"Available: {list(schema)}"
                )
        if self.unit not in _DATE_DIFF_UNITS:
            raise ValueError(
                f"date_diff: unsupported unit '{self.unit}'. "
                f"Supported: {', '.join(sorted(_DATE_DIFF_UNITS))}"
            )
        diff_expr = pl.col(self.col1) - pl.col(self.col2)
        result_expr = _DATE_DIFF_UNITS[self.unit](diff_expr)
        context.lf = context.lf.with_columns(result_expr.alias(self.new_column))
        context.group_by_cols = None


@dataclass
class FilterDateNode(ASTNode):
    """Filter rows by comparing a datetime column to a literal date.

    Operators: ``>``, ``<``, ``>=``, ``<=``, ``==``.

    Example::

        filter_date order_date >= 2024-01-01
        filter_date order_date < 2024-06-01
    """

    column: str
    operator: str
    date_str: str

    def execute(self, context: "PipelineContext") -> None:
        import datetime as _dt
        if context.lf is None:
            raise RuntimeError("filter_date: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"filter_date: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        try:
            date_val = _dt.date.fromisoformat(self.date_str)
        except ValueError as exc:
            raise ValueError(
                f"filter_date: invalid date '{self.date_str}'. "
                "Use YYYY-MM-DD format."
            ) from exc
        mask = _apply_polars_filter_expr(
            pl.col(self.column), self.operator, pl.lit(date_val)
        )
        context.lf = context.lf.filter(mask)
        context.group_by_cols = None


@dataclass
class TruncateDateNode(ASTNode):
    """Truncate a datetime column to a given precision, in place.

    Units: ``year``, ``month``, ``week``, ``day``, ``hour``.

    Example::

        truncate_date order_date to month
    """

    column: str
    unit: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("truncate_date: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"truncate_date: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        if self.unit not in _TRUNCATE_UNIT_MAP:
            raise ValueError(
                f"truncate_date: unsupported unit '{self.unit}'. "
                f"Supported: {', '.join(sorted(_TRUNCATE_UNIT_MAP))}"
            )
        duration = _TRUNCATE_UNIT_MAP[self.unit]
        context.lf = context.lf.with_columns(
            pl.col(self.column).dt.truncate(duration).alias(self.column)
        )
        context.group_by_cols = None


@dataclass
class TsSortNode(ASTNode):
    """Sort the pipeline chronologically by a datetime column (ascending).

    Shorthand for ``sort by <column> asc`` typed for datetime columns.

    Example::

        ts_sort order_date
    """

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.lf is None:
            raise RuntimeError("ts_sort: no data loaded — use 'source' first")
        schema = dict(context.lf.collect_schema())
        if self.column not in schema:
            raise KeyError(
                f"ts_sort: column '{self.column}' not found. "
                f"Available: {list(schema)}"
            )
        context.lf = context.lf.sort(self.column, descending=False)
        context.group_by_cols = None
