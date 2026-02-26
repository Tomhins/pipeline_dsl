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

import pandas as pd

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


def _apply_operator(col: pd.Series, op: str, rhs: float | str) -> pd.Series:
    """Return a boolean mask applying *op* between *col* and *rhs*."""
    if op == ">":
        return col > rhs
    if op == "<":
        return col < rhs
    if op == ">=":
        return col >= rhs
    if op == "<=":
        return col <= rhs
    if op == "==":
        return col == rhs
    if op == "!=":
        return col != rhs
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
    """Load a CSV or Parquet file into the pipeline context.

    Supports ``$variable`` references in the file path.

    When *chunk_size* is set the executor automatically switches to
    chunked mode (see :func:`~executor._run_chunked_pipeline`).  Calling
    ``execute`` directly still loads the full file so tests and sub-pipelines
    work without changes.

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
                context.df = pd.read_parquet(path)
            except ImportError:
                raise RuntimeError(
                    "source: reading Parquet files requires 'pyarrow'. "
                    "Install it with: pip install pyarrow"
                )
        else:
            # Try pyarrow CSV engine first (3-6x faster than C engine for
            # large files); fall back silently if the file has mixed-type
            # columns that pyarrow can't classify.
            try:
                context.df = pd.read_csv(path, engine="pyarrow")
            except Exception:
                context.df = pd.read_csv(path, low_memory=False)
        context.grouped = None


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
        dfs = [pd.read_csv(f) for f in files]
        context.df = pd.concat(dfs, ignore_index=True)
        context.grouped = None


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
        if context.df is None:
            raise RuntimeError("filter: no data loaded — use 'source' first")
        df = context.df
        if self.column not in df.columns:
            raise KeyError(
                f"filter: column '{self.column}' not found. "
                f"Available: {list(df.columns)}"
            )
        raw = _resolve_value(self.value, context)
        rhs = _coerce_rhs(raw)
        try:
            mask = _apply_operator(df[self.column], self.operator, rhs)
        except ValueError as exc:
            raise ValueError(f"filter: {exc}") from exc
        context.df = df[mask]
        context.grouped = None


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
        if context.df is None:
            raise RuntimeError("filter: no data loaded — use 'source' first")
        df = context.df
        mask = None
        for i, (col, op, val) in enumerate(self.conditions):
            if col not in df.columns:
                raise KeyError(
                    f"filter: column '{col}' not found. "
                    f"Available: {list(df.columns)}"
                )
            raw = _resolve_value(val, context)
            rhs = _coerce_rhs(raw)
            cond_mask = _apply_operator(df[col], op, rhs)
            if mask is None:
                mask = cond_mask
            else:
                lg = self.logic[i - 1]
                mask = (mask & cond_mask) if lg == "and" else (mask | cond_mask)
        context.df = df[mask]
        context.grouped = None


# ---------------------------------------------------------------------------
# Column selection / projection nodes
# ---------------------------------------------------------------------------

@dataclass
class SelectNode(ASTNode):
    """Keep only the specified columns in the current DataFrame."""

    columns: list[str]

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("select: no data loaded — use 'source' first")
        missing = [c for c in self.columns if c not in context.df.columns]
        if missing:
            raise KeyError(
                f"select: unknown column(s) {missing}. "
                f"Available: {list(context.df.columns)}"
            )
        context.df = context.df[self.columns]
        context.grouped = None


@dataclass
class DropNode(ASTNode):
    """Remove one or more columns from the current DataFrame."""

    columns: list[str]

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("drop: no data loaded — use 'source' first")
        missing = [c for c in self.columns if c not in context.df.columns]
        if missing:
            raise KeyError(
                f"drop: unknown column(s) {missing}. "
                f"Available: {list(context.df.columns)}"
            )
        context.df = context.df.drop(columns=self.columns)
        context.grouped = None


@dataclass
class LimitNode(ASTNode):
    """Keep only the first *n* rows."""

    n: int

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("limit: no data loaded — use 'source' first")
        context.df = context.df.head(self.n).reset_index(drop=True)
        context.grouped = None


@dataclass
class DistinctNode(ASTNode):
    """Remove duplicate rows from the current DataFrame."""

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("distinct: no data loaded — use 'source' first")
        context.df = context.df.drop_duplicates().reset_index(drop=True)
        context.grouped = None


@dataclass
class SampleNode(ASTNode):
    """Take a random sample of N rows or N% of the data.

    Examples: ``sample 100``  or  ``sample 10%``
    """

    n: int | None
    pct: float | None

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("sample: no data loaded — use 'source' first")
        df = context.df
        if self.pct is not None:
            context.df = df.sample(frac=self.pct / 100.0).reset_index(drop=True)
        else:
            n = min(self.n, len(df))
            context.df = df.sample(n=n).reset_index(drop=True)
        context.grouped = None


# ---------------------------------------------------------------------------
# Data transformation nodes
# ---------------------------------------------------------------------------

@dataclass
class SortNode(ASTNode):
    """Sort the current DataFrame by one or more columns."""

    columns: list[str]
    ascending: list[bool]

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("sort: no data loaded — use 'source' first")
        missing = [c for c in self.columns if c not in context.df.columns]
        if missing:
            raise KeyError(
                f"sort: unknown column(s) {missing}. "
                f"Available: {list(context.df.columns)}"
            )
        context.df = context.df.sort_values(
            self.columns, ascending=self.ascending
        ).reset_index(drop=True)
        context.grouped = None


@dataclass
class RenameNode(ASTNode):
    """Rename a single column."""

    old_name: str
    new_name: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("rename: no data loaded — use 'source' first")
        if self.old_name not in context.df.columns:
            raise KeyError(
                f"rename: column '{self.old_name}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        context.df = context.df.rename(columns={self.old_name: self.new_name})
        context.grouped = None


@dataclass
class AddNode(ASTNode):
    """Add a computed column using an arithmetic expression.

    Supports ``$variable`` substitution in the expression.
    """

    column: str
    expression: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("add: no data loaded — use 'source' first")
        try:
            expr = _substitute_vars(self.expression, context)
            context.df[self.column] = context.df.eval(expr)
        except Exception as exc:
            raise ValueError(
                f"add: could not evaluate expression '{self.expression}': {exc}"
            ) from exc
        context.grouped = None


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
        import numpy as np  # lazy import — numpy is a pandas transitive dep

        if context.df is None:
            raise RuntimeError("add: no data loaded — use 'source' first")
        df = context.df
        if self.cond_col not in df.columns:
            raise KeyError(
                f"add: column '{self.cond_col}' not found. "
                f"Available: {list(df.columns)}"
            )
        raw = _resolve_value(self.cond_val, context)
        rhs = _coerce_rhs(raw)
        mask = _apply_operator(df[self.cond_col], self.cond_op, rhs)

        def _to_val(v: str) -> Any:
            resolved = _resolve_value(v, context).strip("\"'")
            if resolved in df.columns:
                return df[resolved]
            try:
                return float(resolved)
            except ValueError:
                return resolved

        context.df[self.column] = np.where(
            mask, _to_val(self.true_val), _to_val(self.false_val)
        )
        context.grouped = None


@dataclass
class TrimNode(ASTNode):
    """Strip leading/trailing whitespace from a string column."""

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("trim: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"trim: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        context.df[self.column] = context.df[self.column].astype(str).str.strip()
        context.grouped = None


@dataclass
class UppercaseNode(ASTNode):
    """Convert a string column to uppercase."""

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("uppercase: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"uppercase: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        context.df[self.column] = context.df[self.column].astype(str).str.upper()
        context.grouped = None


@dataclass
class LowercaseNode(ASTNode):
    """Convert a string column to lowercase."""

    column: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("lowercase: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"lowercase: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        context.df[self.column] = context.df[self.column].astype(str).str.lower()
        context.grouped = None


_CAST_TYPE_MAP: dict[str, Any] = {
    "int":      lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "integer":  lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "float":    lambda s: pd.to_numeric(s, errors="coerce").astype(float),
    "double":   lambda s: pd.to_numeric(s, errors="coerce").astype(float),
    "str":      lambda s: s.astype(str),
    "string":   lambda s: s.astype(str),
    "text":     lambda s: s.astype(str),
    "datetime": lambda s: pd.to_datetime(s, errors="coerce"),
    "date":     lambda s: pd.to_datetime(s, errors="coerce"),
    "bool":     lambda s: s.astype(bool),
    "boolean":  lambda s: s.astype(bool),
}


@dataclass
class CastNode(ASTNode):
    """Cast a column to a different data type.

    Example: ``cast age int``  |  ``cast score float``  |  ``cast ts datetime``
    """

    column: str
    type_name: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("cast: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"cast: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        t = self.type_name.lower()
        if t not in _CAST_TYPE_MAP:
            raise ValueError(
                f"cast: unknown type '{self.type_name}'. "
                f"Supported: {', '.join(sorted(_CAST_TYPE_MAP))}"
            )
        context.df[self.column] = _CAST_TYPE_MAP[t](context.df[self.column])
        context.grouped = None


@dataclass
class ReplaceNode(ASTNode):
    """Replace occurrences of a value in a column.

    Example: ``replace country "Germany" "DE"``
    """

    column: str
    old_value: str
    new_value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("replace: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"replace: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        old = _coerce_rhs(self.old_value)
        new = _coerce_rhs(self.new_value)
        context.df[self.column] = context.df[self.column].replace(old, new)
        context.grouped = None


@dataclass
class PivotNode(ASTNode):
    """Reshape data from long to wide format.

    Example: ``pivot index=country column=year value=revenue``
    """

    index: str
    column: str
    value: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("pivot: no data loaded — use 'source' first")
        df = context.df
        for col in [self.index, self.column, self.value]:
            if col not in df.columns:
                raise KeyError(
                    f"pivot: column '{col}' not found. "
                    f"Available: {list(df.columns)}"
                )
        result = df.pivot_table(
            index=self.index,
            columns=self.column,
            values=self.value,
            aggfunc="sum",
        ).reset_index()
        result.columns.name = None
        context.df = result
        context.grouped = None


# ---------------------------------------------------------------------------
# Grouping node
# ---------------------------------------------------------------------------

@dataclass
class GroupByNode(ASTNode):
    """Group the current DataFrame by one or more columns."""

    columns: list[str]

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("group by: no data loaded — use 'source' first")
        missing = [c for c in self.columns if c not in context.df.columns]
        if missing:
            raise KeyError(
                f"group by: unknown column(s) {missing}. "
                f"Available: {list(context.df.columns)}"
            )
        context.grouped = context.df.groupby(self.columns, sort=False)


# ---------------------------------------------------------------------------
# Aggregation nodes
# ---------------------------------------------------------------------------

@dataclass
class CountNode(ASTNode):
    """Count rows — per group if ``group by`` is active, otherwise total."""

    def execute(self, context: "PipelineContext") -> None:
        if context.grouped is not None:
            context.df = context.grouped.size().reset_index(name="count")
            context.grouped = None
        elif context.df is not None:
            context.df = pd.DataFrame({"count": [len(context.df)]})
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
        if context.df is None:
            raise RuntimeError("count if: no data loaded — use 'source' first")
        df = context.df
        if self.column not in df.columns:
            raise KeyError(
                f"count if: column '{self.column}' not found. "
                f"Available: {list(df.columns)}"
            )
        raw = _resolve_value(self.value, context)
        rhs = _coerce_rhs(raw)
        mask = _apply_operator(df[self.column], self.operator, rhs)
        count = int(mask.sum())
        print(f"count if {self.column} {self.operator} {self.value}: {count}")


def _agg_node(verb: str, agg_fn: str):
    """Factory that returns a dataclass-based single-column aggregation node."""

    @dataclass
    class _AggNode(ASTNode):
        column: str

        def execute(self, context: "PipelineContext") -> None:
            if context.df is None:
                raise RuntimeError(
                    f"{verb}: no data loaded — use 'source' first"
                )
            if self.column not in context.df.columns:
                raise KeyError(
                    f"{verb}: column '{self.column}' not found. "
                    f"Available: {list(context.df.columns)}"
                )
            if context.grouped is not None:
                fn = getattr(context.grouped[self.column], agg_fn)
                context.df = fn().reset_index()
                context.grouped = None
            else:
                fn = getattr(context.df[self.column], agg_fn)
                context.df = pd.DataFrame({self.column: [fn()]})

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
        if context.df is None:
            raise RuntimeError("agg: no data loaded — use 'source' first")
        if context.grouped is None:
            raise RuntimeError(
                "agg: must follow 'group by'. "
                "Example:\n  group by country\n  agg sum salary, avg age, count"
            )

        grouped = context.grouped
        group_cols = (
            grouped.keys if isinstance(grouped.keys, list) else [grouped.keys]
        )

        _FN_MAP = {"sum": "sum", "avg": "mean", "min": "min", "max": "max"}
        agg_dict: dict[str, str] = {}
        has_count = False

        for verb, col in self.specs:
            if verb == "count":
                has_count = True
                continue
            if col is None:
                raise ValueError(f"agg: '{verb}' requires a column name")
            if col not in context.df.columns:
                raise KeyError(
                    f"agg: column '{col}' not found. "
                    f"Available: {list(context.df.columns)}"
                )
            agg_dict[col] = _FN_MAP[verb]

        parts: list[pd.DataFrame] = []
        if agg_dict:
            parts.append(grouped.agg(agg_dict).reset_index())
        if has_count:
            parts.append(grouped.size().reset_index(name="count"))

        if not parts:
            raise ValueError("agg: no valid aggregation specs provided")

        if len(parts) == 1:
            context.df = parts[0]
        else:
            result = parts[0]
            for part in parts[1:]:
                result = pd.merge(result, part, on=group_cols)
            context.df = result

        context.grouped = None


# ---------------------------------------------------------------------------
# Data joining / multi-source nodes
# ---------------------------------------------------------------------------

@dataclass
class JoinNode(ASTNode):
    """Join the current DataFrame with another CSV on a key column.

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
        if context.df is None:
            raise RuntimeError("join: no data loaded — use 'source' first")
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        if not os.path.exists(path):
            raise FileNotFoundError(f"join: file not found: '{path}'")
        right = pd.read_csv(path)
        if self.key not in context.df.columns:
            raise KeyError(
                f"join: key '{self.key}' not in current data. "
                f"Available: {list(context.df.columns)}"
            )
        if self.key not in right.columns:
            raise KeyError(
                f"join: key '{self.key}' not found in '{path}'. "
                f"Available: {list(right.columns)}"
            )
        context.df = pd.merge(context.df, right, on=self.key, how=self.how)
        context.grouped = None


@dataclass
class MergeNode(ASTNode):
    """Append rows from another CSV file (union / stack)."""

    file_path: str

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("merge: no data loaded — use 'source' first")
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        if not os.path.exists(path):
            raise FileNotFoundError(f"merge: file not found: '{path}'")
        other = pd.read_csv(path)
        context.df = pd.concat([context.df, other], ignore_index=True)
        context.grouped = None


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
        if context.df is None:
            raise RuntimeError("save: no data to save — pipeline produced no output")
        path = _substitute_vars(self.file_path, context)
        _check_path_sandbox(path, context)
        out_dir = os.path.dirname(path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        ext = os.path.splitext(path)[1].lower()
        if ext == ".json":
            context.df.to_json(path, orient="records", indent=2)
        elif ext == ".parquet":
            try:
                context.df.to_parquet(path, index=False)
            except ImportError:
                raise RuntimeError(
                    "save: writing Parquet files requires 'pyarrow'. "
                    "Install it with: pip install pyarrow"
                )
        else:
            context.df.to_csv(path, index=False)


@dataclass
class PrintNode(ASTNode):
    """Print the current DataFrame to stdout without saving."""

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("print: no data loaded — use 'source' first")
        print(context.df.to_string(index=False))


@dataclass
class SchemaNode(ASTNode):
    """Print the column names and data types of the current DataFrame."""

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("schema: no data loaded — use 'source' first")
        print(
            f"\nSchema  ({len(context.df.columns)} column(s), "
            f"{len(context.df)} row(s)):"
        )
        print(f"  {'Column':<22} {'Type'}")
        print("  " + "-" * 34)
        for col in context.df.columns:
            print(f"  {col:<22} {context.df[col].dtype}")
        print()


@dataclass
class InspectNode(ASTNode):
    """Print column names, types, null counts, and unique value counts."""

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("inspect: no data loaded — use 'source' first")
        print(
            f"\nInspect  ({len(context.df.columns)} column(s), "
            f"{len(context.df)} row(s)):"
        )
        print(f"  {'Column':<22} {'Type':<12} {'Nulls':<8} {'Unique'}")
        print("  " + "-" * 50)
        for col in context.df.columns:
            nulls = int(
                context.df[col].isna().sum()
                + (context.df[col] == "").sum()
            )
            unique = int(context.df[col].nunique(dropna=False))
            print(
                f"  {col:<22} {str(context.df[col].dtype):<12} "
                f"{nulls:<8} {unique}"
            )
        print()


@dataclass
class HeadNode(ASTNode):
    """Print the first *n* rows to stdout without modifying pipeline data."""

    n: int

    def execute(self, context: "PipelineContext") -> None:
        if context.df is None:
            raise RuntimeError("head: no data loaded — use 'source' first")
        print(f"\nHead ({self.n} row(s)):")
        print(context.df.head(self.n).to_string(index=False))
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
        if context.df is None:
            raise RuntimeError("assert: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"assert: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        raw = _resolve_value(self.value, context)
        rhs = _coerce_rhs(raw)
        try:
            mask = _apply_operator(context.df[self.column], self.operator, rhs)
        except ValueError as exc:
            raise ValueError(f"assert: {exc}") from exc
        failures = int((~mask).sum())
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
        if context.df is None:
            raise RuntimeError("fill: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"fill: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        col = self.column
        context.df[col] = context.df[col].replace("", pd.NA)
        s = self.strategy.strip().lower()

        if s == "mean":
            context.df[col] = pd.to_numeric(context.df[col], errors="coerce")
            context.df[col] = context.df[col].fillna(context.df[col].mean())
        elif s == "median":
            context.df[col] = pd.to_numeric(context.df[col], errors="coerce")
            context.df[col] = context.df[col].fillna(context.df[col].median())
        elif s == "mode":
            mode_val = context.df[col].mode()
            if not mode_val.empty:
                context.df[col] = context.df[col].fillna(mode_val[0])
        elif s == "forward":
            context.df[col] = context.df[col].ffill()
        elif s == "backward":
            context.df[col] = context.df[col].bfill()
        elif s == "drop":
            context.df = context.df.dropna(subset=[col]).reset_index(drop=True)
        else:
            raw = self.strategy.strip("\"'")
            try:
                fill_val: float | int | str = float(raw)
                if fill_val == int(fill_val):
                    fill_val = int(fill_val)
            except ValueError:
                fill_val = raw
            context.df[col] = context.df[col].fillna(fill_val)

        context.grouped = None


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

    The pipeline context at the point of failure is preserved so the
    ``on_error`` handler can inspect or repair it.

    Example::

        try
          cast age int
          assert age > 0
        on_error fill age 0

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
