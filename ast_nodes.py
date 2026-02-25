"""AST node definitions for the pipeline DSL.

Each node corresponds to one DSL command and knows how to execute
itself against a :class:`~executor.PipelineContext`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

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


# ---------------------------------------------------------------------------
# Base node
# ---------------------------------------------------------------------------

class ASTNode:
    """Abstract base class for all pipeline AST nodes."""

    def execute(self, context: PipelineContext) -> None:  # noqa: D102
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement execute()"
        )


# ---------------------------------------------------------------------------
# Concrete nodes
# ---------------------------------------------------------------------------

@dataclass
class SourceNode(ASTNode):
    """Load a CSV file into the pipeline context.

    Args:
        file_path: Path to the CSV file to load.
    """

    file_path: str

    def execute(self, context: PipelineContext) -> None:
        """Read *file_path* as a :class:`pandas.DataFrame` and store it in *context*."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(
                f"Source file not found: '{self.file_path}'"
            )
        context.df = pd.read_csv(self.file_path)
        context.grouped = None


@dataclass
class FilterNode(ASTNode):
    """Filter the current DataFrame by a simple column condition.

    Supports operators: ``>``, ``<``, ``>=``, ``<=``, ``==``, ``!=``.

    Args:
        column: Name of the column to test.
        operator: Comparison operator string.
        value: Right-hand side value (parsed as float when possible).
    """

    column: str
    operator: str
    value: str

    def execute(self, context: PipelineContext) -> None:
        """Apply the filter and reassign *context.df*."""
        if context.df is None:
            raise RuntimeError("filter: no data loaded — use 'source' first")

        df = context.df

        if self.column not in df.columns:
            raise KeyError(
                f"filter: column '{self.column}' not found. "
                f"Available: {list(df.columns)}"
            )

        # Attempt numeric coercion; fall back to string (strip outer quotes)
        raw = self.value.strip("\"'")
        try:
            rhs: float | str = float(raw)
        except ValueError:
            rhs = raw

        try:
            mask = _apply_operator(df[self.column], self.operator, rhs)
        except ValueError as exc:
            raise ValueError(f"filter: {exc}") from exc

        context.df = df[mask]
        context.grouped = None


@dataclass
class SelectNode(ASTNode):
    """Keep only the specified columns in the current DataFrame.

    Args:
        columns: Ordered list of column names to retain.
    """

    columns: list[str]

    def execute(self, context: PipelineContext) -> None:
        """Project *context.df* down to *columns*."""
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
class GroupByNode(ASTNode):
    """Group the current DataFrame by one or more columns.

    The resulting :class:`pandas.DataFrameGroupBy` object is stored on
    *context.grouped* and consumed by the next :class:`CountNode`.

    Args:
        columns: Column names to group by.
    """

    columns: list[str]

    def execute(self, context: PipelineContext) -> None:
        """Call :meth:`pandas.DataFrame.groupby` and store the result."""
        if context.df is None:
            raise RuntimeError("group by: no data loaded — use 'source' first")

        missing = [c for c in self.columns if c not in context.df.columns]
        if missing:
            raise KeyError(
                f"group by: unknown column(s) {missing}. "
                f"Available: {list(context.df.columns)}"
            )

        context.grouped = context.df.groupby(self.columns, sort=False)


@dataclass
class CountNode(ASTNode):
    """Count rows, optionally within groups.

    * With an active ``group by``: produces a ``count`` column per group and
      replaces *context.df* with the aggregated result.
    * Without grouping: replaces *context.df* with a single-row DataFrame
      containing the total row count.
    """

    def execute(self, context: PipelineContext) -> None:
        """Resolve the count and store the result in *context.df*."""
        if context.grouped is not None:
            context.df = (
                context.grouped.size().reset_index(name="count")
            )
            context.grouped = None
        elif context.df is not None:
            context.df = pd.DataFrame({"count": [len(context.df)]})
        else:
            raise RuntimeError("count: no data loaded — use 'source' first")


@dataclass
class SaveNode(ASTNode):
    """Write the current DataFrame to a CSV file.

    Parent directories are created automatically if they do not exist.

    Args:
        file_path: Destination path for the output CSV.
    """

    file_path: str

    def execute(self, context: PipelineContext) -> None:
        """Persist *context.df* to *file_path* as CSV or JSON (no index column)."""
        if context.df is None:
            raise RuntimeError("save: no data to save — pipeline produced no output")

        out_dir = os.path.dirname(self.file_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        ext = os.path.splitext(self.file_path)[1].lower()
        if ext == ".json":
            context.df.to_json(self.file_path, orient="records", indent=2)
        else:
            context.df.to_csv(self.file_path, index=False)


# ---------------------------------------------------------------------------
# Data transformation nodes
# ---------------------------------------------------------------------------

@dataclass
class SortNode(ASTNode):
    """Sort the current DataFrame by one or more columns.

    Args:
        columns: Column names to sort by.
        ascending: Matching list of booleans (True = asc, False = desc).
    """

    columns: list[str]
    ascending: list[bool]

    def execute(self, context: PipelineContext) -> None:
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
    """Rename a single column.

    Args:
        old_name: Existing column name.
        new_name: New column name.
    """

    old_name: str
    new_name: str

    def execute(self, context: PipelineContext) -> None:
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

    The expression is evaluated via :meth:`pandas.DataFrame.eval` so it can
    reference existing column names directly (e.g. ``price * 0.2``).

    Args:
        column: Name of the new column to create.
        expression: Arithmetic expression referencing existing columns.
    """

    column: str
    expression: str

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("add: no data loaded — use 'source' first")
        try:
            context.df = context.df.copy()
            context.df[self.column] = context.df.eval(self.expression)
        except Exception as exc:
            raise ValueError(
                f"add: could not evaluate expression '{self.expression}': {exc}"
            ) from exc
        context.grouped = None


@dataclass
class DropNode(ASTNode):
    """Remove one or more columns from the current DataFrame.

    Args:
        columns: Column names to drop.
    """

    columns: list[str]

    def execute(self, context: PipelineContext) -> None:
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
    """Keep only the first *n* rows.

    Args:
        n: Maximum number of rows to retain.
    """

    n: int

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("limit: no data loaded — use 'source' first")
        context.df = context.df.head(self.n).reset_index(drop=True)
        context.grouped = None


@dataclass
class DistinctNode(ASTNode):
    """Remove duplicate rows from the current DataFrame."""

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("distinct: no data loaded — use 'source' first")
        context.df = context.df.drop_duplicates().reset_index(drop=True)
        context.grouped = None


# ---------------------------------------------------------------------------
# Aggregation nodes
# ---------------------------------------------------------------------------

def _agg_node(verb: str, agg_fn: str):
    """Factory that returns a dataclass-based aggregation node class."""

    @dataclass
    class _AggNode(ASTNode):
        column: str

        def execute(self, context: PipelineContext) -> None:
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


# ---------------------------------------------------------------------------
# Data joining / multi-source nodes
# ---------------------------------------------------------------------------

@dataclass
class JoinNode(ASTNode):
    """Inner-join the current DataFrame with another CSV on a key column.

    Args:
        file_path: Path to the CSV file to join with.
        key: Column name to join on (must exist in both datasets).
    """

    file_path: str
    key: str

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("join: no data loaded — use 'source' first")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(
                f"join: file not found: '{self.file_path}'"
            )
        right = pd.read_csv(self.file_path)
        if self.key not in context.df.columns:
            raise KeyError(
                f"join: key '{self.key}' not in current data. "
                f"Available: {list(context.df.columns)}"
            )
        if self.key not in right.columns:
            raise KeyError(
                f"join: key '{self.key}' not found in '{self.file_path}'. "
                f"Available: {list(right.columns)}"
            )
        context.df = pd.merge(context.df, right, on=self.key, how="inner")
        context.grouped = None


@dataclass
class MergeNode(ASTNode):
    """Append rows from another CSV file (union / stack).

    Args:
        file_path: Path to the CSV file whose rows are appended.
    """

    file_path: str

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("merge: no data loaded — use 'source' first")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(
                f"merge: file not found: '{self.file_path}'"
            )
        other = pd.read_csv(self.file_path)
        context.df = pd.concat([context.df, other], ignore_index=True)
        context.grouped = None


# ---------------------------------------------------------------------------
# Output / inspection nodes
# ---------------------------------------------------------------------------

@dataclass
class PrintNode(ASTNode):
    """Print the current DataFrame to stdout without saving."""

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("print: no data loaded — use 'source' first")
        print(context.df.to_string(index=False))


@dataclass
class SchemaNode(ASTNode):
    """Print the column names and data types of the current DataFrame."""

    def execute(self, context: PipelineContext) -> None:
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

    def execute(self, context: PipelineContext) -> None:
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
    """Print the first *n* rows to stdout without modifying the pipeline data.

    Args:
        n: Number of rows to display.
    """

    n: int

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("head: no data loaded — use 'source' first")
        print(f"\nHead ({self.n} row(s)):")
        print(context.df.head(self.n).to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# Quality / validation nodes
# ---------------------------------------------------------------------------

@dataclass
class AssertNode(ASTNode):
    """Fail the pipeline if any row violates a column condition.

    Uses the same operators as :class:`FilterNode`.

    Args:
        column: Column to test.
        operator: Comparison operator.
        value: Right-hand side value.
    """

    column: str
    operator: str
    value: str

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("assert: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"assert: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        raw = self.value.strip("\"'")
        try:
            rhs: float | str = float(raw)
        except ValueError:
            rhs = raw

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

    *strategy* can be:

    * A literal value (number or quoted string) — fills NaN/empty with that value.
    * ``mean`` / ``median`` / ``mode`` — fills with the column's statistic.
    * ``forward`` / ``backward`` — propagate the last/next non-null value.
    * ``drop`` — remove rows where this column is null/empty.

    Args:
        column: Column to fill.
        strategy: Fill strategy or literal value.
    """

    column: str
    strategy: str

    def execute(self, context: PipelineContext) -> None:
        if context.df is None:
            raise RuntimeError("fill: no data loaded — use 'source' first")
        if self.column not in context.df.columns:
            raise KeyError(
                f"fill: column '{self.column}' not found. "
                f"Available: {list(context.df.columns)}"
            )
        df = context.df.copy()
        # Normalise empty strings to NaN so all strategies work uniformly
        df[self.column] = df[self.column].replace("", pd.NA)

        s = self.strategy.strip().lower()

        if s == "mean":
            df[self.column] = pd.to_numeric(df[self.column], errors="coerce")
            df[self.column] = df[self.column].fillna(df[self.column].mean())
        elif s == "median":
            df[self.column] = pd.to_numeric(df[self.column], errors="coerce")
            df[self.column] = df[self.column].fillna(df[self.column].median())
        elif s == "mode":
            mode_val = df[self.column].mode()
            if not mode_val.empty:
                df[self.column] = df[self.column].fillna(mode_val[0])
        elif s == "forward":
            df[self.column] = df[self.column].ffill()
        elif s == "backward":
            df[self.column] = df[self.column].bfill()
        elif s == "drop":
            df = df.dropna(subset=[self.column]).reset_index(drop=True)
        else:
            # Treat strategy as a literal fill value
            raw = self.strategy.strip("\"'")
            try:
                fill_val: float | int | str = float(raw)
                if fill_val == int(fill_val):
                    fill_val = int(fill_val)
            except ValueError:
                fill_val = raw
            df[self.column] = df[self.column].fillna(fill_val)

        context.df = df
        context.grouped = None
