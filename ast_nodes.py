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
# Supported filter operators
# ---------------------------------------------------------------------------
_OPERATORS: dict[str, str] = {
    ">=": ">=",
    "<=": "<=",
    "!=": "!=",
    "==": "==",
    ">":  ">",
    "<":  "<",
}


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

        op = self.operator
        col = df[self.column]

        if op == ">":
            context.df = df[col > rhs]
        elif op == "<":
            context.df = df[col < rhs]
        elif op == ">=":
            context.df = df[col >= rhs]
        elif op == "<=":
            context.df = df[col <= rhs]
        elif op == "==":
            context.df = df[col == rhs]
        elif op == "!=":
            context.df = df[col != rhs]
        else:
            raise ValueError(
                f"filter: unsupported operator '{op}'. "
                f"Supported: {list(_OPERATORS)}"
            )

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
        """Persist *context.df* to *file_path* as CSV (no index column)."""
        if context.df is None:
            raise RuntimeError("save: no data to save — pipeline produced no output")

        out_dir = os.path.dirname(self.file_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        context.df.to_csv(self.file_path, index=False)
