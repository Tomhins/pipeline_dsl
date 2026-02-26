"""Pipeline executor for the pipeline DSL.

Defines :class:`PipelineContext` (shared mutable state passed between nodes)
and :func:`run_pipeline` (the entry point that drives execution).
"""

from __future__ import annotations

from typing import Any

import polars as pl

from ast_nodes import ASTNode


# ---------------------------------------------------------------------------
# LazyFrame conversion helper
# ---------------------------------------------------------------------------

def _to_lazy(df: Any) -> pl.LazyFrame:
    """Convert a pandas or Polars DataFrame to a :class:`polars.LazyFrame`."""
    if isinstance(df, pl.LazyFrame):
        return df
    if isinstance(df, pl.DataFrame):
        return df.lazy()
    # Assume pandas DataFrame
    return pl.from_pandas(df, nan_to_null=True).lazy()


# ---------------------------------------------------------------------------
# Pipeline execution context
# ---------------------------------------------------------------------------

class PipelineContext:
    """Mutable execution context shared by all AST nodes during a pipeline run.

    Attributes:
        lf: The current working :class:`polars.LazyFrame`.  Starts as
            ``None`` and is first populated by a :class:`~ast_nodes.SourceNode`.
        group_by_cols: Column names set by a :class:`~ast_nodes.GroupByNode`,
            or ``None`` when no grouping is active.
        variables: Named variables set via ``set`` or ``env`` commands,
            referenced as ``$name`` in other commands.
        sandbox_dir: When set, all file I/O is restricted to this directory
            tree. Set via ``set sandbox = <dir>`` in a pipeline.
        streaming: When ``True``, the final ``.collect()`` uses Polars'
            streaming engine (activated by ``source … chunk N``).
    """

    def __init__(
        self,
        lf: pl.LazyFrame | None = None,
        df: Any = None,                    # backward-compat: pandas / Polars DF
        grouped: Any = None,               # backward-compat: ignored
        group_by_cols: list[str] | None = None,
        variables: dict | None = None,
        sandbox_dir: str | None = None,
        streaming: bool = False,
    ) -> None:
        if lf is not None:
            self.lf: pl.LazyFrame | None = lf
        elif df is not None:
            self.lf = _to_lazy(df)
        else:
            self.lf = None
        self.group_by_cols: list[str] | None = group_by_cols
        self.variables: dict = variables if variables is not None else {}
        self.sandbox_dir: str | None = sandbox_dir
        self.streaming: bool = streaming

    # ------------------------------------------------------------------
    # Backward-compatibility shims so legacy code and tests keep working
    # ------------------------------------------------------------------

    @property
    def df(self) -> Any:
        """Collect the current LazyFrame and return a :class:`pandas.DataFrame`."""
        if self.lf is None:
            return None
        return self.lf.collect().to_pandas()

    @df.setter
    def df(self, value: Any) -> None:
        """Accept a pandas/Polars DataFrame (or ``None``) and store as LazyFrame."""
        if value is None:
            self.lf = None
        else:
            self.lf = _to_lazy(value)

    @property
    def grouped(self) -> list[str] | None:
        """Backward-compat — returns ``group_by_cols`` (truthy when grouping is active)."""
        return self.group_by_cols

    @grouped.setter
    def grouped(self, value: Any) -> None:
        """Setting ``grouped = None`` clears ``group_by_cols``."""
        if value is None:
            self.group_by_cols = None
        # Non-None assignments (legacy pandas GroupBy objects) are ignored;
        # GroupByNode sets group_by_cols directly instead.


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(nodes: list[ASTNode]) -> Any:
    """Execute a sequence of AST nodes and return the resulting DataFrame.

    The pipeline runs in Polars **lazy mode** throughout.  The final
    LazyFrame is collected at the end and converted to a
    :class:`pandas.DataFrame` for compatibility with the CLI.

    When the first node is a :class:`~ast_nodes.SourceNode` with a
    ``chunk_size``, Polars' streaming engine is used for the final collect,
    reducing peak memory for large files.

    Args:
        nodes: Ordered list of :class:`~ast_nodes.ASTNode` objects as
            produced by :func:`~ppl_parser.parse_lines`.

    Returns:
        A :class:`pandas.DataFrame` of the pipeline result, or ``None`` if
        no data was produced.

    Raises:
        Exception: Re-raised from any node that encountered a semantic
            error, prefixed with the failing node class name.
    """
    context = PipelineContext()

    for node in nodes:
        node_name = node.__class__.__name__
        try:
            node.execute(context)
        except (AssertionError, FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
            # Re-raise with the failing node type in the message for clarity.
            raise type(exc)(f"[{node_name}] {exc}") from exc

    if context.lf is None:
        return None

    try:
        if context.streaming:
            try:
                polars_df = context.lf.collect(engine="streaming")
            except TypeError:
                polars_df = context.lf.collect()
        else:
            polars_df = context.lf.collect()
    except Exception as exc:
        raise RuntimeError(f"Pipeline collection failed: {exc}") from exc

    return polars_df.to_pandas()
