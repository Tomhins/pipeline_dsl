"""Pipeline executor for the pipeline DSL.

Defines :class:`PipelineContext` (shared mutable state passed between nodes)
and :func:`run_pipeline` (the entry point that drives execution).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from ast_nodes import ASTNode


@dataclass
class PipelineContext:
    """Mutable execution context shared by all AST nodes during a pipeline run.

    Attributes:
        df: The current working :class:`pandas.DataFrame`.  Starts as
            ``None`` and is first populated by a :class:`~ast_nodes.SourceNode`.
        grouped: The current :class:`pandas.core.groupby.DataFrameGroupBy`
            object produced by a :class:`~ast_nodes.GroupByNode`, or ``None``
            when no grouping is active.
        variables: Named variables set via ``set`` or ``env`` commands,
            referenced as ``$name`` in other commands.
    """

    df: pd.DataFrame | None = field(default=None)
    grouped: Any | None = field(default=None)
    variables: dict = field(default_factory=dict)


def run_pipeline(nodes: list[ASTNode]) -> pd.DataFrame | None:
    """Execute a sequence of AST nodes and return the resulting DataFrame.

    Each node receives the same :class:`PipelineContext` instance and may
    read or mutate ``context.df`` and ``context.grouped``.

    Args:
        nodes: Ordered list of :class:`~ast_nodes.ASTNode` objects as
            produced by :func:`~parser.parse_lines`.

    Returns:
        The final :class:`pandas.DataFrame` stored in the context after all
        nodes have been executed, or ``None`` if no data was produced.

    Raises:
        RuntimeError: Re-raised from any node that encounters a semantic
            error during execution (e.g. missing source, unknown column).
        Exception: Any unexpected error from a node will propagate with an
            informative message indicating which node type failed.
    """
    context = PipelineContext()

    for node in nodes:
        node_name = node.__class__.__name__
        try:
            node.execute(context)
        except (AssertionError, FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
            # Re-raise with the failing node type in the message for clarity.
            raise type(exc)(f"[{node_name}] {exc}") from exc

    return context.df
