"""Pipeline executor for the pipeline DSL.

Defines :class:`PipelineContext` (shared mutable state passed between nodes)
and :func:`run_pipeline` (the entry point that drives execution).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from ast_nodes import ASTNode

# ---------------------------------------------------------------------------
# Node class names that are safe to apply independently on each chunk.
# These operate row-by-row and do not require the full dataset.
# ---------------------------------------------------------------------------
_CHUNK_SAFE_NODE_NAMES: frozenset[str] = frozenset([
    "FilterNode", "CompoundFilterNode", "SelectNode", "DropNode",
    "CastNode", "RenameNode", "TrimNode", "UppercaseNode", "LowercaseNode",
    "AddNode", "AddIfNode", "ReplaceNode", "FillNode",
])


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
        sandbox_dir: When set, all file I/O is restricted to this directory
            tree. Set via ``set sandbox = <dir>`` in a pipeline.
    """

    df: pd.DataFrame | None = field(default=None)
    grouped: Any | None = field(default=None)
    variables: dict = field(default_factory=dict)
    sandbox_dir: str | None = field(default=None)


def _run_chunked_pipeline(
    source_node: ASTNode,
    remaining_nodes: list[ASTNode],
    context: PipelineContext,
) -> pd.DataFrame | None:
    """Execute a pipeline whose source is read in fixed-size chunks.

    Chunk-safe nodes (filter, select, cast, rename, etc.) are applied
    independently to each chunk, reducing peak memory usage.  The chunks
    are then concatenated and the remaining nodes (sort, group-by, etc.)
    are applied to the full result.

    Args:
        source_node: The :class:`~ast_nodes.SourceNode` with a non-null
            ``chunk_size``.
        remaining_nodes: All nodes after the source node.
        context: Shared pipeline context.

    Returns:
        The final :class:`pandas.DataFrame` after all nodes have run.
    """
    from ast_nodes import _substitute_vars, _check_path_sandbox

    path = _substitute_vars(source_node.file_path, context)
    _check_path_sandbox(path, context)

    # Split remaining nodes into per-chunk phase and post-concat phase.
    chunk_phase: list[ASTNode] = []
    post_phase: list[ASTNode] = []
    in_chunk_phase = True
    for node in remaining_nodes:
        if in_chunk_phase and node.__class__.__name__ in _CHUNK_SAFE_NODE_NAMES:
            chunk_phase.append(node)
        else:
            in_chunk_phase = False
            post_phase.append(node)

    chunks_out: list[pd.DataFrame] = []
    for chunk_df in pd.read_csv(path, chunksize=source_node.chunk_size):
        chunk_ctx = PipelineContext(
            df=chunk_df,
            variables=dict(context.variables),
            sandbox_dir=context.sandbox_dir,
        )
        for node in chunk_phase:
            node_name = node.__class__.__name__
            try:
                node.execute(chunk_ctx)
            except (AssertionError, FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
                raise type(exc)(f"[{node_name}] {exc}") from exc
        if chunk_ctx.df is not None and not chunk_ctx.df.empty:
            chunks_out.append(chunk_ctx.df)

    context.df = pd.concat(chunks_out, ignore_index=True) if chunks_out else pd.DataFrame()
    context.grouped = None

    for node in post_phase:
        node_name = node.__class__.__name__
        try:
            node.execute(context)
        except (AssertionError, FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
            raise type(exc)(f"[{node_name}] {exc}") from exc

    return context.df


def run_pipeline(nodes: list[ASTNode]) -> pd.DataFrame | None:
    """Execute a sequence of AST nodes and return the resulting DataFrame.

    Each node receives the same :class:`PipelineContext` instance and may
    read or mutate ``context.df`` and ``context.grouped``.

    If the first node is a :class:`~ast_nodes.SourceNode` with a
    ``chunk_size``, the pipeline runs in chunked mode via
    :func:`_run_chunked_pipeline` for large-file support.

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

    # Chunked mode: first node is a SourceNode with chunk_size set.
    if nodes and getattr(nodes[0], "chunk_size", None):
        return _run_chunked_pipeline(nodes[0], nodes[1:], context)

    for node in nodes:
        node_name = node.__class__.__name__
        try:
            node.execute(context)
        except (AssertionError, FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
            # Re-raise with the failing node type in the message for clarity.
            raise type(exc)(f"[{node_name}] {exc}") from exc

    return context.df
