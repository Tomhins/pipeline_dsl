"""Parser for the pipeline DSL.

Converts a list of cleaned text lines (as produced by
:func:`~file_reader.read_ppl_file`) into a list of
:class:`~ast_nodes.ASTNode` objects ready for execution.

.. note::

    This module is named ``ppl_parser`` (not ``parser``) to avoid
    shadowing the Python standard-library ``parser`` module on Python ≤ 3.9.

Supported commands
------------------
``source "file.csv"``
    Load a CSV file.
``filter <column> <op> <value>``
    Filter rows by a column condition (operators: >, <, >=, <=, ==, !=).
``select <col1>, <col2>, ...``
    Keep only the listed columns.
``group by <col1>, <col2>, ...``
    Group by one or more columns.
``count``
    Count rows (optionally within groups).
``save "file.csv"``
    Write the current DataFrame to a CSV file.
"""

from __future__ import annotations

import re

from ast_nodes import (
    ASTNode,
    CountNode,
    FilterNode,
    GroupByNode,
    SaveNode,
    SelectNode,
    SourceNode,
)

# Ordered so multi-character operators are tried before single-character ones.
_FILTER_OPERATORS: list[str] = [">=", "<=", "!=", "==", ">", "<"]

# Regex that matches a quoted string (single or double quotes).
_QUOTED_RE = re.compile(r'^["\'](.+)["\']$')


def _strip_quotes(value: str) -> str:
    """Remove surrounding single or double quotes from *value* if present."""
    m = _QUOTED_RE.match(value.strip())
    return m.group(1) if m else value.strip()


def _parse_source(args: str, line_no: int) -> SourceNode:
    """Parse ``source "file.csv"`` into a :class:`SourceNode`."""
    path = _strip_quotes(args.strip())
    if not path:
        raise SyntaxError(
            f"Line {line_no}: 'source' requires a file path. "
            "Example: source \"data/people.csv\""
        )
    return SourceNode(file_path=path)


def _parse_filter(args: str, line_no: int) -> FilterNode:
    """Parse ``filter <column> <op> <value>`` into a :class:`FilterNode`.

    Handles multi-character operators (>=, <=, !=, ==) before single-char
    ones (>, <) to avoid ambiguous splits.
    """
    args = args.strip()
    for op in _FILTER_OPERATORS:
        if op in args:
            parts = args.split(op, maxsplit=1)
            column = parts[0].strip()
            value  = parts[1].strip()
            if not column or not value:
                break
            return FilterNode(column=column, operator=op, value=value)

    raise SyntaxError(
        f"Line {line_no}: could not parse 'filter' condition '{args}'. "
        "Expected: filter <column> <op> <value>  (e.g. filter age > 18)"
    )


def _parse_select(args: str, line_no: int) -> SelectNode:
    """Parse ``select col1, col2, ...`` into a :class:`SelectNode`."""
    columns = [c.strip() for c in args.split(",") if c.strip()]
    if not columns:
        raise SyntaxError(
            f"Line {line_no}: 'select' requires at least one column. "
            "Example: select name, age"
        )
    return SelectNode(columns=columns)


def _parse_group_by(args: str, line_no: int) -> GroupByNode:
    """Parse ``group by col1, col2, ...`` into a :class:`GroupByNode`.

    *args* is everything after the leading keyword ``group``, so this
    function expects it to start with ``by``.
    """
    lowered = args.strip().lower()
    if not lowered.startswith("by"):
        raise SyntaxError(
            f"Line {line_no}: 'group' must be followed by 'by'. "
            "Example: group by country"
        )
    remainder = args.strip()[2:].strip()  # everything after "by"
    columns = [c.strip() for c in remainder.split(",") if c.strip()]
    if not columns:
        raise SyntaxError(
            f"Line {line_no}: 'group by' requires at least one column. "
            "Example: group by country"
        )
    return GroupByNode(columns=columns)


def _parse_save(args: str, line_no: int) -> SaveNode:
    """Parse ``save "file.csv"`` into a :class:`SaveNode`."""
    path = _strip_quotes(args.strip())
    if not path:
        raise SyntaxError(
            f"Line {line_no}: 'save' requires a file path. "
            "Example: save \"output/results.csv\""
        )
    return SaveNode(file_path=path)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_PARSERS = {
    "source": _parse_source,
    "filter": _parse_filter,
    "select": _parse_select,
    "group":  _parse_group_by,
    "save":   _parse_save,
}


def parse_lines(lines: list[str]) -> list[ASTNode]:
    """Convert a list of cleaned DSL lines into a list of :class:`ASTNode` objects.

    Args:
        lines: Cleaned lines as returned by :func:`~file_reader.read_ppl_file`.

    Returns:
        An ordered list of AST nodes ready to be passed to
        :func:`~executor.run_pipeline`.

    Raises:
        SyntaxError: If an unknown command is encountered or a command
            cannot be parsed correctly.
    """
    nodes: list[ASTNode] = []

    for line_no, line in enumerate(lines, start=1):
        keyword, _, rest = line.partition(" ")
        keyword_lower = keyword.lower()

        if keyword_lower == "count":
            nodes.append(CountNode())
            continue

        if keyword_lower in _PARSERS:
            node = _PARSERS[keyword_lower](rest, line_no)
            nodes.append(node)
        else:
            raise SyntaxError(
                f"Line {line_no}: unknown command '{keyword}'. "
                "Supported commands: "
                + ", ".join(sorted(_PARSERS) + ["count"])
            )

    return nodes
