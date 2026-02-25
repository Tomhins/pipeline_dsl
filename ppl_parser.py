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
``sort by <col> [asc|desc], ...``
    Sort rows by one or more columns.
``rename <old> <new>``
    Rename a column.
``add <col> = <expression>``
    Add a computed column (arithmetic expression using column names).
``drop <col1>, <col2>, ...``
    Remove columns.
``limit <n>``
    Keep only the first n rows.
``distinct``
    Remove duplicate rows.
``sum <column>``
    Sum a numeric column (per group or total).
``avg <column>``
    Average of a numeric column (per group or total).
``min <column>``
    Minimum value of a column (per group or total).
``max <column>``
    Maximum value of a column (per group or total).
``join "file.csv" on <column>``
    Inner-join with another CSV on a key column.
``merge "file.csv"``
    Append rows from another CSV file (union).
``save "file.csv|json"``
    Write the current DataFrame to a CSV or JSON file.
``print``
    Print the current DataFrame to stdout.
``schema``
    Print column names and data types.
``inspect``
    Print column names, types, null counts and unique counts.
``head <n>``
    Print the first n rows to stdout (does not modify pipeline data).
``assert <column> <op> <value>``
    Fail the pipeline if any row violates the condition.
``fill <column> <strategy|value>``
    Fill missing values (strategies: mean, median, mode, forward, backward, drop).
"""

from __future__ import annotations

import re

from ast_nodes import (
    AddNode,
    AssertNode,
    ASTNode,
    AvgNode,
    CountNode,
    DistinctNode,
    DropNode,
    FillNode,
    FilterNode,
    GroupByNode,
    HeadNode,
    InspectNode,
    JoinNode,
    LimitNode,
    MaxNode,
    MergeNode,
    MinNode,
    PrintNode,
    RenameNode,
    SaveNode,
    SchemaNode,
    SelectNode,
    SortNode,
    SourceNode,
    SumNode,
)

# Ordered so multi-character operators are tried before single-character ones.
_FILTER_OPERATORS: list[str] = [">=", "<=", "!=", "==", ">", "<"]

# Regex that matches a quoted string (single or double quotes).
_QUOTED_RE = re.compile(r'^["\'](.+)["\']$')


def _strip_quotes(value: str) -> str:
    """Remove surrounding single or double quotes from *value* if present."""
    m = _QUOTED_RE.match(value.strip())
    return m.group(1) if m else value.strip()


# ---------------------------------------------------------------------------
# Individual command parsers
# ---------------------------------------------------------------------------

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
    """Parse ``filter <column> <op> <value>`` into a :class:`FilterNode`."""
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


def _parse_sort(args: str, line_no: int) -> SortNode:
    """Parse ``sort by col1 [asc|desc], col2 [asc|desc], ...`` into a :class:`SortNode`.

    Defaults to ascending when no direction is given.
    """
    lowered = args.strip().lower()
    if not lowered.startswith("by"):
        raise SyntaxError(
            f"Line {line_no}: 'sort' must be followed by 'by'. "
            "Example: sort by age desc"
        )
    remainder = args.strip()[2:].strip()
    parts = [p.strip() for p in remainder.split(",") if p.strip()]
    if not parts:
        raise SyntaxError(
            f"Line {line_no}: 'sort by' requires at least one column. "
            "Example: sort by age desc"
        )
    columns: list[str] = []
    ascending: list[bool] = []
    for part in parts:
        tokens = part.split()
        col = tokens[0]
        direction = tokens[1].lower() if len(tokens) > 1 else "asc"
        if direction not in ("asc", "desc"):
            raise SyntaxError(
                f"Line {line_no}: sort direction must be 'asc' or 'desc', "
                f"got '{direction}'"
            )
        columns.append(col)
        ascending.append(direction == "asc")
    return SortNode(columns=columns, ascending=ascending)


def _parse_rename(args: str, line_no: int) -> RenameNode:
    """Parse ``rename <old> <new>`` into a :class:`RenameNode`."""
    parts = args.strip().split()
    if len(parts) != 2:
        raise SyntaxError(
            f"Line {line_no}: 'rename' requires exactly two column names. "
            "Example: rename old_name new_name"
        )
    return RenameNode(old_name=parts[0], new_name=parts[1])


def _parse_add(args: str, line_no: int) -> AddNode:
    """Parse ``add <col> = <expression>`` into an :class:`AddNode`."""
    if "=" not in args:
        raise SyntaxError(
            f"Line {line_no}: 'add' requires '='. "
            "Example: add tax = price * 0.2"
        )
    col, _, expr = args.partition("=")
    col = col.strip()
    expr = expr.strip()
    if not col or not expr:
        raise SyntaxError(
            f"Line {line_no}: 'add' requires a column name and expression. "
            "Example: add tax = price * 0.2"
        )
    return AddNode(column=col, expression=expr)


def _parse_drop(args: str, line_no: int) -> DropNode:
    """Parse ``drop col1, col2, ...`` into a :class:`DropNode`."""
    columns = [c.strip() for c in args.split(",") if c.strip()]
    if not columns:
        raise SyntaxError(
            f"Line {line_no}: 'drop' requires at least one column. "
            "Example: drop salary, department"
        )
    return DropNode(columns=columns)


def _parse_limit(args: str, line_no: int) -> LimitNode:
    """Parse ``limit <n>`` into a :class:`LimitNode`."""
    try:
        n = int(args.strip())
        if n < 0:
            raise ValueError
    except ValueError:
        raise SyntaxError(
            f"Line {line_no}: 'limit' requires a positive integer. "
            "Example: limit 100"
        )
    return LimitNode(n=n)


def _parse_agg(node_cls: type, verb: str) -> callable:
    """Return a parser function for single-column aggregation commands."""
    def _parser(args: str, line_no: int):
        col = args.strip()
        if not col:
            raise SyntaxError(
                f"Line {line_no}: '{verb}' requires a column name. "
                f"Example: {verb} salary"
            )
        return node_cls(column=col)
    return _parser


def _parse_join(args: str, line_no: int) -> JoinNode:
    """Parse ``join "file.csv" on <column>`` into a :class:`JoinNode`."""
    # Split off a quoted path first
    m = re.match(r'^["\']([^"\']+)["\'](.*)$', args.strip())
    if not m:
        raise SyntaxError(
            f"Line {line_no}: 'join' requires a quoted file path. "
            "Example: join \"data/other.csv\" on id"
        )
    file_path = m.group(1)
    remainder = m.group(2).strip()
    if not remainder.lower().startswith("on"):
        raise SyntaxError(
            f"Line {line_no}: 'join' requires 'on <column>'. "
            "Example: join \"data/other.csv\" on id"
        )
    key = remainder[2:].strip()
    if not key:
        raise SyntaxError(
            f"Line {line_no}: 'join … on' requires a key column name."
        )
    return JoinNode(file_path=file_path, key=key)


def _parse_merge(args: str, line_no: int) -> MergeNode:
    """Parse ``merge "file.csv"`` into a :class:`MergeNode`."""
    path = _strip_quotes(args.strip())
    if not path:
        raise SyntaxError(
            f"Line {line_no}: 'merge' requires a file path. "
            "Example: merge \"data/extra.csv\""
        )
    return MergeNode(file_path=path)


def _parse_head(args: str, line_no: int) -> HeadNode:
    """Parse ``head <n>`` into a :class:`HeadNode`."""
    try:
        n = int(args.strip())
        if n < 0:
            raise ValueError
    except ValueError:
        raise SyntaxError(
            f"Line {line_no}: 'head' requires a positive integer. "
            "Example: head 10"
        )
    return HeadNode(n=n)


def _parse_assert(args: str, line_no: int) -> AssertNode:
    """Parse ``assert <column> <op> <value>`` into an :class:`AssertNode`."""
    args = args.strip()
    for op in _FILTER_OPERATORS:
        if op in args:
            parts = args.split(op, maxsplit=1)
            column = parts[0].strip()
            value  = parts[1].strip()
            if column and value:
                return AssertNode(column=column, operator=op, value=value)
    raise SyntaxError(
        f"Line {line_no}: could not parse 'assert' condition '{args}'. "
        "Expected: assert <column> <op> <value>  (e.g. assert age > 0)"
    )


def _parse_fill(args: str, line_no: int) -> FillNode:
    """Parse ``fill <column> <strategy|value>`` into a :class:`FillNode`."""
    parts = args.strip().split(maxsplit=1)
    if len(parts) < 2:
        raise SyntaxError(
            f"Line {line_no}: 'fill' requires a column and a strategy or value. "
            "Example: fill age mean  |  fill country \"Unknown\""
        )
    return FillNode(column=parts[0], strategy=parts[1])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_PARSERS: dict[str, callable] = {
    "source":    _parse_source,
    "filter":    _parse_filter,
    "select":    _parse_select,
    "group":     _parse_group_by,
    "save":      _parse_save,
    "sort":      _parse_sort,
    "rename":    _parse_rename,
    "add":       _parse_add,
    "drop":      _parse_drop,
    "limit":     _parse_limit,
    "sum":       _parse_agg(SumNode, "sum"),
    "avg":       _parse_agg(AvgNode, "avg"),
    "min":       _parse_agg(MinNode, "min"),
    "max":       _parse_agg(MaxNode, "max"),
    "join":      _parse_join,
    "merge":     _parse_merge,
    "head":      _parse_head,
    "assert":    _parse_assert,
    "fill":      _parse_fill,
}

# Commands that take no arguments
_NO_ARG_NODES: dict[str, ASTNode] = {
    "count":    CountNode,
    "distinct": DistinctNode,
    "print":    PrintNode,
    "schema":   SchemaNode,
    "inspect":  InspectNode,
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

        if keyword_lower in _NO_ARG_NODES:
            nodes.append(_NO_ARG_NODES[keyword_lower]())
            continue

        if keyword_lower in _PARSERS:
            node = _PARSERS[keyword_lower](rest, line_no)
            nodes.append(node)
        else:
            all_commands = sorted(list(_PARSERS) + list(_NO_ARG_NODES))
            raise SyntaxError(
                f"Line {line_no}: unknown command '{keyword}'. "
                "Supported commands: " + ", ".join(all_commands)
            )

    return nodes
