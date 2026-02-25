"""Parser for the pipeline DSL.

Converts a list of cleaned text lines (as produced by
:func:`~file_reader.read_ppl_file`) into a list of
:class:`~ast_nodes.ASTNode` objects ready for execution.

.. note::

    This module is named ``ppl_parser`` (not ``parser``) to avoid
    shadowing the Python standard-library ``parser`` module on Python <= 3.9.

Supported commands
------------------
``source "file.csv"``
    Load a CSV file.
``filter <column> <op> <value>``
    Filter rows by a condition (operators: >, <, >=, <=, ==, !=).
``filter <col> <op> <val> and/or <col> <op> <val>``
    Multi-condition filter on one line.
``where <column> <op> <value>``
    Alias for ``filter``.
``select <col1>, <col2>, ...``
    Keep only the listed columns.
``group by <col1>, <col2>, ...``
    Group by one or more columns.
``count``
    Count rows (optionally within groups).
``count if <column> <op> <value>``
    Print count of matching rows without filtering the dataset.
``sort by <col> [asc|desc], ...``
    Sort rows by one or more columns.
``rename <old> <new>``
    Rename a column.
``add <col> = <expression>``
    Add a computed column (arithmetic expression using column names).
``add <col> = if <cond> then <val> else <val>``
    Add a column with a conditional value.
``drop <col1>, <col2>, ...``
    Remove columns.
``limit <n>``
    Keep only the first n rows.
``distinct``
    Remove duplicate rows.
``sample <n>``
    Take a random sample of n rows.
``sample <n>%``
    Take a random sample of n% of the data.
``trim <column>``
    Strip leading/trailing whitespace from a string column.
``uppercase <column>``
    Convert a column to uppercase.
``lowercase <column>``
    Convert a column to lowercase.
``cast <column> <type>``
    Cast a column to a different type (int, float, str, datetime, bool).
``replace <column> <old> <new>``
    Replace occurrences of a value in a column.
``pivot index=<col> column=<col> value=<col>``
    Reshape data from long to wide format.
``sum <column>``
    Sum a numeric column (per group or total).
``avg <column>``
    Average of a numeric column (per group or total).
``min <column>``
    Minimum value of a column (per group or total).
``max <column>``
    Maximum value of a column (per group or total).
``agg sum <col>, avg <col>, count``
    Multiple aggregations at once after a group by.
``join "file.csv" on <column>``
    Inner-join with another CSV on a key column.
``merge "file.csv"``
    Append rows from another CSV file (union).
``foreach "glob_pattern"``
    Load and concatenate all CSVs matching a glob pattern.
``include "file.ppl"``
    Include and execute another pipeline file.
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
``log <message>``
    Print a message to the terminal during execution.
``assert <column> <op> <value>``
    Fail the pipeline if any row violates the condition.
``fill <column> <strategy|value>``
    Fill missing values (strategies: mean, median, mode, forward, backward, drop).
``set <name> = <value>``
    Set a named variable referenceable as $name in other commands.
``env <VAR_NAME>``
    Load an OS environment variable into the pipeline variable store.
"""

from __future__ import annotations

import re

from ast_nodes import (
    AddIfNode,
    AddNode,
    AssertNode,
    ASTNode,
    AvgNode,
    CastNode,
    CompoundFilterNode,
    CountIfNode,
    CountNode,
    DistinctNode,
    DropNode,
    EnvNode,
    FillNode,
    FilterNode,
    ForeachNode,
    GroupByNode,
    HeadNode,
    IncludeNode,
    InspectNode,
    JoinNode,
    LimitNode,
    LogNode,
    LowercaseNode,
    MaxNode,
    MergeNode,
    MinNode,
    MultiAggNode,
    PivotNode,
    PrintNode,
    RenameNode,
    ReplaceNode,
    SampleNode,
    SaveNode,
    SchemaNode,
    SelectNode,
    SetNode,
    SortNode,
    SourceNode,
    SumNode,
    TrimNode,
    TryNode,
    UppercaseNode,
)

# Ordered so multi-character operators are tried before single-character ones.
_FILTER_OPERATORS: list[str] = [">=", "<=", "!=", "==", ">", "<"]

# Regex that matches a quoted string (single or double quotes).
_QUOTED_RE = re.compile(r'^["\'](.+)["\']$')

# Regex that splits on ' and ' / ' or ' (case-insensitive) for compound filters.
_COMPOUND_SPLIT_RE = re.compile(r'\s+(and|or)\s+', re.IGNORECASE)

# Regex that detects 'if <cond> then <val> else <val>' in an add expression.
_IF_THEN_ELSE_RE = re.compile(
    r'^if\s+(.+?)\s+then\s+(.+?)\s+else\s+(.+)$', re.IGNORECASE
)


def _strip_quotes(value: str) -> str:
    """Remove surrounding single or double quotes from *value* if present."""
    m = _QUOTED_RE.match(value.strip())
    return m.group(1) if m else value.strip()


# ---------------------------------------------------------------------------
# Shared condition parsing helper
# ---------------------------------------------------------------------------

def _parse_condition_tuple(
    cond_str: str, line_no: int, context: str = "filter"
) -> tuple[str, str, str]:
    """Parse ``col op val`` and return ``(col, op, val)``."""
    cond_str = cond_str.strip()
    for op in _FILTER_OPERATORS:
        if op in cond_str:
            parts = cond_str.split(op, maxsplit=1)
            column = parts[0].strip()
            value  = parts[1].strip()
            if column and value:
                return (column, op, value)
    raise SyntaxError(
        f"Line {line_no}: could not parse '{context}' condition '{cond_str}'. "
        f"Expected: {context} <column> <op> <value>  (e.g. {context} age > 18)"
    )


# ---------------------------------------------------------------------------
# Individual command parsers
# ---------------------------------------------------------------------------

def _parse_source(args: str, line_no: int) -> SourceNode:
    """Parse ``source "file.csv"`` or ``source "file.csv" chunk N``.

    The optional ``chunk N`` suffix enables chunked reading for large files.
    The executor automatically applies row-safe operations per chunk,
    reducing peak memory usage.

    Examples::

        source "data/people.csv"
        source "data/big.csv" chunk 100000
        source "data/snapshot.parquet"
    """
    args = args.strip()
    m = re.match(
        r'^["\']([^"\']+)["\']\s*(?:chunk\s+(\d+))?\s*$', args, re.IGNORECASE
    )
    if not m or not m.group(1):
        raise SyntaxError(
            f"Line {line_no}: 'source' requires a file path. "
            "Example: source \"data/people.csv\"\n"
            "         source \"data/big.csv\" chunk 100000"
        )
    path = m.group(1)
    chunk_size = int(m.group(2)) if m.group(2) else None
    if chunk_size is not None and chunk_size <= 0:
        raise SyntaxError(
            f"Line {line_no}: chunk size must be a positive integer. "
            "Example: source \"data/big.csv\" chunk 100000"
        )
    return SourceNode(file_path=path, chunk_size=chunk_size)


def _parse_filter(args: str, line_no: int):
    """Parse ``filter``/``where`` — supports single and AND/OR compound conditions."""
    args = args.strip()
    # re.split with a capturing group includes the captured tokens in the list
    parts = _COMPOUND_SPLIT_RE.split(args)
    # parts alternates: [cond0, "and"/"or", cond1, ...]
    if len(parts) == 1:
        col, op, val = _parse_condition_tuple(args, line_no, "filter")
        return FilterNode(column=col, operator=op, value=val)

    conditions: list[tuple[str, str, str]] = []
    logic: list[str] = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            col, op, val = _parse_condition_tuple(part.strip(), line_no, "filter")
            conditions.append((col, op, val))
        else:
            logic.append(part.lower())
    return CompoundFilterNode(conditions=conditions, logic=logic)


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
    """Parse ``group by col1, col2, ...`` into a :class:`GroupByNode`."""
    lowered = args.strip().lower()
    if not lowered.startswith("by"):
        raise SyntaxError(
            f"Line {line_no}: 'group' must be followed by 'by'. "
            "Example: group by country"
        )
    remainder = args.strip()[2:].strip()
    columns = [c.strip() for c in remainder.split(",") if c.strip()]
    if not columns:
        raise SyntaxError(
            f"Line {line_no}: 'group by' requires at least one column. "
            "Example: group by country"
        )
    return GroupByNode(columns=columns)


def _parse_count(args: str, line_no: int):
    """Parse ``count`` or ``count if <condition>``."""
    args = args.strip()
    if not args:
        return CountNode()
    if args.lower().startswith("if "):
        cond_str = args[3:].strip()
        col, op, val = _parse_condition_tuple(cond_str, line_no, "count if")
        return CountIfNode(column=col, operator=op, value=val)
    raise SyntaxError(
        f"Line {line_no}: 'count' takes no arguments or 'count if <condition>'. "
        "Example: count  or  count if salary > 50000"
    )


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
    """Parse ``sort by col [asc|desc], ...`` into a :class:`SortNode`."""
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


def _parse_add(args: str, line_no: int):
    """Parse ``add <col> = <expression>`` or the if/then/else variant."""
    if "=" not in args:
        raise SyntaxError(
            f"Line {line_no}: 'add' requires '='. "
            "Example: add tax = price * 0.2"
        )
    col, _, expr = args.partition("=")
    col  = col.strip()
    expr = expr.strip()
    if not col or not expr:
        raise SyntaxError(
            f"Line {line_no}: 'add' requires a column name and expression. "
            "Example: add tax = price * 0.2"
        )
    m = _IF_THEN_ELSE_RE.match(expr)
    if m:
        cond_str  = m.group(1).strip()
        true_val  = m.group(2).strip()
        false_val = m.group(3).strip()
        cond_col, cond_op, cond_val = _parse_condition_tuple(
            cond_str, line_no, "add"
        )
        return AddIfNode(
            column=col,
            cond_col=cond_col,
            cond_op=cond_op,
            cond_val=cond_val,
            true_val=true_val,
            false_val=false_val,
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


def _parse_sample(args: str, line_no: int) -> SampleNode:
    """Parse ``sample N`` or ``sample N%`` into a :class:`SampleNode`."""
    args = args.strip()
    if args.endswith("%"):
        try:
            pct = float(args[:-1])
            if not (0 < pct <= 100):
                raise ValueError
        except ValueError:
            raise SyntaxError(
                f"Line {line_no}: 'sample N%' requires a percentage between 0 and 100. "
                "Example: sample 10%"
            )
        return SampleNode(n=None, pct=pct)
    try:
        n = int(args)
        if n < 0:
            raise ValueError
    except ValueError:
        raise SyntaxError(
            f"Line {line_no}: 'sample' requires a positive integer or percentage. "
            "Example: sample 100  or  sample 10%"
        )
    return SampleNode(n=n, pct=None)


def _parse_string_transform(node_cls, verb: str):
    """Return a parser for ``trim``/``uppercase``/``lowercase``."""
    def _parser(args: str, line_no: int):
        col = args.strip()
        if not col:
            raise SyntaxError(
                f"Line {line_no}: '{verb}' requires a column name. "
                f"Example: {verb} country"
            )
        return node_cls(column=col)
    return _parser


def _parse_cast(args: str, line_no: int) -> CastNode:
    """Parse ``cast <column> <type>`` into a :class:`CastNode`."""
    parts = args.strip().split()
    if len(parts) != 2:
        raise SyntaxError(
            f"Line {line_no}: 'cast' requires a column name and a type. "
            "Example: cast age int"
        )
    return CastNode(column=parts[0], type_name=parts[1])


def _parse_replace(args: str, line_no: int) -> ReplaceNode:
    """Parse ``replace <col> <old> <new>`` into a :class:`ReplaceNode`."""
    args = args.strip()
    m = re.match(r'^(\S+)\s+(["\'].*?["\']|\S+)\s+(["\'].*?["\']|\S+)$', args)
    if not m:
        raise SyntaxError(
            f"Line {line_no}: 'replace' requires column, old value, and new value. "
            "Example: replace country \"Germany\" \"DE\""
        )
    return ReplaceNode(
        column=m.group(1),
        old_value=_strip_quotes(m.group(2)),
        new_value=_strip_quotes(m.group(3)),
    )


def _parse_pivot(args: str, line_no: int) -> PivotNode:
    """Parse ``pivot index=col column=col value=col`` into a :class:`PivotNode`."""
    m = re.match(
        r'index=(\S+)\s+column=(\S+)\s+value=(\S+)', args.strip(), re.IGNORECASE
    )
    if not m:
        raise SyntaxError(
            f"Line {line_no}: 'pivot' requires index=, column=, and value= parameters. "
            "Example: pivot index=country column=year value=revenue"
        )
    return PivotNode(index=m.group(1), column=m.group(2), value=m.group(3))


def _parse_agg(node_cls: type, verb: str):
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


def _parse_agg_multi(args: str, line_no: int) -> MultiAggNode:
    """Parse ``agg sum col, avg col, count`` into a :class:`MultiAggNode`."""
    _VALID_VERBS = {"sum", "avg", "min", "max", "count"}
    parts = [p.strip() for p in args.split(",") if p.strip()]
    if not parts:
        raise SyntaxError(
            f"Line {line_no}: 'agg' requires at least one spec. "
            "Example: agg sum salary, avg age, count"
        )
    specs: list[tuple[str, str | None]] = []
    for part in parts:
        tokens = part.split()
        verb = tokens[0].lower()
        if verb not in _VALID_VERBS:
            raise SyntaxError(
                f"Line {line_no}: unknown agg verb '{verb}'. "
                f"Supported: {', '.join(sorted(_VALID_VERBS))}"
            )
        if verb == "count":
            specs.append(("count", None))
        elif len(tokens) == 2:
            specs.append((verb, tokens[1]))
        else:
            raise SyntaxError(
                f"Line {line_no}: '{verb}' requires a column name in agg. "
                f"Example: {verb} salary"
            )
    return MultiAggNode(specs=specs)


def _parse_join(args: str, line_no: int) -> JoinNode:
    """Parse ``join "file.csv" on <column> [inner|left|right|outer]``.

    The join type defaults to ``inner`` when omitted.

    Examples::

        join "data/other.csv" on id
        join "data/other.csv" on id left
        join "data/other.csv" on id outer
    """
    _VALID_HOW = ("inner", "left", "right", "outer")
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
    after_on = remainder[2:].strip()
    parts = after_on.split()
    if not parts:
        raise SyntaxError(f"Line {line_no}: 'join … on' requires a key column name.")
    key = parts[0]
    how = parts[1].lower() if len(parts) > 1 else "inner"
    if how not in _VALID_HOW:
        raise SyntaxError(
            f"Line {line_no}: join type must be one of: {', '.join(_VALID_HOW)}. "
            f"Got '{how}'. Example: join \"data/other.csv\" on id left"
        )
    return JoinNode(file_path=file_path, key=key, how=how)


def _parse_merge(args: str, line_no: int) -> MergeNode:
    """Parse ``merge "file.csv"`` into a :class:`MergeNode`."""
    path = _strip_quotes(args.strip())
    if not path:
        raise SyntaxError(
            f"Line {line_no}: 'merge' requires a file path. "
            "Example: merge \"data/extra.csv\""
        )
    return MergeNode(file_path=path)


def _parse_foreach(args: str, line_no: int) -> ForeachNode:
    """Parse ``foreach "pattern"`` into a :class:`ForeachNode`."""
    pattern = _strip_quotes(args.strip())
    if not pattern:
        raise SyntaxError(
            f"Line {line_no}: 'foreach' requires a glob pattern. "
            "Example: foreach \"data/monthly/*.csv\""
        )
    return ForeachNode(pattern=pattern)


def _parse_include(args: str, line_no: int) -> IncludeNode:
    """Parse ``include "file.ppl"`` into an :class:`IncludeNode`."""
    path = _strip_quotes(args.strip())
    if not path:
        raise SyntaxError(
            f"Line {line_no}: 'include' requires a file path. "
            "Example: include \"pipelines/shared/clean.ppl\""
        )
    return IncludeNode(file_path=path)


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


def _parse_log(args: str, line_no: int) -> LogNode:
    """Parse ``log <message>`` into a :class:`LogNode`."""
    if not args.strip():
        raise SyntaxError(
            f"Line {line_no}: 'log' requires a message. "
            "Example: log \"Processing complete\""
        )
    return LogNode(message=args.strip())


def _parse_assert(args: str, line_no: int) -> AssertNode:
    """Parse ``assert <column> <op> <value>`` into an :class:`AssertNode`."""
    col, op, val = _parse_condition_tuple(args.strip(), line_no, "assert")
    return AssertNode(column=col, operator=op, value=val)


def _parse_fill(args: str, line_no: int) -> FillNode:
    """Parse ``fill <column> <strategy|value>`` into a :class:`FillNode`."""
    parts = args.strip().split(maxsplit=1)
    if len(parts) < 2:
        raise SyntaxError(
            f"Line {line_no}: 'fill' requires a column and a strategy or value. "
            "Example: fill age mean  |  fill country \"Unknown\""
        )
    return FillNode(column=parts[0], strategy=parts[1])


def _parse_set(args: str, line_no: int) -> SetNode:
    """Parse ``set <name> = <value>`` into a :class:`SetNode`."""
    if "=" not in args:
        raise SyntaxError(
            f"Line {line_no}: 'set' requires '='. "
            "Example: set threshold = 50000"
        )
    name, _, value = args.partition("=")
    name  = name.strip()
    value = value.strip()
    if not name or not value:
        raise SyntaxError(
            f"Line {line_no}: 'set' requires a name and a value. "
            "Example: set threshold = 50000"
        )
    return SetNode(name=name, value=value)


def _parse_env(args: str, line_no: int) -> EnvNode:
    """Parse ``env VAR_NAME`` into an :class:`EnvNode`."""
    var_name = args.strip()
    if not var_name:
        raise SyntaxError(
            f"Line {line_no}: 'env' requires an environment variable name. "
            "Example: env DATA_PATH"
        )
    return EnvNode(var_name=var_name)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_PARSERS: dict[str, callable] = {
    "source":    _parse_source,
    "filter":    _parse_filter,
    "where":     _parse_filter,           # SQL-friendly alias for filter
    "select":    _parse_select,
    "group":     _parse_group_by,
    "count":     _parse_count,            # handles both 'count' and 'count if'
    "save":      _parse_save,
    "sort":      _parse_sort,
    "rename":    _parse_rename,
    "add":       _parse_add,
    "drop":      _parse_drop,
    "limit":     _parse_limit,
    "sample":    _parse_sample,
    "trim":      _parse_string_transform(TrimNode, "trim"),
    "uppercase": _parse_string_transform(UppercaseNode, "uppercase"),
    "lowercase": _parse_string_transform(LowercaseNode, "lowercase"),
    "cast":      _parse_cast,
    "replace":   _parse_replace,
    "pivot":     _parse_pivot,
    "sum":       _parse_agg(SumNode, "sum"),
    "avg":       _parse_agg(AvgNode, "avg"),
    "min":       _parse_agg(MinNode, "min"),
    "max":       _parse_agg(MaxNode, "max"),
    "agg":       _parse_agg_multi,
    "join":      _parse_join,
    "merge":     _parse_merge,
    "foreach":   _parse_foreach,
    "include":   _parse_include,
    "head":      _parse_head,
    "log":       _parse_log,
    "assert":    _parse_assert,
    "fill":      _parse_fill,
    "set":       _parse_set,
    "env":       _parse_env,
}

# Commands that take no arguments
_NO_ARG_NODES: dict[str, ASTNode] = {
    "distinct": DistinctNode,
    "print":    PrintNode,
    "schema":   SchemaNode,
    "inspect":  InspectNode,
}


def parse_lines(lines: list[str]) -> list[ASTNode]:
    """Convert a list of cleaned DSL lines into a list of :class:`ASTNode` objects.

    Supports ``try`` / ``on_error`` block syntax for error recovery in addition
    to the standard single-line commands.

    Args:
        lines: Cleaned lines as returned by :func:`~file_reader.read_ppl_file`.

    Returns:
        An ordered list of AST nodes ready to be passed to
        :func:`~executor.run_pipeline`.

    Raises:
        SyntaxError: If an unknown command is encountered, a command
            cannot be parsed correctly, or a ``try`` block is missing its
            ``on_error`` handler.
    """
    nodes: list[ASTNode] = []
    i = 0

    while i < len(lines):
        line = lines[i]
        line_no = i + 1  # 1-indexed for error messages
        keyword, _, rest = line.partition(" ")
        keyword_lower = keyword.lower()

        # ------------------------------------------------------------------ #
        # try / on_error block                                                #
        # ------------------------------------------------------------------ #
        if keyword_lower == "try":
            body_lines: list[str] = []
            i += 1
            depth = 1  # track nesting: each inner 'try' increments, each 'on_error' decrements
            found_on_error = False
            while i < len(lines):
                token = lines[i].strip().split()[0].lower() if lines[i].strip() else ""
                if token == "try":
                    depth += 1
                    body_lines.append(lines[i])
                elif token == "on_error":
                    depth -= 1
                    if depth == 0:
                        found_on_error = True
                        break
                    body_lines.append(lines[i])  # inner on_error is part of body
                else:
                    body_lines.append(lines[i])
                i += 1
            if not found_on_error:
                raise SyntaxError(
                    f"Line {line_no}: 'try' block has no 'on_error' handler. "
                    "Example:\n  try\n    cast age int\n  on_error skip"
                )
            on_error_line = lines[i]
            _, _, error_action = on_error_line.partition(" ")
            error_action = error_action.strip()

            body_nodes = parse_lines(body_lines)

            # Pre-parse the error handler for non-skip/log actions.
            action_lower = error_action.lower()
            if action_lower == "skip" or action_lower.startswith("log "):
                on_error_nodes: list[ASTNode] = []
            elif error_action:
                on_error_nodes = parse_lines([error_action])
            else:
                raise SyntaxError(
                    f"Line {line_no}: 'on_error' requires an action. "
                    "Use 'skip', 'log \"message\"', or any valid command."
                )

            nodes.append(
                TryNode(
                    body=body_nodes,
                    on_error_nodes=on_error_nodes,
                    error_action=error_action,
                )
            )
            i += 1
            continue

        # ------------------------------------------------------------------ #
        # Standard single-line commands                                       #
        # ------------------------------------------------------------------ #
        if keyword_lower in _NO_ARG_NODES:
            nodes.append(_NO_ARG_NODES[keyword_lower]())
        elif keyword_lower in _PARSERS:
            node = _PARSERS[keyword_lower](rest, line_no)
            nodes.append(node)
        else:
            all_commands = sorted(list(_PARSERS) + list(_NO_ARG_NODES) + ["try"])
            raise SyntaxError(
                f"Line {line_no}: unknown command '{keyword}'. "
                "Supported commands: " + ", ".join(all_commands)
            )

        i += 1

    return nodes
