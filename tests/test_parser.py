"""Tests for ppl_parser.parse_lines — one test per command + error cases."""

from __future__ import annotations

import pytest

from ast_nodes import (
    AddIfNode,
    AddNode,
    AssertNode,
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
from ppl_parser import parse_lines


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse1(line: str):
    """Parse a single line and return the single resulting node."""
    nodes = parse_lines([line])
    assert len(nodes) == 1
    return nodes[0]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

class TestSource:
    def test_basic(self):
        node = parse1('source "data/people.csv"')
        assert isinstance(node, SourceNode)
        assert node.file_path == "data/people.csv"
        assert node.chunk_size is None

    def test_chunk_size(self):
        node = parse1('source "data/big.csv" chunk 50000')
        assert isinstance(node, SourceNode)
        assert node.chunk_size == 50000

    def test_single_quotes(self):
        node = parse1("source 'data/file.csv'")
        assert node.file_path == "data/file.csv"

    def test_parquet_extension(self):
        node = parse1('source "data/snapshot.parquet"')
        assert node.file_path == "data/snapshot.parquet"

    def test_chunk_case_insensitive(self):
        node = parse1('source "data/big.csv" CHUNK 1000')
        assert node.chunk_size == 1000

    def test_missing_path_raises(self):
        with pytest.raises(SyntaxError, match="source"):
            parse_lines(["source"])

    def test_bad_chunk_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(['source "file.csv" chunk abc'])


class TestForeach:
    def test_basic(self):
        node = parse1('foreach "data/monthly/*.csv"')
        assert isinstance(node, ForeachNode)
        assert node.pattern == "data/monthly/*.csv"

    def test_missing_pattern_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["foreach"])


class TestInclude:
    def test_basic(self):
        node = parse1('include "shared/clean.ppl"')
        assert isinstance(node, IncludeNode)
        assert node.file_path == "shared/clean.ppl"


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

class TestFilter:
    def test_gt(self):
        node = parse1("filter age > 18")
        assert isinstance(node, FilterNode)
        assert node.column == "age"
        assert node.operator == ">"
        assert node.value == "18"

    def test_all_operators(self):
        for op in [">", "<", ">=", "<=", "==", "!="]:
            node = parse1(f"filter age {op} 18")
            assert node.operator == op

    def test_where_alias(self):
        node = parse1("where age > 18")
        assert isinstance(node, FilterNode)

    def test_compound_and(self):
        node = parse1('filter age >= 18 and country == "Germany"')
        assert isinstance(node, CompoundFilterNode)
        assert len(node.conditions) == 2
        assert node.logic == ["and"]

    def test_compound_or(self):
        node = parse1('filter salary == 0 or age < 18')
        assert isinstance(node, CompoundFilterNode)
        assert node.logic == ["or"]

    def test_compound_multiple(self):
        node = parse1("filter a > 1 and b < 2 or c == 3")
        assert isinstance(node, CompoundFilterNode)
        assert len(node.conditions) == 3
        assert node.logic == ["and", "or"]

    def test_missing_condition_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["filter"])


# ---------------------------------------------------------------------------
# Column selection
# ---------------------------------------------------------------------------

class TestSelect:
    def test_single(self):
        node = parse1("select name")
        assert isinstance(node, SelectNode)
        assert node.columns == ["name"]

    def test_multiple(self):
        node = parse1("select name, age, salary")
        assert node.columns == ["name", "age", "salary"]

    def test_missing_columns_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["select"])


class TestDrop:
    def test_basic(self):
        node = parse1("drop salary, country")
        assert isinstance(node, DropNode)
        assert node.columns == ["salary", "country"]


class TestLimit:
    def test_basic(self):
        node = parse1("limit 100")
        assert isinstance(node, LimitNode)
        assert node.n == 100

    def test_negative_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["limit -5"])

    def test_non_int_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["limit abc"])


class TestDistinct:
    def test_basic(self):
        node = parse1("distinct")
        assert isinstance(node, DistinctNode)


class TestSample:
    def test_absolute(self):
        node = parse1("sample 50")
        assert isinstance(node, SampleNode)
        assert node.n == 50
        assert node.pct is None

    def test_percentage(self):
        node = parse1("sample 10%")
        assert node.pct == 10.0
        assert node.n is None

    def test_invalid_pct_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["sample 200%"])


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

class TestSort:
    def test_single_asc(self):
        node = parse1("sort by age")
        assert isinstance(node, SortNode)
        assert node.columns == ["age"]
        assert node.ascending == [True]

    def test_desc(self):
        node = parse1("sort by salary desc")
        assert node.ascending == [False]

    def test_multi_column(self):
        node = parse1("sort by country asc, salary desc")
        assert node.columns == ["country", "salary"]
        assert node.ascending == [True, False]

    def test_invalid_direction_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["sort by age sideways"])


class TestRename:
    def test_basic(self):
        node = parse1("rename salary income")
        assert isinstance(node, RenameNode)
        assert node.old_name == "salary"
        assert node.new_name == "income"

    def test_wrong_arg_count_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["rename a"])


class TestAdd:
    def test_arithmetic(self):
        node = parse1("add tax = salary * 0.2")
        assert isinstance(node, AddNode)
        assert node.column == "tax"
        assert "salary" in node.expression

    def test_if_then_else(self):
        node = parse1('add tier = if salary > 80000 then "senior" else "junior"')
        assert isinstance(node, AddIfNode)
        assert node.column == "tier"
        assert node.cond_op == ">"

    def test_missing_equals_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["add tax salary"])


class TestStringTransforms:
    def test_trim(self):
        node = parse1("trim country")
        assert isinstance(node, TrimNode)
        assert node.column == "country"

    def test_uppercase(self):
        node = parse1("uppercase name")
        assert isinstance(node, UppercaseNode)

    def test_lowercase(self):
        node = parse1("lowercase name")
        assert isinstance(node, LowercaseNode)


class TestCast:
    def test_int(self):
        node = parse1("cast age int")
        assert isinstance(node, CastNode)
        assert node.type_name == "int"

    def test_float(self):
        node = parse1("cast salary float")
        assert node.type_name == "float"

    def test_missing_type_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["cast age"])


class TestReplace:
    def test_basic(self):
        node = parse1('replace country "Germany" "DE"')
        assert isinstance(node, ReplaceNode)
        assert node.old_value == "Germany"
        assert node.new_value == "DE"


class TestPivot:
    def test_basic(self):
        node = parse1("pivot index=country column=year value=revenue")
        assert isinstance(node, PivotNode)
        assert node.index == "country"
        assert node.column == "year"
        assert node.value == "revenue"


# ---------------------------------------------------------------------------
# Grouping & Aggregation
# ---------------------------------------------------------------------------

class TestGroupBy:
    def test_single(self):
        node = parse1("group by country")
        assert isinstance(node, GroupByNode)
        assert node.columns == ["country"]

    def test_multi(self):
        node = parse1("group by country, age")
        assert node.columns == ["country", "age"]

    def test_missing_by_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["group country"])


class TestAggregations:
    @pytest.mark.parametrize("verb,cls", [
        ("sum", SumNode),
        ("avg", AvgNode),
        ("min", MinNode),
        ("max", MaxNode),
    ])
    def test_single_agg(self, verb, cls):
        node = parse1(f"{verb} salary")
        assert isinstance(node, cls)
        assert node.column == "salary"

    def test_count_no_args(self):
        node = parse1("count")
        assert isinstance(node, CountNode)

    def test_count_if(self):
        node = parse1("count if salary > 50000")
        assert isinstance(node, CountIfNode)
        assert node.operator == ">"

    def test_multi_agg(self):
        node = parse1("agg sum salary, avg age, count")
        assert isinstance(node, MultiAggNode)
        assert ("sum", "salary") in node.specs
        assert ("count", None) in node.specs

    def test_agg_missing_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["agg"])


# ---------------------------------------------------------------------------
# Joining
# ---------------------------------------------------------------------------

class TestJoin:
    def test_inner_default(self):
        node = parse1('join "lookup.csv" on id')
        assert isinstance(node, JoinNode)
        assert node.key == "id"
        assert node.how == "inner"

    def test_left(self):
        node = parse1('join "lookup.csv" on id left')
        assert node.how == "left"

    def test_right(self):
        node = parse1('join "lookup.csv" on id right')
        assert node.how == "right"

    def test_outer(self):
        node = parse1('join "lookup.csv" on id outer')
        assert node.how == "outer"

    def test_inner_explicit(self):
        node = parse1('join "lookup.csv" on id inner')
        assert node.how == "inner"

    def test_invalid_how_raises(self):
        with pytest.raises(SyntaxError, match="join type"):
            parse_lines(['join "f.csv" on id cross'])

    def test_missing_on_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(['join "f.csv" id'])

    def test_missing_path_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["join on id"])


class TestMerge:
    def test_basic(self):
        node = parse1('merge "extra.csv"')
        assert isinstance(node, MergeNode)
        assert node.file_path == "extra.csv"


# ---------------------------------------------------------------------------
# Output / inspection
# ---------------------------------------------------------------------------

class TestOutput:
    def test_save_csv(self):
        node = parse1('save "output/result.csv"')
        assert isinstance(node, SaveNode)
        assert node.file_path == "output/result.csv"

    def test_save_json(self):
        node = parse1('save "output/result.json"')
        assert isinstance(node, SaveNode)

    def test_save_parquet(self):
        node = parse1('save "output/result.parquet"')
        assert isinstance(node, SaveNode)

    def test_print(self):
        assert isinstance(parse1("print"), PrintNode)

    def test_schema(self):
        assert isinstance(parse1("schema"), SchemaNode)

    def test_inspect(self):
        assert isinstance(parse1("inspect"), InspectNode)

    def test_head(self):
        node = parse1("head 10")
        assert isinstance(node, HeadNode)
        assert node.n == 10

    def test_log(self):
        node = parse1('log "Processing complete"')
        assert isinstance(node, LogNode)


# ---------------------------------------------------------------------------
# Quality / variables
# ---------------------------------------------------------------------------

class TestAssert:
    def test_basic(self):
        node = parse1("assert age > 0")
        assert isinstance(node, AssertNode)
        assert node.operator == ">"


class TestFill:
    def test_mean(self):
        node = parse1("fill salary mean")
        assert isinstance(node, FillNode)
        assert node.strategy == "mean"

    def test_literal(self):
        node = parse1("fill salary 0")
        assert node.strategy == "0"

    @pytest.mark.parametrize("strategy", ["mean", "median", "mode", "forward", "backward", "drop"])
    def test_all_strategies(self, strategy):
        node = parse1(f"fill salary {strategy}")
        assert node.strategy == strategy


class TestSet:
    def test_basic(self):
        node = parse1("set threshold = 50000")
        assert isinstance(node, SetNode)
        assert node.name == "threshold"
        assert node.value == "50000"

    def test_sandbox(self):
        node = parse1("set sandbox = ./data")
        assert node.name == "sandbox"

    def test_missing_equals_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["set threshold 50000"])


class TestEnv:
    def test_basic(self):
        node = parse1("env DATA_PATH")
        assert isinstance(node, EnvNode)
        assert node.var_name == "DATA_PATH"


# ---------------------------------------------------------------------------
# try / on_error (new feature)
# ---------------------------------------------------------------------------

class TestTryOnError:
    def test_skip(self):
        nodes = parse_lines(["try", "cast age int", "on_error skip"])
        assert len(nodes) == 1
        node = nodes[0]
        assert isinstance(node, TryNode)
        assert len(node.body) == 1
        assert node.error_action == "skip"
        assert node.on_error_nodes == []

    def test_log(self):
        nodes = parse_lines(['try', 'assert age > 200', 'on_error log "bad data"'])
        node = nodes[0]
        assert node.error_action.startswith("log")
        assert node.on_error_nodes == []

    def test_command_handler(self):
        nodes = parse_lines(["try", "cast age int", "on_error fill age 0"])
        node = nodes[0]
        assert node.error_action == "fill age 0"
        assert len(node.on_error_nodes) == 1
        assert isinstance(node.on_error_nodes[0], FillNode)

    def test_multi_body_lines(self):
        nodes = parse_lines([
            "try",
            "cast age int",
            "filter age > 0",
            "on_error skip",
        ])
        node = nodes[0]
        assert len(node.body) == 2

    def test_try_followed_by_more_commands(self):
        nodes = parse_lines([
            "try",
            "cast age int",
            "on_error skip",
            "select name",
        ])
        assert len(nodes) == 2
        assert isinstance(nodes[0], TryNode)
        assert isinstance(nodes[1], SelectNode)

    def test_missing_on_error_raises(self):
        with pytest.raises(SyntaxError, match="on_error"):
            parse_lines(["try", "cast age int"])

    def test_empty_on_error_action_raises(self):
        with pytest.raises(SyntaxError):
            parse_lines(["try", "cast age int", "on_error"])

    def test_case_insensitive_try(self):
        nodes = parse_lines(["TRY", "cast age int", "on_error skip"])
        assert isinstance(nodes[0], TryNode)


# ---------------------------------------------------------------------------
# Inline comment stripping (file_reader integration)
# ---------------------------------------------------------------------------

class TestInlineComments:
    def test_strip_trailing_comment(self):
        from file_reader import _strip_inline_comment
        assert _strip_inline_comment('source "people.csv"  # load data') == 'source "people.csv"'

    def test_no_comment(self):
        from file_reader import _strip_inline_comment
        assert _strip_inline_comment("filter age > 18") == "filter age > 18"

    def test_hash_in_quoted_value_preserved(self):
        from file_reader import _strip_inline_comment
        # '#' immediately after non-whitespace is NOT stripped
        assert _strip_inline_comment('replace col "#" "x"') == 'replace col "#" "x"'

    def test_full_line_comment_excluded(self):
        from file_reader import read_ppl_file
        import tempfile, os
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ppl", delete=False) as f:
            f.write('# full line comment\n')
            f.write('filter age > 18  # inline\n')
            name = f.name
        try:
            lines = read_ppl_file(name)
            assert lines == ["filter age > 18"]
        finally:
            os.unlink(name)


# ---------------------------------------------------------------------------
# Unknown command
# ---------------------------------------------------------------------------

class TestUnknownCommand:
    def test_raises_syntax_error(self):
        with pytest.raises(SyntaxError, match="unknown command"):
            parse_lines(["foobar arg"])
