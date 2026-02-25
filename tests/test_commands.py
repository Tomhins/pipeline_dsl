"""Tests for AST node execution — one class per command."""

from __future__ import annotations

import os

import pandas as pd
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
    FillNode,
    FilterNode,
    GroupByNode,
    HeadNode,
    JoinNode,
    LimitNode,
    LogNode,
    LowercaseNode,
    MaxNode,
    MergeNode,
    MinNode,
    MultiAggNode,
    PivotNode,
    RenameNode,
    ReplaceNode,
    SampleNode,
    SaveNode,
    SelectNode,
    SetNode,
    SortNode,
    SourceNode,
    SumNode,
    TrimNode,
    TryNode,
    UppercaseNode,
)
from executor import PipelineContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_ctx(df: pd.DataFrame) -> PipelineContext:
    return PipelineContext(df=df.copy())


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

class TestSourceNode:
    def test_loads_csv(self, csv_file):
        ctx = PipelineContext()
        SourceNode(file_path=csv_file).execute(ctx)
        assert ctx.df is not None
        assert len(ctx.df) == 5

    def test_loads_parquet(self, parquet_file):
        ctx = PipelineContext()
        SourceNode(file_path=parquet_file).execute(ctx)
        assert ctx.df is not None
        assert len(ctx.df) == 5

    def test_missing_file_raises(self):
        ctx = PipelineContext()
        with pytest.raises(FileNotFoundError):
            SourceNode(file_path="nonexistent.csv").execute(ctx)

    def test_clears_grouped(self, csv_file):
        ctx = PipelineContext()
        SourceNode(file_path=csv_file).execute(ctx)
        assert ctx.grouped is None

    def test_variable_in_path(self, csv_file):
        ctx = PipelineContext(variables={"f": csv_file})
        SourceNode(file_path="$f").execute(ctx)
        assert ctx.df is not None


class TestMergeNode:
    def test_appends_rows(self, ctx, extra_csv):
        original_len = len(ctx.df)
        MergeNode(file_path=extra_csv).execute(ctx)
        assert len(ctx.df) == original_len + 2

    def test_missing_file_raises(self, ctx):
        with pytest.raises(FileNotFoundError):
            MergeNode(file_path="missing.csv").execute(ctx)

    def test_no_source_raises(self):
        ctx = PipelineContext()
        with pytest.raises(RuntimeError, match="merge"):
            MergeNode(file_path="f.csv").execute(ctx)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

class TestFilterNode:
    def test_gt(self, ctx):
        FilterNode(column="age", operator=">", value="18").execute(ctx)
        assert all(ctx.df["age"] > 18)

    def test_eq_string(self, ctx):
        FilterNode(column="country", operator="==", value='"Germany"').execute(ctx)
        assert all(ctx.df["country"] == "Germany")

    def test_ne(self, ctx):
        FilterNode(column="salary", operator="!=", value="0").execute(ctx)
        assert all(ctx.df["salary"] != 0)

    def test_variable_in_value(self, ctx):
        ctx.variables["min_age"] = "18"
        FilterNode(column="age", operator=">", value="$min_age").execute(ctx)
        assert all(ctx.df["age"] > 18)

    def test_missing_column_raises(self, ctx):
        with pytest.raises(KeyError):
            FilterNode(column="height", operator=">", value="170").execute(ctx)

    def test_no_source_raises(self):
        ctx = PipelineContext()
        with pytest.raises(RuntimeError):
            FilterNode(column="age", operator=">", value="18").execute(ctx)


class TestCompoundFilterNode:
    def test_and(self, ctx):
        CompoundFilterNode(
            conditions=[("age", ">=", "18"), ("salary", ">", "0")],
            logic=["and"],
        ).execute(ctx)
        assert all(ctx.df["age"] >= 18)
        assert all(ctx.df["salary"] > 0)

    def test_or(self, ctx):
        before = len(ctx.df)
        CompoundFilterNode(
            conditions=[("age", "<", "18"), ("country", "==", '"Germany"')],
            logic=["or"],
        ).execute(ctx)
        # At least the Germany rows and the minors
        assert len(ctx.df) <= before


# ---------------------------------------------------------------------------
# Column selection
# ---------------------------------------------------------------------------

class TestSelectNode:
    def test_keeps_columns(self, ctx):
        SelectNode(columns=["name", "age"]).execute(ctx)
        assert list(ctx.df.columns) == ["name", "age"]

    def test_unknown_column_raises(self, ctx):
        with pytest.raises(KeyError):
            SelectNode(columns=["name", "height"]).execute(ctx)


class TestDropNode:
    def test_removes_columns(self, ctx):
        DropNode(columns=["salary"]).execute(ctx)
        assert "salary" not in ctx.df.columns

    def test_unknown_column_raises(self, ctx):
        with pytest.raises(KeyError):
            DropNode(columns=["nonexistent"]).execute(ctx)


class TestLimitNode:
    def test_keeps_n_rows(self, ctx):
        LimitNode(n=3).execute(ctx)
        assert len(ctx.df) == 3

    def test_limit_larger_than_data(self, ctx):
        LimitNode(n=100).execute(ctx)
        assert len(ctx.df) == 5


class TestDistinctNode:
    def test_removes_duplicates(self, ctx):
        ctx.df = pd.concat([ctx.df, ctx.df], ignore_index=True)
        before = len(ctx.df)
        DistinctNode().execute(ctx)
        assert len(ctx.df) == before // 2

    def test_no_change_when_all_unique(self, ctx):
        before = len(ctx.df)
        DistinctNode().execute(ctx)
        assert len(ctx.df) == before


class TestSampleNode:
    def test_absolute(self, ctx):
        SampleNode(n=3, pct=None).execute(ctx)
        assert len(ctx.df) == 3

    def test_percentage(self, ctx):
        SampleNode(n=None, pct=100.0).execute(ctx)
        assert len(ctx.df) == 5  # 100% of 5

    def test_sample_larger_than_data(self, ctx):
        SampleNode(n=100, pct=None).execute(ctx)
        assert len(ctx.df) == 5


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

class TestSortNode:
    def test_ascending(self, ctx):
        SortNode(columns=["age"], ascending=[True]).execute(ctx)
        ages = ctx.df["age"].tolist()
        assert ages == sorted(ages)

    def test_descending(self, ctx):
        SortNode(columns=["salary"], ascending=[False]).execute(ctx)
        salaries = ctx.df["salary"].tolist()
        assert salaries == sorted(salaries, reverse=True)

    def test_unknown_column_raises(self, ctx):
        with pytest.raises(KeyError):
            SortNode(columns=["height"], ascending=[True]).execute(ctx)


class TestRenameNode:
    def test_renames(self, ctx):
        RenameNode(old_name="salary", new_name="income").execute(ctx)
        assert "income" in ctx.df.columns
        assert "salary" not in ctx.df.columns

    def test_missing_column_raises(self, ctx):
        with pytest.raises(KeyError):
            RenameNode(old_name="height", new_name="h").execute(ctx)


class TestAddNode:
    def test_arithmetic(self, ctx):
        AddNode(column="tax", expression="salary * 0.2").execute(ctx)
        assert "tax" in ctx.df.columns
        expected = ctx.df["salary"] * 0.2
        # tax was added from original df
        assert ctx.df["tax"].iloc[0] == pytest.approx(72000 * 0.2)

    def test_bad_expression_raises(self, ctx):
        with pytest.raises(ValueError):
            AddNode(column="bad", expression="totally_invalid $$$").execute(ctx)


class TestAddIfNode:
    def test_conditional(self, ctx):
        AddIfNode(
            column="tier",
            cond_col="salary",
            cond_op=">",
            cond_val="50000",
            true_val='"senior"',
            false_val='"junior"',
        ).execute(ctx)
        assert "tier" in ctx.df.columns
        assert ctx.df.loc[ctx.df["salary"] > 50000, "tier"].eq("senior").all()
        assert ctx.df.loc[ctx.df["salary"] <= 50000, "tier"].eq("junior").all()


class TestTrimNode:
    def test_strips_whitespace(self, ctx):
        ctx.df["name"] = ctx.df["name"].apply(lambda x: f"  {x}  ")
        TrimNode(column="name").execute(ctx)
        assert all(not v.startswith(" ") for v in ctx.df["name"])


class TestUppercaseNode:
    def test_uppercases(self, ctx):
        UppercaseNode(column="country").execute(ctx)
        assert all(v == v.upper() for v in ctx.df["country"])


class TestLowercaseNode:
    def test_lowercases(self, ctx):
        LowercaseNode(column="name").execute(ctx)
        assert all(v == v.lower() for v in ctx.df["name"])


class TestCastNode:
    def test_cast_to_float(self, ctx):
        CastNode(column="age", type_name="float").execute(ctx)
        assert ctx.df["age"].dtype in [float, "float64"]

    def test_cast_to_str(self, ctx):
        CastNode(column="salary", type_name="str").execute(ctx)
        # pandas 3.x returns StringDtype; older pandas returns object — both are string types
        assert isinstance(ctx.df["salary"].iloc[0], str)

    def test_unknown_type_raises(self, ctx):
        with pytest.raises(ValueError, match="unknown type"):
            CastNode(column="age", type_name="complex").execute(ctx)


class TestReplaceNode:
    def test_replaces_value(self, ctx):
        ReplaceNode(column="country", old_value="Germany", new_value="DE").execute(ctx)
        assert "Germany" not in ctx.df["country"].values
        assert "DE" in ctx.df["country"].values


class TestPivotNode:
    def test_pivot(self):
        df = pd.DataFrame({
            "country": ["DE", "DE", "FR"],
            "year": [2022, 2023, 2022],
            "revenue": [100, 200, 150],
        })
        ctx = make_ctx(df)
        PivotNode(index="country", column="year", value="revenue").execute(ctx)
        assert 2022 in ctx.df.columns or "2022" in str(ctx.df.columns.tolist())


# ---------------------------------------------------------------------------
# Grouping & Aggregation
# ---------------------------------------------------------------------------

class TestGroupByNode:
    def test_sets_grouped(self, ctx):
        GroupByNode(columns=["country"]).execute(ctx)
        assert ctx.grouped is not None

    def test_missing_column_raises(self, ctx):
        with pytest.raises(KeyError):
            GroupByNode(columns=["height"]).execute(ctx)


class TestAggregationNodes:
    def test_count_total(self, ctx):
        CountNode().execute(ctx)
        assert ctx.df["count"].iloc[0] == 5

    def test_count_grouped(self, ctx):
        GroupByNode(columns=["country"]).execute(ctx)
        CountNode().execute(ctx)
        assert "count" in ctx.df.columns
        assert ctx.grouped is None

    def test_sum_total(self, ctx):
        SumNode(column="salary").execute(ctx)
        assert ctx.df["salary"].iloc[0] == pytest.approx(72000 + 55000 + 98000)

    def test_avg_total(self, ctx):
        AvgNode(column="age").execute(ctx)
        expected = (30 + 17 + 25 + 42 + 16) / 5
        assert ctx.df["age"].iloc[0] == pytest.approx(expected)

    def test_min_total(self, ctx):
        MinNode(column="age").execute(ctx)
        assert ctx.df["age"].iloc[0] == 16

    def test_max_total(self, ctx):
        MaxNode(column="age").execute(ctx)
        assert ctx.df["age"].iloc[0] == 42

    def test_count_if(self, ctx):
        CountIfNode(column="salary", operator=">", value="0").execute(ctx)
        # Does not modify the DataFrame
        assert len(ctx.df) == 5

    def test_multi_agg(self, ctx):
        GroupByNode(columns=["country"]).execute(ctx)
        MultiAggNode(specs=[("sum", "salary"), ("count", None)]).execute(ctx)
        assert "salary" in ctx.df.columns
        assert "count" in ctx.df.columns

    def test_multi_agg_without_group_raises(self, ctx):
        with pytest.raises(RuntimeError, match="group by"):
            MultiAggNode(specs=[("sum", "salary")]).execute(ctx)


# ---------------------------------------------------------------------------
# Joining
# ---------------------------------------------------------------------------

class TestJoinNode:
    def test_inner_join(self, ctx, lookup_csv):
        # lookup has Alice, Charlie, Diana — not Bob and Eve
        JoinNode(file_path=lookup_csv, key="name", how="inner").execute(ctx)
        assert len(ctx.df) == 3
        assert "dept" in ctx.df.columns

    def test_left_join(self, ctx, lookup_csv):
        JoinNode(file_path=lookup_csv, key="name", how="left").execute(ctx)
        assert len(ctx.df) == 5  # all left rows kept
        assert "dept" in ctx.df.columns

    def test_outer_join(self, ctx, lookup_csv):
        JoinNode(file_path=lookup_csv, key="name", how="outer").execute(ctx)
        # All rows from both sides
        assert len(ctx.df) >= 5

    def test_missing_key_raises(self, ctx, lookup_csv):
        with pytest.raises(KeyError, match="join"):
            JoinNode(file_path=lookup_csv, key="nonexistent", how="inner").execute(ctx)

    def test_missing_file_raises(self, ctx):
        with pytest.raises(FileNotFoundError):
            JoinNode(file_path="missing.csv", key="name", how="inner").execute(ctx)

    def test_no_source_raises(self):
        ctx = PipelineContext()
        with pytest.raises(RuntimeError, match="join"):
            JoinNode(file_path="f.csv", key="id", how="inner").execute(ctx)


# ---------------------------------------------------------------------------
# Output / inspection
# ---------------------------------------------------------------------------

class TestSaveNode:
    def test_saves_csv(self, ctx, tmp_path):
        out = str(tmp_path / "out.csv")
        SaveNode(file_path=out).execute(ctx)
        assert os.path.exists(out)
        reloaded = pd.read_csv(out)
        assert len(reloaded) == len(ctx.df)

    def test_saves_json(self, ctx, tmp_path):
        out = str(tmp_path / "out.json")
        SaveNode(file_path=out).execute(ctx)
        assert os.path.exists(out)

    def test_saves_parquet(self, ctx, tmp_path):
        out = str(tmp_path / "out.parquet")
        SaveNode(file_path=out).execute(ctx)
        assert os.path.exists(out)
        reloaded = pd.read_parquet(out)
        assert len(reloaded) == len(ctx.df)

    def test_creates_output_directory(self, ctx, tmp_path):
        out = str(tmp_path / "subdir" / "out.csv")
        SaveNode(file_path=out).execute(ctx)
        assert os.path.exists(out)

    def test_no_data_raises(self):
        ctx = PipelineContext()
        with pytest.raises(RuntimeError, match="save"):
            SaveNode(file_path="out.csv").execute(ctx)


class TestHeadNode:
    def test_does_not_modify_df(self, ctx, capsys):
        before = ctx.df.copy()
        HeadNode(n=2).execute(ctx)
        assert len(ctx.df) == len(before)

    def test_prints_n_rows(self, ctx, capsys):
        HeadNode(n=2).execute(ctx)
        out = capsys.readouterr().out
        assert "Alice" in out


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

class TestAssertNode:
    def test_passes_when_all_satisfy(self, ctx):
        AssertNode(column="age", operator=">", value="0").execute(ctx)  # no error

    def test_fails_when_violations_exist(self, ctx):
        with pytest.raises(AssertionError, match="row"):
            AssertNode(column="salary", operator=">", value="50000").execute(ctx)

    def test_missing_column_raises(self, ctx):
        with pytest.raises(KeyError):
            AssertNode(column="height", operator=">", value="0").execute(ctx)


class TestFillNode:
    def test_fill_mean(self):
        df = pd.DataFrame({"score": [10.0, None, 20.0]})
        ctx = make_ctx(df)
        FillNode(column="score", strategy="mean").execute(ctx)
        assert ctx.df["score"].notna().all()
        assert ctx.df["score"].iloc[1] == pytest.approx(15.0)

    def test_fill_literal_zero(self):
        df = pd.DataFrame({"salary": [None, 50000.0]})
        ctx = make_ctx(df)
        FillNode(column="salary", strategy="0").execute(ctx)
        assert ctx.df["salary"].iloc[0] == 0

    def test_fill_forward(self):
        df = pd.DataFrame({"x": [1.0, None, None]})
        ctx = make_ctx(df)
        FillNode(column="x", strategy="forward").execute(ctx)
        assert ctx.df["x"].tolist() == [1.0, 1.0, 1.0]

    def test_fill_backward(self):
        df = pd.DataFrame({"x": [None, None, 3.0]})
        ctx = make_ctx(df)
        FillNode(column="x", strategy="backward").execute(ctx)
        assert ctx.df["x"].tolist() == [3.0, 3.0, 3.0]

    def test_fill_drop(self):
        df = pd.DataFrame({"x": [1.0, None, 3.0]})
        ctx = make_ctx(df)
        FillNode(column="x", strategy="drop").execute(ctx)
        assert len(ctx.df) == 2


# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

class TestSetNode:
    def test_sets_variable(self, ctx):
        SetNode(name="threshold", value="50000").execute(ctx)
        assert ctx.variables["threshold"] == "50000"

    def test_sets_sandbox_dir(self, ctx):
        SetNode(name="sandbox", value="./data").execute(ctx)
        assert ctx.sandbox_dir == "./data"

    def test_strips_quotes(self, ctx):
        SetNode(name="label", value='"hello"').execute(ctx)
        assert ctx.variables["label"] == "hello"
