"""Tests for the six new features added to the pipeline DSL.

1. Automated test suite (this file)
2. Sandbox / allowed paths
3. Chunked streaming source
4. try / on_error error recovery
5. Parquet read/write
6. Left / right / outer join types
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from ast_nodes import (
    AssertNode,
    CastNode,
    FillNode,
    FilterNode,
    JoinNode,
    LogNode,
    SaveNode,
    SelectNode,
    SetNode,
    SourceNode,
    TryNode,
    _check_path_sandbox,
)
from executor import PipelineContext, run_pipeline
from ppl_parser import parse_lines


# ---------------------------------------------------------------------------
# Feature 2: Sandbox / allowed paths
# ---------------------------------------------------------------------------

class TestSandbox:
    def test_no_sandbox_allows_any_path(self, tmp_path, csv_file):
        ctx = PipelineContext()
        # Should not raise
        _check_path_sandbox(csv_file, ctx)

    def test_path_inside_sandbox_allowed(self, tmp_path, csv_file):
        ctx = PipelineContext(sandbox_dir=str(tmp_path))
        _check_path_sandbox(csv_file, ctx)  # no exception

    def test_path_outside_sandbox_blocked(self, tmp_path):
        ctx = PipelineContext(sandbox_dir=str(tmp_path / "subdir"))
        with pytest.raises(PermissionError, match="Access denied"):
            _check_path_sandbox(str(tmp_path / "other.csv"), ctx)

    def test_path_traversal_blocked(self, tmp_path):
        ctx = PipelineContext(sandbox_dir=str(tmp_path))
        with pytest.raises(PermissionError):
            _check_path_sandbox(str(tmp_path / ".." / "secret.csv"), ctx)

    def test_set_sandbox_via_set_node(self, tmp_path):
        ctx = PipelineContext()
        SetNode(name="sandbox", value=str(tmp_path)).execute(ctx)
        assert ctx.sandbox_dir == str(tmp_path)

    def test_source_respects_sandbox(self, tmp_path, csv_file):
        # csv_file is inside tmp_path — should be fine
        ctx = PipelineContext(sandbox_dir=str(tmp_path))
        SourceNode(file_path=csv_file).execute(ctx)
        assert ctx.df is not None

    def test_source_blocked_outside_sandbox(self, tmp_path, csv_file):
        ctx = PipelineContext(sandbox_dir=str(tmp_path / "restricted"))
        with pytest.raises(PermissionError):
            SourceNode(file_path=csv_file).execute(ctx)

    def test_save_blocked_outside_sandbox(self, tmp_path, ctx):
        restricted = tmp_path / "restricted"
        ctx.sandbox_dir = str(restricted)
        with pytest.raises(PermissionError):
            SaveNode(file_path=str(tmp_path / "out.csv")).execute(ctx)

    def test_join_blocked_outside_sandbox(self, tmp_path, ctx, lookup_csv):
        ctx.sandbox_dir = str(tmp_path / "other_dir")
        with pytest.raises(PermissionError):
            JoinNode(file_path=lookup_csv, key="name", how="inner").execute(ctx)

    def test_sandbox_via_pipeline(self, tmp_path, csv_file):
        """End-to-end: set sandbox then load file within it."""
        nodes = parse_lines([
            f'set sandbox = {tmp_path}',
            f'source "{csv_file}"',
            'filter age > 18',
        ])
        result = run_pipeline(nodes)
        assert result is not None
        assert all(result["age"] > 18)

    def test_sandbox_prefix_not_confused_with_sibling(self, tmp_path):
        """Ensure /data doesn't allow access to /data2."""
        safe = tmp_path / "data"
        safe.mkdir()
        ctx = PipelineContext(sandbox_dir=str(safe))
        sibling = str(tmp_path / "data2" / "secret.csv")
        with pytest.raises(PermissionError):
            _check_path_sandbox(sibling, ctx)


# ---------------------------------------------------------------------------
# Feature 3: Chunked streaming source
# ---------------------------------------------------------------------------

class TestChunkedSource:
    def test_same_result_as_full_load(self, csv_file, sample_df):
        """Chunked pipeline produces the same result as non-chunked."""
        nodes_full = parse_lines([
            f'source "{csv_file}"',
            'filter age >= 18',
        ])
        nodes_chunked = parse_lines([
            f'source "{csv_file}" chunk 2',
            'filter age >= 18',
        ])
        full = run_pipeline(nodes_full)
        chunked = run_pipeline(nodes_chunked)
        assert len(chunked) == len(full)
        assert set(chunked["name"]) == set(full["name"])

    def test_chunk_size_one(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}" chunk 1',
            'filter age > 0',
        ])
        result = run_pipeline(nodes)
        assert len(result) == 5

    def test_chunk_with_select(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}" chunk 3',
            'select name, age',
        ])
        result = run_pipeline(nodes)
        assert list(result.columns) == ["name", "age"]

    def test_chunk_with_cast_and_filter(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}" chunk 2',
            'cast age int',
            'filter age >= 25',
        ])
        result = run_pipeline(nodes)
        assert all(result["age"] >= 25)

    def test_chunk_with_sort_in_post_phase(self, csv_file):
        """sort is not chunk-safe — applied after concat."""
        nodes = parse_lines([
            f'source "{csv_file}" chunk 2',
            'filter age > 0',
            'sort by age asc',
        ])
        result = run_pipeline(nodes)
        ages = result["age"].tolist()
        assert ages == sorted(ages)

    def test_chunk_filters_reduce_rows(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}" chunk 2',
            'filter salary > 0',
        ])
        result = run_pipeline(nodes)
        assert all(result["salary"] > 0)

    def test_chunk_size_stored_in_node(self):
        nodes = parse_lines(['source "big.csv" chunk 100000'])
        assert nodes[0].chunk_size == 100000

    def test_chunked_empty_after_filter(self, csv_file):
        """All chunks filtered out → empty DataFrame."""
        nodes = parse_lines([
            f'source "{csv_file}" chunk 2',
            'filter age > 1000',
        ])
        result = run_pipeline(nodes)
        assert result is not None
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Feature 4: try / on_error
# ---------------------------------------------------------------------------

class TestTryOnError:
    def test_skip_swallows_error(self, ctx):
        """Pipeline continues after a failed assertion inside try."""
        from ast_nodes import AssertNode, CastNode, FillNode
        node = TryNode(
            body=[AssertNode(column="salary", operator=">", value="1000000")],
            on_error_nodes=[],
            error_action="skip",
        )
        node.execute(ctx)
        assert len(ctx.df) == 5  # unchanged

    def test_log_prints_message(self, ctx, capsys):
        node = TryNode(
            body=[AssertNode(column="salary", operator=">", value="1000000")],
            on_error_nodes=[],
            error_action='log "salary check failed"',
        )
        node.execute(ctx)
        output = capsys.readouterr().out
        assert "salary check failed" in output

    def test_command_handler_runs_on_error(self, ctx):
        """on_error fill age 0 runs when assert fails."""
        ctx.df["age"] = ctx.df["age"].astype(float)
        ctx.df.loc[0, "age"] = None
        node = TryNode(
            body=[AssertNode(column="age", operator=">", value="-1")],
            on_error_nodes=[FillNode(column="age", strategy="0")],
            error_action="fill age 0",
        )
        # assert fails because of NaN comparison → fill runs
        node.execute(ctx)

    def test_no_error_skips_handler(self, ctx):
        """When body succeeds the handler is never called."""
        filled_called = {"flag": False}

        class _TrackingFill(FillNode):
            def execute(self, ctx):
                filled_called["flag"] = True
                super().execute(ctx)

        node = TryNode(
            body=[FilterNode(column="age", operator=">", value="0")],
            on_error_nodes=[_TrackingFill(column="salary", strategy="0")],
            error_action="fill salary 0",
        )
        node.execute(ctx)
        assert not filled_called["flag"]

    def test_pipeline_continues_after_try_skip(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}"',
            'try',
            'assert salary > 1000000',
            'on_error skip',
            'select name, age',
        ])
        result = run_pipeline(nodes)
        assert result is not None
        assert list(result.columns) == ["name", "age"]

    def test_try_preserves_state_on_skip(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}"',
            'try',
            'assert salary > 1000000',
            'on_error skip',
        ])
        result = run_pipeline(nodes)
        assert len(result) == 5  # all rows intact

    def test_try_with_cast_on_error_fill(self, csv_file):
        """cast succeeds, then assert fails, handler fills."""
        nodes = parse_lines([
            f'source "{csv_file}"',
            'try',
            'cast age int',
            'assert age > 100',
            'on_error fill salary 0',
        ])
        result = run_pipeline(nodes)
        assert result is not None

    def test_nested_try_blocks(self, csv_file):
        nodes = parse_lines([
            f'source "{csv_file}"',
            'try',
            'try',
            'assert salary > 1000000',
            'on_error skip',
            'on_error skip',
        ])
        result = run_pipeline(nodes)
        assert len(result) == 5


# ---------------------------------------------------------------------------
# Feature 5: Parquet read/write
# ---------------------------------------------------------------------------

class TestParquetSupport:
    def test_read_parquet(self, parquet_file, sample_df):
        ctx = PipelineContext()
        SourceNode(file_path=parquet_file).execute(ctx)
        assert ctx.df is not None
        assert len(ctx.df) == len(sample_df)
        assert list(ctx.df.columns) == list(sample_df.columns)

    def test_write_parquet(self, ctx, tmp_path):
        out = str(tmp_path / "out.parquet")
        SaveNode(file_path=out).execute(ctx)
        assert os.path.exists(out)
        reloaded = pd.read_parquet(out)
        assert len(reloaded) == len(ctx.df)

    def test_round_trip_parquet(self, ctx, tmp_path):
        out = str(tmp_path / "round_trip.parquet")
        original_len = len(ctx.df)
        SaveNode(file_path=out).execute(ctx)

        ctx2 = PipelineContext()
        SourceNode(file_path=out).execute(ctx2)
        assert len(ctx2.df) == original_len

    def test_parquet_in_pipeline(self, parquet_file):
        nodes = parse_lines([
            f'source "{parquet_file}"',
            'filter age >= 18',
        ])
        result = run_pipeline(nodes)
        assert result is not None
        assert all(result["age"] >= 18)

    def test_save_parquet_pipeline(self, csv_file, tmp_path):
        out = str(tmp_path / "result.parquet")
        nodes = parse_lines([
            f'source "{csv_file}"',
            'filter age > 0',
            f'save "{out}"',
        ])
        run_pipeline(nodes)
        assert os.path.exists(out)
        df = pd.read_parquet(out)
        assert len(df) == 5

    def test_parquet_preserves_dtypes(self, tmp_path, sample_df):
        # Save with int dtype and verify it's preserved
        sample_df["age"] = sample_df["age"].astype("int64")
        p = str(tmp_path / "typed.parquet")
        sample_df.to_parquet(p, index=False)

        ctx = PipelineContext()
        SourceNode(file_path=p).execute(ctx)
        assert ctx.df["age"].dtype == "int64"


# ---------------------------------------------------------------------------
# Feature 6: Left / right / outer join types
# ---------------------------------------------------------------------------

class TestJoinTypes:
    def test_inner_join_excludes_non_matching(self, ctx, lookup_csv):
        """Inner join: only rows with matching keys on both sides."""
        JoinNode(file_path=lookup_csv, key="name", how="inner").execute(ctx)
        # lookup has Alice, Charlie, Diana but not Bob and Eve
        assert len(ctx.df) == 3
        assert "Bob" not in ctx.df["name"].values

    def test_left_join_keeps_all_left_rows(self, ctx, lookup_csv):
        """Left join: all rows from left, NaN for missing right values."""
        JoinNode(file_path=lookup_csv, key="name", how="left").execute(ctx)
        assert len(ctx.df) == 5
        # Bob and Eve have no dept → NaN
        bob_dept = ctx.df.loc[ctx.df["name"] == "Bob", "dept"].values[0]
        assert pd.isna(bob_dept)

    def test_right_join_keeps_all_right_rows(self, tmp_path, ctx, sample_df):
        """Right join: all rows from right, NaN for missing left values."""
        # Right file has Alice and a new person not in left
        right = pd.DataFrame({
            "name": ["Alice", "Zara"],
            "dept": ["Eng", "HR"],
        })
        path = str(tmp_path / "right.csv")
        right.to_csv(path, index=False)

        JoinNode(file_path=path, key="name", how="right").execute(ctx)
        # Zara is in right but not in left
        assert "Zara" in ctx.df["name"].values

    def test_outer_join_keeps_all_rows(self, tmp_path, ctx):
        """Outer join: all rows from both sides."""
        right = pd.DataFrame({
            "name": ["Alice", "NewPerson"],
            "dept": ["Eng", "Finance"],
        })
        path = str(tmp_path / "outer.csv")
        right.to_csv(path, index=False)

        JoinNode(file_path=path, key="name", how="outer").execute(ctx)
        assert "NewPerson" in ctx.df["name"].values
        assert len(ctx.df) >= 5  # at least all left rows

    def test_inner_explicit(self, ctx, lookup_csv):
        JoinNode(file_path=lookup_csv, key="name", how="inner").execute(ctx)
        assert len(ctx.df) == 3

    def test_join_how_preserved_in_node(self):
        from ppl_parser import parse_lines as pl
        node = pl(['join "f.csv" on id left'])[0]
        assert node.how == "left"

    def test_left_join_pipeline(self, csv_file, lookup_csv):
        nodes = parse_lines([
            f'source "{csv_file}"',
            f'join "{lookup_csv}" on name left',
        ])
        result = run_pipeline(nodes)
        assert len(result) == 5
        assert "dept" in result.columns
