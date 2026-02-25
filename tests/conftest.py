"""Shared pytest fixtures for the pipeline DSL test suite."""

from __future__ import annotations

import os

import pandas as pd
import pytest


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Small DataFrame with four columns used across most tests."""
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [30, 17, 25, 42, 16],
            "country": ["Germany", "France", "Germany", "USA", "France"],
            "salary": [72000, 0, 55000, 98000, 0],
        }
    )


@pytest.fixture
def ctx(sample_df):
    """A PipelineContext pre-loaded with *sample_df*."""
    from executor import PipelineContext

    return PipelineContext(df=sample_df.copy())


@pytest.fixture
def csv_file(tmp_path, sample_df) -> str:
    """Write *sample_df* to a temporary CSV and return the path."""
    path = str(tmp_path / "people.csv")
    sample_df.to_csv(path, index=False)
    return path


@pytest.fixture
def parquet_file(tmp_path, sample_df) -> str:
    """Write *sample_df* to a temporary Parquet and return the path."""
    path = str(tmp_path / "people.parquet")
    sample_df.to_parquet(path, index=False)
    return path


@pytest.fixture
def lookup_csv(tmp_path) -> str:
    """A small lookup CSV joinable on 'name'."""
    df = pd.DataFrame({"name": ["Alice", "Charlie", "Diana"], "dept": ["Eng", "HR", "Sales"]})
    path = str(tmp_path / "lookup.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def extra_csv(tmp_path) -> str:
    """A CSV with identical columns to *sample_df* for merge tests."""
    df = pd.DataFrame(
        {
            "name": ["Zara", "Yuri"],
            "age": [28, 35],
            "country": ["UK", "Germany"],
            "salary": [60000, 70000],
        }
    )
    path = str(tmp_path / "extra.csv")
    df.to_csv(path, index=False)
    return path
