"""
Tests for the pure utility functions filter_72h and get_vars from dashboard.py.

These functions are tested inline to avoid Streamlit's module-level side effects
(set_page_config, filesystem reads) that would fire on import.
The logic below mirrors dashboard.py lines 68-74 and 151-154 exactly.
"""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

# --- functions mirrored from dashboard.py ---


def filter_72h(df, ts_col="TIMESTAMP"):
    if ts_col not in df.columns:
        return df
    if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        df[ts_col] = pd.to_datetime(df[ts_col], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return df[df[ts_col] >= datetime.now() - timedelta(hours=72)].copy()


def get_vars(df):
    exclude = {
        "TIMESTAMP", "source_file", "station", "data_type",
        "file_path", "Id", "Min", "RECORD", "Year", "Jday",
    }
    return [c for c in df.select_dtypes(include=[np.number]).columns if c not in exclude]


# --- tests ---


def _make_ts_df(hours_old):
    """DataFrame with one row whose TIMESTAMP is `hours_old` hours ago."""
    ts = datetime.now() - timedelta(hours=hours_old)
    return pd.DataFrame({"TIMESTAMP": pd.to_datetime([ts]), "value": [1.0]})


def test_filter_72h_keeps_recent_rows():
    df = _make_ts_df(hours_old=1)
    result = filter_72h(df)
    assert len(result) == 1


def test_filter_72h_drops_old_rows():
    df = _make_ts_df(hours_old=73)
    result = filter_72h(df)
    assert len(result) == 0


def test_filter_72h_missing_column_returns_as_is():
    df = pd.DataFrame({"value": [1, 2, 3]})
    result = filter_72h(df)
    assert list(result.columns) == ["value"]
    assert len(result) == 3


def test_get_vars_excludes_metadata_cols(sample_dataframe):
    result = get_vars(sample_dataframe)
    for excluded in ("TIMESTAMP", "station", "source_file"):
        assert excluded not in result


def test_get_vars_only_numeric(sample_dataframe):
    result = get_vars(sample_dataframe)
    for col in result:
        assert pd.api.types.is_numeric_dtype(sample_dataframe[col]), (
            f"Column '{col}' is not numeric"
        )
