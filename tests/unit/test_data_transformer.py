import pandas as pd
import pytest

from data_transformer_plugin import DataTransformerOperator

FIVE_HEADERS = ["H1", "H2", "H3", "H4", "H5"]
THREE_HEADERS = ["H1", "H2", "H3"]


def _transformer(df, headers, station="tst", file_type="SD"):
    return DataTransformerOperator(
        task_id="test_transformer",
        data=df,
        station=station,
        file_type=file_type,
        header_sensor={station: {f"{file_type}_RAW_HEADER": headers}},
    )


def test_applies_headers_correctly():
    # df has 3 cols, raw_headers has 5 → pattern [0:-2] gives first 3 headers
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    result = _transformer(df, FIVE_HEADERS).execute()

    assert result is not None
    assert list(result.columns) == FIVE_HEADERS[:3]


def test_handles_column_count_excess():
    # df has 5 cols, raw_headers has 3 → pads header list with extra_col_N
    df = pd.DataFrame([[1, 2, 3, 4, 5]])
    result = _transformer(df, THREE_HEADERS).execute()

    assert result is not None
    assert result.shape[1] == 5
    assert "extra_col_1" in result.columns
    assert "extra_col_2" in result.columns


def test_handles_column_count_deficit():
    # df has 2 cols, raw_headers has 5 → no pattern matches → truncates to first 2 headers
    df = pd.DataFrame([[1, 2], [3, 4]])
    result = _transformer(df, FIVE_HEADERS).execute()

    assert result is not None
    assert list(result.columns) == FIVE_HEADERS[:2]


def test_missing_station_returns_none():
    df = pd.DataFrame([[1, 2, 3]])
    op = DataTransformerOperator(
        task_id="test_transformer",
        data=df,
        station="unknown_station",
        file_type="SD",
        header_sensor={"tst": {"SD_RAW_HEADER": THREE_HEADERS}},
    )
    result = op.execute()

    assert result is None
