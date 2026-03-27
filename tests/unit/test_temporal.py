from datetime import timedelta, time as dt_time

import pandas as pd
import pytest

from sonda_translator.sdt.testeTemporal import testeTemporal

INTERVAL = timedelta(minutes=10)
N_ROWS = 5
# 5 rows at 10-min intervals starting 00:00 → last timestamp 00:40
LAST_TIME = dt_time(0, 40, 0)


def _make_df(n=N_ROWS, interval_minutes=10):
    base = pd.Timestamp("2024-01-01 00:00:00")
    return pd.DataFrame({
        "timestamp": [base + timedelta(minutes=i * interval_minutes) for i in range(n)]
    })


def test_valid_data_returns_empty_string():
    df = _make_df()
    assert testeTemporal(df, N_ROWS, INTERVAL, LAST_TIME) == ""


def test_wrong_row_count_fails():
    df = _make_df(n=N_ROWS - 1)  # one fewer row than expected
    result = testeTemporal(df, N_ROWS, INTERVAL, LAST_TIME)

    assert result != ""
    assert "linhas" in result


def test_wrong_last_timestamp_fails():
    df = _make_df()
    wrong_time = dt_time(0, 50, 0)  # actual last is 00:40, not 00:50
    result = testeTemporal(df, N_ROWS, INTERVAL, wrong_time)

    assert result != ""
    assert "timestamp" in result


def test_irregular_intervals_fails():
    df = _make_df()
    # Shift middle row by 5 min → two intervals become 15 min and 5 min
    df.loc[2, "timestamp"] = pd.Timestamp("2024-01-01 00:25:00")
    result = testeTemporal(df, N_ROWS, INTERVAL, LAST_TIME)

    assert result != ""
    assert "intervalo" in result
