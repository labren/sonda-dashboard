from pathlib import Path

import pandas as pd
import pytest

from file_saver_plugin import FileSaverOperator


def _saver(data, output_path, file_format="parquet"):
    return FileSaverOperator(
        task_id="test_saver",
        data=data,
        output_path=str(output_path),
        file_format=file_format,
    )


def test_saves_parquet(sample_dataframe, tmp_path):
    out = tmp_path / "out.parquet"
    _saver(sample_dataframe, out, "parquet").execute()

    assert out.exists()
    result = pd.read_parquet(out)
    assert not result.empty
    assert result.shape[0] == len(sample_dataframe)


def test_saves_csv(sample_dataframe, tmp_path):
    out = tmp_path / "out.csv"
    _saver(sample_dataframe, out, "csv").execute()

    assert out.exists()
    result = pd.read_csv(out)
    assert not result.empty
    assert result.shape[0] == len(sample_dataframe)


def test_empty_dataframe_raises(tmp_path):
    out = tmp_path / "out.parquet"
    with pytest.raises(ValueError):
        _saver(pd.DataFrame(), out, "parquet").execute()


def test_creates_parent_directories(sample_dataframe, tmp_path):
    out = tmp_path / "deep" / "nested" / "dir" / "out.parquet"
    _saver(sample_dataframe, out, "parquet").execute()

    assert out.exists()
