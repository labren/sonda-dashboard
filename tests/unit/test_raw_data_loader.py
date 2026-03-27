import pandas as pd
import pytest

from raw_data_loader_plugin import RawDataLoaderOperator


def _loader(file_path, station=None, file_type=None, header_sensor=None, validate_columns=False):
    return RawDataLoaderOperator(
        task_id="test_loader",
        file_path=file_path,
        station=station,
        file_type=file_type,
        header_sensor=header_sensor,
        validate_columns=validate_columns,
    )


def test_loads_dat_file_successfully(sample_dat_file):
    result = _loader(sample_dat_file).execute()

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result.shape == (3, 4)


def test_column_count_mismatch_truncates(sample_dat_file):
    # File has 4 cols; header has only 2 → operator should truncate to 2
    header_sensor = {"tst": {"SD_RAW_HEADER": ["TIMESTAMP", "Radiation_Avg"]}}
    result = _loader(
        sample_dat_file,
        station="tst",
        file_type="SD",
        header_sensor=header_sensor,
        validate_columns=True,
    ).execute()

    assert result.shape[1] == 2
    assert list(result.columns) == ["TIMESTAMP", "Radiation_Avg"]


def test_column_count_mismatch_pads(sample_dat_file):
    # File has 4 cols; header expects 6 → operator should pad with None cols
    header_sensor = {
        "tst": {
            "SD_RAW_HEADER": [
                "TIMESTAMP", "Radiation_Avg", "Radiation_Std",
                "Temp_Avg", "Extra1", "Extra2",
            ]
        }
    }
    result = _loader(
        sample_dat_file,
        station="tst",
        file_type="SD",
        header_sensor=header_sensor,
        validate_columns=True,
    ).execute()

    assert result.shape[1] == 6


def test_missing_file_returns_empty(tmp_path):
    result = _loader(str(tmp_path / "nonexistent.DAT")).execute()

    assert isinstance(result, pd.DataFrame)
    assert result.empty
