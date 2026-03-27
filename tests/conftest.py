import logging
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "plugins"))

# ── Airflow shim ──────────────────────────────────────────────────────────────
# In Airflow 3.x, BaseOperator moved out of airflow.models into the task SDK.
# Inject a minimal substitute so plugin unit tests run without depending on any
# specific Airflow version or a running Airflow instance.
class _BaseOperator:
    """Lightweight BaseOperator substitute (task_id + self.log only)."""
    def __init__(self, task_id: str = "test", *args, **kwargs):
        self.task_id = task_id
        self.log = logging.getLogger(f"test.{self.__class__.__name__}")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

try:
    import airflow.models  # keep real Airflow if it's available and compatible
    from airflow.models import BaseOperator as _RealBO  # noqa: F401
except (ImportError, ModuleNotFoundError):
    _airflow_pkg = sys.modules.get("airflow") or types.ModuleType("airflow")
    _airflow_models = types.ModuleType("airflow.models")
    _airflow_models.BaseOperator = _BaseOperator
    sys.modules.setdefault("airflow", _airflow_pkg)
    sys.modules["airflow.models"] = _airflow_models
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_dat_file(tmp_path):
    """Temp TST_SD.DAT with 3 rows × 4 quoted CSV columns."""
    content = "\n".join([
        '"2024-01-01 00:00:00","100.5","200.3","50.1"',
        '"2024-01-01 00:01:00","101.2","199.8","50.5"',
        '"2024-01-01 00:02:00","102.0","200.1","49.8"',
    ])
    dat = tmp_path / "TST_SD.DAT"
    dat.write_text(content)
    return str(dat)


@pytest.fixture
def sample_header_sensor():
    """Minimal header_sensor dict with a 4-column SD entry for station 'tst'."""
    return {
        "tst": {
            "SD_RAW_HEADER": ["TIMESTAMP", "Radiation_Avg", "Radiation_Std", "Temp_Avg"],
        }
    }


@pytest.fixture
def sample_dataframe():
    """Small DataFrame with TIMESTAMP, numeric cols, and metadata cols."""
    now = datetime.now()
    return pd.DataFrame({
        "TIMESTAMP": pd.to_datetime([now - timedelta(hours=i) for i in range(5)]),
        "Radiation_Avg": [100.0, 110.0, 120.0, 130.0, 140.0],
        "Temp_Avg": [25.0, 25.5, 26.0, 26.5, 27.0],
        "station": ["tst"] * 5,
        "source_file": ["TST_SD.parquet"] * 5,
    })
