"""
DAG integrity tests using AST parsing — no live Airflow DB or external config
files required. Tests verify syntax, structure, schedules, and absence of
Celery/Redis imports.
"""
import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parent.parent.parent / "dags"

DAG_FILES = [f for f in DAGS_DIR.glob("*.py") if f.name != "config.py"]

EXPECTED_DAG_IDS = {
    "ftp_multi_station_download",
    "initial_data_setup",
    "process_multistation_data",
    "header_discovery_dag",
    "refresh_data_pipeline",
}


def _source(filename):
    return (DAGS_DIR / filename).read_text()


def _task_count(source):
    """Count @task decorators + EmptyOperator + TriggerDagRunOperator instantiations."""
    return (
        source.count("@task")
        + source.count("EmptyOperator(")
        + source.count("TriggerDagRunOperator(")
    )


def test_no_syntax_errors():
    for dag_file in DAG_FILES:
        try:
            ast.parse(dag_file.read_text())
        except SyntaxError as exc:
            pytest.fail(f"{dag_file.name} has a syntax error: {exc}")


def test_expected_dags_exist():
    found = set()
    for dag_file in DAG_FILES:
        source = dag_file.read_text()
        for dag_id in EXPECTED_DAG_IDS:
            if f"'{dag_id}'" in source or f'"{dag_id}"' in source:
                found.add(dag_id)

    missing = EXPECTED_DAG_IDS - found
    assert not missing, f"DAG IDs not found in any source file: {missing}"


def test_schedules():
    assert "schedule='@hourly'" in _source("refresh_data_pipeline_dag.py")
    assert "schedule='@weekly'" in _source("header_discovery_dag.py")
    for fname in [
        "async_multi_ftp_download_dag.py",
        "initial_data_setup_dag.py",
        "process_multistation_data.py",
    ]:
        assert "schedule=None" in _source(fname), f"{fname} should have schedule=None"


def test_task_counts():
    minimums = {
        "async_multi_ftp_download_dag.py": 3,
        "refresh_data_pipeline_dag.py": 4,
        "header_discovery_dag.py": 4,
        "initial_data_setup_dag.py": 5,
        "process_multistation_data.py": 3,
    }
    for fname, min_count in minimums.items():
        count = _task_count(_source(fname))
        assert count >= min_count, (
            f"{fname} has only {count} tasks/operators, expected >= {min_count}"
        )


def test_no_celery_dependency():
    for dag_file in DAG_FILES:
        source = dag_file.read_text()
        assert "import celery" not in source, f"{dag_file.name} imports celery"
        assert "from celery" not in source, f"{dag_file.name} imports from celery"
        assert "import redis" not in source, f"{dag_file.name} imports redis"
        assert "from redis" not in source, f"{dag_file.name} imports from redis"
