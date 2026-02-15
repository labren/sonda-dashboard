from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ftp.hooks.ftp import FTPHook

from datetime import datetime, timedelta
from collections import deque
from pathlib import Path
import json
import time

FTP_CONN_ID = 'solter.ftp.1'
DAYS_TO_KEEP = 1
CONFIG_FILE = '/opt/airflow/config_files/stations_download_config.json'
BASE_REMOTE = '/coleta'
RAW_DIR = Path('/opt/airflow/data/raw')
INTERIM_DIR = Path('/opt/airflow/data/interim')
STATIONS = ['ptr', 'cpa', 'sms', 'nat', 'cgr', 'orn', 'sjc', 'cai', 'pma', 'stm']
LINES_PER_DAY = 60 * 24  # 1-minute intervals


def _has_files(directory, pattern, min_size=0):
    """Check if directory has files matching pattern above min_size."""
    return directory.is_dir() and any(f.stat().st_size > min_size for f in directory.glob(pattern))


@task
def set_config():
    """Read configuration from file."""
    with open(CONFIG_FILE) as f:
        return json.load(f)


@task
def check_existing_data():
    """Check if any processed data already exists."""
    if not INTERIM_DIR.exists():
        return False
    found = any(_has_files(d, '*.parquet') for d in INTERIM_DIR.iterdir() if d.is_dir())
    if not found:
        print("No existing data found in interim directory")
    return found


@task
def get_station_files(config: dict) -> list:
    """Get list of station files to download (skipping stations with existing data)."""
    files = []
    for k, v in config.items():
        if not v.get('enabled', True):
            continue
        if _has_files(RAW_DIR / k, '*.DAT', min_size=100):
            print(f"Station {k} already has raw data, skipping")
            continue
        print(f"Station {k} needs raw data download")
        files.extend({'station': k, 'remote_file': f"{BASE_REMOTE}/{k}/data/{f}",
                       'local_file': str(RAW_DIR / k / f)} for f in v['files'])
    print(f"Created {len(files)} download tasks")
    return files


@task
def download_station_file(station_file: dict):
    """Download a single station file via FTP."""
    local = Path(station_file['local_file'])
    local.parent.mkdir(parents=True, exist_ok=True)

    lines = deque(maxlen=LINES_PER_DAY * DAYS_TO_KEEP)
    try:
        ftp = FTPHook(ftp_conn_id=FTP_CONN_ID).get_conn()
        ftp.retrbinary(f"RETR {station_file['remote_file']}",
                       lambda data: lines.extend(data.decode('utf-8', errors='ignore').splitlines(keepends=True)))
        local.write_text(''.join(lines))
        print(f"Downloaded {station_file['station']}/{local.name}")
        return f"Success: {station_file['station']}"
    except Exception as e:
        print(f"Error downloading {station_file['station']}: {e}")
        local.write_text("# Minimal data file\n")
        return f"Error: {station_file['station']} - {e}"


@task
def check_download_results():
    """Report which stations have/lack raw data."""
    ok = [s for s in STATIONS if _has_files(RAW_DIR / s, '*.DAT', 100)]
    failed = [s for s in STATIONS if s not in ok]
    for s in ok:
        print(f"Station {s} has raw data")
    for s in failed:
        print(f"Station {s} has no raw data")
    if failed:
        return f"WARNING: Raw data missing for {len(failed)} stations: {', '.join(failed)}"
    return "All stations have raw data available"


@task
def summarize_download_results():
    """Summarize which stations were freshly downloaded vs pre-existing."""
    cutoff = time.time() - 600  # 10 minutes ago
    downloaded, existing = [], []
    for s in STATIONS:
        d = RAW_DIR / s
        if not d.is_dir():
            print(f"Station {s} has no data directory")
            continue
        recent = any(f.stat().st_mtime > cutoff for f in d.glob('*.DAT') if f.stat().st_size > 100)
        (downloaded if recent else existing).append(s)
    summary = f"Download Summary: {len(downloaded)} downloaded, {len(existing)} already existed"
    print(summary)
    return summary


@task
def wait_for_processing_completion():
    """Wait briefly then check processing DAG status."""
    from airflow.models import DagRun
    from airflow.utils.state import State

    time.sleep(30)
    try:
        for state in (State.RUNNING, State.SUCCESS):
            runs = DagRun.find(dag_id='process_multistation_data', state=state)
            if runs:
                label = "running" if state == State.RUNNING else "completed"
                print(f"Processing DAG is {label}: {runs[0].run_id}")
                return f"Processing DAG {label}: {runs[0].run_id}"
        return "Processing DAG status unknown"
    except Exception as e:
        return f"Error checking processing DAG status: {e}"


@task
def check_processing_results():
    """Check if processing produced valid parquet files for all stations."""
    ok = [s for s in STATIONS if _has_files(INTERIM_DIR / s, 'processed_data*.parquet', 1000)]
    failed = [s for s in STATIONS if s not in ok]
    for s in ok:
        print(f"Station {s} processed successfully")
    for s in failed:
        print(f"Station {s} processing failed")
    if failed:
        return f"WARNING: Processing failed for {len(failed)} stations: {', '.join(failed)}"

    try:
        marker = Path("logs/dashboard")
        marker.mkdir(parents=True, exist_ok=True)
        (marker / "cache_clear_needed").write_text(f"Initial data setup completed at {datetime.now()}")
    except Exception as e:
        print(f"Could not create cache clearing marker: {e}")
    return "All stations processed successfully"


with DAG(
    'initial_data_setup',
    default_args={'owner': 'airflow', 'depends_on_past': False,
                  'retries': 1, 'retry_delay': timedelta(minutes=5)},
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['initialization', 'data_setup', 'fresh_install'],
    description='Initial data setup for fresh installations - downloads and processes initial data'
) as dag:

    start, end = EmptyOperator(task_id='start'), EmptyOperator(task_id='end')
    config = set_config()
    data_exists = check_existing_data()
    station_files = get_station_files(config)
    downloads = download_station_file.expand(station_file=station_files)
    check_dl = check_download_results()
    summary = summarize_download_results()
    trigger = TriggerDagRunOperator(task_id='trigger_processing_dag',
                                    trigger_dag_id='process_multistation_data',
                                    conf={'triggered_by': 'initial_data_setup'})
    wait = wait_for_processing_completion()
    check_proc = check_processing_results()

    start >> data_exists >> config >> station_files >> downloads >> check_dl >> summary >> trigger >> wait >> check_proc >> end
