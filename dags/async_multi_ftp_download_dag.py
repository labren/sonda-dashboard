from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from datetime import datetime, timedelta
from collections import deque
import os
import json
from config import FTP_CONN_ID, STATIONS_CONFIG, BASE_REMOTE, RAW_DIR

DAYS_TO_KEEP = 3

@task
def set_config():
    """Read configuration"""
    with open(str(STATIONS_CONFIG)) as f:
        return json.load(f)

@task
def get_station_files(config):
    """Get station files list"""
    return [{'station': k, 'remote_file': f"{BASE_REMOTE}/{k}/data/{f}", 'local_file': f"{str(RAW_DIR)}/{k}/{f}"}
            for k, v in config.items() for f in v['files']]

@task(retries=5, retry_delay=timedelta(seconds=120), max_retry_delay=timedelta(hours=2))
def download_station_file(station_file):
    """Download single station file with retries"""
    ftp = FTPHook(ftp_conn_id=FTP_CONN_ID).get_conn()
    os.makedirs(os.path.dirname(station_file['local_file']), exist_ok=True)

    try:
        ftp.size(station_file['remote_file'])
    except:
        raise FileNotFoundError(f"Remote: {station_file['remote_file']}")

    # Download last N days using deque (60 min/h * 24 h/d * DAYS_TO_KEEP)
    lines = deque(maxlen=60 * 24 * DAYS_TO_KEEP)
    ftp.retrbinary(f"RETR {station_file['remote_file']}",
                  lambda data: [lines.append(line) for line in data.decode('utf-8', errors='ignore').splitlines(keepends=True)])

    with open(station_file['local_file'], 'w') as f:
        f.writelines(lines)

    return f"Success: {station_file['station']}"

with DAG(
    'ftp_multi_station_download',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'retries': 1, 'retry_delay': timedelta(seconds=30)},
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['ftp', 'data_download', 'multi_station'],
    description='Download from multiple FTP stations in parallel'
) as dag:
    start, end = EmptyOperator(task_id='start'), EmptyOperator(task_id='end')
    cfg = set_config()
    files = get_station_files(cfg)
    downloads = download_station_file.expand(station_file=files)
    start >> cfg >> files >> downloads >> end