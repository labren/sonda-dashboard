from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
import requests
import os
from config import RAW_DIR

logger = logging.getLogger(__name__)

# Config
DEFAULT_ARGS = {'owner': 'airflow', 'depends_on_past': False, 'retries': 1, 'retry_delay': timedelta(minutes=5), 'start_date': datetime(2025, 1, 1)}
ENV = {'AIRFLOW_BASE_URL': 'http://airflow-apiserver:8080', 'AIRFLOW_USERNAME': 'airflow', 'AIRFLOW_PASSWORD': 'airflow'}

def _get_auth_token(base_url, username, password):
    """Get JWT auth token"""
    resp = requests.post(f"{base_url}/auth/token", json={"username": username, "password": password}, timeout=10)
    return resp.json().get('access_token') if resp.status_code in [200, 201] else None

def _verify_recent_files():
    """Check for files modified in last 5 min"""
    try:
        if not RAW_DIR.exists():
            return False
        cutoff = datetime.now() - timedelta(minutes=5)
        return any(f.stat().st_mtime > cutoff.timestamp()
                  for station in RAW_DIR.iterdir() if station.is_dir()
                  for f in station.iterdir() if f.is_file())
    except:
        return False

@task
def wait_for_download_completion():
    """Wait for download completion via Airflow REST API"""
    start, timeout, interval = time.time(), 1800, 30  # 30min timeout, 30s interval
    base_url = os.getenv('AIRFLOW_BASE_URL', ENV['AIRFLOW_BASE_URL'])

    while time.time() - start < timeout:
        try:
            token = _get_auth_token(base_url, os.getenv('AIRFLOW_USERNAME', ENV['AIRFLOW_USERNAME']),
                                   os.getenv('AIRFLOW_PASSWORD', ENV['AIRFLOW_PASSWORD']))
            if not token:
                time.sleep(interval)
                continue

            headers = {"Authorization": f"Bearer {token}"}
            runs = requests.get(f"{base_url}/api/v2/dags/ftp_multi_station_download/dagRuns",
                              headers=headers, timeout=10).json().get('dag_runs', [])

            if not runs:
                time.sleep(interval)
                continue

            # Get most recent run
            target = max((r for r in runs if r.get('start_date')),
                        key=lambda r: datetime.fromisoformat(r['start_date'].replace('Z', '+00:00')),
                        default=runs[0])

            run_id, state = target.get('dag_run_id'), target.get('state', 'unknown')

            if state in ['failed', 'upstream_failed']:
                time.sleep(interval)
                continue

            # Check 'end' task
            tasks = requests.get(f"{base_url}/api/v2/dags/ftp_multi_station_download/dagRuns/{run_id}/taskInstances",
                               headers=headers, timeout=10).json().get('task_instances', [])

            end_task = next((t for t in tasks if t.get('task_id') == 'end'), None)
            if end_task:
                end_state = end_task.get('state', 'unknown')
                if end_state == 'success' and _verify_recent_files():
                    logger.info("Download completed")
                    return True
                elif end_state in ['failed', 'upstream_failed']:
                    raise Exception(f"Download failed: {end_state}")

            time.sleep(interval)
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(interval)

    raise Exception("Download timeout (30min)")

@task
def log_status():
    """Log pipeline status"""
    logger.info("="*80 + "\nREFRESH PIPELINE\n" + "="*80 +
               f"\nStarted: {datetime.now()}\nSteps: Download→Process\n" + "="*80)

with DAG(
    'refresh_data_pipeline',
    default_args=DEFAULT_ARGS,
    description='Orchestrate data refresh: download then process',
    schedule='@hourly',
    catchup=False,
    tags=['refresh', 'pipeline', 'orchestration', 'dashboard'],
    max_active_runs=1
) as dag:
    os.environ.update(ENV)  # Set env vars for API access

    start, end = EmptyOperator(task_id='start'), EmptyOperator(task_id='end')
    status = log_status()
    dl_trigger = TriggerDagRunOperator(task_id='trigger_download', trigger_dag_id='ftp_multi_station_download', wait_for_completion=False)
    wait_dl = wait_for_download_completion()
    proc_trigger = TriggerDagRunOperator(task_id='trigger_processing', trigger_dag_id='process_multistation_data', wait_for_completion=False)

    start >> status >> dl_trigger >> wait_dl >> proc_trigger >> end
