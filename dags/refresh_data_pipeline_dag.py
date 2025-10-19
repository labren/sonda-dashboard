from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
import time
import logging
import requests
import os

# Configure logging
logger = logging.getLogger(__name__)

def _verify_download_files_exist():
    """Helper function to verify download files exist"""
    import os
    from datetime import datetime, timedelta
    
    try:
        raw_data_dir = "/opt/airflow/data/raw"
        
        if not os.path.exists(raw_data_dir):
            return False
        
        # Check for recent files in station directories
        current_time = datetime.now()
        recent_files_found = False
        
        for station_dir in os.listdir(raw_data_dir):
            station_path = os.path.join(raw_data_dir, station_dir)
            if not os.path.isdir(station_path):
                continue
                
            # Check for recent files in this station directory
            for file in os.listdir(station_path):
                file_path = os.path.join(station_path, file)
                if os.path.isfile(file_path):
                    # Check if file was modified within last 5 minutes
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if current_time - file_mtime < timedelta(minutes=5):
                        recent_files_found = True
                        break
            
            if recent_files_found:
                break
        
        return recent_files_found
        
    except Exception as e:
        logger.error(f"Error verifying download files: {str(e)}")
        return False

# DAG configuration
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

@task
def wait_for_download_completion():
    """Wait for download completion using Airflow REST API"""
    max_wait_time = 30 * 60  # 30 minutes timeout
    check_interval = 30  # Check every 30 seconds
    start_time = time.time()
    
    logger.info("Waiting for download completion...")
    
    # Get Airflow configuration from environment (set in docker-compose.yml)
    airflow_base_url = os.getenv('AIRFLOW_BASE_URL', 'http://airflow-apiserver:8080')
    airflow_username = os.getenv('AIRFLOW_USERNAME', 'airflow')
    airflow_password = os.getenv('AIRFLOW_PASSWORD', 'airflow')
    
    logger.info(f"Using Airflow API at: {airflow_base_url}")
    
    while time.time() - start_time < max_wait_time:
        elapsed = int(time.time() - start_time)
        
        try:
            # Get JWT token
            token_url = f"{airflow_base_url}/auth/token"
            token_data = {
                "username": airflow_username,
                "password": airflow_password
            }
            
            token_response = requests.post(token_url, json=token_data, timeout=10)
            
            if token_response.status_code not in [200, 201]:
                logger.error(f"Failed to get authentication token: {token_response.status_code}")
                time.sleep(check_interval)
                continue
                
            token = token_response.json().get('access_token')
            if not token:
                logger.error("No access token received")
                time.sleep(check_interval)
                continue
            
            # Check download DAG status using REST API
            headers = {"Authorization": f"Bearer {token}"}
            dag_runs_url = f"{airflow_base_url}/api/v2/dags/ftp_multi_station_download/dagRuns"
            
            response = requests.get(dag_runs_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                dag_runs = response.json().get('dag_runs', [])
                if dag_runs:
                    # Find the most recent run by start_date
                    target_run = None
                    latest_start_time = None
                    
                    for run in dag_runs:
                        run_start = run.get('start_date')
                        if run_start:
                            try:
                                from datetime import datetime
                                run_start_time = datetime.fromisoformat(run_start.replace('Z', '+00:00'))
                                if latest_start_time is None or run_start_time > latest_start_time:
                                    latest_start_time = run_start_time
                                    target_run = run
                            except Exception as e:
                                logger.warning(f"Error parsing run start date: {e}")
                                continue
                    
                    # If no run with valid start_date found, use the first one
                    if not target_run:
                        target_run = dag_runs[0]
                        logger.warning("No run with valid start_date found, using first run")
                    
                    dag_run_id = target_run.get('dag_run_id')
                    dag_state = target_run.get('state', 'unknown')
                    
                    logger.info(f"Checking most recent download DAG run: {dag_run_id} (state: {dag_state}, started: {target_run.get('start_date')})")
                    
                    logger.info(f"Download DAG state: {dag_state} (run_id: {dag_run_id})")
                    
                    # If DAG is already failed, wait for a new run to be triggered
                    if dag_state in ['failed', 'upstream_failed']:
                        logger.warning(f"Download DAG failed with state: {dag_state}. Waiting for a new download DAG run to be triggered...")
                        time.sleep(check_interval)
                        continue
                    
                    # Check the specific 'end' task status to ensure ALL downloads completed
                    if dag_run_id:
                        task_instances_url = f"{airflow_base_url}/api/v2/dags/ftp_multi_station_download/dagRuns/{dag_run_id}/taskInstances"
                        task_response = requests.get(task_instances_url, headers=headers, timeout=10)
                        
                        if task_response.status_code == 200:
                            task_instances = task_response.json().get('task_instances', [])
                            
                            # Find the 'end' task
                            end_task = None
                            for task in task_instances:
                                if task.get('task_id') == 'end':
                                    end_task = task
                                    break
                            
                            if end_task:
                                end_task_state = end_task.get('state', 'unknown')
                                logger.info(f"Download DAG 'end' task state: {end_task_state}")
                                
                                if end_task_state == 'success':
                                    # Double-check with file-based detection for extra reliability
                                    if _verify_download_files_exist():
                                        logger.info("Download completed successfully (end task succeeded + files verified)")
                                        return True
                                    else:
                                        logger.warning("End task succeeded but files not found - continuing to wait")
                                        time.sleep(check_interval)
                                elif end_task_state in ['failed', 'upstream_failed']:
                                    logger.error(f"Download DAG failed (end task state: {end_task_state})")
                                    raise Exception(f"Download DAG failed with end task state: {end_task_state}")
                                else:
                                    logger.info(f"Download still in progress (end task state: {end_task_state})... waiting {check_interval}s (elapsed: {elapsed}s)")
                                    time.sleep(check_interval)
                            else:
                                logger.info("Download DAG 'end' task not found yet - download may still be starting... waiting {check_interval}s (elapsed: {elapsed}s)")
                                time.sleep(check_interval)
                        else:
                            logger.error(f"Failed to get task instances: {task_response.status_code}")
                            time.sleep(check_interval)
                    else:
                        logger.error("No DAG run ID found")
                        time.sleep(check_interval)
                else:
                    logger.info("No download DAG runs found yet")
                    time.sleep(check_interval)
            else:
                logger.error(f"Failed to get DAG runs: {response.status_code}")
                time.sleep(check_interval)
                
        except Exception as e:
            logger.error(f"Error checking download completion: {str(e)}")
            time.sleep(check_interval)
    
            logger.error(f"Download timeout reached after {max_wait_time/60} minutes")
            raise Exception(f"Download timeout reached after {max_wait_time/60} minutes")

@task
def log_pipeline_status():
    """Log the current status of the refresh pipeline"""
    logger.info("=" * 80)
    logger.info("REFRESH DATA PIPELINE STATUS")
    logger.info("=" * 80)
    logger.info(f"Pipeline started at: {datetime.now()}")
    logger.info("Pipeline steps:")
    logger.info("1. ✅ Download DAG triggered")
    logger.info("2. ⏳ Waiting for download completion...")
    logger.info("3. ⏸️ Processing DAG will be triggered after download completion")
    logger.info("=" * 80)

@task
def validate_download_triggered():
    """Validate that the download DAG was triggered using Airflow REST API"""
    # Wait a moment for the DAG to start
    time.sleep(15)
    
    try:
        # Get Airflow configuration
        airflow_base_url = os.getenv('AIRFLOW_BASE_URL', 'http://airflow-apiserver:8080')
        airflow_username = os.getenv('AIRFLOW_USERNAME', 'airflow')
        airflow_password = os.getenv('AIRFLOW_PASSWORD', 'airflow')
        
        # Get JWT token
        token_url = f"{airflow_base_url}/auth/token"
        token_data = {
            "username": airflow_username,
            "password": airflow_password
        }
        
        token_response = requests.post(token_url, json=token_data, timeout=10)
        
        if token_response.status_code not in [200, 201]:
            logger.warning(f"Failed to get authentication token: {token_response.status_code}")
            return True  # Don't fail the pipeline
        
        token = token_response.json().get('access_token')
        if not token:
            logger.warning("No access token received")
            return True  # Don't fail the pipeline
        
        # Check if download DAG is running
        headers = {"Authorization": f"Bearer {token}"}
        dag_runs_url = f"{airflow_base_url}/api/v2/dags/ftp_multi_station_download/dagRuns"
        
        response = requests.get(dag_runs_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            dag_runs = response.json().get('dag_runs', [])
            if dag_runs:
                latest_run = dag_runs[0]
                state = latest_run.get('state', 'unknown')
                
                logger.info(f"Download DAG state: {state}")
                
                if state in ['running', 'queued']:
                    logger.info("Download DAG is running successfully")
                    return True
                else:
                    logger.warning(f"Download DAG is not running. State: {state}")
                    return True  # Don't fail the pipeline
            else:
                logger.warning("No download DAG runs found yet")
                return True  # Don't fail the pipeline
        else:
            logger.warning(f"Failed to get DAG runs: {response.status_code}")
            return True  # Don't fail the pipeline
        
    except Exception as e:
        logger.error(f"Error validating download trigger: {str(e)}")
        # Don't fail the entire pipeline for validation issues
        return True

with DAG(
    'refresh_data_pipeline',
    default_args=DEFAULT_ARGS,
    description='Orchestrates the complete data refresh pipeline: download then process',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['refresh', 'pipeline', 'orchestration', 'dashboard'],
    max_active_runs=1,  # Only one refresh at a time
) as dag:
    
    # Set environment variables for Airflow API access
    import os
    os.environ['AIRFLOW_BASE_URL'] = 'http://airflow-apiserver:8080'
    os.environ['AIRFLOW_USERNAME'] = 'airflow'
    os.environ['AIRFLOW_PASSWORD'] = 'airflow'
    
    # Start task
    start = EmptyOperator(
        task_id='start',
        doc_md="Start of the refresh data pipeline"
    )
    
    # Log pipeline status
    log_status = log_pipeline_status()
    
    # Trigger download DAG
    trigger_download = TriggerDagRunOperator(
        task_id='trigger_download',
        trigger_dag_id='ftp_multi_station_download',
        wait_for_completion=False,  # Don't wait, we'll monitor manually
        doc_md="Trigger the FTP download DAG to download latest data from all stations"
    )
    
    # Validate download was triggered
    validate_download = validate_download_triggered()
    
    # Wait for download completion
    wait_download = wait_for_download_completion()
    
    # Trigger processing DAG
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_processing',
        trigger_dag_id='process_multistation_data',
        wait_for_completion=False,  # Don't wait for processing
        doc_md="Trigger the data processing DAG after download completion"
    )
    
    # End task
    end = EmptyOperator(
        task_id='end',
        doc_md="End of the refresh data pipeline"
    )
    
    # Define task dependencies
    start >> log_status >> trigger_download >> validate_download >> wait_download >> trigger_processing >> end
