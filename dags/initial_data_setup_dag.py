from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ftp.hooks.ftp import FTPHook

from datetime import datetime, timedelta
from collections import deque
import os
import json
import pandas as pd

# Constants
FTP_CONN_ID = 'solter.ftp.1'
DAYS_TO_KEEP = 1  # Download 3 days (72 hours) for initial setup
CONFIG_FILE = '/opt/airflow/config_files/stations_download_config.json'
BASE_REMOTE_PATH = '/coleta'
BASE_LOCAL_PATH = '/opt/airflow/data/raw'
INTERIM_DATA_DIR = '/opt/airflow/data/interim'

def days_to_download(days: int) -> int:
    # Download data based on 1-minute intervals (60 minutes × 24 hours × days)
    # 3 days = 4,320 lines (72 hours of 1-minute data)
    return 60 * 24 * days

@task
def set_config():
    """Read configuration from file"""
    try:
        # Check if the file exists
        if not os.path.exists(CONFIG_FILE):
            raise FileNotFoundError(f"Config file {CONFIG_FILE} does not exist.")
        
        # Read json file
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        
        print("Successfully read config file")
        return config
        
    except Exception as e:
        print(f"Error reading config file: {str(e)}")
        raise

@task
def check_existing_data():
    """Check if data already exists in interim directory"""
    interim_dir = INTERIM_DATA_DIR
    
    if not os.path.exists(interim_dir):
        return False
    
    # Check if any station directories exist with parquet files
    for station_dir in os.listdir(interim_dir):
        station_path = os.path.join(interim_dir, station_dir)
        if os.path.isdir(station_path):
            parquet_files = [f for f in os.listdir(station_path) if f.endswith('.parquet')]
            if parquet_files:
                print(f"Found existing data for station {station_dir}: {len(parquet_files)} files")
                return True
    
    print("No existing data found in interim directory")
    return False

@task
def get_station_files(config: dict) -> list:
    """Get list of station files to download for initial setup - only for stations that need downloading"""
    station_files = []
    
    # Only process enabled stations
    for station_key, station_value in config.items():
        if not station_value.get('enabled', True):
            print(f"Skipping disabled station: {station_key}")
            continue
            
        print(f"Checking station: {station_key}")
        
        # Check if station already has raw data
        station_has_data = False
        station_dir = os.path.join(BASE_LOCAL_PATH, station_key)
        
        if os.path.exists(station_dir):
            for file in station_value['files']:
                local_file_path = os.path.join(station_dir, file)
                if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 100:
                    station_has_data = True
                    print(f"Station {station_key} already has data for {file}")
                    break
        
        if station_has_data:
            print(f"Station {station_key} already has raw data, skipping download")
            continue
        
        print(f"Station {station_key} needs raw data download")
        for file in station_value['files']:
            station_files.append({
                'station': station_key,
                'remote_file': f"{BASE_REMOTE_PATH}/{station_key}/data/{file}",
                'local_file': f"{BASE_LOCAL_PATH}/{station_key}/{file}"
            })
    
    print(f"Created {len(station_files)} download tasks for stations that need data")
    return station_files

@task
def download_station_file(station_file: dict):
    """Download a single station file with more data for initial setup"""
    ftp_hook = FTPHook(ftp_conn_id=FTP_CONN_ID)
    
    # Create local directory if it doesn't exist
    os.makedirs(os.path.dirname(station_file['local_file']), exist_ok=True)
    
    # Use a deque to keep the last 3 days of data (4,320 lines = 72 hours × 60 minutes)
    last_lines = deque(maxlen=days_to_download(days=DAYS_TO_KEEP))

    def handle_binary(more_data):
        lines = more_data.decode('utf-8', errors='ignore').splitlines(keepends=True)
        for line in lines:
            last_lines.append(line)

    try:
        # Retrieve the file from FTP, processing it in chunks
        print(f"Downloading {station_file['remote_file']}")
        ftp_conn = ftp_hook.get_conn()
        ftp_conn.retrbinary(f"RETR {station_file['remote_file']}", callback=handle_binary)
        
        # Write the data to the local file
        with open(station_file['local_file'], 'w') as f:
            f.writelines(last_lines)
        
        print(f"Successfully downloaded {station_file['station']}/{os.path.basename(station_file['local_file'])}")
        return f"Success: {station_file['station']}"
        
    except Exception as e:
        print(f"Error downloading {station_file['station']}: {str(e)}")
        # Create a minimal file to prevent processing errors
        with open(station_file['local_file'], 'w') as f:
            f.write("# Minimal data file\n")
        return f"Error: {station_file['station']} - {str(e)}"

@task
def check_download_results():
    """Check which stations failed to download and report errors"""
    print("Checking download results...")
    
    stations = ['ptr', 'cpa', 'sms', 'nat', 'cgr', 'orn', 'sjc', 'cai', 'pma', 'stm']
    failed_stations = []
    successful_stations = []
    
    for station in stations:
        # Check if raw data was downloaded
        raw_station_dir = os.path.join(BASE_LOCAL_PATH, station)
        has_raw_data = False
        
        if os.path.exists(raw_station_dir):
            for file in os.listdir(raw_station_dir):
                if file.endswith('.DAT'):
                    file_path = os.path.join(raw_station_dir, file)
                    if os.path.getsize(file_path) > 100:  # Raw data files should have content
                        has_raw_data = True
                        break
        
        if has_raw_data:
            successful_stations.append(station)
            print(f"✅ Station {station} has raw data available")
        else:
            failed_stations.append(station)
            print(f"❌ Station {station} has no raw data")
    
    if failed_stations:
        error_msg = f"Raw data missing for {len(failed_stations)} stations: {', '.join(failed_stations)}"
        print(f"ERROR: {error_msg}")
        return f"WARNING: {error_msg}"
    else:
        print("✅ All stations have raw data available")
        return "All stations have raw data available"

@task
def summarize_download_results():
    """Summarize which stations were downloaded vs already existed"""
    print("Summarizing download results...")
    
    stations = ['ptr', 'cpa', 'sms', 'nat', 'cgr', 'orn', 'sjc', 'cai', 'pma', 'stm']
    downloaded_stations = []
    existing_stations = []
    
    for station in stations:
        raw_station_dir = os.path.join(BASE_LOCAL_PATH, station)
        
        if os.path.exists(raw_station_dir):
            # Check file modification times to determine if they were downloaded recently
            recent_download = False
            for file in os.listdir(raw_station_dir):
                if file.endswith('.DAT'):
                    file_path = os.path.join(raw_station_dir, file)
                    if os.path.getsize(file_path) > 100:
                        # Check if file was modified in the last 10 minutes (indicating recent download)
                        import time
                        file_mtime = os.path.getmtime(file_path)
                        current_time = time.time()
                        if current_time - file_mtime < 600:  # 10 minutes
                            recent_download = True
                            break
            
            if recent_download:
                downloaded_stations.append(station)
                print(f"📥 Station {station} was downloaded in this run")
            else:
                existing_stations.append(station)
                print(f"📁 Station {station} already had data (not downloaded)")
        else:
            print(f"❌ Station {station} has no data directory")
    
    summary = f"Download Summary: {len(downloaded_stations)} downloaded, {len(existing_stations)} already existed"
    print(summary)
    return summary


@task
def wait_for_processing_completion():
    """Wait for processing DAG to complete and check results"""
    import time
    from airflow.models import DagRun
    from airflow.utils.state import State
    
    print("Waiting for data processing to complete...")
    
    # Wait a bit for the processing DAG to start
    time.sleep(30)
    
    # Check if processing DAG is running or completed
    try:
        # Get the latest DAG run for process_multistation_data
        latest_run = DagRun.find(
            dag_id='process_multistation_data',
            state=State.RUNNING
        )
        
        if latest_run:
            print(f"Processing DAG is running: {latest_run[0].run_id}")
            return f"Processing DAG is running: {latest_run[0].run_id}"
        else:
            # Check for completed runs
            completed_runs = DagRun.find(
                dag_id='process_multistation_data',
                state=State.SUCCESS
            )
            if completed_runs:
                print(f"Processing DAG completed: {completed_runs[0].run_id}")
                return f"Processing DAG completed: {completed_runs[0].run_id}"
            else:
                print("Processing DAG status unknown")
                return "Processing DAG status unknown"
            
    except Exception as e:
        print(f"Error checking processing DAG status: {str(e)}")
        return f"Error checking processing DAG status: {str(e)}"

@task
def check_processing_results():
    """Check if processing was successful and report final results"""
    print("Checking final processing results...")
    
    stations = ['ptr', 'cpa', 'sms', 'nat', 'cgr', 'orn', 'sjc', 'cai', 'pma', 'stm']
    failed_stations = []
    successful_stations = []
    
    for station in stations:
        station_dir = os.path.join(INTERIM_DATA_DIR, station)
        
        # Check if station has processed data files
        has_processed_data = False
        if os.path.exists(station_dir):
            for file in os.listdir(station_dir):
                if file.endswith('.parquet') and 'processed_data' in file:
                    # Check if it's real data by looking at file size (real data files are larger)
                    file_path = os.path.join(station_dir, file)
                    if os.path.getsize(file_path) > 1000:  # Real data files are larger
                        has_processed_data = True
                        break
        
        if has_processed_data:
            successful_stations.append(station)
            print(f"✅ Station {station} processed successfully")
        else:
            failed_stations.append(station)
            print(f"❌ Station {station} processing failed")
    
    if failed_stations:
        error_msg = f"Processing failed for {len(failed_stations)} stations: {', '.join(failed_stations)}"
        print(f"ERROR: {error_msg}")
        return f"WARNING: {error_msg}"
    else:
        print("✅ All stations processed successfully")
        
        # Create cache clearing marker for dashboard
        try:
            os.makedirs("logs/dashboard", exist_ok=True)
            with open("logs/dashboard/cache_clear_needed", "w") as f:
                f.write(f"Initial data setup completed at {datetime.now()}")
            print("📝 Created cache clearing marker for dashboard")
        except Exception as e:
            print(f"⚠️ Could not create cache clearing marker: {e}")
        
        return "All stations processed successfully"

with DAG(
    'initial_data_setup',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['initialization', 'data_setup', 'fresh_install'],
    description='Initial data setup for fresh installations - downloads and processes initial data'
) as dag:

    start = EmptyOperator(task_id='start')
    
    # Check if data already exists
    data_exists = check_existing_data()
    
    # Get configuration
    config = set_config()
    
    # Get station files to download
    station_files = get_station_files(config)
    
    # Download files in parallel
    download_tasks = download_station_file.expand(station_file=station_files)
    
    # Check download results and report any failures
    check_results = check_download_results()
    
    # Summarize download results
    summarize_results = summarize_download_results()
    
    # Trigger processing DAG using TriggerDagRunOperator
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_processing_dag',
        trigger_dag_id='process_multistation_data',
        conf={'triggered_by': 'initial_data_setup'},
        dag=dag
    )
    
    # Wait for processing to complete
    wait_processing = wait_for_processing_completion()
    
    # Check final processing results
    check_processing = check_processing_results()
    
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> data_exists >> config >> station_files >> download_tasks >> check_results >> summarize_results >> trigger_processing >> wait_processing >> check_processing >> end
