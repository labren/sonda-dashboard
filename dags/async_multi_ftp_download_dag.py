from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.ftp.hooks.ftp import FTPHook

from datetime import datetime, timedelta
from collections import deque
import os
import json

# Constants
FTP_CONN_ID = 'solter.ftp.1'
DAYS_TO_KEEP = 3  # Keep 3 days (72 hours) for dashboard display
CONFIG_FILE = '/opt/airflow/config_files/stations_download_config.json'
BASE_REMOTE_PATH = '/coleta'
BASE_LOCAL_PATH = '/opt/airflow/data/raw'

# Station configuration - you can load this from a config file

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
def get_station_files(config: dict) -> list:
    """Get list of station files to download"""
    station_files = []
    for station_key, station_value in config.items():
        print(station_key)
        print(station_value)
        for file in station_value['files']:
            station_files.append({
                'station': station_key,
                'remote_file': f"{BASE_REMOTE_PATH}/{station_key}/data/{file}",
                'local_file': f"{BASE_LOCAL_PATH}/{station_key}/{file}"
            })
        print(station_files)
    return station_files

@task(retries=5, retry_delay=timedelta(seconds=120), max_retry_delay=timedelta(hours=2))
def download_station_file(station_file: dict):
    """Download a single station file with proper error handling and retries"""
    try:
        print(f"Starting download for station: {station_file['station']}")
        print(f"Remote file: {station_file['remote_file']}")
        print(f"Local file: {station_file['local_file']}")
        
        ftp_hook = FTPHook(ftp_conn_id=FTP_CONN_ID)
        
        # Create local directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(station_file['local_file']), exist_ok=True)
        
        # Use a deque to keep only the last N days' data
        last_lines = deque(maxlen=days_to_download(days=DAYS_TO_KEEP))

        def handle_binary(more_data):
            lines = more_data.decode('utf-8', errors='ignore').splitlines(keepends=True)
            for line in lines:
                last_lines.append(line)

        # Retrieve the file from FTP, processing it in chunks
        ftp_conn = ftp_hook.get_conn()
        
        # Check if remote file exists before attempting download
        try:
            ftp_conn.size(station_file['remote_file'])
        except Exception as e:
            print(f"Remote file {station_file['remote_file']} not found or inaccessible: {str(e)}")
            raise FileNotFoundError(f"Remote file not found: {station_file['remote_file']}")
        
        ftp_conn.retrbinary(f"RETR {station_file['remote_file']}", callback=handle_binary)
        
        # Write the last lines to the local file
        with open(station_file['local_file'], 'w') as f:
            f.writelines(last_lines)
        
        print(f"Successfully downloaded {station_file['station']}/{os.path.basename(station_file['local_file'])}")
        return f"Success: {station_file['station']}"
        
    except FileNotFoundError as e:
        print(f"File not found error for {station_file['station']}: {str(e)}")
        raise
    except ConnectionError as e:
        print(f"Connection error for {station_file['station']}: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error downloading {station_file['station']}: {str(e)}")
        raise
        


with DAG(
    'ftp_multi_station_download',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),  # 30 seconds delay between retries
    },
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Triggered by refresh_data_pipeline
    catchup=False,
    tags=['ftp', 'data_download', 'multi_station'],
    description='Download data from multiple FTP stations in parallel'

) as dag:

    start = EmptyOperator(task_id='start')
    #get config
    config = set_config()
    
    # Get list of files to download
    station_files = get_station_files(config)
    
    # Download files in parallel using task mapping
    download_tasks = download_station_file.expand(station_file=station_files)
    
    end = EmptyOperator(task_id='end')

    # Define task dependencies
    start >> config >> station_files >> download_tasks >> end