from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from plugins.header_finder_plugin import HeaderFinderOperator, HeaderConfigUpdaterOperator

from datetime import datetime, timedelta
import os
import json

# Constants
CONFIG_FILE = '/opt/airflow/config_files/stations_download_config.json'
HEADER_CONFIG_FILE = '/opt/airflow/config_files/cabecalho_sensor.json'
BASE_REMOTE_PATH = '/coleta'

@task
def load_configurations():
    """Load both configuration files"""
    try:
        # Load stations configuration
        if not os.path.exists(CONFIG_FILE):
            raise FileNotFoundError(f"Config file {CONFIG_FILE} does not exist.")
        
        with open(CONFIG_FILE, 'r') as f:
            stations_config = json.load(f)
        
        # Load header configuration
        if not os.path.exists(HEADER_CONFIG_FILE):
            raise FileNotFoundError(f"Header config file {HEADER_CONFIG_FILE} does not exist.")
        
        with open(HEADER_CONFIG_FILE, 'r') as f:
            header_config = json.load(f)
        
        print("Successfully loaded both configuration files")
        return {
            'stations_config': stations_config,
            'header_config': header_config
        }
        
    except Exception as e:
        print(f"Error loading configurations: {str(e)}")
        raise

@task
def identify_missing_headers(configs: dict) -> list:
    """Identify stations and file types that don't have header configurations"""
    stations_config = configs['stations_config']
    header_config = configs['header_config']
    
    missing_headers = []
    
    for station_key, station_value in stations_config.items():
        if not station_value.get('enabled', True):
            print(f"Skipping disabled station: {station_key}")
            continue
        
        print(f"Checking headers for station: {station_key}")
        
        for file in station_value['files']:
            # Extract file type from filename (e.g., "PTR_SD.DAT" -> "SD")
            file_type = file.split('_')[1].split('.')[0]
            header_key = f"{file_type}_RAW_HEADER"
            
            # Check if header configuration exists
            has_header = (
                station_key in header_config and 
                header_key in header_config[station_key]
            )
            
            if not has_header:
                print(f"Missing header for {station_key}_{file_type}")
                missing_headers.append({
                    'station': station_key,
                    'file_type': file_type,
                    'filename': file,
                    'remote_file_path': f"{BASE_REMOTE_PATH}/{station_key}/data/{file}"
                })
            else:
                print(f"Header exists for {station_key}_{file_type}")
    
    print(f"Found {len(missing_headers)} missing headers")
    return missing_headers

@task
def find_headers(missing_header: dict):
    """Find headers for a specific station and file type"""
    station = missing_header['station']
    file_type = missing_header['file_type']
    remote_file_path = missing_header['remote_file_path']
    
    print(f"Finding headers for {station}_{file_type} from {remote_file_path}")
    
    # Use the HeaderFinderOperator
    finder = HeaderFinderOperator(
        task_id=f'find_headers_{station}_{file_type}',
        station=station,
        file_type=file_type,
        remote_file_path=remote_file_path,
        ftp_conn_id='solter.ftp.1',
        lines_to_download=20
    )
    
    result = finder.execute()
    
    if result:
        print(f"Successfully found headers for {station}_{file_type}: {len(result['headers'])} columns")
    else:
        print(f"Failed to find headers for {station}_{file_type}")
    
    return result

@task
def update_header_configuration(found_headers: list):
    """Update the header configuration file with found headers"""
    # Filter out None results
    valid_headers = [h for h in found_headers if h is not None]
    
    if not valid_headers:
        print("No valid headers found to update")
        return "No headers to update"
    
    print(f"Updating configuration with {len(valid_headers)} headers")
    
    # Use the HeaderConfigUpdaterOperator
    updater = HeaderConfigUpdaterOperator(
        task_id='update_header_configuration',
        new_headers=valid_headers,
        config_file_path=HEADER_CONFIG_FILE
    )
    
    result = updater.execute()
    print(f"Header configuration update result: {result}")
    return result

with DAG(
    'header_discovery_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'start_date': datetime(2025, 1, 1),
    },
    description='Discover missing headers from FTP files and update configuration',
    schedule='@weekly',  # Run weekly to check for new stations or file types
    catchup=False,
    tags=['header_discovery', 'ftp', 'configuration']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Load configurations
    configs = load_configurations()
    
    # Identify missing headers
    missing_headers = identify_missing_headers(configs)
    
    # Find headers for missing configurations in parallel
    found_headers = find_headers.expand(missing_header=missing_headers)
    
    # Update configuration with found headers
    update_result = update_header_configuration(found_headers)
    
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> configs >> missing_headers >> found_headers >> update_result >> end 