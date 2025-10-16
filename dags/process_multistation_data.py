from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from plugins.file_saver_plugin import FileSaverOperator
from plugins.raw_data_loader_plugin import RawDataLoaderOperator
from plugins.data_transformer_plugin import DataTransformerOperator

from datetime import datetime
import os
import json

from sonda_translator.sdt.carregaCabecalhos import carregaCabecalhos

# Set base paths
RAW_DATA_DIR = os.path.expanduser("data/raw")
INTERIM_DATA_DIR = os.path.expanduser("data/interim")
CONFIG_FILE = '/opt/airflow/config_files/stations_download_config.json'

# Load headers
_, header_sensor = carregaCabecalhos()

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
def get_station_processing_tasks(config: dict) -> list:
    """Get list of station processing tasks"""
    processing_tasks = []
    
    for station_key, station_value in config.items():
        if not station_value.get('enabled', True):
            print(f"Skipping disabled station: {station_key}")
            continue
            
        print(f"Processing station: {station_key}")
        print(f"Station config: {station_value}")
        
        for file in station_value['files']:
            # Extract file type from filename (e.g., "PTR_SD.DAT" -> "SD")
            file_type = file.split('_')[1].split('.')[0]
            
            # Create station-specific output directory
            station_output_dir = os.path.join(INTERIM_DATA_DIR, station_key)
            
            processing_tasks.append({
                'station': station_key,
                'file_type': file_type,
                'filename': file,
                'input_path': os.path.join(RAW_DATA_DIR, station_key, file),
                'output_path': os.path.join(station_output_dir, f'processed_data_{station_key.upper()}_{file_type}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
            })
    
    print(f"Created {len(processing_tasks)} processing tasks")
    return processing_tasks

@task
def extract_raw_data(processing_task: dict):
    """Extract raw data for a specific station and file type"""
    station = processing_task['station']
    file_type = processing_task['file_type']
    input_path = processing_task['input_path']
    
    print(f"Processing raw data for station {station}, file type {file_type} from {input_path}")
    
    # Ensure directories exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(INTERIM_DATA_DIR, exist_ok=True)
    
    # Check if file exists
    if not os.path.exists(input_path):
        print(f"Warning: File {input_path} does not exist. Skipping.")
        return None
    
    # Check if station has header configuration
    has_header_config = station in header_sensor and f"{file_type}_RAW_HEADER" in header_sensor[station]
    
    if not has_header_config:
        print(f"Warning: Station {station} does not have header configuration for {file_type}. Processing without validation.")
    else:
        print(f"Info: Station {station} has header configuration for {file_type}. Processing with validation.")
    
    # Load raw data with enhanced validation
    loader = RawDataLoaderOperator(
        task_id=f'load_raw_data_{station}_{file_type}',
        file_path=input_path,
        station=station,
        file_type=file_type,
        header_sensor=header_sensor,
        validate_columns=has_header_config  # Only validate if header config exists
    )
    data = loader.execute()
    
    if data.empty:
        print(f"Warning: No data loaded for {station}_{file_type}")
        return None
    
    return {
        'station': station,
        'file_type': file_type,
        'data': data,
        'output_path': processing_task['output_path'],
        'has_header_config': has_header_config
    }

@task
def transform_data(extracted_data: dict):
    """Transform data for a specific station and file type"""
    if extracted_data is None:
        return None
    
    station = extracted_data['station']
    file_type = extracted_data['file_type']
    data = extracted_data['data']
    output_path = extracted_data['output_path']
    has_header_config = extracted_data.get('has_header_config', False)
    
    print(f"Transforming data for station {station}, file type {file_type}")
    
    # If no header configuration, skip transformation but still save raw data
    if not has_header_config:
        print(f"Warning: Station {station} has no header configuration. Saving raw data without transformation.")
        return {
            'station': station,
            'file_type': file_type,
            'data': data,
            'output_path': output_path,
            'transformed': False
        }
    
    # Check if station exists in header_sensor
    if station not in header_sensor:
        print(f"Warning: Station {station} not found in header_sensor. Skipping transformation.")
        return None
    
    # Check if file_type exists for this station
    if file_type not in [key.replace('_RAW_HEADER', '') for key in header_sensor[station].keys() if key.endswith('_RAW_HEADER')]:
        print(f"Warning: File type {file_type} not found for station {station}. Skipping transformation.")
        return None
    
    # Transform the data
    transformer = DataTransformerOperator(
        task_id=f'transform_data_{station}_{file_type}',
        data=data,
        station=station,
        file_type=file_type,
        header_sensor=header_sensor
    )
    transformed = transformer.execute()
    
    if transformed.empty:
        print(f"Warning: No data transformed for {station}_{file_type}")
        return None
    
    return {
        'station': station,
        'file_type': file_type,
        'data': transformed,
        'output_path': output_path,
        'transformed': True
    }

@task
def save_data(transformed_data: dict):
    """Save transformed data for a specific station and file type"""
    if transformed_data is None:
        return None
    
    station = transformed_data['station']
    file_type = transformed_data['file_type']
    data = transformed_data['data']
    output_path = transformed_data['output_path']
    transformed = transformed_data.get('transformed', True)
    
    data_type = "transformed" if transformed else "raw"
    print(f"Saving {data_type} data for station {station}, file type {file_type} to {output_path}")
    
    # Save the data
    saver = FileSaverOperator(
        task_id=f'save_{data_type}_data_{station}_{file_type}',
        data=data,
        output_path=output_path,
        file_format='parquet',
        compression='snappy'
    )
    saver.execute()
    
    return f"Success: {station}_{file_type} ({data_type})"

with DAG(
    'process_multistation_data',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'start_date': datetime(2025, 1, 1),
    },
    description='Process raw data to interim format for multiple stations and sources',
    schedule='@daily',
    catchup=False,
    tags=['solar_data', 'multi_station', 'multi_source']
) as dag:
    start = EmptyOperator(task_id='start')

    # Get configuration
    config = set_config()
    
    # Get processing tasks
    processing_tasks = get_station_processing_tasks(config)
    
    # Extract raw data for all stations and file types in parallel
    extracted_data = extract_raw_data.expand(processing_task=processing_tasks)
    
    # Transform data for all stations and file types in parallel
    transformed_data = transform_data.expand(extracted_data=extracted_data)
    
    # Save data for all stations and file types in parallel
    save_results = save_data.expand(transformed_data=transformed_data)

    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> config >> processing_tasks >> extracted_data >> transformed_data >> save_results >> end 