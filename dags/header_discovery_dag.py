from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from plugins.header_finder_plugin import HeaderFinderOperator, HeaderConfigUpdaterOperator
from datetime import datetime, timedelta
import json

CONFIG_FILE = '/opt/airflow/config_files/stations_download_config.json'
HEADER_CONFIG_FILE = '/opt/airflow/config_files/cabecalho_sensor.json'
BASE_REMOTE = '/coleta'


@task
def load_configurations():
    """Load station and header configuration files."""
    with open(CONFIG_FILE) as f:
        stations = json.load(f)
    with open(HEADER_CONFIG_FILE) as f:
        headers = json.load(f)
    return {'stations_config': stations, 'header_config': headers}


@task
def identify_missing_headers(configs: dict) -> list:
    """Identify stations/file types without header configurations."""
    stations, headers = configs['stations_config'], configs['header_config']
    missing = []
    for k, v in stations.items():
        if not v.get('enabled', True):
            continue
        for f in v['files']:
            ftype = f.split('_')[1].split('.')[0]
            if not headers.get(k, {}).get(f"{ftype}_RAW_HEADER"):
                print(f"Missing header for {k}_{ftype}")
                missing.append({'station': k, 'file_type': ftype, 'filename': f,
                                'remote_file_path': f"{BASE_REMOTE}/{k}/data/{f}"})
    print(f"Found {len(missing)} missing headers")
    return missing


@task
def find_headers(missing_header: dict):
    """Find headers for a specific station/file type via FTP."""
    s, ft = missing_header['station'], missing_header['file_type']
    result = HeaderFinderOperator(
        task_id=f'find_headers_{s}_{ft}', station=s, file_type=ft,
        remote_file_path=missing_header['remote_file_path'],
        ftp_conn_id='solter.ftp.1', lines_to_download=20
    ).execute()
    if result:
        print(f"Found headers for {s}_{ft}: {len(result['headers'])} columns")
    return result


@task
def update_header_configuration(found_headers: list):
    """Update header config file with discovered headers."""
    valid = [h for h in found_headers if h is not None]
    if not valid:
        return "No headers to update"
    result = HeaderConfigUpdaterOperator(
        task_id='update_header_configuration',
        new_headers=valid, config_file_path=HEADER_CONFIG_FILE
    ).execute()
    print(f"Header configuration update result: {result}")
    return result


with DAG(
    'header_discovery_dag',
    default_args={'owner': 'airflow', 'depends_on_past': False,
                  'retries': 1, 'retry_delay': timedelta(minutes=2),
                  'start_date': datetime(2025, 1, 1)},
    description='Discover missing headers from FTP files and update configuration',
    schedule='@weekly',
    catchup=False,
    tags=['header_discovery', 'ftp', 'configuration']
) as dag:

    start, end = EmptyOperator(task_id='start'), EmptyOperator(task_id='end')
    configs = load_configurations()
    missing = identify_missing_headers(configs)
    found = find_headers.expand(missing_header=missing)
    updated = update_header_configuration(found)

    start >> configs >> missing >> found >> updated >> end
