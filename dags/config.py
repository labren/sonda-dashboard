from pathlib import Path

# Paths
CONFIG_DIR = Path('/opt/airflow/config_files')
STATIONS_CONFIG = CONFIG_DIR / 'stations_download_config.json'
HEADER_CONFIG = CONFIG_DIR / 'cabecalho_sensor.json'
RAW_DIR = Path('/opt/airflow/data/raw')
INTERIM_DIR = Path('/opt/airflow/data/interim')
BASE_REMOTE = '/coleta'

# Airflow
FTP_CONN_ID = 'solter.ftp.1'
