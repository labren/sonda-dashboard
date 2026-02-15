from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from plugins.file_saver_plugin import FileSaverOperator
from plugins.raw_data_loader_plugin import RawDataLoaderOperator
from plugins.data_transformer_plugin import DataTransformerOperator
from datetime import datetime, timedelta
from pathlib import Path
import json
from sonda_translator.sdt.carregaCabecalhos import carregaCabecalhos
from config import STATIONS_CONFIG, RAW_DIR, INTERIM_DIR
_, header_sensor = carregaCabecalhos()

@task
def set_config():
    """Read config"""
    with open(str(STATIONS_CONFIG)) as f:
        return json.load(f)

@task
def get_processing_tasks(config):
    """Get processing tasks list"""
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    return [{'station': k, 'file_type': f.split('_')[1].split('.')[0], 'filename': f,
             'input_path': str(RAW_DIR / k / f),
             'output_path': str(INTERIM_DIR / k / f'processed_data_{k.upper()}_{f.split("_")[1].split(".")[0]}_{now}.parquet')}
            for k, v in config.items() if v.get('enabled', True)
            for f in v['files']]

@task
def extract_raw(task_info):
    """Extract raw data"""
    s, ft, ip, op = task_info['station'], task_info['file_type'], Path(task_info['input_path']), task_info['output_path']
    RAW_DIR.mkdir(exist_ok=True)
    INTERIM_DIR.mkdir(exist_ok=True)

    if not ip.exists():
        return None

    has_hdr = s in header_sensor and f"{ft}_RAW_HEADER" in header_sensor[s]
    data = RawDataLoaderOperator(task_id=f'load_{s}_{ft}', file_path=str(ip), station=s,
                                 file_type=ft, header_sensor=header_sensor, validate_columns=has_hdr).execute()

    return {'station': s, 'file_type': ft, 'data': data, 'output_path': op, 'has_hdr': has_hdr} if not data.empty else None

@task
def transform(extracted):
    """Transform data"""
    if not extracted:
        return None

    s, ft, data, op, has_hdr = extracted['station'], extracted['file_type'], extracted['data'], extracted['output_path'], extracted.get('has_hdr', False)

    if not has_hdr or s not in header_sensor:
        return {**extracted, 'transformed': False}

    if ft not in [k.replace('_RAW_HEADER', '') for k in header_sensor[s].keys() if k.endswith('_RAW_HEADER')]:
        return None

    transformed = DataTransformerOperator(task_id=f'xform_{s}_{ft}', data=data, station=s,
                                         file_type=ft, header_sensor=header_sensor).execute()

    return {'station': s, 'file_type': ft, 'data': transformed, 'output_path': op, 'transformed': True} if not transformed.empty else None

@task
def save(data):
    """Save data"""
    if not data:
        return None

    FileSaverOperator(task_id=f'save_{data["station"]}_{data["file_type"]}', data=data['data'],
                     output_path=data['output_path'], file_format='parquet', compression='snappy').execute()

    return f"Success: {data['station']}_{data['file_type']}"

with DAG(
    'process_multistation_data',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'retries': 1,
                  'retry_delay': timedelta(minutes=2), 'start_date': datetime(2025, 1, 1)},
    description='Process raw data to interim format',
    schedule=None,
    catchup=False,
    tags=['solar_data', 'multi_station', 'multi_source']
) as dag:
    start, end = EmptyOperator(task_id='start'), EmptyOperator(task_id='end')
    cfg = set_config()
    tasks = get_processing_tasks(cfg)
    extracted = extract_raw.expand(task_info=tasks)
    transformed = transform.expand(extracted=extracted)
    saved = save.expand(data=transformed)
    start >> cfg >> tasks >> extracted >> transformed >> saved >> end 