from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from plugins.file_saver_plugin import FileSaverOperator
from plugins.raw_data_loader_plugin import RawDataLoaderOperator
from plugins.data_transformer_plugin import DataTransformerOperator

from datetime import datetime
import os

from sonda_translator.sdt.carregaCabecalhos import carregaCabecalhos

# Set station
estacao = 'ptr'

# Set base paths
RAW_DATA_DIR = os.path.expanduser("data/raw")
INTERIM_DATA_DIR = os.path.expanduser("data/interim")

#loading headers
headers, header_sensor = carregaCabecalhos()



@task
def extract_raw_data():
    print(f"Processing raw data from {RAW_DATA_DIR}")
    extracted_raw_data = []
    
    # Ensure directories exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(INTERIM_DATA_DIR, exist_ok=True)
    
    # Process all raw files
    for filename in os.listdir(RAW_DATA_DIR):
        if filename.endswith('.DAT'):
            input_path = os.path.join(RAW_DATA_DIR, filename)            
            loader = RawDataLoaderOperator(
                task_id='load_raw_data',
                file_path=input_path
            )
            data = loader.execute()  # Execute the operator and get the data
            extracted_raw_data.append(data)
    return extracted_raw_data

@task
def transform_data(extracted_raw_data):
    print(f"Transforming data")
    transformed_data = []
    
    for data in extracted_raw_data:
        transformer = DataTransformerOperator(
            task_id='transform_data',
            data=data,
            station=estacao,
            file_type='SD',
            header_sensor=header_sensor
        )
        transformed = transformer.execute()  # Execute the operator and get the data
        transformed_data.append(transformed)
    
    return transformed_data

@task
def save_data(transformed_data):
    print(f"Saving data")
    for data in transformed_data:
        saver = FileSaverOperator(
            task_id='save_processed_data',
            data=data,  # Pass single data item, not the whole list
            output_path=os.path.join(INTERIM_DATA_DIR, f'processed_data_{estacao.upper()}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'),
            file_format='parquet',
            compression='snappy'
        )
        saver.execute()  # Execute the operator

with DAG(
    'load_solar_data',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'start_date': datetime(2025, 1, 1),
    },
    description='Process raw data to data interim format',
    schedule='@daily',
    catchup=False,
    tags=['solar_data']
) as dag:
    start = EmptyOperator(task_id='start')

    extract_raw_data = extract_raw_data()
    transform_data = transform_data(extract_raw_data)
    save_data = save_data(transform_data)

    end = EmptyOperator(task_id='end')
    start >> extract_raw_data >> transform_data >> save_data >> end