from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.ftp.sensors.ftp import FTPSensor
from airflow.providers.ftp.hooks.ftp import FTPHook
from datetime import datetime
from collections import deque

# Constants
FTP_CONN_ID = 'solter.ftp.1'
DAYS_TO_KEEP = 3
REMOTE_FILEPATH = '/coleta/ptr/data/PTR_SD.DAT'
LOCAL_FILEPATH = '/opt/airflow/data/raw/PTR_SD.DAT'

def days_to_download(days: int) -> int:
    return 60 * 24 * days

@task
def list_and_download_file():
    ftp_hook = FTPHook(ftp_conn_id=FTP_CONN_ID)
    
    # Use a deque to keep only the last 50 lines in memory
    last_50_lines = deque(maxlen=days_to_download(days=DAYS_TO_KEEP))

    # Define a callback to process the file line by line
    def handle_binary(more_data):
        # more_data is bytes, so decode and split into lines
        lines = more_data.decode('utf-8', errors='ignore').splitlines(keepends=True)
        for line in lines:
            last_50_lines.append(line)

    # Retrieve the file from FTP, processing it in chunks
    ftp_conn = ftp_hook.get_conn()
    ftp_conn.retrbinary(f"RETR {REMOTE_FILEPATH}", callback=handle_binary)

    # Write the last 50 lines to the local file
    with open(LOCAL_FILEPATH, 'w') as f:
        f.writelines(last_50_lines)

    # Print the last 50 lines
    print(''.join(last_50_lines))

with DAG(
    'ftp_list_and_download',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
    },
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['ftp', 'data_download'],
    description='Download and process FTP data files'
) as dag:

    start = EmptyOperator(task_id='start')

    # Sensor to check for file availability on FTP
    wait_for_file = FTPSensor(
        task_id='wait_for_file',
        ftp_conn_id=FTP_CONN_ID,
        path=REMOTE_FILEPATH,
        timeout=600,
        poke_interval=10
    )

    download_task = list_and_download_file()

    end = EmptyOperator(task_id='end')

    # Define task dependencies
    start >> wait_for_file >> download_task >> end