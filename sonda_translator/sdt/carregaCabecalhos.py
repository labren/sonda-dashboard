import os
import json

def carregaCabecalhos():
    """
    Carrega os cabeçalhos e os sensores a partir de arquivos JSON.
    """
    # Carrega o arquivo JSON com os cabeçalhos
    script_dir = 'config_files'
    print(script_dir)
    dat_file_path = os.path.join(script_dir, 'cabecalho_sensor.json')
    print(dat_file_path)
    # Carrega o arquivo JSON com os sensores
    header_sensor_path = os.path.join(script_dir, 'cabecalho_sensor.json')
    print(header_sensor_path)
    if os.path.exists(dat_file_path):
        with open(dat_file_path, 'r') as f:
            headers = json.load(f)
    else:
        print(f"Arquivo {dat_file_path} não encontrado.")
        exit()
    if os.path.exists(header_sensor_path):
        with open(header_sensor_path, 'r') as f:
            header_sensor = json.load(f)
    else:
        print(f"Arquivo {header_sensor_path} não encontrado.")
        exit()
    
    return headers, header_sensor