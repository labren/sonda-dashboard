import os
import duckdb
import pandas as pd
import pathlib
import warnings
import logging

from sonda_translator.sdt.qualificaDado import prequalificarDado
warnings.filterwarnings("ignore")


def processRawData(file_path:str,
                   station:str,
                   file_type:str,                   
                   header_sensor:dict,
                   logger:logging.Logger = None,
                   overwrite:bool = False):

    """
    Função para ler arquivos de dados e processá-los.
    Args:
        args: Uma tupla contendo os seguintes parâmetros:
            - file_path: Caminho do arquivo a ser lido.
            - station: Nome da estação.
            - file_type: Tipo do arquivo (MD, SD, WD).
            - output_dir: Diretório de saída para salvar os arquivos maravilhados.
            - overwrite: Flag para sobrescrever arquivos existentes.
            - logger: Logger para registrar erros.
            - headers: Cabeçalhos dos arquivos.
            - header_sensor: Cabeçalhos dos sensores.
    Returns:
        None
    """
    # Check if the file exists
    if not os.path.exists(file_path):
        logger.error(f"Error 0 - O arquivo {file_path} não existe.")
        return pd.DataFrame()
    
    # entender como os headers casam com os dados
    ############################################################################
    ############ Parte 1 - Acoplar Cabeçalho dos dados Raw ao DataFrame ############
    ############################################################################
    # Abre o arquivo e lê os dados
    try:
        data = duckdb.query(f"""
            SELECT * 
            FROM read_csv_auto('{file_path}',
            ignore_errors=true            
        """).df()
    except Exception as e:
        logger.error(f"Error 2 - Não foi possível ler o arquivo {file_path} \
            durante o processo de encontrar dados.\nDetalhes do erro: {str(e)} \
            Os dados encontrados foram: {data}")
        return pd.DataFrame()
    
    # Adiciona o cabeçalho encontrado ao DataFrame
    try:
        data.columns = header_sensor[station.upper()][file_type+'_RAW_HEADER']
    except Exception as e:
        logger.error(f"Error 3 - Não foi possível adicionar o cabeçalho ao arquivo {file_path} \
            durante o processo de encontrar dados.\nDetalhes do erro: {str(e)}")
        return pd.DataFrame()
    
    else:
        return data
    
    # # Cria um dicionário para mapear os cabeçalhos encontrados para os cabeçalhos principais
    # # O dicionário será criado com os valores do cabeçalho principal como chaves
    # normalized_headers = {}
    # for key, values in main_header.items():
    #     for value in values:
    #         normalized_headers[value.strip().upper()] = key
    # renamed_columns = {}
    # for col in data.columns:
    #     if str(col).strip().upper() in normalized_headers:
    #         renamed_columns[col] = normalized_headers[str(col).strip().upper()]
    
    # # Renomeia as colunas do DataFrame com os nomes encontrados
    # if renamed_columns:
    #     data = data.rename(columns=renamed_columns)
    
    # # Verifica se o cabeçalho principal tem o mesmo número de colunas que o arquivo
    # # Isso irá deixar os dados já organizados para o formato correto
    # result = pd.DataFrame()
    # for key in main_header.keys():
    #     result[key] = data.get(key, pd.NA)

    # # Pega colunas que não foram renomeadas e separa em um novo DataFrame
    # outros_dados = data.drop(columns=main_header.keys(), errors='ignore')
    # # Verifica se tem outros dados e cria um log
    # if not outros_dados.empty:
    #     # Registra o erro usando o logger
    #     logger.error(f"WARNING - O arquivo {file_path} contém colunas que não foram processadas: {outros_dados.columns.tolist()}")

    # # Adiciona colunas extras ao DataFrame
    # result['acronym'] = station.upper()
    # result['timestamp'] = pd.to_datetime(result['timestamp'], errors='coerce')

    #  # Encontr atipo completo baseado no file_type
    # # Caso MD, o tipo é Meteorologico
    # # Caso SD, o tipo é Solarimetrico
    # # Caso WD, o tipo é Anemometrico
    # tipo_completo = ''
    # if file_type == 'MD':
    #     tipo_completo = 'Meteorologicos'
    # elif file_type == 'SD':
    #     tipo_completo = 'Solarimetricos'
    # elif file_type == 'WD':
    #     tipo_completo = 'Anemometricos'

    ############################################################################
    ### Parte 3 - Pré-Qualificar os dados e salvar o arquivo formatado ###############
    ############################################################################

    # # Qualifica os dados
    # result, summary = prequalificarDado(result, file_type, logger, estacao, output_dir, tipo_completo)

    # # Adiciona subcabeçalho ao DataFrame
    # try:
    #     sub_header = header_sensor[estacao][file_type]
    # except KeyError:
    #     # Registra o erro usando o logger
    #     logger.error(f"Error 6 - Não foi possível encontrar o cabeçalho para a estação {estacao} e tipo {file_type} no arquivo {file_path}, um subcabeçalho vazio será adicionado.")
    #     sub_header = [''] * len(result.columns)
    # # Adiciona o subcabeçalho ao DataFrame
    # sub_header = ['', '', '', '', ''] + sub_header
    # result.columns = pd.MultiIndex.from_tuples(list(zip(result.columns,sub_header)))

    # try:
    #     # Agrupa por mês e cria um loop para criar os arquivos
    #     result_groups = result.groupby(result['timestamp'].dt.to_period('M'))
    #     for period, group in result_groups:
    #         # Cria o nome do arquivo, o padrão é: SMS_YYYY_MM_SD_formatado.csv
    #         file_name = f"{estacao.upper()}_{period.year}_{period.month:02d}_{file_type}_formatado.csv"
    #         # O output_path será sempre output_dir + estacao + tipo_completo + ano
    #         output_path = os.path.join(output_dir, estacao.upper(), tipo_completo, str(period.year))
    #         # Verifica se o arquivo já existe, caso exista verifica flag de overwrite, acaso não exista, cria o arquivo, caso exista e a flag overwrite for False, pula a criação do arquivo
    #         file_path = os.path.join(output_path, file_name)
    #         # Cria diretorio caso não exista
    #         pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    #         # Verifica se existem linhas duplicadas
    #         if group.duplicated().any():
    #             # Registra o erro usando o logger
    #             # logger.error(f"WARNING - O arquivo {file_path} contém linhas duplicadas.")
    #             # Remove as linhas duplicadas
    #             group = group.drop_duplicates()
    #         # Verifica se o arquivo já existe
    #         if os.path.exists(file_path):
    #             if not overwrite:
    #                 continue
    #             else:
    #                 group.to_csv(file_path, index=False)
    #         else:
    #             # Cria diretorio caso não exista
    #             pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
    #             # Cria o arquivo
    #             group.to_csv(file_path, index=False)
    # except:
    #     # Registra o erro usando o logger
    #     logger.error(f"Não foi possível criar o arquivo {file_path} durante o processo de salvar os dados.\nDetalhes do erro: {result}")
    #     return summary
        
    # # Retorna o resumo dos dados
    # return summary