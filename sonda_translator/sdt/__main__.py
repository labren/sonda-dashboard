import argparse
import json
import os
import pathlib
import pandas as pd
import tqdm
from distutils.util import strtobool
from multiprocessing import Pool
from carregaCabecalhos import carregaCabecalhos
from logger import setup_logger
from processaDado import processarArquivo
from scan_ftp import main as scan_ftp_main
from tratar_quarentena import tratar_quarentena
from gerar_base import gerarBase

# Copyright (c) Helvecio Neto - 2025 - helvecioblneto@gmail.com
# Todos os direitos reservados.

if __name__ == "__main__":
    # Descrição do que o programa faz
    
    parser = argparse.ArgumentParser(
        description="""
        SONDA Translator - Ferramenta para processar e formatar arquivos de dados da rede SONDA.
        
        Esta ferramenta permite filtrar, qualificar e converter arquivos de dados meteorológicos (.dat) 
        para formatos padronizados. O processamento pode ser feito de forma sequencial ou paralela, 
        com suporte a diversos tipos de dados e estações.
        
        Exemplos de uso:
        # Exemplo 1: Processa arquivos das estações 'brb' e 'cai' do tipo SD (Solar Data) do ano de 2020 em modo paralelo
        python -m sdt -e brb cai -tipo SD -ano 2020 -p

        # Exemplo 2: Exibe (sem processar) os arquivos históricos da estação 'pet'
        python -m sdt -e pet -hist True -exibir

        # Exemplo 3: Processa o arquivo com ID 42, salva em um diretório específico e sobrescreve se já existir
        python -m sdt -id 42 -out /caminho/para/saida/ -overwrite
        
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-estacao', nargs='+', type=lambda s: s.lower(), 
                        help='Nome da estação ou lista de estações (ex: brb cai pet)', default=[])
    parser.add_argument('-historico',
                        type=lambda x: bool(strtobool(x)), 
                        help='Se True, irá buscar os dados históricos (ex: True ou False)')
    parser.add_argument('-ano', type=str,
                        help='Ano do dado a ser buscado. Aceita: YYYY, YYYY-MM, YYYY-MM-DD (ex: 2020, 2020-06, 2020-06-15)',
                        default='')
    parser.add_argument('-tipo', type=str,
                help='Tipo de dado a ser buscado: SD (Solar Data), MD (Meteorological Data), WD (Wind Data)',
                choices=['SD', 'MD', 'WD', 'INDEFINIDO', 'TD'])
    parser.add_argument('-parallel', type=lambda x: bool(strtobool(x)), default=False,
                        help='Paraleliza o processamento dos arquivos (recomendado para grandes volumes)')
    parser.add_argument('-id', type=int,
                        help='ID específico do arquivo a ser processado')
    parser.add_argument('-output', type=str, default='output/sonda-formatados/',
                        help='Caminho para salvar os arquivos processados')
    parser.add_argument('-formatar', action='store_true',
                        help='Ativa a formatação especial para os arquivos de saída')
    parser.add_argument('-overwrite', action='store_true',
                        help='Sobrescreve os arquivos de saída caso já existam')
    parser.add_argument('-ftp_dir', type=str, default='/media/helvecioneto/Barracuda/',
                        help='Diretório base onde estão localizados os arquivos a serem processados')
    parser.add_argument('-scan_ftp', action='store_true',
                        help='Escaneia o diretório FTP para encontrar arquivos .dat')
    parser.add_argument('-quarentena', nargs='*', type=str, default=None,
                        help='Trata arquivos em quarentena através de seus IDs (pode ser fornecido sem IDs ou com um ou mais IDs)')
    parser.add_argument('-quarentena_tratado', type=lambda x: bool(strtobool(x)), default=False,
                        help='Trata arquivos em quarentena que já foram tratados')
    parser.add_argument('-gerar_base', action='store_true',
                        help='Gera base de dados dos arquivos formatados')
    args = parser.parse_args()
    
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dat_file_path = os.path.join(script_dir, 'json', 'arquivos_ftp.json')
    
    if os.path.exists(dat_file_path):
        with open(dat_file_path, 'r') as f:
            dat_files = json.load(f)
            dat_files_df = pd.DataFrame(dat_files)
            # Altera nome da coluna date para ano
            dat_files_df.rename(columns={'date': 'ano'}, inplace=True)
            if args.ftp_dir:
                # Update the 'caminho' column with the FTP directory
                dat_files_df['caminho'] = dat_files_df['caminho'].apply(lambda x: os.path.join(args.ftp_dir, x))
    else:
        print(f"Error: File '{dat_file_path}' not found.")
        exit(1)

    # Print de boas vindas falando os parametros que foram passados
    print(f"Sonda-Translator - Processando arquivos com os seguintes parâmetros:")
    print(50 * "-")
    for arg in vars(args):
        if getattr(args, arg) is not None:
            print(f"-{arg.upper()}: {getattr(args, arg)}")
    print(50 * "-")
    print("Iniciando processamento...\n")

    # Apply filters based on parameters
    if args.estacao:
        dat_files_df = dat_files_df[dat_files_df['estacao'].str.lower().isin(args.estacao)]
    
    # Filter by historical data if specified
    if args.historico is not None:
        dat_files_df = dat_files_df[dat_files_df['is_historico'] == args.historico]
    
    # Filter by type if specified
    if args.tipo:
        dat_files_df = dat_files_df[dat_files_df['tipo'] == args.tipo]

    # Filter by year if specified
    if args.ano:
        # Force column 'date' to be int type, if not already
        dat_files_df['ano'] = dat_files_df['ano'].astype(int)
        dat_files_df = dat_files_df[dat_files_df['ano'] == int(args.ano)]

    # Filter by ID if specified
    if args.id:
        dat_files_df = dat_files_df[dat_files_df['id'] == args.id]

    # If scan_ftp is specified, scan the FTP directory for .dat files
    if args.scan_ftp:
        scan_ftp_main(args.ftp_dir)

    # If no files are found after filtering, exit
    if args.quarentena or args.quarentena == []:
        arquivos_quarentena = tratar_quarentena(args.estacao, args.tipo, args.quarentena, 
                                                args.output, args.overwrite, not args.formatar, 
                                                args.quarentena_tratado)
        exit()

    # Exibe os dados filtrados se não for solicitado formatação
    if not args.formatar and not args.gerar_base:
        # Limita o número de caracteres da coluna 'caminho' para exibição
        df_to_show = dat_files_df[['id', 'estacao', 'tipo',  'ano', 'is_historico', 'caminho']].copy()
        max_caminho_len = 120
        df_to_show['caminho'] = df_to_show['caminho'].apply(lambda x: x[:max_caminho_len] + '...' if len(x) > max_caminho_len else x)
        with pd.option_context('display.max_rows', None, 
              'display.max_columns', None, 
              'display.width', 2000):
            print(df_to_show.to_string(index=False))
        exit()

    # Configura o logger
    logger = setup_logger()
    # Carrega os cabeçalhos e os sensores a partir de arquivos JSON.
    headers, header_sensor = carregaCabecalhos()

    # Chama funcao gerar base
    if args.gerar_base:
        # Chama funcao gerar base
        gerarBase(args.output, args.tipo, headers, args.overwrite)
        exit()
    
    # Pegue os dados que serão processados
    dat_files_to_process = dat_files_df['caminho'].tolist()
    # Pegue os tipos de dados que serão processados
    dat_files_types = dat_files_df['tipo'].tolist()
    # Pega todas as estações que serão processadas
    dat_files_stations = dat_files_df['estacao'].tolist()

    # Monta barra de progresso
    pbar = tqdm.tqdm(total=len(dat_files_to_process), desc="Processando arquivos:", unit="file")
    summary_results = []
    # Process files
    if args.parallel:
        # Use multiprocessing to process files in parallel
        with Pool(6) as pool:
            
            for result in pool.imap(processarArquivo, 
                                    zip(dat_files_to_process, 
                                        dat_files_stations,
                                        dat_files_types, 
                                        [args.output] * len(dat_files_to_process), 
                                        [args.overwrite] * len(dat_files_to_process),
                                        [logger] * len(dat_files_to_process),
                                        [headers] * len(dat_files_to_process),
                                        [header_sensor] * len(dat_files_to_process))):
                summary_results.append(result)
                pbar.update()
        pool.close()
        pool.join()
    else:
        # Process files sequentially
        for file_path, file_type, estacao in zip(dat_files_to_process,
                                        dat_files_types,
                                        dat_files_stations):
            result = processarArquivo((file_path, estacao,
                        file_type, args.output,
                        args.overwrite, logger, headers, header_sensor))
            pbar.update()
            summary_results.append(result)
    pbar.close()

    # Concatenar todos os resultados em um único DataFrame
    summary_df = pd.concat(summary_results, ignore_index=True)

    # Cria diretorio de quarentena caso não exista, replace sonda-formatados por quarentena
    quarentena_dir = args.output.replace('sonda-formatados', 'sonda-quarentena')
    # Cria diretorio caso não exista
    pathlib.Path(quarentena_dir).mkdir(parents=True, exist_ok=True)
    quarentena_file = os.path.join(quarentena_dir, 'quarentena.csv')
    # Verifica se o arquivo ainda não existe
    if not os.path.exists(quarentena_file):
        # Cria o arquivo de quarentena
        summary_df.to_csv(quarentena_file, index=False)
        quarentena_df = summary_df
    else:
        # Se arquivo já existe então lê o arquivo
        quarentena_df = pd.read_csv(quarentena_file)

    # Filtrar apenas arquivos com status diferente de 'quarentena'
    novos_arquivos = summary_df[summary_df['status'] != 'quarentena']

    # Adicionar à quarentena apenas se não existirem pelo path
    if not novos_arquivos.empty:
        # Criar lista de paths que já existem na quarentena
        paths_na_quarentena = set(quarentena_df['path'].tolist()) if not quarentena_df.empty else set()
        
        # Filtrar apenas arquivos que não existem na quarentena
        arquivos_para_adicionar = novos_arquivos[~novos_arquivos['path'].isin(paths_na_quarentena)]
        
        if not arquivos_para_adicionar.empty:
            # Concatenar os novos arquivos com o DataFrame de quarentena existente
            quarentena_df = pd.concat([quarentena_df, arquivos_para_adicionar], ignore_index=True)
            
            # Atualizar o arquivo de quarentena
            quarentena_df.to_csv(quarentena_file, index=False)
            print(f"Adicionados {len(arquivos_para_adicionar)} arquivos à quarentena.")
        else:
            print("Nenhum novo arquivo foi adicionado à quarentena.")








