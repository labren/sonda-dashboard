import pandas as pd
import pathlib
from sonda_translator.sdt.testeTemporal import testeTemporal

def prequalificarDado(df, tipo_dado, logger, estacao, output_dir, tipo_completo):

    """
    Função para qualificar os dados de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame de dado formatado.
        tipo_dado (str): Tipo do dado.
        logger (logging.Logger): Logger para registrar erros.
    Returns:
        pd.DataFrame: DataFrame qualificado.
    """
    #### Configurações de qualificação ####

    # - intervalo: Intervalo esperado entre registros (default: 1 minuto)
    #   - tolerancia: Tolerância percentual para a comparação do intervalo esperado (default: 10%)
    #   - mad_factor: Fator multiplicativo para o limiar baseado no MAD (default: 3)

    if tipo_dado == 'MD':
        expected_interval = pd.Timedelta(minutes=10)
    elif tipo_dado == 'SD':
        expected_interval = pd.Timedelta(minutes=1)
    elif tipo_dado == 'WD':
        expected_interval = pd.Timedelta(minutes=10)
    else:
        logger.error(f"Error 7 - Error no prequalificaDado: \
            Tipo de dado inválido: {tipo_dado}.")
        return None, None
        
    # Listas para armazenar as datas consideradas boas ou problemáticas
    good_data = []
    good_indexes = []
    
    # # Número esperado de linhas por dia: 24 horas * 60 minutos = 1440
    expected_rows = int(pd.Timedelta("1 day") / expected_interval)
    expected_last_time = (pd.Timestamp("00:00:00") + (expected_rows - 1) * expected_interval).time()

    # Encontra todas as linhas que comecam com a hora 00:00:00 e salva os indices
    zero_hour_rows = df[df['timestamp'].dt.time == pd.Timestamp("00:00:00").time()].index.tolist()

    # Cria um DataFrame vazio para o sumário com as colunas necessárias
    summary_df = pd.DataFrame(columns=['qid', 'estacao', 'tipo', 'tipo_completo' ,
                                       'data_tratamento', 'status', 'problema', 'data_detecao' , 'path'])

    # Baseado nos indices de horas 00:00:00, separa os dados por data
    # Faça um loop que vai pegar os indices + expected_rows
    for i in zero_hour_rows:
        # Separa a linha atual + expected_rows
        group = df.iloc[i:i + expected_rows].copy()

        indexes = group.index.tolist()
        
        # Testes temporais
        problema = testeTemporal(group, expected_rows, expected_interval, expected_last_time)
                         
        # Se não houver problemas, adiciona a data na lista de dados bons
        if len(problema) == 0:
            good_data.extend(indexes)
            good_indexes.extend(indexes)
            continue

        # Pega as possiveis datas problemáticas
        problematic_dates = group['timestamp'].dt.date.unique()
        # Convert para datetime
        problematic_dates = pd.to_datetime(problematic_dates)
        # Cria string das datas problemáticas fazendo join
        problematic_dates = "_".join([d.strftime('%Y-%m-%d') for d in problematic_dates])
        # Monta o caminho do arquivo de problemas usando problematic_dates
        problem_file = pathlib.Path(output_dir).parent / 'sonda-quarentena' / estacao.upper() / tipo_completo / f"{estacao.upper()}_{tipo_dado}_{problematic_dates}_problemas.csv"
        # Verifica se o arquivo já existe e se as datas problemáticas já estão no arquivo
        if problem_file.exists():
            # Lê o arquivo de problemas
            problem_df = pd.read_csv(problem_file)
            # Se não estiver, adiciona apenas as novas linhas
            # Cria um DataFrame com as novas linhas
            new_rows = group[~group['timestamp'].isin(problem_df['timestamp'])]
            # Adiciona as novas linhas ao arquivo de problemas
            new_rows.to_csv(problem_file, mode='a', header=False, index=False)
        else:
            # Se o arquivo não existir, cria o diretório e salva o arquivo
            problem_file.parent.mkdir(parents=True, exist_ok=True)
            group.to_csv(problem_file, index=False)

        # Gera o qid a partir do tempo atual
        qid = pd.Timestamp.now().strftime('%Y%m%d%H%M%S%f')[-10:]
        # Concatena os dados problemáticos com o sumário
        problem_summary = pd.DataFrame({
            'qid': [qid],
            'estacao': [estacao.upper()],
            'tipo': [tipo_dado],
            'tipo_completo': [tipo_completo],
            'problema': [problema],
            'data_detecao' : [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')],
            'path': [problem_file],
            'data_tratamento': [pd.NaT],
            'status': ['quarentena']
        })

        # Adiciona os dados problemáticos ao sumário
        summary_df = pd.concat([summary_df, problem_summary], ignore_index=True)

    # Pega apenas os dados bons
    good_data = df[df.index.isin(good_indexes)].copy()

    return good_data, summary_df
 

