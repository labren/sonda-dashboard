import pandas as pd
from pathlib import Path
import os
import pathlib
import glob
from sonda_translator.sdt.carregaCabecalhos import carregaCabecalhos
from sonda_translator.sdt.testeTemporal import testeTemporal

def tratar_quarentena(estacao, tipo, quarentena_id, output, overwrite=False, exibir=False, tratado=False):

    # Remove 'sonda-formatados/' que existe no output
    output = output.replace('sonda-formatados/', '')
    # Verifica se arquivo de quarentena existe
    quarentena_file = Path(output) / 'sonda-quarentena' / 'quarentena.csv'
    if not quarentena_file.exists():
        print(f"Arquivo de quarentena não encontrado: {quarentena_file}")
        return
    
    # Lê o arquivo de quarentena
    quarentena_df_main = pd.read_csv(quarentena_file)

    if len(estacao) > 0:
        try:
            estacao = [i.upper() for i in estacao]
            quarentena_df_main = quarentena_df_main[quarentena_df_main['estacao'].isin(estacao)]
        except Exception as e:
            print(f"Erro ao filtrar por estacao: {e}")
            return
    
    if tipo:
        # Tenta fazer filtro por tipo
        try:
            quarentena_df_main = quarentena_df_main[quarentena_df_main['tipo'] == tipo]
        except Exception as e:
            print(f"Erro ao filtrar por tipo: {e}")
            return
    
    if tratado:
        # Filtra apenas os arquivos tratados
        quarentena_df_main = quarentena_df_main[quarentena_df_main['status'] != 'quarentena']

    try:
        quarentena_id = [int(i) for i in quarentena_id]
    except Exception:
        pass

    # Filtra pelo ID de quarentena
    if len(quarentena_id) > 0:
        quarentena_df = quarentena_df_main[quarentena_df_main['qid'].isin(quarentena_id)]
    else:
        quarentena_df = quarentena_df_main.copy()
        
    max_len = 45
    quarentena_display = quarentena_df.copy()
    quarentena_display['path'] = quarentena_display['path'].apply(lambda x: '...'+ x[-max_len:] if len(x) > max_len else x)
    print(f"Lista de arquivos de quarentena para a estação")
    print(quarentena_display[['qid', 'estacao', 'tipo', 'status','data_tratamento','problema','path']].to_string(index=False))
    print('-'*50)
    print('')
    input("Pressione Enter para continuar ou Ctrl+C para cancelar...")
    
    # Carrega os cabeçalhos e os sensores a partir de arquivos JSON.
    _, header_sensor = carregaCabecalhos()

    # Loop por arquivos de quarentena
    for index, row in quarentena_df.iterrows():
        # Pega o caminho do arquivo
        caminho = row['path']
        # Verifica se o arquivo existe
        if not os.path.exists(caminho):
            print(f"Arquivo não encontrado: {caminho}")
            continue
        print('-'*50)
        # Abrindo o arquivo de quarentena
        df_q_orig = pd.read_csv(caminho)
        # Seta colunas de timestamp
        df_q_orig['timestamp'] = pd.to_datetime(df_q_orig['timestamp'], errors='coerce')
        # Cria uma copia do arquivo de quarentena para não alterar o original
        df_q = df_q_orig.copy() 
        # Encontra o ano mais comum no arquivo de quarentena baseado no timestamp
        common_year = str(df_q['timestamp'].dt.year.mode()[0])
        # Encontra o mes mais comum no arquivo de quarentena baseado no timestamp com zfill
        common_month = str(df_q['timestamp'].dt.month.mode()[0]).zfill(2)
        # Seta arquivo de quarentena apenas para o ano mais comum e mes mais comum
        df_q = df_q[(df_q['timestamp'].dt.year == int(common_year)) & (df_q['timestamp'].dt.month == int(common_month))]
        # Montar nome do arquivo de referencia
        arqv_ref = output + 'sonda-formatados/' + row['estacao'].upper() + '/' + row['tipo_completo'] + '/'
        arqv_ref += common_year + '/' + row['estacao'].upper() + '_' + common_year + '_' + common_month + '_' + row['tipo'] + '_formatado.csv'
        print(f"Tratando arquivo com qid: \t{row['qid']} \tda estacao {row['estacao'].upper()} e tipo {tipo}")
        print(f"O status atual do arquivo é: \t{row['status']}")
        print(f"O tipo de problema do arquivo é: \t{row['problema']}")
        print(f"O arquivo de quarentena é: \t{row['path']}...")
        print(f"O arquivo a ser alterado é: \t\t{arqv_ref}")
        resposta = input(f"Deseja continuar o tratamento? (s/n) [s]: ") or 's'
        if resposta.lower() != 's':
            print("Arquivo ignorado.")
            print('-'*50)
            continue

        # Inicia testes temporais do arquivo de quarentena
        if row['tipo'] == 'MD':
            expected_interval = pd.Timedelta(minutes=10)
        elif row['tipo'] == 'SD':
            expected_interval = pd.Timedelta(minutes=1)
        elif row['tipo'] == 'WD':
            expected_interval = pd.Timedelta(minutes=10)
        else:
            print(f"Tipo de dado inválido: {row['tipo']}.")
            print('-'*50)
            continue
        expected_rows = int(pd.Timedelta("1 day") / expected_interval)
        expected_last_time = (pd.Timestamp("00:00:00") + (expected_rows - 1) * expected_interval).time()
        # Verifica se ainda existem problemas no arquivo de quarentena
        problema = testeTemporal(df_q, expected_rows, expected_interval, expected_last_time)
        if len(problema) > 0:
            print(f"\n[ATENÇÃO!] - Arquivo de quarentena {caminho} \n\t\tainda possui problemas: {problema}")
            # Pergunta se deseja continuar
            resposta = input(f"\nDeseja continuar mesmo assim? (s/n) [s]: ") or 's'
            if resposta.lower() != 's':
                print("Arquivo ignorado.")
                print('-'*50)
                continue

        # Cria diretorio de dados tratados caso não exista, ele deve ser o mesmo do arquivo de quarentena mas com 'sonda-tratadps/' ao invés de 'sonda-quarentena/'
        diretorio_tratados = os.path.dirname(caminho).replace('sonda-quarentena', 'sonda-tratados')
        # Cria diretorio caso não exista
        pathlib.Path(diretorio_tratados).mkdir(parents=True, exist_ok=True)

        # Verifica se o arquivo de referencia existe
        if not os.path.exists(arqv_ref):
            print(f"Arquivo de referencia não encontrado: {arqv_ref}")
            print(f"Arquivo de quarentena: {caminho}")
            # perguntar se deseja criar o arquivo de referencia
            resposta = input(f"Arquivo de referencia não encontrado: {arqv_ref}. Deseja criar o arquivo de referencia? (s/n): ")
            if resposta.lower() == 's':
                # Pega primeiro dia do arquivo de quarentena e ultimo dia do arquivo de quarentena
                primeiro_dia = df_q.index.min().date()
                ultimo_dia = df_q.index.max().date()
                # Faz um teste para ver quantos dias ainda faltam para preencher o arquivo de referencia
                dias_faltando = (ultimo_dia - primeiro_dia).days
                print(f"Arquivo de referencia {arqv_ref} possui dados de {primeiro_dia} a {ultimo_dia} e faltam {dias_faltando} dias para completar o arquivo.")

                # Garante que o arquivo df_ref tem os timestamps em ordem
                df_q.sort_index(inplace=True)

                # Pega subcabeçalho do arquivo de quarentena
                sub_header = header_sensor[estacao.lower()][tipo]
                sub_header = ['', '', '', '', ''] + sub_header
                df_q.columns = pd.MultiIndex.from_tuples(list(zip(df_q.columns,sub_header)))
                # Cria diretorio caso não exista
                pathlib.Path(arqv_ref).parent.mkdir(parents=True, exist_ok=True)
                # Salva o arquivo de referencia
                df_q.to_csv(arqv_ref, index=False)
                print(f"Arquivo de referencia criado: {arqv_ref}")
                # Atualiza o arquivo de quarentena com o status 'tratado'
                quarentena_df_main.loc[quarentena_df_main['qid'] == row['qid'], 'status'] = 'tratado'
                # quarentena_df_main o arquivo de quarentena com a data de tratamento
                quarentena_df_main.loc[quarentena_df_main['qid'] == row['qid'], 'data_tratamento'] = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S')
                # Salva o arquivo de quarentena atualizado
                quarentena_df_main.to_csv(quarentena_file, index=False)
                # Mover arquivo de quarentena para o diretorio de tratados
                novo_caminho = os.path.join(diretorio_tratados, os.path.basename(caminho))
                # Mover arquivo de quarentena para o diretorio de tratados
                os.rename(caminho, novo_caminho)
                print(f"Arquivo de quarentena movido para {novo_caminho}")
                print(f"Novo arquivo formatado criado: {arqv_ref}")
                print('-'*50)
            else:
                continue
            
        # Lê o arquivo de referencia, primeira linha é o cabeçalho segunda linha é o subcabeçalho
        df_ref = pd.read_csv(arqv_ref, header=[0, 1])
        # Separa subcabeçalho em uma variável e remove do DataFrame
        sub_header = df_ref.columns.get_level_values(1)
        # Remove qualquer 'Unnamed' que possa existir no sub_header e deixa vazio
        sub_header = ['' if 'Unnamed' in i else i for i in sub_header]
        df_ref.columns = df_ref.columns.get_level_values(0)
        # Salva posicao das colunas
        pos_col = df_ref.columns.tolist()

        # Seta colunas de timestamp
        df_ref['timestamp'] = pd.to_datetime(df_ref['timestamp'], errors='coerce')

        # Verifica se o cabeçalho do arquivo de quarentena é igual ao do arquivo de referencia
        if not df_q.columns.equals(df_ref.columns):
            print(f"Arquivo de quarentena {caminho} não possui o mesmo cabeçalho que o arquivo de referencia {arqv_ref}")
            continue

        # Verifica se o arquivo de quarentena tem os mesmos dados que o arquivo de referencia
        if df_q.equals(df_ref):
            print(f"Arquivo de quarentena {caminho} é igual ao arquivo de referencia {arqv_ref}")
            continue

        # Encontra os dados que estão no arquivo de quarentena e não estão no arquivo de referencia
        df_q = df_q[~df_q['timestamp'].isin(df_ref['timestamp'])]

        # Seta colunas de timestamp como index para comparação
        df_q.set_index('timestamp', inplace=True)
        df_ref.set_index('timestamp', inplace=True)

        # Adiciona os dados do arquivo de quarentena no arquivo de referencia baseado no timestamp
        df_ref = df_ref.combine_first(df_q)

        # Garante que o arquivo df_ref tem os timestamps em ordem
        df_ref.sort_index(inplace=True)

        # Pega primeiro dia do arquivo de quarentena e ultimo dia do arquivo de quarentena
        primeiro_dia = df_ref.index.min().date()
        ultimo_dia = df_ref.index.max().date()
        # Faz um teste para ver quantos dias ainda faltam para preencher o arquivo de referencia
        dias_faltando = (ultimo_dia - primeiro_dia).days
        print(f"Arquivo de referencia {arqv_ref} possui dados de {primeiro_dia} a {ultimo_dia} e faltam {dias_faltando} dias para completar o arquivo.")

        # Reseta o index, retorna posicao das colunas e retorna o subcabeçalho 
        df_ref.reset_index(inplace=True)
        df_ref = df_ref[pos_col]
        # Verifica se existem linhas duplicadas e deixa apenas a ultima
        df_ref = df_ref[~df_ref.duplicated(subset=['timestamp'], keep='last')]

        # Adiciona o subcabeçalho de volta
        df_ref.columns = pd.MultiIndex.from_tuples(list(zip(df_ref.columns, sub_header)))

        # Atualiza o arquivo de referencia com os dados do arquivo de quarentena
        df_ref.to_csv(arqv_ref, index=False)
        

        # Mover arquivo de quarentena para o diretorio de tratados
        novo_caminho = os.path.join(diretorio_tratados, os.path.basename(caminho))
        # Cria diretorio caso não exista
        pathlib.Path(diretorio_tratados).mkdir(parents=True, exist_ok=True)
        # Mover arquivo de quarentena para o diretorio de tratados
        os.rename(caminho, novo_caminho)
        print(f"Arquivo de quarentena movido para {novo_caminho}")

        print(f"Arquivo de referencia {arqv_ref} atualizado com sucesso!")

        # Atualiza o arquivo de quarentena com o status 'tratado'
        quarentena_df_main.loc[quarentena_df_main['qid'] == row['qid'], 'status'] = 'tratado'
        # Atualiza o arquivo de quarentena com a data de tratamento
        quarentena_df_main.loc[quarentena_df_main['qid'] == row['qid'], 'data_tratamento'] = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S')
        # Salva o arquivo de quarentena atualizado
        quarentena_df_main.to_csv(quarentena_file, index=False)
        print('-'*50)
        print('\n')

    # Verifica se ainda existem arquivos de quarentena usando glob
    arquivos_quarentena = glob.glob(os.path.join(output, 'sonda-quarentena', '**', '*.csv'), recursive=True)
    # Exceto arquivo de quarentena principal
    arquivos_quarentena = [i for i in arquivos_quarentena if 'quarentena.csv' not in i]
    if len(arquivos_quarentena) == 0:
        print("Todos os arquivos de quarentena foram tratados.")
    else:
        print(f"Ainda existem {len(arquivos_quarentena)} arquivos de quarentena.")
    print("Tratamento de quarentena finalizado.")

    


    

