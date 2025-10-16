import os
import glob
import duckdb
import pandas as pd
import pathlib


def gerarBase(output_dir, tipo, cabecalhos, overwrite=False):
    # Altera sonda-formatados/ para sonda-banco-dados/
    output_base = output_dir.replace('sonda-formatados/', 'sonda-banco-dados/')
    tipo_completo = tipo
    # Verifica se o tipo é Meteorologico, Solarimetrico ou Anemometrico
    tipo_map = {'MD': 'Meteorologica.parquet', 
                'SD': 'Solarimetrica.parquet',
                 'WD': 'Anemometrica.parquet'}
    if tipo not in tipo_map:
        raise ValueError(f"Tipo inválido: {tipo}. Deve ser MD, SD ou WD.")
    tipo_completo = tipo_map[tipo]
    output_base = os.path.join(output_base)
    pathlib.Path(output_base).mkdir(parents=True, exist_ok=True)

    # Conectar ao banco de dados DuckDB
    pasta_dbs = os.path.join(output_base, 'dbs/')
    pathlib.Path(os.path.dirname(pasta_dbs)).mkdir(parents=True, exist_ok=True)
    arquivo_db = os.path.join(pasta_dbs, tipo_completo.replace('.parquet', '.db'))
    global con
    con = duckdb.connect(database=arquivo_db)

    print(f"Criando base de dados em {output_base}...")
    variaveis = cabecalhos[tipo].keys()
    nome_base = tipo_completo.split('.')[0].upper()

    # Procurar por arquivos CSV no diretório de saída que contenham o tipo
    arquivos = glob.glob(os.path.join(output_dir, '**', f'*{tipo}*.csv'), recursive=True)
    if len(arquivos) == 0:
        raise ValueError(f"Nenhum arquivo encontrado para o tipo {tipo} no diretório {output_dir}.")
    # Pega primeiro arquivo pra servir de base
    arquivo = arquivos[0]
    # Criar base
    criar_base(arquivo, nome_base, variaveis)
    print(50 * '-')
    print('')

    # Popular a base com os dados
    processar_arquivos(arquivos, nome_base, sobreescrever=overwrite)

    # Verificar se as tabelas têm dados antes de salvar
    counter = con.execute(f"SELECT COUNT(*) FROM {nome_base}").fetchone()[0]

    # Salvar a base de dados meteorológicos em um arquivo parquet
    if counter > 0:
        # Obter nomes e tipos das colunas
        schema = con.execute(f"DESCRIBE SELECT * FROM {nome_base}").fetchdf()
        colunas = schema['column_name'].tolist()

        # Valores a substituir por NULL
        valores_a_nular = [3333.0, -5555.0]
        valores_str = ', '.join(str(v) for v in valores_a_nular)

        # Expressões: manter as 4 primeiras colunas como estão, aplicar lógica nas restantes
        exprs = []

        for i, col in enumerate(colunas):
            if i < 4:
                # Primeiras 4 colunas: mantém sem alteração
                exprs.append(col)
            else:
                # A partir da 5ª coluna: cast para DOUBLE + nulos para valores indesejados
                exprs.append(f"""
                CASE 
                    WHEN try_cast({col} AS DOUBLE) IN ({valores_str}) THEN NULL
                    ELSE try_cast({col} AS DOUBLE)
                END AS {col}
                """)

        # Executa a query
        con.execute(f"""
            CREATE OR REPLACE TABLE {nome_base} AS
            SELECT {', '.join(exprs)}
            FROM {nome_base}
        """)

        # Salvar a tabela em um arquivo parquet sem duplicatas
        print(f"Salvando tabela em arquivo parquet: {output_base + tipo_completo}")
        con.execute(f"""
            COPY (
                SELECT DISTINCT *
                FROM {nome_base}
            )
            TO '{output_base + tipo_completo}' (FORMAT 'parquet');
        """)

        
    else:
        print(f"A tabela {nome_base} está vazia. Nenhum arquivo parquet foi criado.")
    
    con.close()
    print(f"Base de dados criada e salva em {output_base}")
    # Remover arquivos temporários
    os.system('rm -rf .tmp')


# Função para criar a tabela no banco de dados e inicializa todas as variáveis vazias
def criar_base(arquivo, nome_base, variaveis):
    # Verifica se o arquivo existe, se sim carrega o arquivo
    if os.path.exists(arquivo):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {nome_base} AS SELECT * FROM read_csv_auto('{arquivo}')
        """)
        print(f"Tabela {nome_base} criada com sucesso a partir do arquivo {arquivo}.")
    else:
        # Criar a string com os nomes das colunas e os tipos corretos
        columns_def = ', '.join(
            f"{col} VARCHAR" if col == "acronym" else
            f"{col} TIMESTAMP" if col == "timestamp" else
            f"{col} INT" if col in ["year", "day", "min"] else
            f"{col} FLOAT" 
            for col in variaveis        )
        
        # Criar a tabela
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {nome_base} ({columns_def})
        """)
    

# Função para processar os arquivos em paralelo
def processar_arquivos(arquivos, base, sobreescrever):
    
    # Cria uma lista de argumentos para cada arquivo
    args = [(arquivo, base, sobreescrever) for arquivo in arquivos]
    
    # Processa os arquivos em serie
    for arg in args:
        inserir_dados(arg)
        print('' * 50)
    
    # Verificar quantos registros foram inseridos
    count = con.execute(f"SELECT COUNT(*) FROM {base}").fetchone()[0]
    print(f"\nTotal de registros na base {base} após processamento: {count}")

# Função para popular a tabela com os dados, nessa função será possível fazer o append dos dados, atualizar os dados variavel a variavel
def inserir_dados(args):
    """
    Função para popular a base de dados com os dados de um arquivo CSV.
    
    Parâmetros:
    - args: tupla contendo (arquivo, base, sobreescrever)
      - arquivo: caminho para o arquivo CSV
      - base: nome da tabela no banco de dados
      - sobreescrever: se True, sobrescreve os dados existentes
    """
    arquivo, base, sobreescrever = args
    
    print(f"Processando arquivo {arquivo} para a base {base}...")
    
    # Limpar tabelas temporárias
    try:
        con.execute("DROP TABLE IF EXISTS temp_new_data")
        con.execute("DROP VIEW IF EXISTS new_data_view")
    except Exception:
        pass
    
    try:
        # 1. Leitura dos dados
        try:
            new_data = pd.read_csv(arquivo, skiprows=[1])
            if new_data.empty:
                print(f" - Arquivo {arquivo} não contém dados.")
                return
        except Exception as e:
            print(f" - Erro ao ler o arquivo {arquivo}: {str(e)}")
            return
            
        # 2. Preparação das colunas
        colunas_meta = ['acronym', 'timestamp', 'year', 'day', 'min']
        variaveis = [col for col in new_data.columns if col not in colunas_meta]
        
        if 'acronym' not in new_data.columns or new_data['acronym'].isna().all():
            print(f" - Erro: Arquivo {arquivo} não possui informação de estação (acronym)")
            return
            
        # 3. Limpeza e preparação dos dados
        # Substituir vírgulas por pontos e tratar valores inválidos
        for var in variaveis:
            if var in new_data.columns:
                new_data[var] = new_data[var].astype(str).str.replace(',', '.').replace({r'\d+-': '0'}, regex=True)
        
        # Converter para numérico e timestamp
        try:
            for var in variaveis:
                if var in new_data.columns:
                    new_data[var] = pd.to_numeric(new_data[var], errors='coerce')
                    
            new_data['timestamp'] = pd.to_datetime(new_data['timestamp'], errors='coerce')
            new_data = new_data.dropna(subset=['timestamp'])
            
            if new_data.empty:
                print(f" - Erro: Todos os timestamps são inválidos no arquivo {arquivo}")
                return
        except Exception as e:
            print(f" - Erro ao converter dados: {str(e)}")
            return
        
        # 4. Informações básicas
        estacao = new_data['acronym'].iloc[0]
        total_registros = len(new_data)
        tempo_min = new_data['timestamp'].min()
        tempo_max = new_data['timestamp'].max()
        
        # 5. Verificar colunas da tabela
        try:
            table_columns = con.execute(f"DESCRIBE {base}").fetchall()
            table_columns = [col[0].lower() for col in table_columns]
            columns_to_insert = [col for col in new_data.columns if col.lower() in table_columns]
            new_data = new_data[columns_to_insert]
            columns_str = ', '.join(columns_to_insert)
        except Exception as e:
            print(f" - Erro ao verificar colunas da tabela: {str(e)}")
            return
        
        # 6. Registrar DataFrame para operações SQL
        con.register('temp_df', new_data)
        con.execute("CREATE OR REPLACE VIEW new_data_view AS SELECT * FROM temp_df")
        
        # 7. Verificar registros duplicados
        duplicates = con.execute(f"""
            SELECT COUNT(*) FROM {base} b
            WHERE EXISTS (
                SELECT 1 FROM new_data_view n
                WHERE b.acronym = n.acronym AND b.timestamp = n.timestamp
            )
        """).fetchone()[0]
        
        # 8. Processar conforme a flag sobreescrever
        if duplicates > 0:
            if sobreescrever:
                # Remover registros existentes
                deleted = con.execute(f"""
                    DELETE FROM {base} 
                    WHERE acronym = '{estacao}' 
                    AND timestamp BETWEEN '{tempo_min}' AND '{tempo_max}'
                """).fetchone()[0]
                
                # Inserir novos dados
                con.execute(f"""
                    INSERT INTO {base} ({columns_str})
                    SELECT {columns_str} FROM new_data_view
                """)
                
                print(f" - Sobreescritos {deleted} registros existentes para estação {estacao}")
            else:
                # Verificar se todos já existem
                if duplicates == total_registros:
                    print(f" - Todos os {total_registros} registros já existem para estação {estacao}. Use a opção de sobreescrever (--overwrite) para atualizar.")
                    return
                
                # Inserir apenas os que não existem
                inserted = con.execute(f"""
                    INSERT INTO {base} ({columns_str})
                    SELECT n.* FROM new_data_view n
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {base} b
                        WHERE b.acronym = n.acronym AND b.timestamp = n.timestamp
                    )
                """).fetchone()[0]
                
                print(f" - {duplicates} registros já existem e foram ignorados")
                print(f" - Inseridos {inserted} novos registros para estação {estacao}")
        else:
            # Nenhum registro duplicado, inserir todos
            inserted = con.execute(f"""
                INSERT INTO {base} ({columns_str})
                SELECT {columns_str} FROM new_data_view
            """).fetchone()[0]
            
            print(f" - Inseridos {inserted} novos registros para estação {estacao}")
        
        # Limpar recursos temporários
        con.execute("DROP TABLE IF EXISTS temp_new_data")
        con.execute("DROP VIEW IF EXISTS new_data_view")
        
    except Exception as e:
        print(f" - Erro ao processar arquivo {arquivo}: {str(e)}")
        # Garantir limpeza
        try:
            con.execute("DROP TABLE IF EXISTS temp_new_data")
            con.execute("DROP VIEW IF EXISTS new_data_view")
        except:
            pass
