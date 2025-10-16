import os
import json
import time
import re
from tqdm import tqdm
import threading
import concurrent.futures
import collections

# Adicionar variável global para contagem
contador_global = 1
contador_lock = threading.Lock()

# Pré-compilar expressões regulares para maior eficiência
PADRAO_ANO_CAMINHO = re.compile(r'/(\d{4})(?:/|$)')
PADRAO_ANO_ARQUIVO_1 = re.compile(r'_(\d{4})_')
PADRAO_ANO_ARQUIVO_2 = re.compile(r'(\d{2})(\d{2})')

def determinar_tipo(nome_arquivo):
    """Determina o tipo de dado baseado no padrão do nome do arquivo"""
    nome_arquivo = nome_arquivo.upper()
    
    if '_AMB.DAT' in nome_arquivo or '_MD.DAT' in nome_arquivo:
        return 'MD'
    elif '_RAD_01.DAT' in nome_arquivo or '_SD.DAT' in nome_arquivo:
        return 'SD'
    elif '_TD.DAT' in nome_arquivo:
        return 'TD'
    elif '_ANE.DAT' in nome_arquivo or '_10.DAT' in nome_arquivo or '_25.DAT' in nome_arquivo or '_50.DAT' in nome_arquivo:
        return 'WD'
    else:
        return 'INDEFINIDO'

def extrair_ano(caminho_completo, nome_arquivo):
    """Extrai o ano do caminho ou do nome do arquivo"""
    # Procurar por um ano de 4 dígitos no caminho (formato mais flexível)
    match = PADRAO_ANO_CAMINHO.search(caminho_completo)
    if match:
        ano = match.group(1)
        # Verificar se é um ano válido (entre 1900 e o ano atual)
        if 1900 <= int(ano) <= time.localtime().tm_year:
            return int(ano)
    
    # Procurar no nome do arquivo por padrões como CPA_2020_092_a_121.dat
    match = PADRAO_ANO_ARQUIVO_1.search(nome_arquivo)
    if match:
        return int(match.group(1))
    
    # Se não encontrou com o padrão anterior, procurar no nome do arquivo
    match = PADRAO_ANO_ARQUIVO_2.search(nome_arquivo)
    if match:
        possivel_ano_2_digitos = match.group(1)
        # Converter ano de 2 dígitos para 4 dígitos (20XX ou 19XX)
        ano_completo = ""
        if int(possivel_ano_2_digitos) <= time.localtime().tm_year % 100:
            ano_completo = "20" + possivel_ano_2_digitos
        else:
            ano_completo = "19" + possivel_ano_2_digitos
        return int(ano_completo)
    
    return 1900  # Retornar um ano padrão se não encontrou nada

def processar_arquivo(args):
    """Processa um único arquivo e retorna suas informações categorizadas"""
    global contador_global
    
    caminho_completo = args
    arquivo = os.path.basename(caminho_completo)
    
    # Otimização: apenas dividir o caminho uma vez e armazenar o resultado
    partes_caminho = caminho_completo.split('/')
    
    # Extrair estação (diretório após 'coleta')
    try:
        indice_coleta = partes_caminho.index('coleta')
        if indice_coleta + 1 < len(partes_caminho):
            estacao = partes_caminho[indice_coleta + 1]
        else:
            estacao = "desconhecido"
    except ValueError:
        estacao = "desconhecido"
    
    # Verificar se é histórico (contém 'historico' ou 'coleta_manual' no caminho)
    caminho_lower = caminho_completo.lower()
    is_historico = 'historico' in caminho_lower or 'coleta_manual' in caminho_lower
    
    # Determinar tipo com base nos padrões especificados
    tipo = determinar_tipo(arquivo)
    
    # Extrair ano do caminho ou do nome do arquivo
    date = extrair_ano(caminho_completo, arquivo)
    
    # Gerar ID único de forma thread-safe
    with contador_lock:
        contador_global += 1
        id_unico = contador_global
    
    # Retornar informações do arquivo
    return {
        "id": id_unico,
        "caminho": caminho_completo,
        "estacao": estacao,
        "is_historico": is_historico,
        "tipo": tipo,
        "date": date
    }

def buscar_arquivos_estacao(args):
    """Busca arquivos em uma estação específica"""
    diretorio_base, estacao, extensao, lock = args
    caminhos_arquivos = []
    diretorio_estacao = os.path.join(diretorio_base, estacao)
    
    # Buscar arquivos em toda a árvore dessa estação
    for raiz, _, arquivos in os.walk(diretorio_estacao):
        for arquivo in arquivos:
            if arquivo.lower().endswith(extensao):
                caminhos_arquivos.append(os.path.join(raiz, arquivo))
    
    # Atualizar a barra de progresso de maneira thread-safe
    with lock:
        buscar_arquivos_estacao.pbar.update(1)
        buscar_arquivos_estacao.pbar.set_description(f"Escaneada: {estacao} ({len(caminhos_arquivos)} arquivos)")
    
    return caminhos_arquivos

def encontrar_arquivos_dat(diretorio_base, extensao=".dat"):
    """Encontra todos os arquivos com a extensão especificada em paralelo"""
    print(f"Buscando arquivos {extensao} em {diretorio_base}...")
    
    # Identificar as estações (diretórios logo após diretorio_base)
    try:
        estacoes = [item for item in os.listdir(diretorio_base) 
                  if os.path.isdir(os.path.join(diretorio_base, item))]
        print(f"Encontradas {len(estacoes)} estações para processar")
    except FileNotFoundError:
        print(f"Erro: Diretório base '{diretorio_base}' não encontrado")
        return []
    
    if not estacoes:
        return []
    
    # Lock para atualizações thread-safe da barra de progresso
    lock = threading.Lock()
    
    # Inicializar barra de progresso como atributo estático da função
    buscar_arquivos_estacao.pbar = tqdm(total=len(estacoes), desc="Escaneando estações", unit="estação")
    
    # Processar estações em paralelo
    todos_arquivos = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, len(estacoes))) as executor:
        args_list = [(diretorio_base, estacao, extensao, lock) for estacao in estacoes]
        resultados = list(executor.map(buscar_arquivos_estacao, args_list))
        
        # Combinar resultados
        for arquivos_estacao in resultados:
            todos_arquivos.extend(arquivos_estacao)
    
    # Fechar a barra de progresso
    buscar_arquivos_estacao.pbar.close()
    
    print(f"Encontrados {len(todos_arquivos)} arquivos {extensao} no total")
    return todos_arquivos

def processar_arquivos_estacao(arquivos_estacao, estacao, pbar, resultados_finais, lock):
    """Processa todos os arquivos de uma estação específica"""
    resultados_estacao = []
    for caminho in arquivos_estacao:
        resultado = processar_arquivo(caminho)
        resultados_estacao.append(resultado)
        with lock:
            pbar.update(1)
    
    # Adicionar resultados à lista compartilhada de forma thread-safe
    with lock:
        resultados_finais.extend(resultados_estacao)

def listar_arquivos_dat(diretorio_base="../ftp/restricted/coleta/", extensao=".dat"):
    inicio = time.time()
    print(f"Buscando arquivos .dat em {diretorio_base}...")
    # Agora a busca já é paralela
    caminhos_arquivos = encontrar_arquivos_dat(diretorio_base, extensao)
    
    if not caminhos_arquivos:
        return []
    
    # OTIMIZAÇÃO: Agrupar arquivos por estação antes do processamento
    print("Agrupando arquivos por estação...")
    arquivos_por_estacao = collections.defaultdict(list)
    
    # Identificar estação baseada no caminho
    for caminho in caminhos_arquivos:
        partes = caminho.split('/')
        try:
            idx = partes.index('coleta')
            if idx + 1 < len(partes):
                estacao = partes[idx + 1]
            else:
                estacao = "desconhecido"
        except ValueError:
            estacao = "desconhecido"
        
        arquivos_por_estacao[estacao].append(caminho)
    
    print(f"Arquivos agrupados em {len(arquivos_por_estacao)} estações")
    
    # Ajustar workers com base no número de CPUs disponíveis
    num_workers = os.cpu_count() * 2
    resultados = []
    lock = threading.Lock()
    
    # Usar barra de progresso compartilhada
    with tqdm(total=len(caminhos_arquivos), desc="Processando arquivos", unit="arquivo") as pbar:
        # OTIMIZAÇÃO: Processar cada estação separadamente em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            
            for estacao, arquivos_estacao in arquivos_por_estacao.items():
                # Para estações com muitos arquivos, dividir em múltiplos lotes
                tamanho_lote = 100  # Tamanho de lote fixo para melhor balanceamento
                
                # Se a estação tiver poucos arquivos, processá-los em um único lote
                if len(arquivos_estacao) <= tamanho_lote:
                    future = executor.submit(
                        processar_arquivos_estacao, 
                        arquivos_estacao, 
                        estacao, 
                        pbar, 
                        resultados, 
                        lock
                    )
                    futures.append(future)
                else:
                    # Dividir arquivos de estações grandes em lotes menores
                    for i in range(0, len(arquivos_estacao), tamanho_lote):
                        lote = arquivos_estacao[i:i+tamanho_lote]
                        future = executor.submit(
                            processar_arquivos_estacao, 
                            lote, 
                            f"{estacao} (lote {i//tamanho_lote+1})", 
                            pbar, 
                            resultados, 
                            lock
                        )
                        futures.append(future)
            
            # Aguardar a conclusão de todos os futures
            concurrent.futures.wait(futures)
    
    fim = time.time()
    print(f"Tempo de processamento: {fim - inicio:.2f} segundos")
    
    return resultados

def salvar_json(dados, nome_arquivo="json/arquivos_ftp.json"):
    """Salva os dados em um arquivo JSON"""
    print(f"Salvando {len(dados)} registros em {nome_arquivo}...")
    
    # Garantir que o diretório exista
    os.makedirs(os.path.dirname(nome_arquivo), exist_ok=True)
    
    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=2)
    print(f"✓ Dados salvos com sucesso em {nome_arquivo}")

def salvar_estatisticas(total_arquivos, estacoes, tipos, datas, historicos, tempo_execucao, nome_arquivo="estatisticas.txt"):
    """Salva as estatísticas em um arquivo de texto"""
    print(f"Salvando estatísticas em {nome_arquivo}...")
    
    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        f.write("📊 ESTATÍSTICAS DE ARQUIVOS DAT\n")
        f.write("=" * 40 + "\n\n")
        
        f.write(f"Total de arquivos .dat encontrados: {total_arquivos}\n\n")
        
        f.write("Arquivos por estação:\n")
        for estacao, contagem in sorted(estacoes.items(), key=lambda x: x[1], reverse=True):
            f.write(f"  {estacao}: {contagem}\n")
        
        f.write("\nArquivos por tipo:\n")
        for tipo, contagem in sorted(tipos.items(), key=lambda x: x[1], reverse=True):
            f.write(f"  {tipo}: {contagem}\n")
        
        f.write("\nArquivos por ano:\n")
        for data, contagem in sorted(datas.items(), key=lambda x: (x[0] != 1900, x[0])):
            f.write(f"  {data}: {contagem}\n")
        
        f.write("\nArquivos históricos vs. atuais:\n")
        f.write(f"  Históricos: {historicos['sim']}\n")
        f.write(f"  Atuais: {historicos['não']}\n")
        
        f.write(f"\nTempo total de execução: {tempo_execucao:.2f} segundos\n")
        
        # Adicionar timestamp
        f.write(f"\nRelatório gerado em: {time.strftime('%d/%m/%Y %H:%M:%S')}\n")
    
    print(f"✓ Estatísticas salvas com sucesso em {nome_arquivo}")

def main(diretorio_base="../ftp/restricted/coleta/"):
    try:
        inicio_total = time.time()
        
        # Listar arquivos .dat e categorizar
        arquivos_dat = listar_arquivos_dat(diretorio_base)
        
        # Verificar se encontrou arquivos
        if not arquivos_dat:
            print("Nenhum arquivo .dat encontrado no diretório ftp/restricted/coleta/")
            return
        
        # Salvar no arquivo JSON
        salvar_json(arquivos_dat)
        
        # Exibir estatísticas
        print(f"\n📊 Estatísticas:")
        print(f"Total de arquivos .dat encontrados: {len(arquivos_dat)}")
        
        # Contar por estação, tipo, data e status histórico
        estacoes = {}
        tipos = {}
        datas = {}
        historicos = {"sim": 0, "não": 0}
        
        for arquivo in arquivos_dat:
            estacao = arquivo["estacao"]
            tipo = arquivo["tipo"]
            date = arquivo["date"]
            is_historico = arquivo["is_historico"]
            
            estacoes[estacao] = estacoes.get(estacao, 0) + 1
            tipos[tipo] = tipos.get(tipo, 0) + 1
            datas[date] = datas.get(date, 0) + 1
            
            if is_historico:
                historicos["sim"] += 1
            else:
                historicos["não"] += 1
        
        # Calcular tempo total de execução
        tempo_total = time.time() - inicio_total
        
        # Exibir estatísticas no console
        print("\nArquivos por estação:")
        for estacao, contagem in sorted(estacoes.items(), key=lambda x: x[1], reverse=True):
            print(f"  {estacao}: {contagem}")
        
        print("\nArquivos por tipo:")
        for tipo, contagem in sorted(tipos.items(), key=lambda x: x[1], reverse=True):
            print(f"  {tipo}: {contagem}")
        
        print("\nArquivos por ano:")
        anos_ordenados = sorted(datas.items(), key=lambda item: (item[0] != 1900, item[0]))
        for data, contagem in anos_ordenados:
            print(f"  {data}: {contagem}")
            
        print("\nArquivos históricos vs. atuais:")
        print(f"  Históricos: {historicos['sim']}")
        print(f"  Atuais: {historicos['não']}")
            
        print(f"\n⏱️ Tempo total de execução: {tempo_total:.2f} segundos")
        
        # Salvar estatísticas em arquivo
        salvar_estatisticas(
            total_arquivos=len(arquivos_dat),
            estacoes=estacoes,
            tipos=tipos,
            datas=datas,
            historicos=historicos,
            tempo_execucao=tempo_total
        )
        
    except Exception as e:
        import traceback
        print(f"❌ Erro ao processar arquivos: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main(diretorio_base="../ftp/restricted/coleta/")