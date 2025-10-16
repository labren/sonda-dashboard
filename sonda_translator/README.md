# SONDA Translator

## Visão Geral

O SONDA Translator é uma ferramenta especializada para formatação, padronização e qualificação de dados brutos provenientes da rede SONDA (Sistema de Organização Nacional de Dados Ambientais). Esta ferramenta converte dados em formatos brutos (`.dat`) para estruturas padronizadas, realiza pré-qualificação dos dados e permite o tratamento de dados classificados em quarentena.

Desenvolvida para atender às necessidades do Laboratório de Modelagem e Estudos de Recursos Renováveis de Energia (LABREN) do Instituto Nacional de Pesquisas Espaciais (INPE), esta aplicação facilita o trabalho científico com os extensos conjuntos de dados coletados pelas estações da rede SONDA em todo o Brasil.

## Funcionalidades Detalhadas

- **Formatação e Padronização de Dados**
  - Conversão de dados brutos (.dat) para formatos padronizados
  - Organização estruturada dos dados para análise científica
  - Normalização de diferentes formatos em uma estrutura consistente

- **Pré-qualificação de Dados**
  - Verificação básica de colunas temporais
  - Identificação de problemas na estrutura temporal dos dados
  - Classificação de arquivos para tratamento posterior

- **Tratamento de Dados em Quarentena**
  - Gerenciamento específico para dados classificados em quarentena
  - Rotinas de análise e recuperação de dados problemáticos
  - Opções para revisão manual ou automática

- **Seleção e Filtragem**
  - Por estação: Processamento seletivo por estação meteorológica
  - Por tipo de dado: Solar (SD), Meteorológico (MD), Vento (WD), Temperatura (TD)
  - Por período: Seleção por ano, mês ou data específica
  - Por histórico: Diferenciação entre dados atuais e históricos

- **Modos de Processamento**
  - Processamento sequencial para volumes menores de dados
  - Processamento paralelo para alto desempenho com grandes conjuntos de dados

- **Gerenciamento de Arquivos**
  - Verificação de arquivos existentes
  - Opção de sobrescrita controlada
  - Especificação de diretórios de saída personalizados

- **Funcionalidades Especiais**
  - Escaneamento automático de diretórios FTP
  - Visualização prévia dos dados selecionados antes do processamento
  - Geração de logs e estatísticas detalhadas

- **Geração de Base de Dados**
  - Consolidação de dados formatados em arquivos Parquet
  - Otimização para análises e consultas de grandes volumes de dados
  - Suporte à compressão e esquemas estruturados para melhor desempenho

## Estrutura do Projeto

```
sdt/
    __main__.py           # Script principal (CLI)
    carregaCabecalhos.py  # Carregamento de cabeçalhos e sensores
    processaDado.py       # Função de processamento dos arquivos
    logger.py             # Configuração de logs
    geraArquivos.py       # Geração e estatísticas dos arquivos
    qualificaDado.py      # Qualificação dos dados
    scan_ftp.py           # Escaneamento de diretórios FTP
    tratar_quarentena.py  # Tratamento de arquivos em quarentena
    json/
        arquivos_ftp.json # Metadados dos arquivos encontrados no FTP
        cabecalhos.json   # Configuração dos possíveis nomes de cabeçalhos
        cabecalhos_sensores.json # Configuração dos sensores das estações
output/
    sonda-formatados/     # Saída padrão dos arquivos processados
```

## Requisitos

- Python 3.8 ou superior
- Bibliotecas: pandas, tqdm, numpy, pyarrow, duckdb, sshfs
- Acesso aos arquivos de dados (local ou via FTP)

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/sonda-translator.git
   cd sonda-translator
   ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. (Opcional) Configure o acesso aos dados via FTP:
   ```bash
   sudo apt-get install sshfs
   mkdir ftp
   sshfs -p 22 usuario@servidor:/mnt/ftp/ ftp/
   ```

## Fluxo de Processamento

1. Primeiro, escaneie o diretório FTP para criar o arquivo de metadados:
   ```bash
   python sdt -scan_ftp -ftp_dir /caminho/para/dados/
   ```

2. Em seguida, você pode processar os arquivos conforme necessário:
   ```bash
   python sdt [opções]
   ```

## Configuração dos Cabeçalhos

O SONDA Translator permite configurar os possíveis nomes de cabeçalhos para os diferentes tipos de estações através do arquivo cabecalhos.json. Este arquivo contém mapeamentos para cada tipo de dados (SD, MD, WD, CD) e suas respectivas variáveis.

Exemplo de configuração para dados solares (SD):
```json
"SD": {
    "timestamp": ["TIMESTAMP", "timestamp"],
    "glo_avg": ["GLO_AVG", "glo_avg"],
    "dir_avg": ["DIR_AVG", "dir_avg"],
    ...
}
```

Para dados meteorológicos (MD):
```json
"MD": {
    "timestamp": ["TIMESTAMP", "timestamp"],
    "tp_sfc": ["TP_SFC", "tp_sfc"],
    "humid_sfc": ["humid", "HUMID_SFC", "humid_sfc"],
    ...
}
```

Cada variável possui uma lista de possíveis nomes de cabeçalho que podem ser encontrados nos arquivos originais. Isso permite que a ferramenta funcione corretamente mesmo quando há variações na nomenclatura dos arquivos de diferentes estações ou períodos.

Você pode adicionar novos nomes alternativos ou novos tipos de dados conforme necessário para sua aplicação específica.

## Uso Básico

Para processar arquivos, execute o comando com as opções desejadas:

```bash
python sdt [opções]
```

### Opções de Linha de Comando

| Opção | Argumento | Descrição |
|-------|-----------|-----------|
| `-estacao` | [nomes] | Nome(s) da(s) estação(ões) a processar (ex: `-estacao brb cai`) |
| `-historico` | True/False | Seleciona dados históricos ou atuais |
| `-ano` | YYYY[-MM[-DD]] | Ano, mês ou dia dos dados (ex: `2020`, `2020-05`, `2020-05-01`) |
| `-tipo` | SD/MD/WD/TD/INDEFINIDO | Tipo de dado (Solar, Meteorológico, Vento, Temperatura) |
| `-parallel` | - | Processa arquivos em paralelo |
| `-id` | número | Processa arquivo específico por ID |
| `-output` | caminho | Caminho de saída dos arquivos processados |
| `-formatar` | - | Ativa a formatação para os arquivos de saída |
| `-overwrite` | - | Sobrescreve arquivos existentes |
| `-ftp_dir` | caminho | Diretório base dos arquivos a serem processados |
| `-scan_ftp` | - | Escaneia o diretório FTP para encontrar arquivos .dat |
| `-quarentena` | [ids] | Trata arquivos em quarentena pelos seus IDs |
| `-gerar_base` | - | Gera base de dados consolidada em formato Parquet a partir dos arquivos formatados |

## Casos de Uso

### 1. Visualização de Arquivos Disponíveis

Para listar os arquivos disponíveis sem processá-los:

```bash
# Listar todos os arquivos disponíveis
python sdt

# Selecionar por estação e tipo
python sdt -estacao brb -tipo SD
```

### 2. Formatação e Padronização Básica

```bash
# Formatar e padronizar dados de uma estação específica
python sdt -estacao brb -formatar

# Formatar dados de múltiplas estações de um tipo específico
python sdt -estacao brb cai pet -tipo SD -formatar
```

### 3. Processamento por Período

```bash
# Processar dados de um ano específico
python sdt -estacao brb -ano 2020 -formatar

# Processar dados de um mês específico
python sdt -estacao brb -ano 2020-06 -formatar

# Processar dados de um dia específico
python sdt -estacao brb -ano 2020-06-15 -formatar
```

### 4. Processamento de Grande Volume

```bash
# Processar grande quantidade de dados em paralelo
python sdt -estacao brb cai pet -tipo SD -ano 2020 -parallel -formatar
```

### 5. Tratamento de Quarentena

```bash
# Tratar todos os arquivos em quarentena
python sdt -quarentena -overwrite

# Tratar arquivos específicos em quarentena
python sdt -quarentena 123 456 789 -overwrite

# Tratar arquivos em quarentena de uma estação específica
python sdt -estacao brb -quarentena -overwrite
```

### 6. Controle de Saída

```bash
# Especificar diretório de saída personalizado
python sdt -estacao brb -tipo SD -output /caminho/personalizado/ -formatar

# Sobrescrever arquivos existentes
python sdt -estacao brb -tipo SD -overwrite -formatar
```

### 7. Geração de Base de Dados Consolidada

```bash
# Gerar base de dados para todos os arquivos formatados
python sdt -gerar_base

# Gerar base de dados para um tipo específico de dados
python sdt -tipo SD -gerar_base

# Gerar base de dados sobrescrevendo arquivo existente
python sdt -gerar_base -overwrite
```

## Saída e Resultados

### Estrutura dos Arquivos Formatados

Os arquivos processados seguem uma estrutura padronizada:

```
[estação]_[tipo]_[data]_[formato].csv
```

Exemplo: `BRB_SD_20200615_formatado.csv`

### Logs 

Os logs gerados durante o processamento documentam detalhadamente qualquer erro ou problema encontrado durante a formatação dos arquivos. Estes logs são fundamentais para diagnóstico e incluem:

- Erros de leitura de arquivos
- Problemas de formatação de dados
- Problemas com colunas temporais
- Arquivos enviados para quarentena
- Estatísticas de processamento e tempo de execução

## Troubleshooting

### Problemas Comuns

1. **Arquivos não encontrados**
   - Verifique se o caminho FTP está corretamente montado
   - Execute o escaneamento FTP: `python sdt -scan_ftp`

2. **Erros na formatação**
   - Arquivos do tipo `INDEFINIDO` podem gerar erros caso não haja cabeçalho correspondente
   - Verifique a integridade dos arquivos de dados brutos
   - Consulte os logs para informações detalhadas

3. **Desempenho lento**
   - Para grandes volumes de dados, utilize a opção `-parallel`
   - Considere processar por lotes usando filtros de data ou estação

---

**Desenvolvido por:**  
Helvecio Neto / Equipe SONDA / Labren / INPE

Para mais informações ou suporte:
- Email: helvecioblneto@gmail.com
- GitHub: [https://github.com/labren/sonda-translator](https://github.com/labren/sonda-translator)