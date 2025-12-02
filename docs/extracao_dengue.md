# Documentação do Processo de Extração de Dados de Dengue

Este documento detalha o pipeline de extração, transformação e validação de dados de dengue, desde a fonte de dados bruta (Bronze) até a camada de dados pronta para análise (Silver).

## 1. Visão Geral do Fluxo

O processo é orquestrado em duas camadas principais:

-   **Camada Bronze**: Armazena os dados brutos, exatamente como foram extraídos da fonte, sem transformações.
-   **Camada Silver**: Contém dados limpos, validados, enriquecidos e estruturados, prontos para consumo por modelos de machine learning ou análises.

## 2. Definição das Fontes de Dados

-   **Fonte Principal**: [InfoDengue](https://info.dengue.mat.br/)
-   **API Endpoint**: `https://info.dengue.mat.br/api/alertcity`
-   **Formato dos Dados**: A extração é feita em formato `JSON`, que é então salvo como `CSV` na camada Bronze.

## 3. Configuração do Ambiente e Extração (Bronze)

-   **Ferramentas**: Python, com as bibliotecas `requests` e `pandas`.
-   **Conector**: O script `src/connectors/infodengue_api.py` é responsável por se conectar à API do InfoDengue.
    -   **Resiliência**: Utiliza uma sessão `requests` com `Retry` para lidar com falhas de rede intermitentes e erros `429 (Too Many Requests)`.
    -   **Pool de Conexões**: Configurado para otimizar o uso de conexões HTTP.
-   **Armazenamento (Bronze)**:
    -   **Local**: `data/bronze/infodengue/municipios/`
    -   **Formato**: `CSV`
    -   **Particionamento**: Os dados são particionados por `disease` (doença) e `year` (ano) para facilitar o acesso e a organização. Ex: `disease=dengue/year=2024/3304557.csv`.

## 4. Transformação e Validação (Silver)

O script `src/jobs/transform_silver_dengue.py` executa a transformação da camada Bronze para a Silver.

-   **Ferramenta de Transformação**: DuckDB é usado para ler, transformar e validar os dados de forma eficiente e em memória.

### Etapas da Transformação:

1.  **Leitura dos Dados Bronze**: O DuckDB lê todos os arquivos CSV particionados da camada Bronze de forma recursiva.
2.  **Enriquecimento com Metadados**:
    -   Os dados são cruzados com um arquivo de metadados de municípios (`DTB`) para adicionar informações como **UF** e **nome do município**.
3.  **Padronização do Esquema**:
    -   As colunas são renomeadas para um padrão claro e consistente.
    -   Tipos de dados são convertidos para os formatos corretos (ex: `DATE`, `INTEGER`, `DOUBLE`).
    -   Valores nulos ou vazios são tratados (`NULLIF`).
4.  **Adição de Metadados de Proveniência**:
    -   **`fonte_dados`**: Uma coluna é adicionada para registrar a origem dos dados (ex: 'InfoDengue').
    -   **`data_extracao`**: Um timestamp é adicionado para registrar quando a transformação foi executada.

### Validação e Qualidade dos Dados:

Antes de salvar os dados na camada Silver, uma série de **asserções de qualidade de dados** são executadas para garantir a integridade:

-   Verifica se a tabela não está vazia.
-   Garante que não há geocódigos (`geocode`) nulos.
-   Garante que não há datas de início de semana (`dat-inicio_semana`) nulas.
-   Valida o formato e a presença dos códigos de UF.
-   Assegura que o número de casos notificados não seja negativo.
-   Verifica a lógica de particionamento por ano.

Se qualquer uma dessas validações falhar, o processo é interrompido para evitar a propagação de dados de baixa qualidade.

## 5. Saída e Armazenamento (Silver)

-   **Local**: `data/silver/infodengue/`
-   **Formato**: `Parquet`
    -   **Compressão**: `SNAPPY` para um bom equilíbrio entre tamanho e velocidade de leitura.
-   **Particionamento**:
    -   Os dados na camada Silver são particionados por `uf` e `ano_epidemiologico`. Isso otimiza significativamente as consultas para análises focadas em estados ou anos específicos.

## 6. Como Atualizar e Manter

-   **Extração Histórica**: A DAG `01_ingest_dengue_historical` pode ser executada para (re)processar dados de anos anteriores para um conjunto de municípios.
-   **Extração Semanal**: A DAG `01_ingest_dengue_weekly` (se ativada) pode ser agendada para buscar novos dados semanalmente.
-   **Transformação**: A DAG `03_transform_pipeline` executa o script `transform_silver_dengue.py` para processar quaisquer novos dados na camada Bronze e movê-los para a Silver.
-   **Monitoramento**: Os logs gerados durante a execução fornecem visibilidade sobre o processo, incluindo o número de registros processados e o resultado das validações de qualidade.
