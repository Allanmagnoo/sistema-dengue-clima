# 🦟 Sistema Dengue-Clima: Data Lakehouse Epidemiológico

> **Status:** Em Desenvolvimento (Fase de Implementação na AWS) 🚧
> **Stack:** Python, Airflow (Astronomer), Spark, AWS (S3, Glue, Athena), Docker, PostgreSQL.

## 1. Visão do Projeto (Business Case)

O **Sistema Dengue-Clima** é uma plataforma de Engenharia de Dados projetada para correlacionar dados epidemiológicos (Dengue, Zika) com dados climáticos (Chuva, Temperatura). O objetivo é fornecer uma base de dados analítica (Gold Layer) para prever surtos de arboviroses baseados em padrões meteorológicos, utilizando uma arquitetura de Lakehouse na AWS.

**Fontes de Dados:**

1. **InfoDengue API** (Dados epidemiológicos semanais).
    * Histórico: Últimos 10 anos.
    * Granularidade: Município/Semana.
2. **INMET API** (Dados meteorológicos horários/diários).
    * Variáveis: Temperatura, Precipitação, Umidade.
    * Histórico: Alinhado com dados epidemiológicos.
3. **IBGE API** (Dados Demográficos e Geográficos).
    * População Anual (para cálculo de incidência).
    * Malha Geográfica (Lat/Long).

---

## 2. Escopo e Objetivos

O escopo do projeto é construir um pipeline de dados ponta a ponta, desde a ingestão de dados brutos até a camada de agregação, pronta para consumo por ferramentas de Business Intelligence e Machine Learning.

**Objetivos:**
* **Ingestão Automatizada:** Coletar dados de forma programada e confiável.
* **Arquitetura Lakehouse:** Implementar as camadas Bronze, Silver e Gold em um Data Lake na AWS e localmente.
* **Qualidade de Dados:** Garantir que os dados sejam limpos, consistentes e prontos para análise.
* **Escalabilidade:** Construir uma solução que suporte o crescimento do volume de dados.
* **Análise de Dados:** Permitir a correlação entre dados de dengue e clima para gerar insights.

---

## 3. Arquitetura Medallion (Bronze, Silver, Gold)

### 📊 **Bronze Layer** (Dados Brutos)

- **Formato:** CSV (InfoDengue)
* **Estrutura:** Dados organizados por `disease`, `year`, e `geocode`
* **Volume:** ~40.000 arquivos CSV (2015-2025)
* **Scripts:**
  * Ingestão via APIs (DAGs Airflow)

### 🔄 **Silver Layer** (Dados Limpos e Normalizados)

- **Formato:** Parquet particionado
* **Datasets:**
  * `silver_dengue` - Dados epidemiológicos processados
  * `silver_inmet` - Dados climáticos do INMET
  * `silver_mapping_estacao_geocode` - Mapeamento estação meteorológica → município
* **Scripts:**
  * `transform_silver_dengue.py`
  * `transform_silver_inmet.py`
  * `create_mapping_estacao_geocode.py`

### 🏆 **Gold Layer** (Dados Analíticos - OBT)

- **Formato:** Parquet particionado por UF
* **Dataset:** `gold_dengue_clima`
* **Características:**
  * Join de dados de Dengue + Clima (INMET)
  * Inclui lags de temperatura e precipitação (1-4 semanas)
  * Pronto para análise e ML
* **Scripts:**
  * `create_gold_dengue_clima.py`

---

## 4. Scripts de ETL (src/jobs/)

### **Transformação de Dados:**

| Script | Descrição |
|--------|-----------|
| `transform_bronze_rapids.py` | Transformação Bronze usando RAPIDS (GPU) |
| `transform_silver_dengue.py` | Cria a camada Silver de dados de Dengue |
| `transform_silver_inmet.py` | Cria a camada Silver de dados do INMET |
| `create_mapping_estacao_geocode.py` | Mapeia estações meteorológicas para municípios |
| `create_gold_dengue_clima.py` | Cria a camada Gold com join Dengue + Clima |

### **Ingestão e Exportação:**

| Script | Descrição |
|--------|-----------|
| `bd.py` | Carrega dados (Bronze/Silver/Gold) para PostgreSQL |
| `upload_to_s3.py` | Upload de dados para AWS S3 |
| `run_silver_transformations.py` | Orquestrador de transformações Silver |

### **Utilitários:**

| Script | Descrição |
|--------|-----------|
| `renaming_utils.py` | Funções para renomear arquivos Parquet |

---

## 5. Banco de Dados PostgreSQL

### **Configuração:**

O projeto suporta ingestão de dados para PostgreSQL local via arquivo `.env`:

```env
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=123456
DB_NAME=postgres
DB_PORT=5432
```

### **Tabelas Criadas:**

- `bronze_infodengue` - Dados brutos do InfoDengue
* `silver_dengue` - Dados processados de Dengue
* `silver_silver_inmet` - Dados climáticos
* `gold_gold_dengue_clima` - Tabela Gold (OBT)
* `ingest_log_parquet` - Log de ingestão incremental

### **Execução da Carga:**

```bash
# Carregar todas as camadas
python src/jobs/bd.py --layers bronze,silver,gold

# Carregar apenas Gold
python src/jobs/bd.py --layers gold

# Dry-run (simulação)
python src/jobs/bd.py --dry-run --layers bronze
```

---

## 6. Funcionalidades Implementadas

- ✅ **Orquestração de DAGs com Airflow:** Pipelines de ingestão e processamento de dados.
* ✅ **Arquitetura Medallion Local:** Estrutura de dados em camadas (Bronze, Silver, Gold) implementada localmente.
* ✅ **Ingestão de Dados:** Conectores para as APIs do InfoDengue e INMET.
* ✅ **Processamento de Dados:** Scripts para limpeza, transformação e enriquecimento dos dados.
* ✅ **Containerização:** Ambiente de desenvolvimento local com Docker e Astro CLI.
* ✅ **Integração com PostgreSQL:** Ingestão incremental de dados em banco relacional.
* ✅ **Suporte a GPU (RAPIDS):** Transformação acelerada de dados Bronze.

---

## 7. Próximas Etapas e Tarefas Pendentes

- **Migração para AWS:**
  * ✅ Upload para Amazon S3 (script `upload_to_s3.py` implementado)
  * ⏳ Adaptar os pipelines de dados para usar AWS Glue para ETL
  * ⏳ Utilizar o Amazon Athena para consultas ad-hoc na camada Gold
* **Melhorias nos Conectores:**
  * ⏳ Implementar lógica de retentativas (retry) e tratamento de erros nos conectores de API
* **Monitoramento e Alertas:**
  * ⏳ Configurar alertas para falhas nos pipelines de dados
* **Documentação:**
  * ⏳ Detalhar o dicionário de dados da camada Gold

---

## 8. Requisitos do Sistema e Dependências

- **Desenvolvimento Local:**
  * Docker Desktop
  * Astro CLI
  * Python 3.9+
  * PostgreSQL 12+
  * (Opcional) GPU NVIDIA com CUDA para RAPIDS
* **Produção (AWS):**
  * Conta na AWS
  * Serviços: S3, Glue, Athena

---

## 9. Instruções de Configuração e Execução

### Ambiente de Desenvolvimento Local

1. **Clone o repositório:**

    ```bash
    git clone https://github.com/Allanmagnoo/sistema-dengue-clima.git
    cd sistema-dengue-clima
    ```

2. **Configure as variáveis de ambiente (.env):**

    ```bash
    cp .env.example .env
    # Edite o arquivo .env com suas credenciais
    ```

3. **Inicie o Ambiente Local com Airflow:**

    ```bash
    astro dev start
    ```

    Acesse a interface do Airflow em: `http://localhost:8080` (usuário: `admin`, senha: `admin`).

4. **Instale as dependências locais para desenvolvimento:**

    ```bash
    python -m venv .venv
    source .venv/bin/activate  # ou .venv\Scripts\activate no Windows
    pip install -r requirements.txt
    ```

5. **Execute as transformações:**

    ```bash
    # Criar camada Silver
    python src/jobs/run_silver_transformations.py
    
    # Criar camada Gold
    python src/jobs/create_gold_dengue_clima.py
    
    # Carregar para PostgreSQL
    python src/jobs/bd.py --layers bronze,silver,gold
    ```

---

## 10. Estrutura do Projeto

```
sistema-dengue-clima/
├── dags/                      # DAGs do Airflow
├── data/                      # Camadas de dados (Medallion)
│   ├── bronze/               # Dados brutos
│   ├── silver/               # Dados limpos
│   └── gold/                 # Dados analíticos
├── src/jobs/                 # Scripts ETL
│   ├── bd.py                 # Carga PostgreSQL
│   ├── create_gold_dengue_clima.py
│   ├── transform_silver_*.py
│   └── upload_to_s3.py
├── docs/                     # Documentação
├── tests/                    # Testes unitários
├── .env                      # Configurações (não versionado)
├── docker-compose.yml        # Configuração Docker
├── requirements.txt          # Dependências Python
└── README.md
```

---

## 11. Equipe e Contatos

- **Desenvolvedor Principal:** Allan Magno
* **Contato:** <allanmagno@gmail.com>
* **GitHub:** [https://github.com/Allanmagnoo](https://github.com/Allanmagnoo)
