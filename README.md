# 🦟 Eco-Sentinel: Data Lakehouse Epidemiológico

> **Status:** Em Desenvolvimento 🚧
> **Stack:** Python, Airflow (Astronomer), Spark, AWS (Simulado Localmente), Docker.

## 1. Visão do Projeto (Business Case)
O **Eco-Sentinel** é uma plataforma de Engenharia de Dados projetada para correlacionar dados epidemiológicos (Dengue, Zika) com dados climáticos (Chuva, Temperatura) em tempo real.
O objetivo é fornecer uma base de dados analítica (Gold Layer) para prever surtos de arboviroses baseados em padrões meteorológicos.

**Fontes de Dados:**
1.  **InfoDengue API** (Dados epidemiológicos semanais).
2.  **INMET API** (Dados meteorológicos horários).

---

## 2. Arquitetura Técnica (Medallion)

O projeto segue a arquitetura Medalhão (Lakehouse):

| Camada | Formato | Descrição |
| :--- | :--- | :--- |
| **Bronze** | JSON (Raw) | Dados brutos extraídos das APIs. Imutáveis. Particionados por `source/year`. |
| **Silver** | Parquet | Dados limpos, tipados, deduplicados e enriquecidos. Schema enforcement aplicado. |
| **Gold** | Parquet | Dados agregados (KPIs). Tabela única (One Big Table) pronta para Dashboards. |

**Infraestrutura:**
* **Orquestração:** Apache Airflow 2.x (via Astro CLI).
* **Containerização:** Docker.
* **Linguagem:** Python 3.9+.

---

## 3. Guia de Configuração (Quick Start)

### Pré-requisitos
* Docker Desktop (Running)
* Astro CLI instalado
* Python 3.9+

### Instalação

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/eco-sentinel.git](https://github.com/seu-usuario/eco-sentinel.git)
    cd eco-sentinel
    ```

2.  **Inicie o Ambiente Local:**
    ```bash
    astro dev start
    ```
    *Acesse o Airflow UI em: `http://localhost:8080` (User: admin / Pass: admin)*

3.  **Instale dependências locais (para desenvolvimento no VS Code):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # ou .venv\Scripts\activate no Windows
    pip install -r requirements.txt
    ```

---

## 4. Roadmap de Execução (Step-by-Step)

### FASE 1: Ingestão (Bronze Layer) 🛠️
- [ ] **Configurar Conectores**: Implementar scripts em `src/connectors/` com retry logic.
    - `infodengue_api.py`: Busca dados por geocode/ano.
    - `inmet_api.py`: Busca dados de estações automáticas.
- [ ] **Criar DAGs de Ingestão**:
    - `dags/ingest_dengue_historical.py`: Backfill de 5 anos.
    - `dags/ingest_daily_weather.py`: Execução diária (D-1).
- [ ] **Validar Bronze**: Verificar se os JSONs estão sendo salvos em `data/bronze/`.

### FASE 2: Refinamento (Silver Layer) 🧹
- [ ] **Processamento Spark/Pandas**:
    - Ler JSONs da Bronze.
    - Tratamento de Tipagem (String -> Date/Float).
    - Limpeza de Outliers (Ex: Temperaturas > 60°C).
- [ ] **Escrita Parquet**: Salvar em `data/silver` particionado por `UF`.

### FASE 3: Agregação (Gold Layer) 📊
- [ ] **Regras de Negócio**:
    - Agregar Clima (Horário) -> Semanal (Média/Máx/Mín).
    - Join `Dengue` + `Clima` via chaves `Geocode` e `Semana Epidemiológica`.
- [ ] **Criação de Features**:
    - Calcular *Lags* (Chuva de 2 semanas atrás).

### FASE 4: Visualização 📈
- [ ] Conectar ferramenta de Data Viz (Streamlit ou Metabase) ao Data Lake.
- [ ] Criar Gráfico de Correlação (Curva de Chuva x Curva de Casos).

---

## 5. Estrutura de Diretórios

```text
eco-sentinel/
├── dags/                  # Pipelines do Airflow
├── data/                  # Data Lake Local (Gitignored)
├── include/               # Arquivos de config auxiliares
├── src/                   # Lógica de Negócio (ETL Core)
│   ├── connectors/        # Scripts de extração
│   └── common/            # Logs e Utilitários
├── tests/                 # Testes Unitários
├── Dockerfile             # Configuração da imagem Astro
└── requirements.txt       # Libs Python do Airflow