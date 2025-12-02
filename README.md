# 🦟 Sistema Dengue-Clima: Data Lakehouse Epidemiológico

> **Status:** Em Desenvolvimento (Fase de Implementação na AWS) 🚧
> **Stack:** Python, Airflow (Astronomer), Spark, AWS (S3, Glue, Athena), Docker.

## 1. Visão do Projeto (Business Case)
O **Sistema Dengue-Clima** é uma plataforma de Engenharia de Dados projetada para correlacionar dados epidemiológicos (Dengue, Zika) com dados climáticos (Chuva, Temperatura). O objetivo é fornecer uma base de dados analítica (Gold Layer) para prever surtos de arboviroses baseados em padrões meteorológicos, utilizando uma arquitetura de Lakehouse na AWS.

**Fontes de Dados:**
1.  **InfoDengue API** (Dados epidemiológicos semanais).
    *   Histórico: Últimos 10 anos.
    *   Granularidade: Município/Semana.
2.  **INMET API** (Dados meteorológicos horários/diários).
    *   Variáveis: Temperatura, Precipitação, Umidade.
    *   Histórico: Alinhado com dados epidemiológicos.
3.  **IBGE API** (Dados Demográficos e Geográficos).
    *   População Anual (para cálculo de incidência).
    *   Malha Geográfica (Lat/Long).

---

## 2. Escopo e Objetivos
O escopo do projeto é construir um pipeline de dados ponta a ponta, desde a ingestão de dados brutos até a camada de agregação, pronta para consumo por ferramentas de Business Intelligence e Machine Learning.

**Objetivos:**
- **Ingestão Automatizada:** Coletar dados de forma programada e confiável.
- **Arquitetura Lakehouse:** Implementar as camadas Bronze, Silver e Gold em um Data Lake na AWS.
- **Qualidade de Dados:** Garantir que os dados sejam limpos, consistentes e prontos para análise.
- **Escalabilidade:** Construir uma solução que suporte o crescimento do volume de dados.
- **Análise de Dados:** Permitir a correlação entre dados de dengue e clima para gerar insights.

---

## 3. Funcionalidades Implementadas
- **Orquestração de DAGs com Airflow:** Pipelines de ingestão e processamento de dados.
- **Arquitetura Medallion Local:** Estrutura de dados em camadas (Bronze, Silver, Gold) simulada localmente.
- **Ingestão de Dados:** Conectores para as APIs do InfoDengue e INMET.
- **Processamento de Dados:** Scripts para limpeza, transformação e enriquecimento dos dados.
- **Containerização:** Ambiente de desenvolvimento local com Docker e Astro CLI.

---

## 4. Próximas Etapas e Tarefas Pendentes
- **Migração para AWS:**
    - Configurar o armazenamento de dados no Amazon S3 para as camadas do Lakehouse.
    - Adaptar os pipelines de dados para usar AWS Glue para ETL.
    - Utilizar o Amazon Athena para consultas ad-hoc na camada Gold.
- **Melhorias nos Conectores:**
    - Implementar lógica de retentativas (retry) e tratamento de erros nos conectores de API.
- **Monitoramento e Alertas:**
    - Configurar alertas para falhas nos pipelines de dados.
- **Documentação:**
    - Detalhar o dicionário de dados da camada Gold.

---

## 5. Requisitos do Sistema e Dependências
- **Desenvolvimento Local:**
    - Docker Desktop
    - Astro CLI
    - Python 3.9+
- **Produção (AWS):**
    - Conta na AWS
    - Serviços: S3, Glue, Athena

---

## 6. Instruções de Configuração e Execução

### Ambiente de Desenvolvimento Local

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/Allanmagnoo/sistema-dengue-clima.git
    cd sistema-dengue-clima
    ```

2.  **Inicie o Ambiente Local com Airflow:**
    ```bash
    astro dev start
    ```
    Acesse a interface do Airflow em: `http://localhost:8080` (usuário: `admin`, senha: `admin`).

3.  **Instale as dependências locais para desenvolvimento:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # ou .venv\Scripts\activate no Windows
    pip install -r requirements.txt
    ```

---

## 7. Equipe e Contatos
- **Desenvolvedor Principal:** Allan Magno
- **Contato:** allanmagno@gmail.com
- **GitHub:** [https://github.com/Allanmagnoo](https://github.com/Allanmagnoo)
