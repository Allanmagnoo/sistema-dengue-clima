# 🦟 Sistema Dengue-Clima: Data Lakehouse Epidemiológico

> **Status:** Em Migração para Google Cloud Platform (GCP) �
> **Stack:** Python, Dataform (SQLX), BigQuery, Cloud Composer (Airflow), Vertex AI, Looker Studio.

## 1. Visão do Projeto (Business Case)

O **Sistema Dengue-Clima** é uma plataforma de Engenharia de Dados projetada para correlacionar dados epidemiológicos (Dengue, Zika) com dados climáticos (Chuva, Temperatura). O objetivo é fornecer uma base de dados analítica (Gold Layer) para prever surtos de arboviroses baseados em padrões meteorológicos, utilizando uma arquitetura moderna na nuvem.

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
* **Arquitetura Lakehouse:** Implementar as camadas Bronze, Silver e Gold no BigQuery.
* **Qualidade de Dados:** Garantir que os dados sejam limpos, consistentes e prontos para análise usando Dataform Assertions.
* **Escalabilidade:** Utilizar serviços serverless do GCP para suportar grandes volumes de dados.
* **Análise de Dados:** Permitir a correlação entre dados de dengue e clima para gerar insights via Looker Studio.

---

## 3. Arquitetura (GCP)

A arquitetura do projeto foi migrada da AWS para o Google Cloud Platform para aproveitar recursos nativos de Big Data e ML.

### 📊 **Bronze Layer** (Raw Data)
* **Armazenamento:** Google Cloud Storage (GCS) / BigQuery (External Tables)
* **Formato:** JSON/CSV originais
* **Processo:** Ingestão via Cloud Functions ou Cloud Composer (Airflow)

### 🔄 **Silver Layer** (Refined Data)
* **Armazenamento:** BigQuery (Native Tables)
* **Ferramenta de Transformação:** Dataform (SQLX)
* **Processos:**
    * Limpeza de dados
    * Padronização de tipos
    * Deduplicação
    * Enriquecimento com dados geográficos

### 🏆 **Gold Layer** (Analytics Ready)
* **Armazenamento:** BigQuery
* **Ferramenta de Transformação:** Dataform (SQLX)
* **Modelos:**
    * Marts dimensionais (Star Schema)
    * Tabelas agregadas para dashboards
* **ML Integration:** Vertex AI / BigQuery ML para previsões de surtos

### 📈 **Visualização**
* **Ferramenta:** Looker Studio
* **Conexão:** Direta com BigQuery

---

## 4. Justificativa da Migração (AWS → GCP)

A infraestrutura foi migrada da AWS para o GCP visando otimização de custos e integração facilitada de ferramentas de dados.

* **Integração Nativa:** O uso do **Dataform** integrado ao BigQuery simplifica drasticamente a gestão de dependências e transformações SQL, substituindo scripts complexos em Python/Glue.
* **Serverless First:** O BigQuery oferece uma capacidade de processamento serverless que elimina a necessidade de gerenciamento de clusters (como no EMR/Glue), reduzindo o overhead operacional.
* **Machine Learning:** A integração direta do BigQuery com o **Vertex AI** facilita a criação e deploy de modelos preditivos sem movimentação excessiva de dados.
* **Histórico:** A versão anterior do projeto utilizava AWS S3, Glue e Athena. Essa experiência serviu de base para a modelagem atual, mas a stack GCP provou-se mais ágil para este caso de uso específico.

---

## 5. Requisitos de Configuração (GCP)

Para executar este projeto no ambiente GCP, são necessários:

1. **Conta Google Cloud:**
   * Projeto ativo com billing habilitado.
   * APIs habilitadas: BigQuery API, Dataform API, Cloud Storage API, Vertex AI API.

2. **Ferramentas Locais:**
   * [Google Cloud SDK (gcloud)](https://cloud.google.com/sdk/docs/install)
   * [Dataform CLI](https://cloud.google.com/dataform/docs/use-dataform-cli) (opcional, para dev local)
   * Python 3.9+

3. **Permissões (IAM):**
   * O usuário ou Service Account deve ter permissões de `BigQuery Data Editor`, `BigQuery Job User` e `Dataform Editor`.

---

## 6. Guia de Implantação

### Configuração Inicial

1. **Autenticação:**
   ```bash
   gcloud auth application-default login
   gcloud config set project SEU_PROJETO_GCP
   ```

2. **Setup do Dataform:**
   * Navegue até a pasta `etl_project`.
   * Configure o arquivo `dataform.json` com o ID do seu projeto GCP.
   * Instale as dependências:
     ```bash
     npm install
     ```

3. **Execução do Pipeline (Manual):**
   ```bash
   dataform run
   ```

### Deploy Automático

O deploy contínuo é gerenciado via repositório conectado ao Dataform no console do GCP.

1. Conecte o repositório Git ao Dataform no Console GCP.
2. Crie um "Release Configuration" apontando para a branch `main`.
3. Crie um "Workflow Configuration" para agendar as execuções (ex: Diário às 06:00 UTC).

---

## 7. Estrutura do Projeto (Atualizada)

```
sistema-dengue-clima/
├── etl_project/                 # Projeto Dataform (Novo Core ETL)
│   ├── 01-bronze/              # Declarações de fontes
│   ├── 02-silver/              # Transformações intermediárias
│   ├── 03-gold/                # Modelos finais
│   ├── assertions/             # Testes de qualidade de dados
│   ├── dataform.json           # Configuração do Dataform
│   └── package.json            # Dependências JS
├── src/                        # Scripts Python (Legado/Auxiliar)
│   ├── jobs/                   # Antigos scripts ETL (Referência)
│   └── app/                    # Aplicação Streamlit
├── docs/                       # Documentação
└── README.md                   # Este arquivo
```

---

## 8. Equipe e Contatos

* **Desenvolvedor Principal:** Allan Magno
* **Contato:** <allanmagno@gmail.com>
* **GitHub:** [https://github.com/Allanmagnoo](https://github.com/Allanmagnoo)
* **Suporte GCP:** Para questões relacionadas à infraestrutura GCP, abra uma issue neste repositório com a tag `gcp-infra`.
