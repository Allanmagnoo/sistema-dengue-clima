# 🗺️ Mapa Mental e Visualização da Arquitetura

Este documento fornece representações visuais e tabulares do pipeline ETL para facilitar o entendimento rápido do fluxo de dados.

## 🧠 Mapa Mental da Arquitetura (Mermaid)

```mermaid
mindmap
  root((ETL Dengue Clima))
    Bronze Layer
      InfoDengue
        Format: JSON/CSV
        Part: disease/year
        Raw Data
      INMET
        Format: CSV Latin-1
        Part: year
        Weather Stations
    Silver Layer
      Transforms
        Cleaning
        Schema Enforcement
        Deduplication
      silver_dengue
        Cols: geocode, semana, casos, nivel
        Part: uf/ano_epi
      silver_inmet
        Cols: estacao_id, temp, precip, umid
        Part: uf/ano
        Hourly to Daily
    Gold Layer
      gold_dengue_clima
        One Big Table
        Part: uf
      Transforms
        SpatioTemporal Join
        Feature Engineering
        Climate Lags (1-4 weeks)
      KPIs
        Incidencia
        Alert Level
        Correlation
```

---

## 🌊 Fluxo de Linhagem de Dados (Data Lineage)

Esta tabela consolida o caminho "De-Para" dos principais campos através das camadas.

| Conceito de Negócio | Campo Bronze (Origem) | Campo Silver (Tratado) | Campo Gold (Analítico) | Transformação Chave |
| :--- | :--- | :--- | :--- | :--- |
| **Localização** | `Localidade_id` / Filename | `geocode` (PK), `uf`, `nome_municipio` | `geocode`, `uf`, `nome_municipio` | Regex de arquivo + Join com tabela IBGE (DTB) |
| **Tempo** | `data_iniSE` | `data_inicio_semana`, `semana_epidemiologica` | `data_inicio_semana`, `semana_epidemiologica` | Parse de Data, Extração de Semana (1-53) |
| **Casos Dengue** | `casos` | `casos_notificados` | `casos_notificados` | Conversão String → Int, Tratamento de Nulos |
| **Estimativa** | `casos_est` | `casos_estimados` | `casos_estimados` | Mantido precisão Double |
| **Alerta** | `nivel` | `nivel_alerta` | `nivel_alerta` | Categorização (1-4) |
| **População** | `pop` | `populacao` | `populacao` | Cópia |
| **Temperatura** | `column07` (INMET) | `temperatura_c` | `inmet_temp_media` | Replace ',' → '.', Agregação Semanal (AVG) |
| **Chuva** | `column02` (INMET) | `precipitacao_mm` | `inmet_precip_tot` | Replace ',' → '.', Agregação Semanal (SUM) |
| **Histórico (Lag)** | - | - | `inmet_temp_media_lag[1-4]` | Window Function (Olhar 1-4 semanas atrás) |

---

## 🔄 Resumo do Processo ETL

```mermaid
graph LR
    subgraph Bronze [Bronze Layer - S3]
        B1[(InfoDengue)]
        B2[(INMET)]
    end

    subgraph Silver [Silver Layer - Parquet]
        S1[Clean Dengue]
        S2[Clean INMET]
        Map[Mapping Table]
    end

    subgraph Gold [Gold Layer - Parquet]
        G1[OBT Dengue+Clima]
    end

    B1 -->|Job 1: Parsing & Typing| S1
    B2 -->|Job 2: Cleaning & Regex| S2
    
    S1 -->|Job 3: Left Join| G1
    S2 -->|Agg Weekly| JoinNode((Join))
    Map -->|Geocode Match| JoinNode
    JoinNode -->|Lags| G1
    
    style Bronze fill:#cd7f32,stroke:#333,stroke-width:2px
    style Silver fill:#c0c0c0,stroke:#333,stroke-width:2px
    style Gold fill:#ffd700,stroke:#333,stroke-width:2px
```
