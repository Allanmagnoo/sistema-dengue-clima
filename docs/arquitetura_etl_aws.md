# Arquitetura e Semântica de Dados - Sistema Dengue-Clima

Este documento descreve em detalhes a arquitetura de dados, convenções de nomenclatura, mapeamento completo de colunas, tipos de dados e lógica semântica aplicada nas transformações ETL do Data Lakehouse, desde a camada Raw (Bronze) até a camada Analítica (Gold).

> 💡 **Visualização Rápida:** Para um **Mapa Mental** e tabelas de linhagem intuitivas, consulte [visualizacao_arquitetura.md](./visualizacao_arquitetura.md).

---

## 🏗️ 1. Arquitetura Medallion (AWS Lakehouse)

O pipeline segue a arquitetura Medallion, dividida em três camadas lógicas armazenadas no Amazon S3:

| Camada | S3 Bucket Path | Formato | Particionamento | Descrição |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** (Raw) | `s3://.../data/bronze/` | CSV / JSON | `disease=dengue/year=YYYY` (InfoDengue) / `ano=YYYY` (INMET) | Dados brutos, imutáveis, conforme recebidos da fonte. |
| **Silver** (Trusted) | `s3://.../data/silver/` | Parquet (Snappy) | `uf=XX/ano_epidemiologico=YYYY` (Dengue) / `uf=XX/ano=YYYY` (INMET) | Dados limpos, tipados, deduplicados e enriquecidos. |
| **Gold** (Refined) | `s3://.../data/gold/` | Parquet (Snappy) | `uf=XX` | One Big Table (OBT) pronto para BI e ML. |

---

## 📊 2. Wireframe da Arquitetura

![Arquitetura ETL - Medallion](../../../.gemini/antigravity/brain/97641de4-ae31-43b0-8395-eff4c265c995/etl_architecture_wireframe_1765041689151.png)

O diagrama acima ilustra o fluxo completo de dados através das três camadas (Bronze → Silver → Gold), mostrando as transformações de colunas e tipos de dados em cada etapa.

---

## 🔄 3. Transformações Detalhadas por Camada

### 3.1. Bronze ➡️ Silver: InfoDengue (`silver_dengue`)

#### **3.1.1. Origem dos Dados**

* **Fonte:** API InfoDengue (<https://info.dengue.mat.br/>)
* **Formato:** CSV
* **Particionamento Bronze:** `data/bronze/infodengue/municipios/disease=dengue/year=YYYY/`
* **Padrão de Arquivo:** `{geocode}.csv` (ex: `3550308.csv` para São Paulo)

#### **3.1.2. Mapeamento de Colunas Bronze → Silver**

| Coluna Bronze | Tipo Bronze | Coluna Silver | Tipo Silver | Transformação Aplicada |
|:---|:---|:---|:---|:---|
| `data_iniSE` | `VARCHAR` | `data_inicio_semana` | `DATE` | Parse direto via `try_cast()` |
| `SE` | `INTEGER` | `semana_epidemiologica` | `INTEGER` | Extração: `SE % 100` (remove ano do código YYYYWW) |
| `casos` | `VARCHAR` | `casos_notificados` | `INTEGER` | `try_cast(NULLIF(casos, '') as INTEGER)` |
| `casos_est` | `DOUBLE` | `casos_estimados` | `DOUBLE` | Cópia direta |
| `casos_est_min` | `DOUBLE` | `casos_estimados_min` | `DOUBLE` | Cópia direta |
| `casos_est_max` | `DOUBLE` | `casos_estimados_max` | `DOUBLE` | Cópia direta |
| `casconf` | `VARCHAR` | `casos_confirmados` | `INTEGER` | `try_cast(NULLIF(casconf, '') as INTEGER)` |
| `notif_accum_year` | `VARCHAR` | `notificacoes_acumuladas` | `INTEGER` | `try_cast(NULLIF(notif_accum_year, '') as INTEGER)` |
| `p_inc100k` | `DOUBLE` | `incidencia_100k` | `DOUBLE` | Cópia direta |
| `nivel` | `INTEGER` | `nivel_alerta` | `INTEGER` | Cópia direta (1=Verde, 2=Amarelo, 3=Laranja, 4=Vermelho) |
| `receptivo` | `INTEGER` | `condicoes_receptivas` | `INTEGER` | Cópia direta (0/1 flag) |
| `transmissao` | `INTEGER` | `evidencia_transmissao` | `INTEGER` | Cópia direta (0/1 flag) |
| `pop` | `DOUBLE` | `populacao` | `DOUBLE` | Cópia direta |
| `tempmed` | `DOUBLE` | `temperatura_media` | `DOUBLE` | Cópia direta (fonte: InfoDengue) |
| `umidmed` | `DOUBLE` | `umidade_media` | `DOUBLE` | Cópia direta (fonte: InfoDengue) |
| `casprov` | `VARCHAR` | `casos_provaveis` | `INTEGER` | `try_cast(NULLIF(casprov, '') as INTEGER)` |
| `casprov_est` | `DOUBLE` | `casos_provaveis_estimados` | `DOUBLE` | Cópia direta |
| `casprov_est_min` | `DOUBLE` | `casos_provaveis_estimados_min` | `DOUBLE` | Cópia direta |
| `casprov_est_max` | `DOUBLE` | `casos_provaveis_estimados_max` | `DOUBLE` | Cópia direta |
| `Localidade_id` | `INTEGER` | `geocode` | `BIGINT` | `COALESCE(NULLIF(Localidade_id, 0), geocode_from_filename)` |
| *(nome do arquivo)* | - | `geocode` | `BIGINT` | Extraído via regex: `([0-9]{7})` |
| *(derivado)* | - | `uf` | `VARCHAR(2)` | Mapeamento dos 2 primeiros dígitos do geocode |
| *(join DTB)* | - | `nome_municipio` | `VARCHAR` | Left join com tabela DTB via geocode |
| *(partição)* | - | `ano_epidemiologico` | `INTEGER` | Extraído da partição `year=YYYY` |
| *(metadado)* | - | `fonte_dados` | `VARCHAR` | Constante: `'InfoDengue'` |
| *(metadado)* | - | `data_extracao` | `TIMESTAMP` | `now()` no momento da transformação |

#### **3.1.3. Regras de Qualidade de Dados (DQ Assertions)**

1. ✅ **Total de linhas > 0**
2. ✅ **Sem geocodes nulos** (`geocode IS NOT NULL`)
3. ✅ **Sem datas nulas** (`data_inicio_semana IS NOT NULL`)
4. ✅ **Sem UF nulos** (`uf IS NOT NULL`)
5. ✅ **UF com 2 caracteres** (`length(uf) = 2`)
6. ✅ **Casos notificados não-negativos** (`casos_notificados >= 0`)
7. ✅ **Semana epidemiológica válida** (`1 <= semana_epidemiologica <= 53`)
8. ✅ **Ano epidemiológico válido** (`2020 <= ano_epidemiologico <= 2025`)
9. ✅ **Sem duplicatas** (chave: `geocode` + `data_inicio_semana`)

#### **3.1.4. Particionamento Silver**

* **Esquema:** `uf=XX/ano_epidemiologico=YYYY/`
* **Formato:** Parquet com compressão Snappy
* **Exemplo:** `data/silver/silver_dengue/uf=SP/ano_epidemiologico=2024/silver_dengue_001.parquet`

---

### 3.2. Bronze ➡️ Silver: INMET (`silver_inmet`)

#### **3.2.1. Origem dos Dados**

* **Fonte:** INMET (Instituto Nacional de Meteorologia - <https://portal.inmet.gov.br/>)
* **Formato:** CSV (encoding Latin-1, separador `;`)
* **Particionamento Bronze:** `data/bronze/inmet/YYYY/`
* **Padrão de Arquivo:** `INMET_{REGIAO}_{UF}_{ESTACAO_ID}_{NOME_ESTACAO}_..._{ANO}.CSV`
* **Características:**
  * Skip 9 primeiras linhas (header metadata)
  * Sem header de colunas (colunas genéricas: `column00`, `column01`, ...)
  * Dados horários (UTC)

#### **3.2.2. Mapeamento de Colunas Bronze → Silver**

| Coluna Bronze | Índice | Tipo Bronze | Coluna Silver | Tipo Silver | Transformação Aplicada |
|:---|:---|:---|:---|:---|:---|
| `column00` | 0 | `VARCHAR` | `data_medicao` | `DATE` | `try_cast(strptime(column00, '%Y/%m/%d') as DATE)` |

* Cada arquivo contém dados horários de uma estação para um ano completo

#### **3.2.3. Regras de Qualidade de Dados**

1. ✅ **Total de linhas > 0**
2. ✅ **Sem UF nulos** (`uf IS NOT NULL AND uf != ''`)
3. ✅ **Sem datas nulas** (`data_medicao IS NOT NULL`)
4. ✅ **Temperatura válida** (`-10 <= temperatura_c <= 50`)
5. ✅ **Ano válido** (`2000 <= ano <= 2100`)

#### **3.2.4. Particionamento Silver**

* **Esquema:** `uf=XX/ano=YYYY/`
* **Formato:** Parquet com compressão Snappy
* **Exemplo:** `data/silver/silver_inmet/uf=SP/ano=2024/silver_inmet_001.parquet`

---

### 3.3. Silver ➡️ Gold: One Big Table (`gold_dengue_clima`)

#### **3.3.1. Objetivo**

Criar uma tabela analítica única (OBT) que correlaciona dados epidemiológicos (dengue) com dados climáticos (INMET), incluindo features de lag temporal para análise preditiva.

#### **3.3.2. Etapas da Transformação**

##### **Etapa 1: Mapeamento Estação → Geocode**

* **Tabela Intermediária:** `silver_mapping_estacao_geocode.parquet`
* **Lógica:** Para cada estação meteorológica, encontra o município mais próximo usando distância Haversine
* **Colunas:** `estacao_id`, `geocode`, `distancia_km`

##### **Etapa 2: Agregação INMET (Horário → Semanal)**

```sql
-- 2.1 Mapear estações para geocodes
inmet_geocoded = INMET × Mapping ON estacao_id

-- 2.2 Agregar por semana epidemiológica
inmet_weekly = GROUP BY geocode, ano, semana
  - inmet_temp_media = AVG(temperatura_c)
  - inmet_temp_min = MIN(temperatura_c)
  - inmet_temp_max = MAX(temperatura_c)
  - inmet_precip_tot = SUM(precipitacao_mm)
```

##### **Etapa 3: Criação de Lags (Window Functions)**

```sql
inmet_lags = inmet_weekly + LAG(inmet_temp_media, 1..4) OVER (PARTITION BY geocode ORDER BY join_key)
  - inmet_temp_media_lag1, lag2, lag3, lag4
  - inmet_precip_tot_lag1, lag2, lag3, lag4
```

##### **Etapa 4: Join Principal**

```sql
gold_dengue_clima = silver_dengue LEFT JOIN inmet_lags 
  ON geocode AND join_key (YYYYWW)
```

#### **3.3.3. Mapeamento de Colunas Silver → Gold**

**Identificadores e Dimensões:**

| Coluna Gold | Origem | Tipo | Descrição |
|:---|:---|:---|:---|
| `geocode` | `silver_dengue.geocode` | `BIGINT` | Código IBGE do município (PK composta) |
| `nome_municipio` | `silver_dengue.nome_municipio` | `VARCHAR` | Nome oficial do município |
| `uf` | `silver_dengue.uf` | `VARCHAR(2)` | Sigla da Unidade Federativa |
| `data_inicio_semana` | `silver_dengue.data_inicio_semana` | `DATE` | Domingo de início da semana epidemiológica |
| `semana_epidemiologica` | `silver_dengue.semana_epidemiologica` | `INTEGER` | Semana (1-53) |
| `ano_epidemiologico` | `silver_dengue.ano_epidemiologico` | `INTEGER` | Ano epidemiológico |

**Métricas Epidemiológicas:**

| Coluna Gold | Origem | Tipo | Descrição |
|:---|:---|:---|:---|
| `casos_notificados` | `silver_dengue.casos_notificados` | `INTEGER` | Casos notificados ao SINAN |
| `casos_estimados` | `silver_dengue.casos_estimados` | `DOUBLE` | Estimativa corrigida (InfoDengue) |
| `casos_confirmados` | `silver_dengue.casos_confirmados` | `INTEGER` | Casos laboratorialmente confirmados |
| `incidencia_100k` | `silver_dengue.incidencia_100k` | `DOUBLE` | Taxa por 100 mil habitantes |
| `nivel_alerta` | `silver_dengue.nivel_alerta` | `INTEGER` | 1=Verde, 2=Amarelo, 3=Laranja, 4=Vermelho |
| `populacao` | `silver_dengue.populacao` | `DOUBLE` | População do município no ano |

**Clima: Fonte InfoDengue (Comparação):**

| Coluna Gold | Origem | Tipo | Descrição |
|:---|:---|:---|:---|
| `id_temp_media` | `silver_dengue.temperatura_media` | `DOUBLE` | Temperatura média (°C) - InfoDengue |
| `id_umidade_media` | `silver_dengue.umidade_media` | `DOUBLE` | Umidade média (%) - InfoDengue |

**Clima: Fonte INMET (Ground Truth):**

| Coluna Gold | Origem | Tipo | Descrição |
|:---|:---|:---|:---|
| `inmet_temp_media` | `inmet_weekly.inmet_temp_media` | `DOUBLE` | Temperatura média (°C) - INMET |
| `inmet_temp_min` | `inmet_weekly.inmet_temp_min` | `DOUBLE` | Temperatura mínima (°C) - INMET |
| `inmet_temp_max` | `inmet_weekly.inmet_temp_max` | `DOUBLE` | Temperatura máxima (°C) - INMET |
| `inmet_precip_tot` | `inmet_weekly.inmet_precip_tot` | `DOUBLE` | Precipitação total (mm) - INMET |

**Features de Lag Temporal (Temperatura):**

| Coluna Gold | Origem | Tipo | Descrição |
|:---|:---|:---|:---|
| `inmet_temp_media_lag1` | `LAG(inmet_temp_media, 1)` | `DOUBLE` | Temperatura 1 semana atrás |
| `inmet_temp_media_lag2` | `LAG(inmet_temp_media, 2)` | `DOUBLE` | Temperatura 2 semanas atrás |
| `inmet_temp_media_lag3` | `LAG(inmet_temp_media, 3)` | `DOUBLE` | Temperatura 3 semanas atrás |
| `inmet_temp_media_lag4` | `LAG(inmet_temp_media, 4)` | `DOUBLE` | Temperatura 4 semanas atrás |

**Features de Lag Temporal (Precipitação):**

| Coluna Gold | Origem | Tipo | Descrição |
|:---|:---|:---|:---|
| `inmet_precip_tot_lag1` | `LAG(inmet_precip_tot, 1)` | `DOUBLE` | Precipitação 1 semana atrás |
| `inmet_precip_tot_lag2` | `LAG(inmet_precip_tot, 2)` | `DOUBLE` | Precipitação 2 semanas atrás |
| `inmet_precip_tot_lag3` | `LAG(inmet_precip_tot, 3)` | `DOUBLE` | Precipitação 3 semanas atrás |
| `inmet_precip_tot_lag4` | `LAG(inmet_precip_tot, 4)` | `DOUBLE` | Precipitação 4 semanas atrás |

#### **3.3.4. Lógica de Join e Chave Composta**

* **Chave de Join:** `join_key = ano_epidemiologico * 100 + semana_epidemiologica`
  * Exemplo: Ano 2024, Semana 15 → `join_key = 202415`
* **Tipo de Join:** `LEFT JOIN`
  * Base: `silver_dengue` (todos os registros epidemiológicos são preservados)
  * Enriquecimento: `inmet_lags` (adicionado quando disponível)
* **Resultado:** Alguns municípios podem ter `inmet_*` = `NULL` se não houver estação meteorológica próxima

#### **3.3.5. Particionamento Gold**

* **Esquema:** `uf=XX/`
* **Formato:** Parquet com compressão Snappy
* **Exemplo:** `data/gold/gold_dengue_clima/uf=SP/gold_dengue_clima_001.parquet`

---

## 🛠️ 4. Fluxo para AWS Glue

### 4.1. Job 1: `bronze_to_silver_dengue`

**Tecnologia:** DuckDB (local) → AWS Glue (Spark SQL)

```python
# Pseudocódigo AWS Glue
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_db",
    table_name="infodengue_dengue"
)

# Apply Mapping (Tipagem)
mapping = [
    ("data_iniSE", "string", "data_inicio_semana", "date"),
    ("SE", "int", "semana_epidemiologica", "int"),
    ("casos", "string", "casos_notificados", "int"),
    # ... demais colunas conforme tabela 3.1.2
]

transformed = datasource.apply_mapping(mappings=mapping)

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=transformed,
    connection_type="s3",
    connection_options={
        "path": "s3://bucket/data/silver/silver_dengue",
        "partitionKeys": ["uf", "ano_epidemiologico"]
    },
    format="parquet",
    format_options={"compression": "snappy"}
)
```

### 4.2. Job 2: `bronze_to_silver_inmet`

**Tecnologia:** DuckDB (local) → AWS Glue (Spark SQL)

```python
# Leitura com encoding Latin-1 e separador ;
datasource = spark.read.csv(
    "s3://bucket/data/bronze/inmet/",
    sep=";",
    encoding="ISO-8859-1",
    skipRows=9
)

# Transformação (Replace vírgula, regex de filename)
transformed = datasource \
    .withColumn("temperatura_c", regexp_replace("column07", ",", ".").cast("double")) \
    .withColumn("uf", regexp_extract(input_file_name(), "INMET_[A-Z]{1,2}_([A-Z]{2})_", 1))

# Write particionado por UF e Ano
transformed.write.partitionBy("uf", "ano").parquet(
    "s3://bucket/data/silver/silver_inmet",
    mode="overwrite",
    compression="snappy"
)
```

### 4.3. Job 3: `silver_to_gold_join`

**Tecnologia:** DuckDB (local) → AWS Glue (Spark SQL)

```python
# Leitura das camadas Silver
dengue = spark.read.parquet("s3://bucket/data/silver/silver_dengue")
inmet = spark.read.parquet("s3://bucket/data/silver/silver_inmet")
mapping = spark.read.parquet("s3://bucket/data/silver/silver_mapping_estacao_geocode.parquet")

# Agregação INMET Semanal
inmet_weekly = inmet.join(mapping, "estacao_id") \
    .withColumn("join_key", expr("year(data_medicao) * 100 + weekofyear(data_medicao)")) \
    .groupBy("geocode", "join_key").agg(
        avg("temperatura_c").alias("inmet_temp_media"),
        sum("precipitacao_mm").alias("inmet_precip_tot")
    )

# Window Function para Lags
window_spec = Window.partitionBy("geocode").orderBy("join_key")
inmet_lags = inmet_weekly \
    .withColumn("inmet_temp_media_lag1", lag("inmet_temp_media", 1).over(window_spec)) \
    .withColumn("inmet_temp_media_lag2", lag("inmet_temp_media", 2).over(window_spec))
    # ... lag3, lag4

# Join Principal
gold = dengue \
    .withColumn("join_key", expr("ano_epidemiologico * 100 + semana_epidemiologica")) \
    .join(inmet_lags, ["geocode", "join_key"], "left")

# Write Gold
gold.write.partitionBy("uf").parquet(
    "s3://bucket/data/gold/gold_dengue_clima",
    mode="overwrite",
    compression="snappy"
)
```

---

## 📋 5. Resumo de Tipos de Dados por Camada

### Bronze Layer

| Dataset | Colunas Categoria (String) | Colunas Numéricas | Colunas Data/Hora |
|:---|:---|:---|:---|
| **InfoDengue** | `casos`, `casconf`, `casprov`, `notif_accum_year`, `versao_modelo`, `tweet` | `SE`, `casos_est`, `casos_est_min`, `casos_est_max`, `p_rt1`, `p_inc100k`, `Localidade_id`, `nivel`, `id`, `Rt`, `pop`, `tempmin`, `tempmax`, `tempmed`, `umidmax`, `umidmed`, `umidmin`, `receptivo`, `transmissao`, `nivel_inc`, `casprov_est`, `casprov_est_min`, `casprov_est_max` | `data_iniSE` |
| **INMET** | Todas as colunas (`column00`-`column20`) são lidas como `VARCHAR` para performance | - | - |

### Silver Layer

| Dataset | Colunas VARCHAR | Colunas INTEGER | Colunas DOUBLE | Colunas DATE | Colunas BIGINT |
|:---|:---|:---|:---|:---|:---|
| **silver_dengue** | `uf`, `nome_municipio`, `fonte_dados` | `semana_epidemiologica`, `casos_notificados`, `casos_confirmados`, `notificacoes_acumuladas`, `nivel_alerta`, `condicoes_receptivas`, `evidencia_transmissao`, `casos_provaveis`, `ano_epidemiologico` | `casos_estimados`, `casos_estimados_min`, `casos_estimados_max`, `incidencia_100k`, `populacao`, `temperatura_media`, `umidade_media`, `casos_provaveis_estimados`, `casos_provaveis_estimados_min`, `casos_provaveis_estimados_max` | `data_inicio_semana` | `geocode` |
| **silver_inmet** | `uf`, `estacao_id`, `filename` | `hora_utc`, `ano` | `precipitacao_mm`, `temperatura_c`, `umidade_relativa_percent` | `data_medicao` | - |

### Gold Layer

| Dataset | Colunas VARCHAR | Colunas INTEGER | Colunas DOUBLE | Colunas DATE | Colunas BIGINT |
|:---|:---|:---|:---|:---|:---|
| **gold_dengue_clima** | `uf`, `nome_municipio` | `semana_epidemiologica`, `ano_epidemiologico`, `casos_notificados`, `casos_confirmados`, `nivel_alerta` | `casos_estimados`, `incidencia_100k`, `populacao`, `id_temp_media`, `id_umidade_media`, `inmet_temp_media`, `inmet_temp_min`, `inmet_temp_max`, `inmet_precip_tot`, `inmet_temp_media_lag1`, `inmet_temp_media_lag2`, `inmet_temp_media_lag3`, `inmet_temp_media_lag4`, `inmet_precip_tot_lag1`, `inmet_precip_tot_lag2`, `inmet_precip_tot_lag3`, `inmet_precip_tot_lag4` | `data_inicio_semana` | `geocode` |

---

## 🎯 6. Convenções de Nomenclatura

### 6.1. Padrões Adotados

* **Colunas:** `snake_case` (ex: `casos_estimados`, `inmet_temp_media`)
* **Tabelas/Views:** `snake_case` com prefixo de camada (ex: `silver_dengue`, `gold_dengue_clima`)
* **Partições:** Hive style `chave=valor` (ex: `uf=SP`, `ano=2024`)
* **Arquivos Parquet:** `{nome_tabela}_{sequencial}.parquet` (ex: `silver_dengue_001.parquet`)

### 6.2. Sufixos Semânticos

* `_id`: Identificadores únicos (ex: `estacao_id`)
* `_codigo`/`code`: Códigos padronizados (ex: `geocode`)
* `_media`/`_avg`: Agregações de média
* `_tot`/`_sum`: Agregações de soma
* `_min`/`_max`: Agregações de mínimo/máximo
* `_lag{N}`: Features de lag temporal (ex: `_lag1`, `_lag2`)
* `_percent`: Valores percentuais (ex: `umidade_relativa_percent`)

---

## ✅ 7. Checklist de Qualidade de Dados

### Validações Bronze → Silver

* [ ] Taxa de parsing bem-sucedido > 95%
* [ ] Sem registros com geocode nulo
* [ ] Sem registros com data inválida
* [ ] Deduplicação completa por chaves compostas
* [ ] Tipos de dados consistentes (sem strings em colunas numéricas)

### Validações Silver → Gold

* [ ] Join coverage > 70% (% de registros com dados climáticos)
* [ ] Lags calculados corretamente (sem descontinuidades)
* [ ] Particionamento balanceado por UF
* [ ] Sem chaves primárias duplicadas

---

## 📖 8. Referências

* **InfoDengue API:** <https://info.dengue.mat.br/services/api>
* **INMET:** <https://portal.inmet.gov.br/dadoshistoricos>
* **DTB/IBGE:** <https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/23701-divisao-territorial-brasileira.html>
* **AWS Glue:** <https://docs.aws.amazon.com/glue/>
* **DuckDB:** <https://duckdb.org/docs/>

---

**Última Atualização:** 2025-12-06  
**Autor:** Sistema Dengue-Clima Team  
**Versão:** 2.0 (Detalhada)
