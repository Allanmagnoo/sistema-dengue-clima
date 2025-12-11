# Plano de Uso para Novos Dados (IBGE e SNIS)

Este documento detalha a estratégia de integração e uso para os novos conjuntos de dados adicionados à camada Bronze.

## 1. Dados do Censo IBGE 2022 (Setores Censitários)

**Localização Bronze:** `data/bronze/Instituto Brasileiro de Geografia e Estatística/`

### Dados Processados (Silver)
O arquivo `Area_efetivamente_domiciliada_e_densidade_ajustada_dos_Setores_Censitarios.xlsx` foi processado e transformado para Parquet.

- **Arquivo Silver:** `data/silver/ibge/censo_agregado_2022.parquet`
- **Script de Ingestão:** `src/jobs/transform_silver_ibge.py`
- **Conteúdo:**
    - `id_setor_censitario`: Chave primária para join com mapas.
    - `populacao_total`: População residente.
    - `domicilios_total`: Total de domicílios.
    - `densidade_domiciliada`: Densidade demográfica considerando apenas área habitada (mais preciso para epidemiologia).

### Estratégia de Uso
1. **Cálculo de Incidência:** Utilizar a `populacao_total` por setor para calcular a incidência de Dengue (Casos / População * 100k) em nível hiperlocal.
2. **Análise de Risco:** Utilizar `densidade_domiciliada` e `media_moradores_domicilio` como proxies para aglomeração urbana, fator de risco para transmissão de arboviroses.

### Dados Geoespaciais (GeoPackages)
Os arquivos `.gpkg` (Malha Viária, Setores, Bairros) contêm as geometrias.
- **Ação Recomendada:** Ingerir estes arquivos em um banco de dados espacial (PostGIS) ou usar bibliotecas como `geopandas` para análises espaciais.
- **Objetivo:** Permitir a visualização de mapas de calor e análise de clusters de casos.

## 2. Dados SNIS (Saneamento Básico)

**Localização Bronze:**
- `data/bronze/br_mdr_snis_municipio_agua_esgoto.csv` (Dataset consolidado 1995-2021)
- `data/bronze/2Planilhas_AE2021/` (Arquivos originais detalhados)
- `data/bronze/Planilhas_AE2019/`
- `data/bronze/Planilhas_AE2020/`

### Dados Processados (Silver)
O arquivo consolidado CSV foi transformado para Parquet, padronizando nomenclaturas e tipos de dados.

- **Arquivo Silver:** `data/silver/snis/snis_agregado_1995_2021.parquet`
- **Script de Ingestão:** `src/jobs/transform_silver_snis.py`
- **Conteúdo Principal:**
    - `agua_cobertura_total_pct`: Índice de atendimento total de água (IN055).
    - `esgoto_cobertura_total_pct`: Índice de coleta de esgoto (IN015).
    - `agua_perda_distribuicao_pct`: Índice de perdas na distribuição (indicador de ineficiência/vazamentos).
    - `agua_pop_atendida_total`: População absoluta atendida.
    - `investimento_total_municipio`: Investimentos realizados no setor.

### Limitações Identificadas
- O dataset consolidado **não contém** os indicadores diretos de "Intermitência" ou "Paralisações" (IN017/IN018), que são fortes preditores de armazenamento de água domiciliar (risco de Dengue).
- **Ação:** Utilizar `agua_cobertura_total_pct` e `agua_perda_distribuicao_pct` como proxies de qualidade do serviço. Baixa cobertura ou altas perdas sugerem precariedade.

### Estratégia de Uso
1. **Análise de Correlação:** Cruzar `esgoto_cobertura_total_pct` com a incidência de Dengue por município. Hipótese: Menor saneamento -> Maior incidência.
2. **Clusterização:** Agrupar municípios por perfil de saneamento para identificar zonas de risco estrutural.

## 3. Próximos Passos Técnicos
- [x] Ingestão dos dados tabulares do Censo 2022 (Concluído).
- [x] Ingestão e padronização dos dados SNIS 1995-2021 (Concluído).
- [ ] Configuração de ambiente com `geopandas` para leitura dos arquivos `.gpkg`.
- [ ] Carga dos dados Silver (Censo + SNIS) no PostgreSQL.
