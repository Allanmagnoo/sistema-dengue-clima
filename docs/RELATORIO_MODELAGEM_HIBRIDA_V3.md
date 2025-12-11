# Relatório de Modelagem: Híbrido (Semanal v3)

## 1. Visão Geral
Este relatório documenta o treinamento do modelo **XGBoost Híbrido v3**, que combina a dinâmica temporal semanal (casos e clima) com a estrutura estática municipal (saneamento e demografia).

**Objetivo**: Superar a baixa performance do modelo puramente anual (R² 0.06) integrando a granularidade semanal.

## 2. Metodologia

### 2.1. Dados Utilizados
- **Painel Híbrido Semanal**: `data/gold/paineis/painel_hibrido_semanal`
    - **Base**: Casos de Dengue (Semanal) + Clima (INMET).
    - **Enriquecimento**: Join com dados anuais do SNIS (Saneamento) e IBGE (Censo 2022).
- **Período**: 2020 a 2025.

### 2.2. Feature Engineering
- **Target**: `incidencia` (Casos por 100k habitantes, semanal).
- **Auto-regressivas**: `casos_lag1` a `casos_lag4`, `casos_media_4sem`.
- **Estruturais (Novas)**:
    - `densidade_domiciliada` (IBGE)
    - `agua_perda_distribuicao_pct` (SNIS)
    - `esgoto_cobertura_total_pct` (SNIS)
    - `agua_cobertura_total_pct` (SNIS)
- **Sazonais**: Seno/Cosseno da Semana Epidemiológica.
- **Climáticas**: Temperatura e Precipitação (com lags).

### 2.3. Divisão dos Dados
- **Treino**: Anos < 2024 (1.15M registros).
- **Teste**: Anos >= 2024 (551k registros).

## 3. Resultados

### 3.1. Métricas de Desempenho
| Métrica | Valor | Observação |
|---------|-------|------------|
| **R²** | **0.5611** | Grande salto comparado ao v2 (0.06). O modelo explica 56% da variância semanal. |
| **RMSE** | 137.59 | Erro quadrático médio (casos/100k). |
| **MAE** | 33.02 | Erro absoluto médio. |

### 3.2. Importância das Features (Top 10)
| Feature | Importância | Interpretação |
|---------|-------------|---------------|
| `casos_lag1` | 0.330 | A inércia da doença é o maior preditor (epidemia em andamento tende a continuar). |
| **`densidade_domiciliada`** | **0.128** | **Impacto do Censo**: Áreas mais densas têm dinâmica de transmissão acelerada. |
| `casos_media_4sem` | 0.069 | Tendência recente. |
| `casos_lag2` | 0.062 | Histórico recente. |
| `semana_cos` | 0.058 | Sazonalidade anual. |
| `semana_sin` | 0.054 | Sazonalidade anual. |
| **`agua_perda_distribuicao`** | **0.042** | **Impacto do SNIS**: Perda de água indica ineficiência e possíveis focos de mosquito. |
| **`esgoto_cobertura`** | **0.039** | Saneamento precário favorece vetores. |
| **`agua_cobertura`** | **0.037** | Acesso à água afeta armazenamento inadequado. |
| `inmet_temp_media_lag1` | 0.036 | Clima influencia ciclo do vetor. |

## 4. Conclusão
- A estratégia híbrida foi **altamente eficaz**.
- Os dados do **IBGE (Densidade)** provaram ser a variável estrutural mais crítica, superando até métricas de saneamento individualmente.
- Os dados do **SNIS** coletivamente adicionam robustez, explicando variações locais que o clima/lags não capturam.
- O modelo v3 é o candidato ideal para produção/previsão de novos surtos.

## 5. Próximos Passos
- **Fine-tuning**: Otimizar hiperparâmetros do XGBoost para tentar chegar a R² > 0.60.
- **Validação Cruzada Espacial**: Testar se o modelo treinado em uma região generaliza para outra.
- **Dashboard**: Visualizar as predições vs realizado no Streamlit.
