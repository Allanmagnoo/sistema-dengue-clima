# Relatório de Modelagem: Dengue + Saneamento + Censo (v2)

## 1. Visão Geral
Este relatório documenta o processo de treinamento de um novo modelo de Machine Learning (`dengue_saneamento_xgb`) incorporando dados da camada Gold:
- **Dengue** (Casos notificados)
- **SNIS** (Saneamento: Água e Esgoto)
- **IBGE** (Censo 2022: Densidade Demográfica)

## 2. Metodologia

### 2.1. Preparação dos Dados
- **Agregação Temporal**: Anual (devido à granularidade dos dados do SNIS).
- **Tratamento de Nulos**:
  - Dados de saneamento: Forward Fill por município, preenchimento com 0 nos anos iniciais.
  - População: Preenchimento com média onde nulo.
- **Feature Engineering**:
  - `incidencia`: (Casos / População) * 100k.
  - `incidencia_lag1`, `incidencia_lag2`: Histórico de incidência de 1 e 2 anos anteriores.
  - `cod_uf`: Extraído do ID do município.
  - Features de Saneamento: `agua_cobertura_total_pct`, `esgoto_cobertura_total_pct`, `agua_perda_distribuicao_pct`.
  - Feature Demográfica: `densidade_domiciliada` (IBGE).

### 2.2. Divisão dos Dados (Split)
- **Estratégia**: Temporal.
- **Treino**: Anos anteriores a 2023 (2021-2022 no dataset filtrado).
- **Teste**: Anos 2023 a 2025.

### 2.3. Modelo
- **Algoritmo**: XGBoost Regressor.
- **Parâmetros**: 
  - `n_estimators`: 300
  - `max_depth`: 8
  - `learning_rate`: 0.05
  - `early_stopping_rounds`: 20

## 3. Resultados

### 3.1. Métricas de Desempenho
| Métrica | Valor |
|---------|-------|
| **MAE** | 3303.62 |
| **RMSE** | 6248.07 |
| **R²** | 0.0617 |

### 3.2. Importância das Features
As variáveis mais relevantes para o modelo foram:
1. **Ano** (0.293): Indica forte tendência temporal ou mudanças anuais nos padrões.
2. **Incidência Lag 2** (0.165): Histórico de 2 anos atrás (possível ciclo epidêmico).
3. **UF** (0.154): Fator regional forte.
4. **Incidência Lag 1** (0.091).
5. **Densidade Domiciliada** (0.088): Feature nova do IBGE mostrando relevância.
6. **Cobertura de Água** (0.073): Feature nova do SNIS.

## 4. Análise e Comparação
- **Desempenho**: O R² de 0.06 é baixo, indicando que as variáveis anuais de saneamento e demografia, por si sós, explicam pouco da variância ano a ano da dengue em nível municipal neste recorte.
- **Comparação com Modelos Semanais**: Modelos baseados em clima e dados semanais tendem a ter desempenho superior para prever surtos de curto prazo. A agregação anual perde a sazonalidade fina da doença.
- **Contribuição dos Novos Dados**: Apesar do baixo desempenho global, as features `densidade_domiciliada` e `agua_cobertura_total_pct` apareceram com importância considerável, sugerindo que são fatores de risco estruturais relevantes, mas talvez precisem ser combinados com dados climáticos semanais para melhor predição.

## 5. Próximos Passos
1. **Integração Híbrida**: Combinar as features estáticas/anuais (Saneamento/IBGE) com o painel semanal de clima. Repetir os dados anuais para todas as semanas do ano correspondente.
2. **Refinamento de Features**: Criar taxas de variação de saneamento em vez de valores absolutos.
3. **Análise de Clusters**: Usar os dados do IBGE/SNIS para segmentar municípios e treinar modelos específicos por cluster.
