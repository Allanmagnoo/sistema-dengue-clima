# Planejamento de Orquestração com Astronomer (Airflow)

Este documento detalha a estratégia de orquestração do projeto **Sistema Dengue-Clima**, esclarecendo o papel da ferramenta **Astronomer (Astro CLI)** e definindo o roteiro para integração dos novos pipelines de Machine Learning.

---

## 1. Objetivos do Astronomer no Projeto

O termo "Astronomy" mencionado refere-se à **Astronomer**, a plataforma enterprise construída sobre o Apache Airflow. No contexto do nosso projeto, utilizamos a **Astro CLI** para gerenciar o ambiente local de desenvolvimento do Airflow.

### 1.1. Quais problemas ele resolve?
Atualmente, temos scripts Python isolados em `src/jobs/` e `src/models/`. Executá-los manualmente gera:
*   **Falta de Dependência:** Não há garantia automática de que a camada Gold só rode após o sucesso da Silver.
*   **Ausência de Retries:** Se a API do InfoDengue falhar momentaneamente, o script para e requer intervenção humana.
*   **Visibilidade Limitada:** Não temos um histórico centralizado de logs para saber quando e por que um modelo falhou.

### 1.2. Funcionalidades Utilizadas
*   **DAGs (Directed Acyclic Graphs):** Definição visual do fluxo de dados (Bronze -> Silver -> Gold -> ML).
*   **Operators:** 
    *   `BashOperator` / `PythonOperator`: Para executar nossos scripts existentes em `src/`.
    *   `Sensors`: Para aguardar a chegada de arquivos novos (futuro).
*   **Scheduler:** Agendamento automático (ex: ingestão semanal toda segunda-feira).
*   **Alertas:** Notificação em caso de falha nos treinos de ML.

### 1.3. Integração com Arquitetura Existente
O Airflow atua como o **maestro**. Ele não substitui os scripts em `src/`, mas os orquestra.
*   **Código de Negócio:** Continua em `src/jobs/` e `src/models/`.
*   **Código de Orquestração:** Fica em `dags/`, importando ou chamando o código de negócio.

---

## 2. Plano de Atualização das DAGs

Com a introdução do **Modelo Híbrido (v3)** e da **Camada Gold de Saneamento**, precisamos atualizar nossos pipelines.

### 2.1. DAGs Existentes (Modificações Necessárias)

| DAG | Status Atual | Ação Necessária |
| :--- | :--- | :--- |
| `ingest_dengue_weekly` | Ativa | **Manter**. Monitorar se novos geocodes são necessários. |
| `ingest_ibge_population` | Ativa | **Manter**. Execução anual. |
| `transform_pipeline` | Parcial | **Atualizar**. Adicionar passos para `create_gold_saneamento.py` e `create_gold_hybrid.py`. |

### 2.2. Novas DAGs (Requisitos)

#### A. DAG: `05_ml_training_pipeline`
Responsável pelo ciclo de vida do modelo de Machine Learning.
*   **Trigger:** Após sucesso do `transform_pipeline` (Dataset Híbrido atualizado).
*   **Tasks:**
    1.  `train_model_v3`: Executa `src/models/train_v3_hybrid.py`.
    2.  `validate_metrics`: Verifica se RMSE/R² estão dentro dos limites aceitáveis.
    3.  `notify_result`: Loga ou envia alerta com a performance do modelo.

### 2.3. Critérios de Qualidade
*   **Idempotência:** Rodar a DAG duas vezes no mesmo dia não pode duplicar dados (já garantido pelo `overwrite` no Parquet).
*   **Atomicidade:** Se o treino falhar, o modelo antigo não deve ser sobrescrito (usar staging area ou versionamento).

---

## 3. Fluxos de Trabalho Atualizados

### Pipeline Completo (End-to-End)

```mermaid
graph TD
    A[Ingestão Semanal (InfoDengue)] -->|Raw JSON| B(Camada Bronze)
    C[Ingestão IBGE/SNIS] -->|Arquivos| B
    
    subgraph DAG: Transformação
    B -->|Processamento| D[Camada Silver]
    D -->|Join| E[Gold Saneamento]
    D -->|Join| F[Gold Dengue+Clima]
    E & F -->|Merge| G[Gold Híbrido Semanal]
    end
    
    subgraph DAG: Machine Learning
    G -->|Input| H[Treinamento XGBoost v3]
    H -->|Artifact| I[Modelo .joblib + Metadata]
    end
```

---

## 4. Cronograma de Implementação

| Fase | Atividade | Estimativa |
| :--- | :--- | :--- |
| **Fase 1** | Atualizar `transform_pipeline` para incluir criação do Painel Híbrido. | Imediato |
| **Fase 2** | Criar `ml_training_pipeline` para automatizar o script `train_v3_hybrid.py`. | Curto Prazo |
| **Fase 3** | Testes de integração (rodar ingestão -> treino sequencialmente). | Médio Prazo |
| **Fase 4** | Configurar alertas de falha (Email/Slack). | Longo Prazo |

---

## 5. Guias de Implementação (Specs Técnicas)

### Estrutura da Nova DAG de ML
```python
# Exemplo de estrutura para dags/ml_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("05_ml_training_pipeline", schedule="0 6 * * 1") as dag:
    train_task = BashOperator(
        task_id="train_xgboost_v3",
        bash_command="python src/models/train_v3_hybrid.py"
    )
```
