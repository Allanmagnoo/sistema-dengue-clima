"""
Treinamento Avançado - XGBoost com Saneamento e Lag
Adaptado para incluir novas features de saneamento e densidade demográfica.
"""

import argparse
import json
import warnings
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb
import pandera as pa

warnings.filterwarnings('ignore')

def get_project_root() -> Path:
    script_path = Path(__file__).resolve().parent
    for parent in [script_path] + list(script_path.parents):
        if (parent / 'data').exists():
            return parent
    return Path.cwd()

# ---------------------------------------------------------
# 🛡️ Schema de Validação (Atualizado)
# ---------------------------------------------------------
DengueInputSchema = pa.DataFrameSchema({
    "id_municipio": pa.Column(str, coerce=True),
    "ano": pa.Column(int, checks=[pa.Check.ge(1995), pa.Check.le(2025)]),
    "total_casos_dengue": pa.Column(float, checks=pa.Check.ge(0), nullable=True),
    "populacao_total": pa.Column(float, checks=pa.Check.ge(0), nullable=True),
    "densidade_demografica_domiciliada": pa.Column(float, nullable=True),
    "agua_cobertura_total_pct": pa.Column(float, nullable=True),
    "esgoto_cobertura_total_pct": pa.Column(float, nullable=True),
    "agua_perda_distribuicao_pct": pa.Column(float, nullable=True),
}, coerce=True, strict=False)

def load_data() -> pd.DataFrame:
    """Carrega dados da Gold Layer (Painel Municipal)."""
    root = get_project_root()
    data_path = root / "data" / "gold" / "paineis" / "painel_municipal_dengue_saneamento.parquet"
    
    print(f"\n📂 Carregando: {data_path}")
    if not data_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {data_path}")
    
    df = pd.read_parquet(data_path)
    print(f"   Registros brutos: {len(df):,}")
    
    # Validação preliminar
    # Garantir que temos as colunas necessárias
    required_cols = ['id_municipio', 'ano', 'total_casos_dengue']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes: {missing}")
        
    return df

def feature_engineering(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Cria features, incluindo lags e tratamento de nulos."""
    print("\n🔧 Engenharia de features...")
    
    model_df = df.copy()
    
    # 1. Tratamento de Nulos
    # Saneamento: Preencher com média do município ou 0 se faltar
    # Para simplificar neste MVP: preencher com média global ou mediana onde faltar
    # Idealmente: forward fill por município
    
    model_df = model_df.sort_values(['id_municipio', 'ano'])
    
    sanitation_cols = [
        'agua_cobertura_total_pct', 'esgoto_cobertura_total_pct', 
        'agua_perda_distribuicao_pct', 'densidade_domiciliada'
    ]
    
    # Forward Fill por município para preencher anos recentes sem dados de saneamento (2022-2024)
    model_df[sanitation_cols] = model_df.groupby('id_municipio')[sanitation_cols].ffill()
    
    # Se ainda sobrar nulo (anos iniciais), preencher com 0 ou mediana
    model_df[sanitation_cols] = model_df[sanitation_cols].fillna(0)
    
    # 2. Target: Incidência (Casos por 100k)
    # Se população for nula, usar média
    model_df['populacao_total'] = model_df['populacao_total'].fillna(model_df['populacao_total'].mean())
    # Garantir que população não seja zero para evitar divisão por zero
    model_df['populacao_total'] = model_df['populacao_total'].replace(0, np.nan)
    model_df['populacao_total'] = model_df['populacao_total'].fillna(model_df['populacao_total'].mean())
    
    model_df['incidencia'] = (model_df['total_casos_dengue'] / model_df['populacao_total']) * 100000
    
    # Substituir infinitos por NaN
    model_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Remover linhas onde target é nulo (casos não reportados)
    model_df = model_df.dropna(subset=['incidencia'])
    
    # 3. Lags do Target (Incidência do ano anterior)
    # Isso é fortíssimo preditor
    model_df['incidencia_lag1'] = model_df.groupby('id_municipio')['incidencia'].shift(1)
    model_df['incidencia_lag2'] = model_df.groupby('id_municipio')['incidencia'].shift(2)
    
    # Remover primeiros anos que ficaram sem lag
    model_df = model_df.dropna(subset=['incidencia_lag1'])
    
    # 4. Features Geográficas (Região)
    # id_municipio começa com código da UF (2 dígitos)
    model_df['cod_uf'] = model_df['id_municipio'].str.slice(0, 2).astype(int)
    
    features = [
        'incidencia_lag1', 'incidencia_lag2',
        'agua_cobertura_total_pct', 'esgoto_cobertura_total_pct',
        'agua_perda_distribuicao_pct', 'densidade_domiciliada',
        'cod_uf', 'ano'
    ]
    
    return model_df, features

def train_xgboost(X_train, y_train, X_test, y_test):
    print("\n🚀 Treinando XGBoost...")
    model = xgb.XGBRegressor(
        n_estimators=300,
        max_depth=8,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        early_stopping_rounds=20,
        eval_metric='rmse'
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    return model

def evaluate(model, X_test, y_test, model_name):
    y_pred = model.predict(X_test)
    # Garantir não negativo
    y_pred = np.maximum(y_pred, 0)
    
    metrics = {
        'mae': float(mean_absolute_error(y_test, y_pred)),
        'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
        'r2': float(r2_score(y_test, y_pred))
    }
    
    print(f"\n📊 {model_name}:")
    print(f"   MAE:  {metrics['mae']:.2f}")
    print(f"   RMSE: {metrics['rmse']:.2f}")
    print(f"   R²:   {metrics['r2']:.4f}")
    return metrics

def save_model(model, features, metrics):
    root = get_project_root()
    output_dir = root / "models"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    joblib.dump(model, output_dir / 'dengue_saneamento_xgb.joblib')
    
    metadata = {
        'features': features,
        'metrics': metrics,
        'trained_at': datetime.now().isoformat()
    }
    with open(output_dir / 'model_metadata_v2.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"\n💾 Modelo salvo em: {output_dir}")

def main():
    print("="*60)
    print("🧪 Treinamento Experimental: Dengue + Saneamento + Censo")
    print("="*60)
    
    # 1. Load
    df = load_data()
    
    # 2. Prep
    df_model, features = feature_engineering(df)
    print(f"   Features selecionadas: {features}")
    print(f"   Dataset final: {len(df_model):,} linhas")
    
    # 3. Split (Temporal: Treino < 2023, Teste >= 2023)
    # Testar capacidade de generalização futura
    cutoff_year = 2023
    train = df_model[df_model['ano'] < cutoff_year]
    test = df_model[df_model['ano'] >= cutoff_year]
    
    X_train, y_train = train[features], train['incidencia']
    X_test, y_test = test[features], test['incidencia']
    
    print(f"\n✂️  Split Temporal (Corte: {cutoff_year}):")
    print(f"   Treino: {len(X_train):,} (Anos: {train.ano.min()}-{train.ano.max()})")
    print(f"   Teste:  {len(X_test):,} (Anos: {test.ano.min()}-{test.ano.max()})")
    
    # 4. Train
    model = train_xgboost(X_train, y_train, X_test, y_test)
    
    # 5. Eval
    metrics = evaluate(model, X_test, y_test, "XGBoost (Saneamento)")
    
    # Feature Importance
    print("\n🔍 Importância das Features:")
    importance = pd.DataFrame({
        'feature': features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(importance)
    
    # 6. Save
    save_model(model, features, metrics)

if __name__ == "__main__":
    main()
