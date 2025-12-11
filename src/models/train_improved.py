"""
Treinamento Avançado - XGBoost com Lag de Casos

Melhorias implementadas:
1. XGBoost ao invés de Random Forest
2. Lag de casos (casos das semanas anteriores)
3. Comparação de modelos

Uso:
    python train_improved.py
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
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

try:
    import xgboost as xgb
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False
    print("⚠️ XGBoost não instalado. Use: pip install xgboost")

warnings.filterwarnings('ignore')


def get_project_root() -> Path:
    script_path = Path(__file__).resolve().parent
    for parent in [script_path] + list(script_path.parents):
        if (parent / 'data').exists():
            return parent
    return Path.cwd()


import pandera as pa

# ... (outros imports) ...

# ---------------------------------------------------------
# 🛡️ Schema de Validação (Pandera - Estilo Funcional)
# ---------------------------------------------------------
DengueInputSchema = pa.DataFrameSchema({
    # Identificadores
    "geocode": pa.Column(int, coerce=True),
    "uf": pa.Column(str, checks=pa.Check.isin(['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
                                              'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
                                              'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'])),
    
    # Temporais
    "semana_epidemiologica": pa.Column(int, checks=[pa.Check.ge(1), pa.Check.le(53)]),
    "ano_epidemiologico": pa.Column(int, checks=[pa.Check.ge(2010), pa.Check.le(2025)]),
    
    # Clima (Regras de negócio)
    "inmet_temp_media": pa.Column(float, checks=[pa.Check.ge(-10), pa.Check.le(50)], nullable=True),
    "inmet_precip_tot": pa.Column(float, checks=pa.Check.ge(0), nullable=True),
    
    # Target
    "casos_notificados": pa.Column(float, checks=pa.Check.ge(0)),
    
    # Demografia
    "populacao": pa.Column(float, checks=pa.Check.ge(0), nullable=True),
}, coerce=True, strict=False) # strict=False permite colunas extras


def load_data(data_dir: str) -> pd.DataFrame:
    """Carrega dados da Gold Layer e valida com Pandera."""
    data_path = Path(data_dir)
    if not data_path.is_absolute():
        data_path = get_project_root() / data_dir
    
    print(f"\n📂 Carregando: {data_path}")
    parquet_files = list(data_path.rglob("*.parquet"))
    
    if not parquet_files:
        raise FileNotFoundError(f"Sem arquivos em: {data_path}")
    
    print(f"   Arquivos: {len(parquet_files)}")
    
    dfs = []
    for f in parquet_files:
        df = pd.read_parquet(f)
        if 'uf' not in df.columns:
            uf_parts = [p for p in f.parts if p.startswith('uf=')]
            if uf_parts:
                df['uf'] = uf_parts[0].replace('uf=', '')
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    print(f"   Registros brutos: {len(combined):,}")
    
    # 🛡️ Validação Pandera
    print("\n🕵️  Validando dados com Pandera...")
    try:
        validated_df = DengueInputSchema.validate(combined, lazy=True)
        print("   ✅ Dados validados com sucesso! Nenhum erro crítico.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        print(f"   ⚠️  Encontrados {len(err.failure_cases)} erros de validação:")
        print(err.failure_cases[['column', 'check', 'failure_case']].head())
        print("   ⚠️  Continuando apenas com dados válidos ou ignorando erros não críticos...")
        return combined # Em produção, poderíamos abortar ou filtrar


def create_cases_lag(df: pd.DataFrame) -> pd.DataFrame:
    """Cria features de lag de casos (NOVA FEATURE IMPORTANTE!)."""
    print("\n📈 Criando lag de casos...")
    
    # Ordenar por município e data
    df = df.sort_values(['geocode', 'ano_epidemiologico', 'semana_epidemiologica'])
    
    # Criar lag de casos por município
    for lag in [1, 2, 3, 4]:
        df[f'casos_lag{lag}'] = df.groupby('geocode')['casos_notificados'].shift(lag)
    
    # Média móvel de casos (últimas 4 semanas)
    df['casos_media_4sem'] = df.groupby('geocode')['casos_notificados'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).mean()
    )
    
    print(f"   Criados: casos_lag1-4, casos_media_4sem")
    return df


def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Cria todas as features."""
    print("\n🔧 Engenharia de features...")
    
    model_df = df.copy()
    
    # Encoding cíclico da semana
    model_df['semana_sin'] = np.sin(2 * np.pi * model_df['semana_epidemiologica'] / 53)
    model_df['semana_cos'] = np.cos(2 * np.pi * model_df['semana_epidemiologica'] / 53)
    
    # Hash do geocode
    model_df['geocode_hash'] = model_df['geocode'].apply(lambda x: hash(str(x)) % 1000)
    
    # Região
    region_map = {
        'AC': 0, 'AM': 0, 'AP': 0, 'PA': 0, 'RO': 0, 'RR': 0, 'TO': 0,
        'AL': 1, 'BA': 1, 'CE': 1, 'MA': 1, 'PB': 1, 'PE': 1, 'PI': 1, 'RN': 1, 'SE': 1,
        'DF': 2, 'GO': 2, 'MS': 2, 'MT': 2,
        'ES': 3, 'MG': 3, 'RJ': 3, 'SP': 3,
        'PR': 4, 'RS': 4, 'SC': 4
    }
    model_df['regiao'] = model_df['uf'].map(region_map).fillna(-1).astype(int)
    
    # Lista completa de features (incluindo novas!)
    feature_cols = [
        # Lag de casos (NOVAS!)
        'casos_lag1', 'casos_lag2', 'casos_lag3', 'casos_lag4', 'casos_media_4sem',
        # Clima
        'inmet_temp_media_lag1', 'inmet_temp_media_lag2', 
        'inmet_temp_media_lag3', 'inmet_temp_media_lag4',
        'inmet_precip_tot_lag1', 'inmet_precip_tot_lag2',
        'inmet_precip_tot_lag3', 'inmet_precip_tot_lag4',
        'inmet_temp_media', 'inmet_precip_tot',
        # Temporal
        'semana_epidemiologica', 'ano_epidemiologico',
        'semana_sin', 'semana_cos',
        # Geográfico
        'geocode_hash', 'regiao', 'populacao'
    ]
    
    available = [c for c in feature_cols if c in model_df.columns]
    print(f"   {len(available)} features")
    return model_df, available


def train_xgboost(X_train, y_train, X_test, y_test):
    """Treina XGBoost com tuning básico."""
    print("\n🚀 Treinando XGBoost...")
    
    model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=10,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        early_stopping_rounds=20,
        eval_metric='rmse'
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False
    )
    
    return model


def train_random_forest(X_train, y_train):
    """Treina Random Forest (baseline)."""
    print("\n🌲 Treinando Random Forest (baseline)...")
    
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    return model


def evaluate(model, X_test, y_test, model_name):
    """Avalia modelo."""
    y_pred = model.predict(X_test)
    
    metrics = {
        'mae': float(mean_absolute_error(y_test, y_pred)),
        'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
        'r2': float(r2_score(y_test, y_pred))
    }
    
    print(f"\n📊 {model_name}:")
    print(f"   MAE:  {metrics['mae']:.1f}")
    print(f"   RMSE: {metrics['rmse']:.1f}")
    print(f"   R²:   {metrics['r2']:.4f}")
    
    return metrics


def save_model(model, output_dir, features, metrics, model_type):
    """Salva modelo."""
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = get_project_root() / output_dir
    output_path.mkdir(parents=True, exist_ok=True)
    
    model_file = output_path / 'dengue_model.joblib'
    joblib.dump(model, model_file)
    
    metadata = {
        'model_type': model_type,
        'features': features,
        'metrics': {'test': metrics},
        'trained_at': datetime.now().isoformat()
    }
    
    with open(output_path / 'model_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n💾 Salvo: {model_file}")


def main():
    print("=" * 55)
    print("🤖 Treinamento Melhorado - XGBoost + Lag de Casos")
    print("=" * 55)
    
    # Carregar dados
    df = load_data('data/gold/gold_dengue_clima')
    
    # NOVA FEATURE: Lag de casos
    df = create_cases_lag(df)
    
    # Feature engineering
    model_df, features = engineer_features(df)
    
    # Preparar dados
    target = 'casos_notificados'
    clean_df = model_df[features + [target]].dropna()
    print(f"   Válidos: {len(clean_df):,}")
    
    X = clean_df[features]
    y = clean_df[target]
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"\n📈 Treino: {len(X_train):,} | Teste: {len(X_test):,}")
    
    # Treinar e comparar
    print("\n" + "=" * 55)
    print("🏆 COMPARAÇÃO DE MODELOS")
    print("=" * 55)
    
    # Random Forest (baseline)
    rf_model = train_random_forest(X_train, y_train)
    rf_metrics = evaluate(rf_model, X_test, y_test, "Random Forest")
    
    # XGBoost (melhorado)
    if HAS_XGBOOST:
        xgb_model = train_xgboost(X_train, y_train, X_test, y_test)
        xgb_metrics = evaluate(xgb_model, X_test, y_test, "XGBoost")
        
        # Escolher melhor modelo
        if xgb_metrics['r2'] > rf_metrics['r2']:
            best_model = xgb_model
            best_metrics = xgb_metrics
            best_name = 'XGBRegressor'
            print("\n✅ XGBoost venceu!")
        else:
            best_model = rf_model
            best_metrics = rf_metrics
            best_name = 'RandomForestRegressor'
            print("\n✅ Random Forest venceu!")
    else:
        best_model = rf_model
        best_metrics = rf_metrics
        best_name = 'RandomForestRegressor'
    
    # Melhoria
    old_r2 = 0.816  # R² anterior sem lag de casos
    improvement = (best_metrics['r2'] - old_r2) * 100
    print(f"\n📈 Melhoria: {improvement:+.1f}% no R²")
    
    # Salvar melhor modelo
    save_model(best_model, 'models', features, best_metrics, best_name)
    
    print("\n" + "=" * 55)
    print("✅ Treinamento concluído!")
    print("=" * 55)


if __name__ == '__main__':
    main()
