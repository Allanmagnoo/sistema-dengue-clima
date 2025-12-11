"""
Treinamento Local do Modelo de Predição de Dengue

Uso:
    python train_local.py
    python train_local.py --n-estimators 200 --max-depth 20
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

warnings.filterwarnings('ignore')


def parse_args():
    parser = argparse.ArgumentParser(description='Treinar modelo de dengue')
    parser.add_argument('--data-dir', type=str, default='data/gold/gold_dengue_clima')
    parser.add_argument('--output-dir', type=str, default='models')
    parser.add_argument('--n-estimators', type=int, default=100)
    parser.add_argument('--max-depth', type=int, default=15)
    parser.add_argument('--test-split', type=float, default=0.2)
    return parser.parse_args()


def get_project_root() -> Path:
    """Detecta o diretório raiz do projeto."""
    # Primeiro tenta a partir do script
    script_path = Path(__file__).resolve().parent
    
    # Sobe até encontrar o diretório com 'data'
    for parent in [script_path] + list(script_path.parents):
        if (parent / 'data').exists():
            return parent
    
    # Fallback: usa o diretório atual
    return Path.cwd()


def load_data(data_dir: str) -> pd.DataFrame:
    """Carrega dados da Gold Layer."""
    # Converte para Path absoluto
    data_path = Path(data_dir)
    if not data_path.is_absolute():
        data_path = get_project_root() / data_dir
    
    print(f"\n📂 Carregando: {data_path}")
    
    parquet_files = list(data_path.rglob("*.parquet"))
    
    if not parquet_files:
        raise FileNotFoundError(f"Nenhum arquivo .parquet em: {data_path}")
    
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
    print(f"   Registros: {len(combined):,}")
    return combined


def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Cria features para o modelo."""
    print("\n🔧 Features...")
    
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
    
    feature_cols = [
        'inmet_temp_media_lag1', 'inmet_temp_media_lag2', 
        'inmet_temp_media_lag3', 'inmet_temp_media_lag4',
        'inmet_precip_tot_lag1', 'inmet_precip_tot_lag2',
        'inmet_precip_tot_lag3', 'inmet_precip_tot_lag4',
        'inmet_temp_media', 'inmet_precip_tot',
        'semana_epidemiologica', 'ano_epidemiologico',
        'semana_sin', 'semana_cos',
        'geocode_hash', 'regiao', 'populacao'
    ]
    
    available = [c for c in feature_cols if c in model_df.columns]
    print(f"   {len(available)} features")
    return model_df, available


def train(X_train, y_train, n_estimators, max_depth):
    """Treina Random Forest."""
    print(f"\n🎯 Treinando (n={n_estimators}, depth={max_depth})...")
    
    model = RandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    return model


def evaluate(model, X_train, y_train, X_test, y_test):
    """Avalia o modelo."""
    metrics = {
        'train': {
            'mae': float(mean_absolute_error(y_train, model.predict(X_train))),
            'r2': float(r2_score(y_train, model.predict(X_train)))
        },
        'test': {
            'mae': float(mean_absolute_error(y_test, model.predict(X_test))),
            'r2': float(r2_score(y_test, model.predict(X_test)))
        }
    }
    
    print(f"\n📊 Métricas:")
    print(f"   Treino: MAE={metrics['train']['mae']:.1f}, R²={metrics['train']['r2']:.3f}")
    print(f"   Teste:  MAE={metrics['test']['mae']:.1f}, R²={metrics['test']['r2']:.3f}")
    return metrics


def save_model(model, output_dir, features, metrics, hyperparams):
    """Salva modelo e metadata."""
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = get_project_root() / output_dir
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Modelo
    model_file = output_path / 'dengue_model.joblib'
    joblib.dump(model, model_file)
    
    # Metadata
    metadata = {
        'model_type': 'RandomForestRegressor',
        'features': features,
        'metrics': metrics,
        'hyperparameters': hyperparams,
        'trained_at': datetime.now().isoformat()
    }
    
    with open(output_path / 'model_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n💾 Salvo em: {model_file}")


def main():
    print("=" * 50)
    print("🤖 Treinamento - Modelo de Dengue")
    print("=" * 50)
    
    args = parse_args()
    
    # Carregar
    df = load_data(args.data_dir)
    
    # Features
    model_df, features = engineer_features(df)
    
    # Preparar
    target = 'casos_notificados'
    clean_df = model_df[features + [target]].dropna()
    print(f"   Válidos: {len(clean_df):,}")
    
    X = clean_df[features]
    y = clean_df[target]
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_split, random_state=42
    )
    print(f"\n📈 Treino: {len(X_train):,} | Teste: {len(X_test):,}")
    
    # Treinar
    model = train(X_train, y_train, args.n_estimators, args.max_depth)
    
    # Avaliar
    metrics = evaluate(model, X_train, y_train, X_test, y_test)
    
    # Salvar
    hyperparams = {'n_estimators': args.n_estimators, 'max_depth': args.max_depth}
    save_model(model, args.output_dir, features, metrics, hyperparams)
    
    print("\n✅ Concluído!")


if __name__ == '__main__':
    main()
