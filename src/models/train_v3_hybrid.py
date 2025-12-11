import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import joblib
import json
import logging
from pathlib import Path
from datetime import datetime

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent

def load_data():
    root = get_project_root()
    file_path = root / "data/gold/paineis/painel_hibrido_semanal"
    
    logger.info(f"📂 Carregando: {file_path}")
    # Lê o dataset particionado (pasta)
    df = pd.read_parquet(file_path)
    logger.info(f"   Registros brutos: {len(df):,}")
    return df

def feature_engineering(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Prepara features para o modelo híbrido (Semanal + Estrutural)."""
    print("\n🔧 Engenharia de features...")
    
    model_df = df.copy()
    
    # 1. Target: Incidência Semanal
    # Se população for nula, usar média global (ou idealmente por municipio, mas simplificando)
    # Mas aqui temos muitos municípios.
    
    # Garantir tipos numéricos
    cols_to_numeric = ['populacao', 'casos_notificados', 'agua_cobertura_total_pct', 'densidade_domiciliada']
    for c in cols_to_numeric:
        if c in model_df.columns:
            model_df[c] = pd.to_numeric(model_df[c], errors='coerce')

    # Tratar população zero/nula
    model_df['populacao'] = model_df['populacao'].replace(0, np.nan)
    # Se não tiver população, não dá pra calcular incidência corretamente. 
    # Mas podemos prever CASOS brutos se usarmos população como feature?
    # Melhor prever incidência para normalizar entre cidades grandes e pequenas.
    
    # Preencher população faltante com mediana
    model_df['populacao'] = model_df['populacao'].fillna(model_df['populacao'].median())
    
    model_df['incidencia'] = (model_df['casos_notificados'] / model_df['populacao']) * 100000
    
    # 2. Features Temporais
    model_df['mes'] = model_df['semana_epidemiologica'].apply(lambda x: int(x/4.3) + 1 if x > 0 else 1) # Aprox
    model_df['semana_sin'] = np.sin(2 * np.pi * model_df['semana_epidemiologica'] / 53)
    model_df['semana_cos'] = np.cos(2 * np.pi * model_df['semana_epidemiologica'] / 53)
    
    # 3. Features Climáticas (INMET) - Tratar Nulos
    # Como temos poucos dados climáticos (5%), se filtrarmos vai sobrar pouco.
    # Vamos usar XGBoost que lida com nulos, mas se tudo for nulo, ele não aprende o clima.
    # O foco aqui é testar o ganho das features híbridas.
    
    # 4. Features de Saneamento - Tratar Nulos
    # Preencher com -1 ou média? XGBoost lida com nulos. Vamos deixar nulo ou preencher com mediana.
    # Saneamento é estrutural, varia pouco.
    
    # 5. Lags de Casos (Fundamentais)
    # O dataset já deve vir com lags de casos do pipeline anterior (gold_dengue_clima).
    # Verificar colunas: casos_lag1, casos_lag2, etc.
    
    # Seleção de Features
    features = [
        # Autoregressivo (Casos passados)
        'casos_lag1', 'casos_lag2', 'casos_lag3', 'casos_lag4',
        'casos_media_4sem',
        
        # Saneamento (Estrutural)
        'agua_cobertura_total_pct', 
        'esgoto_cobertura_total_pct',
        'agua_perda_distribuicao_pct',
        
        # Demográfico (Estrutural)
        'densidade_domiciliada',
        
        # Temporal (Sazonalidade)
        'semana_sin', 'semana_cos', 'ano_epidemiologico',
        
        # Regional
        # 'uf' # UF é string, precisa de encoding. O dataset original é particionado por UF, 
        # então a coluna 'uf' pode não estar no dataframe se lido via parquet dataset filters, 
        # ou estar como category. Vamos checar se existe.
    ]
    
    # Adicionar Clima se disponível (mas sabendo que tem muito nulo)
    climate_cols = [
        'inmet_temp_media_lag1', 'inmet_temp_media_lag2',
        'inmet_precip_tot_lag1', 'inmet_precip_tot_lag2'
    ]
    features.extend(climate_cols)
    
    # Limpeza final
    # Remover linhas onde o target (incidencia) é nulo ou infinito
    model_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    model_df = model_df.dropna(subset=['incidencia'])
    
    # Remover linhas onde casos_lag1 é nulo (primeiras semanas)
    model_df = model_df.dropna(subset=['casos_lag1'])
    
    return model_df, features

def train_xgboost(X_train, y_train, X_test, y_test):
    print("\n🚀 Treinando XGBoost Híbrido...")
    model = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=9,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        early_stopping_rounds=30,
        eval_metric='rmse'
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=100)
    return model

def evaluate(model, X_test, y_test, model_name):
    y_pred = model.predict(X_test)
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
    
    # Feature Importance
    importance = pd.DataFrame({
        'feature': X_test.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\n🔍 Top 10 Features mais importantes:")
    print(importance.head(10))
    
    return metrics, importance

def save_model(model, features, metrics):
    root = get_project_root()
    output_dir = root / "models"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    joblib.dump(model, output_dir / 'dengue_hibrido_v3.joblib')
    
    metadata = {
        'features': features,
        'metrics': metrics,
        'trained_at': datetime.now().isoformat(),
        'description': 'Modelo Híbrido (Semanal): Casos Lags + Saneamento + Clima'
    }
    with open(output_dir / 'model_metadata_v3.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"\n💾 Modelo salvo em: {output_dir}")

def main():
    print("="*60)
    print("🧪 Treinamento V3: Modelo Híbrido (Semanal)")
    print("="*60)
    
    df = load_data()
    
    df_model, features = feature_engineering(df)
    
    print(f"   Dataset final para treino: {len(df_model):,} linhas")
    
    # Filtro de colunas
    X = df_model[features]
    y = df_model['incidencia']
    
    # Split Temporal (Corte: 2024 para teste, já que temos dados até 2025 no nome dos arquivos, mas vamos ver os dados reais)
    # Verificar anos disponíveis
    anos = df_model['ano_epidemiologico'].unique()
    print(f"   Anos disponíveis: {sorted(anos)}")
    
    corte_teste = 2024
    
    mask_train = df_model['ano_epidemiologico'] < corte_teste
    mask_test = df_model['ano_epidemiologico'] >= corte_teste
    
    X_train = X[mask_train]
    y_train = y[mask_train]
    X_test = X[mask_test]
    y_test = y[mask_test]
    
    print(f"\n✂️  Split Temporal (Corte: {corte_teste}):")
    print(f"   Treino: {len(X_train):,}")
    print(f"   Teste:  {len(X_test):,}")
    
    if len(X_train) == 0 or len(X_test) == 0:
        logger.error("Split gerou conjunto vazio. Verifique os anos.")
        return

    model = train_xgboost(X_train, y_train, X_test, y_test)
    
    metrics, _ = evaluate(model, X_test, y_test, "XGBoost Híbrido v3")
    
    save_model(model, features, metrics)

if __name__ == "__main__":
    main()
