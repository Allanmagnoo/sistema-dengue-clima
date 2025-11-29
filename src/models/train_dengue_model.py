import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import warnings
warnings.filterwarnings('ignore')

def main():
    print("🤖 Iniciando Modelo Preditivo de Dengue com Random Forest")
    
    # Paths
    base_dir = Path(__file__).parent.parent.parent
    gold_file = base_dir / "data/gold/dengue_clima.parquet"
    model_dir = base_dir / "models"
    model_dir.mkdir(exist_ok=True)
    
    # Load data
    print("📊 Carregando dados...")
    con = duckdb.connect(database=':memory:')
    df = con.execute(f"SELECT * FROM read_parquet('{gold_file}')").df()
    
    # Feature engineering - focus on lag variables that showed correlation
    print("🔧 Engenharia de features...")
    
    # Select relevant features based on correlation analysis
    feature_cols = [
        'inmet_temp_media_lag1', 'inmet_temp_media_lag2', 'inmet_temp_media_lag3', 'inmet_temp_media_lag4',
        'inmet_precip_tot_lag1', 'inmet_precip_tot_lag2', 'inmet_precip_tot_lag3', 'inmet_precip_tot_lag4',
        'inmet_temp_media', 'inmet_precip_tot'
    ]
    
    # Add temporal features
    df['ano'] = df['semana_epidemiologica'].astype(str).str[:4].astype(int)
    df['semana'] = df['semana_epidemiologica'].astype(str).str[4:].astype(int)
    
    # Create dataset with complete cases (remove rows with NaN in lag features)
    model_df = df[feature_cols + ['casos_notificados', 'geocode', 'ano', 'semana', 'uf', 'nome_municipio', 'semana_epidemiologica']].copy()
    model_df = model_df.dropna()
    
    print(f"📈 Dados para modelagem: {model_df.shape[0]} registros")
    
    # Prepare features and target
    X = model_df[feature_cols + ['ano', 'semana']]
    y = model_df['casos_notificados']
    
    # Split data chronologically (80% train, 20% test)
    # Since we only have 2024 data, split by week: first 80% weeks for train, last 20% for test
    unique_semanas = sorted(model_df['semana_epidemiologica'].unique())
    split_idx = int(len(unique_semanas) * 0.8)
    split_semana = unique_semanas[split_idx]
    
    train_mask = model_df['semana_epidemiologica'] <= split_semana
    
    X_train = X[train_mask]
    X_test = X[~train_mask]
    y_train = y[train_mask]
    y_test = y[~train_mask]
    
    print(f"Treino: {X_train.shape[0]} amostras")
    print(f"Teste: {X_test.shape[0]} amostras")
    
    # Train Random Forest with hyperparameter tuning
    print("🎯 Treinando modelo Random Forest...")
    
    rf_model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    
    rf_model.fit(X_train, y_train)
    
    # Predictions
    print("🔮 Fazendo previsões...")
    y_pred_train = rf_model.predict(X_train)
    y_pred_test = rf_model.predict(X_test)
    
    # Evaluation
    print("📊 Avaliação do Modelo:")
    print("Treino:")
    print(f"  MAE: {mean_absolute_error(y_train, y_pred_train):.2f}")
    print(f"  RMSE: {np.sqrt(mean_squared_error(y_train, y_pred_train)):.2f}")
    print(f"  R²: {r2_score(y_train, y_pred_train):.4f}")
    
    print("Teste:")
    print(f"  MAE: {mean_absolute_error(y_test, y_pred_test):.2f}")
    print(f"  RMSE: {np.sqrt(mean_squared_error(y_test, y_pred_test)):.2f}")
    print(f"  R²: {r2_score(y_test, y_pred_test):.4f}")
    
    # Feature importance
    print("\n🔍 Importância das Features:")
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': rf_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(feature_importance.head(10))
    
    # Save model
    model_path = model_dir / "dengue_predictor_rf.pkl"
    joblib.dump(rf_model, model_path)
    print(f"\n💾 Modelo salvo em: {model_path}")
    
    # Save predictions for analysis
    predictions_df = model_df[~train_mask].copy()
    predictions_df['predicted_cases'] = y_pred_test
    predictions_df['actual_cases'] = y_test
    predictions_df['error'] = predictions_df['actual_cases'] - predictions_df['predicted_cases']
    predictions_df['error_pct'] = (predictions_df['error'] / predictions_df['actual_cases'] * 100).round(2)
    
    predictions_file = model_dir / "model_predictions.parquet"
    predictions_df.to_parquet(predictions_file)
    print(f"Previsões salvas em: {predictions_file}")
    
    con.close()
    print("✅ Modelo preditivo finalizado com sucesso!")

if __name__ == "__main__":
    main()
