"""
Predição de Casos de Dengue

Usa o modelo treinado para fazer predições de casos de dengue.

Uso:
    python predict.py --temp 25.5 --precip 80.0 --semana 10 --populacao 500000
    python predict.py --interactive
"""

import argparse
import json
import joblib
import numpy as np
from pathlib import Path


def get_project_root() -> Path:
    """Detecta o diretório raiz do projeto."""
    script_path = Path(__file__).resolve().parent
    for parent in [script_path] + list(script_path.parents):
        if (parent / 'data').exists() and (parent / 'src').exists():
            return parent
    return Path.cwd()


def load_model(model_dir='models'):
    """Carrega modelo e metadata."""
    model_path = Path(model_dir)
    if not model_path.is_absolute():
        model_path = get_project_root() / model_dir
    
    model = joblib.load(model_path / 'dengue_model.joblib')
    
    with open(model_path / 'model_metadata.json', 'r') as f:
        metadata = json.load(f)
    
    return model, metadata


def prepare_features(temp_media, precip_total, semana, ano, populacao, regiao=3):
    """Prepara features para predição."""
    
    # Encoding cíclico da semana
    semana_sin = np.sin(2 * np.pi * semana / 53)
    semana_cos = np.cos(2 * np.pi * semana / 53)
    
    # Geocode hash fictício (média dos hashes)
    geocode_hash = 500  
    
    # Criar array de features na ordem correta
    features = {
        'inmet_temp_media_lag1': temp_media - 0.5,
        'inmet_temp_media_lag2': temp_media - 1.0,
        'inmet_temp_media_lag3': temp_media - 1.5,
        'inmet_temp_media_lag4': temp_media - 2.0,
        'inmet_precip_tot_lag1': precip_total * 0.9,
        'inmet_precip_tot_lag2': precip_total * 1.1,
        'inmet_precip_tot_lag3': precip_total * 1.2,
        'inmet_precip_tot_lag4': precip_total * 0.8,
        'inmet_temp_media': temp_media,
        'inmet_precip_tot': precip_total,
        'semana_epidemiologica': semana,
        'ano_epidemiologico': ano,
        'semana_sin': semana_sin,
        'semana_cos': semana_cos,
        'geocode_hash': geocode_hash,
        'regiao': regiao,
        'populacao': populacao
    }
    
    return features


def predict(model, feature_order, **kwargs):
    """Faz predição usando o modelo."""
    import pandas as pd
    # Usar DataFrame para preservar nomes das features
    X = pd.DataFrame([{f: kwargs[f] for f in feature_order}])
    
    prediction = model.predict(X)[0]
    return max(0, prediction)  # Não pode ser negativo


def interactive_mode(model, metadata):
    """Modo interativo para fazer predições."""
    print("\n" + "=" * 50)
    print("🔮 Predição de Dengue - Modo Interativo")
    print("=" * 50)
    print("\nDigite os dados climáticos para fazer uma predição.")
    print("(Digite 'sair' para encerrar)\n")
    
    while True:
        try:
            # Coletar inputs
            print("-" * 40)
            temp = input("🌡️  Temperatura média (°C) [ex: 25.5]: ")
            if temp.lower() == 'sair':
                break
            temp = float(temp)
            
            precip = float(input("🌧️  Precipitação total (mm) [ex: 80]: "))
            semana = int(input("📅 Semana epidemiológica (1-53) [ex: 10]: "))
            ano = int(input("📅 Ano [ex: 2024]: "))
            pop = float(input("👥 População do município [ex: 500000]: "))
            
            regiao_str = input("📍 Região (N=0, NE=1, CO=2, SE=3, S=4) [ex: 3]: ")
            regiao = int(regiao_str) if regiao_str else 3
            
            # Preparar e prever
            features = prepare_features(temp, precip, semana, ano, pop, regiao)
            casos = predict(model, metadata['features'], **features)
            
            # Resultado
            print(f"\n🎯 Predição: {casos:.0f} casos de dengue\n")
            
            # Nível de alerta
            if casos < 10:
                print("   ✅ Risco BAIXO")
            elif casos < 50:
                print("   ⚠️  Risco MÉDIO")
            elif casos < 200:
                print("   🟠 Risco ALTO")
            else:
                print("   🔴 Risco CRÍTICO!")
            
        except ValueError as e:
            print(f"❌ Valor inválido: {e}")
        except KeyboardInterrupt:
            break
    
    print("\n👋 Até logo!")


def main():
    parser = argparse.ArgumentParser(description='Predição de casos de dengue')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Modo interativo')
    parser.add_argument('--temp', type=float, help='Temperatura média (°C)')
    parser.add_argument('--precip', type=float, help='Precipitação (mm)')
    parser.add_argument('--semana', type=int, default=1, help='Semana epidemiológica')
    parser.add_argument('--ano', type=int, default=2024, help='Ano')
    parser.add_argument('--populacao', type=float, default=100000, help='População')
    parser.add_argument('--regiao', type=int, default=3, help='Região (0=N, 1=NE, 2=CO, 3=SE, 4=S)')
    parser.add_argument('--model-dir', type=str, default='models', help='Diretório do modelo')
    
    args = parser.parse_args()
    
    # Carregar modelo
    print("📂 Carregando modelo...")
    model, metadata = load_model(args.model_dir)
    print(f"   R² = {metadata['metrics']['test']['r2']:.3f}")
    
    if args.interactive:
        interactive_mode(model, metadata)
    elif args.temp is not None and args.precip is not None:
        # Modo direto
        features = prepare_features(
            args.temp, args.precip, args.semana, args.ano, 
            args.populacao, args.regiao
        )
        casos = predict(model, metadata['features'], **features)
        print(f"\n🎯 Predição: {casos:.0f} casos de dengue")
    else:
        print("\n⚠️  Use --interactive para modo interativo, ou forneça --temp e --precip")
        print("   Exemplo: python predict.py --temp 25.5 --precip 80 --semana 10 --populacao 500000")


if __name__ == '__main__':
    main()
