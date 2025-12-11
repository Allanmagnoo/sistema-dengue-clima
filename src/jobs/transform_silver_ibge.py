import pandas as pd
import logging
import argparse
from pathlib import Path

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_ibge_census_data(source_path, target_path):
    """
    Transforma os dados de Áreas e Densidade do IBGE (Bronze) para Silver (Parquet).
    """
    file_path = source_path / "Instituto Brasileiro de Geografia e Estatística" / "Area_efetivamente_domiciliada_e_densidade_ajustada_dos_Setores_Censitarios.xlsx"
    
    if not file_path.exists():
        logger.error(f"Arquivo não encontrado: {file_path}")
        return

    logger.info(f"Lendo arquivo: {file_path.name}")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"Erro ao ler Excel: {e}")
        return

    # Mapeamento de colunas baseado no dicionário de dados
    col_map = {
        'CD_SETOR': 'id_setor_censitario',
        'CD_SIT': 'codigo_situacao_setor',
        'CD_TIPO': 'codigo_tipo_setor',
        'v0001': 'populacao_total',
        'v0002': 'domicilios_total',
        'v0003': 'domicilios_particulares',
        'v0004': 'domicilios_coletivos',
        'v0005': 'media_moradores_domicilio',
        'v0006': 'percentual_domicilios_imputados',
        'v0007': 'domicilios_particulares_ocupados',
        'AREA_DOMICILIADA_KM2': 'area_domiciliada_km2',
        'AREA_KM2': 'area_total_km2',
        'DENSIDADE_DEMOGRAFICA_DOMICILIADA_HAB_KM2': 'densidade_domiciliada',
        'DENSIDADE_DEMOGRAFICA_SETOR_HAB_KM2': 'densidade_geral'
    }

    # Renomear
    df.rename(columns=col_map, inplace=True)
    
    # Padronizar nomes restantes para lowercase
    df.columns = [c.lower() for c in df.columns]

    # Converter ID para string (geocódigo)
    if 'id_setor_censitario' in df.columns:
        df['id_setor_censitario'] = df['id_setor_censitario'].astype(str)

    # Criar diretório destino
    target_dir = target_path / "ibge"
    target_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = target_dir / "censo_agregado_2022.parquet"
    
    logger.info(f"Salvando em: {output_file}")
    df.to_parquet(output_file, index=False)
    logger.info("Transformação concluída com sucesso.")

def main():
    parser = argparse.ArgumentParser(description="Transforma dados do Censo IBGE 2022")
    parser.add_argument("--source", default=r"d:\_data-science\GitHub\sistema-dengue-clima\data\bronze", help="Diretório Bronze")
    parser.add_argument("--target", default=r"d:\_data-science\GitHub\sistema-dengue-clima\data\silver", help="Diretório Silver")
    args = parser.parse_args()

    source_path = Path(args.source)
    target_path = Path(args.target)

    transform_ibge_census_data(source_path, target_path)

if __name__ == "__main__":
    main()
