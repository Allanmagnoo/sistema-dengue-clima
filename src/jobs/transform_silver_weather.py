import pandas as pd
import glob
import os
from pathlib import Path
import logging
import argparse

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_inmet_header(file_path):
    """
    Lê as primeiras 8 linhas do arquivo INMET para extrair metadados.
    Retorna um dicionário com os metadados.
    """
    metadata = {}
    try:
        with open(file_path, 'r', encoding='latin1') as f:
            for i in range(8):
                line = f.readline().strip()
                if not line:
                    break
                # Formato esperado: "CHAVE:;VALOR" ou "CHAVE:;VALOR"
                parts = line.split(';')
                if len(parts) >= 2:
                    key = parts[0].replace(':', '').strip()
                    value = parts[1].strip()
                    metadata[key] = value
    except Exception as e:
        logger.warning(f"Erro ao ler cabeçalho de {file_path}: {e}")
    return metadata

def clean_column_name(name):
    """Limpa e padroniza nomes de colunas."""
    return (name.strip()
            .lower()
            .replace(' ', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('.', '')
            .replace(',', '')
            .replace('/', '_')
            .replace('ç', 'c')
            .replace('ã', 'a')
            .replace('á', 'a')
            .replace('é', 'e')
            .replace('í', 'i')
            .replace('ó', 'o')
            .replace('ú', 'u')
            .replace('º', '')
           )

def process_inmet_file(file_path):
    """
    Processa um único arquivo CSV do INMET.
    Retorna um DataFrame limpo com metadados adicionados.
    """
    try:
        # Extrair metadados
        metadata = parse_inmet_header(file_path)
        
        # Ler dados (pula 8 linhas de metadados)
        # encoding latin1 é padrão para INMET
        # sep=';' e decimal=','
        df = pd.read_csv(
            file_path, 
            encoding='latin1', 
            sep=';', 
            decimal=',', 
            skiprows=8,
            on_bad_lines='skip' # Evita erro se houver linhas mal formadas
        )
        
        # Remover coluna "Unnamed" que aparece devido ao ; final
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Padronizar colunas
        df.columns = [clean_column_name(c) for c in df.columns]
        
        # Adicionar metadados como colunas constantes
        # Mapeamento de chaves do header para colunas
        meta_map = {
            'UF': 'uf',
            'ESTACAO': 'estacao',
            'CODIGO WMO': 'codigo_estacao',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude',
            'ALTITUDE': 'altitude',
            'DATA DE FUNDACAO': 'data_fundacao'
        }
        
        for file_key, col_name in meta_map.items():
            # Tenta encontrar a chave no metadata (pode variar ligeiramente)
            val = None
            for k, v in metadata.items():
                if file_key in k.upper():
                    val = v
                    break
            
            if val:
                # Tratamento específico para Lat/Lon/Alt que vêm com vírgula
                if col_name in ['latitude', 'longitude', 'altitude']:
                    try:
                        val = float(val.replace(',', '.'))
                    except:
                        pass
                df[col_name] = val
            else:
                df[col_name] = None

        # Criar coluna de data/hora combinada
        # Colunas esperadas: 'data' (YYYY/MM/DD) e 'hora_utc' (HHMM UTC)
        if 'data' in df.columns and 'hora_utc' in df.columns:
            # Limpar hora (remover " UTC")
            df['hora_utc_clean'] = df['hora_utc'].astype(str).str.replace(' UTC', '').str.zfill(4)
            # Converter data
            df['data_clean'] = df['data'].astype(str).str.replace('/', '-')
            
            # Tentar criar datetime
            try:
                df['data_hora'] = pd.to_datetime(
                    df['data_clean'] + ' ' + df['hora_utc_clean'], 
                    format='%Y-%m-%d %H%M',
                    errors='coerce'
                )
            except Exception as e:
                logger.warning(f"Erro conversão data em {file_path}: {e}")
        
        return df

    except Exception as e:
        logger.error(f"Falha ao processar arquivo {file_path}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Transforma dados Bronze INMET para Silver")
    parser.add_argument("--source", default=r"d:\_data-science\GitHub\sistema-dengue-clima\data\bronze\inmet", help="Diretório Bronze INMET")
    parser.add_argument("--target", default=r"d:\_data-science\GitHub\sistema-dengue-clima\data\silver\inmet", help="Diretório Silver Destino")
    args = parser.parse_args()

    source_path = Path(args.source)
    target_path = Path(args.target)
    target_path.mkdir(parents=True, exist_ok=True)

    # Encontrar todos os CSVs recursivamente
    all_files = list(source_path.rglob("*.CSV")) + list(source_path.rglob("*.csv"))
    logger.info(f"Encontrados {len(all_files)} arquivos para processar.")

    if not all_files:
        logger.warning("Nenhum arquivo encontrado.")
        return

    # Processar em batches (por exemplo, por ano, ou simplesmente acumular e salvar)
    # Como são muitos arquivos pequenos, podemos agrupar e salvar um parquet por ano ou estado.
    # Vamos tentar agrupar todos e salvar particionado por ANO.
    
    dfs = []
    processed_count = 0
    
    for i, file_path in enumerate(all_files):
        df = process_inmet_file(file_path)
        if df is not None and not df.empty:
            dfs.append(df)
            processed_count += 1
        
        if (i + 1) % 50 == 0:
            logger.info(f"Processados {i + 1}/{len(all_files)} arquivos...")

    if not dfs:
        logger.warning("Nenhum dado processado com sucesso.")
        return

    logger.info("Concatenando DataFrames...")
    full_df = pd.concat(dfs, ignore_index=True)

    # Garantir tipos
    # Converter colunas numéricas que podem estar como object
    numeric_cols = [c for c in full_df.columns if c not in ['data', 'hora_utc', 'data_hora', 'uf', 'estacao', 'codigo_estacao', 'data_fundacao']]
    for col in numeric_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')

    # Extrair ano para partição (se data_hora existir)
    if 'data_hora' in full_df.columns:
        full_df['ano'] = full_df['data_hora'].dt.year
    else:
        # Fallback se data_hora falhar
        full_df['ano'] = 0

    # Salvar em Parquet
    output_file = target_path / "clima_brasil_unificado.parquet"
    logger.info(f"Salvando resultado em {output_file}...")
    
    # Salvar particionado pode ser melhor se for muito grande, mas um arquivo único parquet costuma lidar bem com ~alguns GBs.
    # Vamos salvar particionado por ano se houver dados de anos diferentes.
    try:
        full_df.to_parquet(target_path, partition_cols=['ano'], index=False)
        logger.info("Processamento concluído com sucesso (particionado por ano).")
    except Exception as e:
        logger.error(f"Erro ao salvar parquet: {e}")
        # Tenta salvar sem particionar
        full_df.to_parquet(output_file, index=False)
        logger.info("Processamento concluído (arquivo único).")

if __name__ == "__main__":
    main()
