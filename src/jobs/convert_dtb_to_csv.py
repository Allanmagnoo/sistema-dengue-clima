
import pandas as pd
from pathlib import Path
import os

def convert_files_to_csv(source_dir: str, dest_dir: str):
    """
    Converte todos os arquivos .xls e .ods de um diretório de origem para CSV,
    salvando-os em um diretório de destino.

    Args:
        source_dir (str): O caminho para o diretório contendo os arquivos a serem convertidos.
        dest_dir (str): O caminho para o diretório onde os arquivos CSV serão salvos.
    """
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)

    # 1. Garante que o diretório de destino exista
    dest_path.mkdir(parents=True, exist_ok=True)
    print(f"Diretório de destino '{dest_path}' pronto.")

    files_to_convert = list(source_path.glob('*.xls')) + list(source_path.glob('*.ods'))

    if not files_to_convert:
        print(f"Nenhum arquivo .xls ou .ods encontrado em '{source_path}'.")
        return

    print(f"Encontrados {len(files_to_convert)} arquivos para conversão...")

    for file_path in files_to_convert:
        try:
            # 2. Lê o arquivo de origem (xls ou ods)
            if file_path.suffix == '.xls':
                # Para arquivos .xls, pode ser necessário o engine 'xlrd'
                df = pd.read_excel(file_path, engine='xlrd')
            elif file_path.suffix == '.ods':
                # Para arquivos .ods, é necessário o engine 'odf'
                df = pd.read_excel(file_path, engine='odf')
            else:
                continue

            # 3. Define o nome do arquivo de saída
            csv_file_name = file_path.stem + '.csv'
            csv_file_path = dest_path / csv_file_name

            # 4. Salva o DataFrame como CSV com codificação UTF-8
            #    - Cabeçalhos são preservados por padrão.
            #    - Valores nulos (NaN em pandas) são salvos como strings vazias.
            df.to_csv(csv_file_path, index=False, encoding='utf-8')

            print(f"SUCESSO: '{file_path.name}' convertido para '{csv_file_path.name}'")
            # 5. Documentação da transformação:
            #    - Nenhuma transformação de dados foi aplicada, apenas a conversão de formato.
            #    - A estrutura original, cabeçalhos e dados foram mantidos.
            #    - Valores ausentes são representados como campos vazios no CSV.

        except Exception as e:
            print(f"ERRO ao converter '{file_path.name}': {e}")

if __name__ == '__main__':
    # Define os diretórios de origem e destino
    base_dir = Path(__file__).resolve().parents[2]
    source_directory = base_dir / 'data' / 'bronze' / 'DTB_2024'
    destination_directory = source_directory / 'csv'

    # Executa a função de conversão
    convert_files_to_csv(str(source_directory), str(destination_directory))
