import pytest
import duckdb
import pandas as pd
import os
from pathlib import Path

# Fixture para criar arquivos temporários
@pytest.fixture
def temp_csv_files(tmp_path):
    dengue_dir = tmp_path / "dengue"
    dengue_dir.mkdir()
    dengue_file = dengue_dir / "3550308.csv"

    # Conteúdo do CSV de Dengue (Separado por vírgula, padrão)
    dengue_content = """data_iniSE,SE,casos_est,casos_est_min,casos_est_max,casos,p_rt1,p_inc100k,Localidade_id,nivel,id,versao_modelo,tweet,Rt,pop,tempmin,umidmax,receptivo,transmissao,nivel_inc,umidmed,umidmin,tempmed,tempmax,casprov,casprov_est,casprov_est_min,casprov_est_max,casconf,notif_accum_year
2024-01-01,202401,10.5,5.0,15.0,10,0.5,12.5,3550308,1,12345,v1,test,1.2,12000000,20.0,80.0,1,1,1,70.0,60.0,25.0,30.0,5,5.5,2.0,8.0,8,100"""
    dengue_file.write_text(dengue_content, encoding='utf-8')

    inmet_dir = tmp_path / "inmet"
    inmet_dir.mkdir()
    inmet_file = inmet_dir / "INMET_SE_SP_A701_SAO PAULO_01-01-2024_A_31-12-2024.CSV"

    # Conteúdo do CSV do INMET (Separado por ponto e vírgula, pula 9 linhas, Latin-1)
    # Metadados do cabeçalho (9 linhas)
    inmet_header = """REGIAO:;SE
UF:;SP
ESTACAO:;SAO PAULO
CODIGO (WMO):;A701
LATITUDE:;-23.5
LONGITUDE:;-46.6
ALTITUDE:;760
DATA DE FUNDACAO:;01/01/2000
Data;Hora UTC;Precipitacao;TempMax;TempMin;Insolacao;Evaporacao;Temp Comp Media;Umidade Relativa Media;Velocidade Vento Media;
"""
    # Linhas de dados (Separado por ponto e vírgula, vírgula para decimais)
    # colunas são genéricas column00...column20
    # column00=Data, column01=Hora, column02=Precip, column07=Temp
    inmet_data = "2024/01/01;0000;0,2;0;0;0;0;25,5;0;0;0;0;0;0;0;80,0;0;0;0;0;0"

    inmet_content = inmet_header + inmet_data
    inmet_file.write_text(inmet_content, encoding='latin-1')

    return {"dengue": str(dengue_file), "inmet": str(inmet_file)}

def test_dengue_ingestion(temp_csv_files):
    """
    Verifica se a leitura do CSV de Dengue lida corretamente com colunas e tipos.
    Simula a lógica de src/jobs/transform_silver_dengue.py
    """
    con = duckdb.connect(database=':memory:')
    file_path = temp_csv_files["dengue"]

    # Query simplificada baseada em transform_silver_dengue.py
    query = f"""
        SELECT *
        FROM read_csv('{file_path}',
            header=True,
            filename=True,
            columns={{
                'data_iniSE': 'DATE',
                'SE': 'INTEGER',
                'casos_est': 'DOUBLE',
                'casos_est_min': 'DOUBLE',
                'casos_est_max': 'DOUBLE',
                'casos': 'VARCHAR',
                'p_rt1': 'DOUBLE',
                'p_inc100k': 'DOUBLE',
                'Localidade_id': 'INTEGER',
                'nivel': 'INTEGER',
                'id': 'BIGINT',
                'versao_modelo': 'VARCHAR',
                'tweet': 'VARCHAR',
                'Rt': 'DOUBLE',
                'pop': 'DOUBLE',
                'tempmin': 'DOUBLE',
                'umidmax': 'DOUBLE',
                'receptivo': 'INTEGER',
                'transmissao': 'INTEGER',
                'nivel_inc': 'INTEGER',
                'umidmed': 'DOUBLE',
                'umidmin': 'DOUBLE',
                'tempmed': 'DOUBLE',
                'tempmax': 'DOUBLE',
                'casprov': 'VARCHAR',
                'casprov_est': 'DOUBLE',
                'casprov_est_min': 'DOUBLE',
                'casprov_est_max': 'DOUBLE',
                'casconf': 'VARCHAR',
                'notif_accum_year': 'VARCHAR'
            }}
        )
    """

    df = con.execute(query).df()

    assert not df.empty
    assert 'data_iniSE' in df.columns
    assert 'SE' in df.columns
    # DuckDB retorna Timestamp para tipo DATE no Pandas
    assert df['data_iniSE'].iloc[0] == pd.Timestamp('2024-01-01')
    assert df['SE'].iloc[0] == 202401
    assert df['casos'].iloc[0] == '10'  # Lido como VARCHAR
    assert df['Localidade_id'].iloc[0] == 3550308

def test_inmet_ingestion_mixed_separators(temp_csv_files):
    """
    Verifica se a leitura do CSV do INMET lida com separador ponto e vírgula, pula linhas,
    e lida com encoding Latin-1 + vírgulas decimais.
    Simula a lógica de src/jobs/transform_silver_inmet.py
    """
    con = duckdb.connect(database=':memory:')
    file_path = temp_csv_files["inmet"]

    # Lógica de transform_silver_inmet.py
    # Lendo tudo como VARCHAR primeiro
    query = f"""
        SELECT *
        FROM read_csv(['{file_path}'],
            header=False,
            skip=9,
            sep=';',
            filename=True,
            encoding='latin-1',
            all_varchar=True,
            auto_detect=False,
            null_padding=True,
            ignore_errors=True,
            columns={{
                'column00': 'VARCHAR', 'column01': 'VARCHAR', 'column02': 'VARCHAR',
                'column03': 'VARCHAR', 'column04': 'VARCHAR', 'column05': 'VARCHAR',
                'column06': 'VARCHAR', 'column07': 'VARCHAR', 'column08': 'VARCHAR',
                'column09': 'VARCHAR', 'column10': 'VARCHAR', 'column11': 'VARCHAR',
                'column12': 'VARCHAR', 'column13': 'VARCHAR', 'column14': 'VARCHAR',
                'column15': 'VARCHAR', 'column16': 'VARCHAR', 'column17': 'VARCHAR',
                'column18': 'VARCHAR', 'column19': 'VARCHAR', 'column20': 'VARCHAR'
            }}
        )
    """

    raw_df = con.execute(query).df()
    assert not raw_df.empty

    # Verificar valores brutos (vírgulas devem estar presentes)
    assert raw_df['column00'].iloc[0] == '2024/01/01'
    assert raw_df['column02'].iloc[0] == '0,2'
    assert raw_df['column07'].iloc[0] == '25,5'

    # Testar Lógica de Transformação
    transform_query = """
    SELECT
        try_cast(strptime(column00, '%Y/%m/%d') as DATE) as data_medicao,
        try_cast(substring(column01, 1, 4) as INTEGER) as hora_utc,
        try_cast(replace(column02, ',', '.') as DOUBLE) as precipitacao_mm,
        try_cast(replace(column07, ',', '.') as DOUBLE) as temperatura_c,
        try_cast(replace(column15, ',', '.') as DOUBLE) as umidade_relativa_percent
    FROM raw_df
    """

    con.register('raw_df', raw_df)
    transformed_df = con.execute(transform_query).df()

    # DuckDB retorna Timestamp para tipo DATE no Pandas
    assert transformed_df['data_medicao'].iloc[0] == pd.Timestamp('2024-01-01')
    assert transformed_df['precipitacao_mm'].iloc[0] == 0.2
    assert transformed_df['temperatura_c'].iloc[0] == 25.5
    assert transformed_df['umidade_relativa_percent'].iloc[0] == 80.0
