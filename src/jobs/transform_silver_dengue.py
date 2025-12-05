"""
Silver Layer Transformation - Dengue Data (DuckDB Version)

Transforms Bronze CSV data into Silver Parquet format using DuckDB.
- Reads raw CSVs from data/bronze/infodengue/municipios
- Enriches with municipality metadata (UF, Name) from DTB
- Standardizes schema and types
- Writes partitioned Parquet to data/silver/infodengue
"""
import sys
import os
import logging
import duckdb
import pandas as pd
from pathlib import Path
import argparse
import re

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.dtb_reader import DTBReader
from src.jobs.renaming_utils import rename_parquet_recursive

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_duckdb(year_start=2020, year_end=2025):
    logger.info("🚀 Starting Silver Layer Transformation (DuckDB)")
    
    # Paths
    # Determine project root relative to this script
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    
    BRONZE_PATH = base_dir / "data/bronze/infodengue/municipios/disease=dengue"
    SILVER_PATH = base_dir / "data/silver/silver_dengue"
    
    # 1. Load Municipality Metadata
    logger.info("📖 Loading Municipality Metadata...")
    dtb_reader = DTBReader()
    try:
        municipios_list = dtb_reader.get_all_municipios()
        
        # Convert to Pandas DataFrame for DuckDB registration
        df_mun = pd.DataFrame(municipios_list)
        
        # UF Mapping
        uf_map = {
            11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
            21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL', 28: 'SE', 29: 'BA',
            31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
            41: 'PR', 42: 'SC', 43: 'RS',
            50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
        }
        
        # Add UF column based on first 2 digits of geocode
        df_mun['uf_code'] = df_mun['geocode'].astype(str).str[:2].astype(int)
        df_mun['uf'] = df_mun['uf_code'].map(uf_map)
        
        logger.info(f"✅ Loaded {len(df_mun)} municipalities metadata")
        
    except Exception as e:
        logger.error(f"❌ Error loading metadata: {e}")
        sys.exit(1)

    # 2. Initialize DuckDB
    con = duckdb.connect(database=':memory:')
    
    # Register metadata table
    con.register('municipios', df_mun)
    
    # 3. Read Bronze Data (Recursive CSV read)
    logger.info(f"📖 Reading Bronze Data from {BRONZE_PATH}...")
    
    try:
        # Use CREATE TABLE instead of VIEW to materialize data and avoid complex query crash
        parts = []
        for y in range(year_start, year_end + 1):
            parts.append(f"""
            SELECT *,
                   {y} AS year_partition,
                   try_cast(NULLIF(regexp_extract(filename, '([0-9]{{7}})', 1), '') AS BIGINT) as geocode_from_filename
            FROM read_csv('{BRONZE_PATH}/year={y}/*.csv',
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
            """)
        union_sql = "\nUNION ALL\n".join(parts)
        query_create_table = f"CREATE TABLE raw_dengue_table AS {union_sql}"
        logger.info("Creating materialized table 'raw_dengue_table'...")
        con.execute(query_create_table)
        
        count = con.execute("SELECT COUNT(*) FROM raw_dengue_table").fetchone()[0]
        logger.info(f"✅ Bronze data table created. Total rows: {count}")
        cnt_sel = con.execute(f"SELECT COUNT(*) FROM raw_dengue_table WHERE geocode_from_filename IS NOT NULL AND year_partition BETWEEN {year_start} AND {year_end} AND (SE % 100) BETWEEN 1 AND 53").fetchone()[0]
        yr_minmax = con.execute("SELECT MIN(year_partition), MAX(year_partition) FROM raw_dengue_table").fetchone()
        logger.info(f"📊 Raw selected rows (filter+geocode): {cnt_sel}")
        logger.info(f"📅 Raw year range: {yr_minmax[0]}–{yr_minmax[1]}")
        
        # Materialize municipios table to avoid Pandas-DuckDB bridge issues during complex join
        logger.info("Materializing 'municipios_table' with proper types...")
        con.execute("CREATE TABLE municipios_table AS SELECT try_cast(geocode AS BIGINT) AS geocode, nome, uf FROM municipios")
        
    except Exception as e:
        logger.error(f"❌ Error reading bronze data: {e}")
        sys.exit(1)

    # 4. Transform and Enrich
    logger.info("🔄 Transforming and Enriching...")
    
    query_transform = f"""
    SELECT 
        COALESCE(NULLIF(r.Localidade_id, 0), r.geocode_from_filename) as geocode,
        CASE CAST(substr(CAST(r.geocode_from_filename AS VARCHAR),1,2) AS INTEGER)
            WHEN 11 THEN 'RO' WHEN 12 THEN 'AC' WHEN 13 THEN 'AM' WHEN 14 THEN 'RR' WHEN 15 THEN 'PA' WHEN 16 THEN 'AP' WHEN 17 THEN 'TO'
            WHEN 21 THEN 'MA' WHEN 22 THEN 'PI' WHEN 23 THEN 'CE' WHEN 24 THEN 'RN' WHEN 25 THEN 'PB' WHEN 26 THEN 'PE' WHEN 27 THEN 'AL' WHEN 28 THEN 'SE' WHEN 29 THEN 'BA'
            WHEN 31 THEN 'MG' WHEN 32 THEN 'ES' WHEN 33 THEN 'RJ' WHEN 35 THEN 'SP'
            WHEN 41 THEN 'PR' WHEN 42 THEN 'SC' WHEN 43 THEN 'RS'
            WHEN 50 THEN 'MS' WHEN 51 THEN 'MT' WHEN 52 THEN 'GO' WHEN 53 THEN 'DF'
            ELSE NULL END AS uf,
        m.nome as nome_municipio,
        r.data_iniSE as data_inicio_semana,
        (r.SE % 100) as semana_epidemiologica,
        try_cast(NULLIF(r.casos, '') as INTEGER) as casos_notificados,
        r.casos_est as casos_estimados,
        r.casos_est_min as casos_estimados_min,
        r.casos_est_max as casos_estimados_max,
        try_cast(NULLIF(r.casconf, '') as INTEGER) as casos_confirmados,
        try_cast(NULLIF(r.notif_accum_year, '') as INTEGER) as notificacoes_acumuladas,
        r.p_inc100k as incidencia_100k,
        r.nivel as nivel_alerta,
        r.receptivo as condicoes_receptivas,
        r.transmissao as evidencia_transmissao,
        r.pop as populacao,
        r.tempmed as temperatura_media,
        r.umidmed as umidade_media,
        try_cast(NULLIF(r.casprov, '') as INTEGER) as casos_provaveis,
        r.casprov_est as casos_provaveis_estimados,
        r.casprov_est_min as casos_provaveis_estimados_min,
        r.casprov_est_max as casos_provaveis_estimados_max,
        r.year_partition as ano_epidemiologico,
        'InfoDengue' as fonte_dados,
        now() as data_extracao
    FROM raw_dengue_table r
    LEFT JOIN municipios_table m ON COALESCE(NULLIF(r.Localidade_id, 0), r.geocode_from_filename) = m.geocode
    WHERE COALESCE(NULLIF(r.Localidade_id, 0), r.geocode_from_filename) IS NOT NULL
      AND r.year_partition BETWEEN {year_start} AND {year_end}
      AND (r.SE % 100) BETWEEN 1 AND 53
    """
    
    # Materialize the final result first to check if JOIN crashes
    logger.info("Materializing 'silver_dengue_table'...")
    con.execute(f"CREATE TABLE silver_dengue_table AS {query_transform}")
    
    count_silver = con.execute("SELECT COUNT(*) FROM silver_dengue_table").fetchone()[0]
    logger.info(f"✅ Silver data prepared. Total rows: {count_silver}")

    # 4.1 Data Quality Assertions
    logger.info("🔍 Running Data Quality Assertions...")
    
    dq_checks = [
        ("Total rows > 0", "SELECT COUNT(*) > 0 FROM silver_dengue_table"),
        ("No NULL Geocodes", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE geocode IS NULL"),
        ("No NULL Data Inicio Semana", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE data_inicio_semana IS NULL"),
        ("No NULL UF", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE uf IS NULL"),
        ("Valid UF Length", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE length(uf) != 2"),
        ("Non-negative Notified Cases", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE casos_notificados < 0"),
        ("Valid Week Range", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE semana_epidemiologica < 1 OR semana_epidemiologica > 53"),
        ("Valid Year Range", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE ano_epidemiologico < 2020 OR ano_epidemiologico > 2025"),
        ("Year matches date (tolerant)", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE NOT (year(data_inicio_semana) = ano_epidemiologico OR year(data_inicio_semana) = ano_epidemiologico - 1)"),
        ("No duplicates (geocode,date)", "SELECT COUNT(*) = 0 FROM (SELECT geocode, data_inicio_semana, COUNT(*) AS c FROM silver_dengue_table WHERE data_inicio_semana IS NOT NULL GROUP BY 1,2 HAVING c > 1) t"),
        ("Year Partition Logic", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE ano_epidemiologico IS NULL OR ano_epidemiologico < 2000 OR ano_epidemiologico > 2100")
    ]
    
    failed_assertions = []
    for check_name, query in dq_checks:
        try:
            result = con.execute(query).fetchone()[0]
            if result:
                logger.info(f"✅ Assertion Passed: {check_name}")
            else:
                logger.error(f"❌ Assertion Failed: {check_name}")
                failed_assertions.append(check_name)
        except Exception as e:
            logger.error(f"❌ Assertion Error ({check_name}): {e}")
            failed_assertions.append(f"{check_name} (Error)")
            
    if failed_assertions:
        logger.error(f"🚨 Data Quality Checks Failed: {', '.join(failed_assertions)}")
    else:
        logger.info("✨ All Data Quality Checks Passed!")

    # 5. Write to Parquet (Partitioned)
    logger.info(f"💾 Writing to Silver Layer: {SILVER_PATH}...")
    
    # Ensure output directory exists (DuckDB creates it, but good practice)
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    copy_query = f"""
    COPY silver_dengue_table TO '{SILVER_PATH}' (
        FORMAT PARQUET, 
        PARTITION_BY (uf, ano_epidemiologico),
        OVERWRITE_OR_IGNORE 1,
        COMPRESSION 'SNAPPY'
    )
    """
    
    con.execute(copy_query)
    
    # Rename files to semantic convention
    rename_parquet_recursive(SILVER_PATH, "silver_dengue")
    
    logger.info("✅ Silver Layer Transformation Complete!")

def run_rapids(year_start=2020, year_end=2025):
    logger.info("🚀 Starting Silver Layer Transformation (RAPIDS)")
    try:
        import cudf
        import dask_cudf
        import dask
    except Exception as e:
        logger.error(f"❌ RAPIDS not available: {e}")
        return False
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    BRONZE_PATH = base_dir / "data/bronze/infodengue/municipios/disease=dengue"
    SILVER_PATH = base_dir / "data/silver/silver_dengue"
    years = list(range(year_start, year_end + 1))
    files = []
    for y in years:
        files.extend(list((BRONZE_PATH / f"year={y}").glob("*.csv")))
    if not files:
        logger.error("❌ No bronze files found for selected years")
        return False
    dtypes = {
        "data_iniSE":"str","SE":"int32","casos":"str","casos_est":"float64","casos_est_min":"float64","casos_est_max":"float64",
        "p_inc100k":"float64","nivel":"int32","receptivo":"int32","transmissao":"int32","pop":"float64","tempmed":"float64","umidmed":"float64",
        "casprov":"str","casconf":"str","notif_accum_year":"str"
    }
    dd = dask_cudf.read_csv([str(p) for p in files], dtype=dtypes, assume_missing=True, include_path_column="path")
    dd["year_partition"] = dd["path"].str.extract(r"year=([0-9]+)", expand=False).astype("int32")
    dd["geocode_from_filename"] = dd["path"].str.extract(r"([0-9]+)\.csv$", expand=False).astype("int64")
    dd["data_inicio_semana"] = cudf.to_datetime(dd["data_iniSE"], errors="coerce")
    
    # Fix SE to week number
    dd["semana_epidemiologica"] = dd["SE"] % 100
    dd = dd[(dd["semana_epidemiologica"] >= 1) & (dd["semana_epidemiologica"] <= 53) & (dd["year_partition"] >= year_start) & (dd["year_partition"] <= year_end)]
    
    dtb_reader = DTBReader()
    df_mun = pd.DataFrame(dtb_reader.get_all_municipios())
    df_mun["uf_code"] = df_mun["geocode"].astype(str).str[:2].astype(int)
    uf_map = {
        11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
        21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
        31: "MG", 32: "ES", 33: "RJ", 35: "SP",
        41: "PR", 42: "SC", 43: "RS",
        50: "MS", 51: "MT", 52: "GO", 53: "DF"
    }
    df_mun["uf"] = df_mun["uf_code"].map(uf_map)
    gdf_mun = cudf.DataFrame.from_pandas(df_mun[["geocode","uf","nome"]])
    gdf_mun["geocode"] = gdf_mun["geocode"].astype("int64")
    dd = dd.merge(gdf_mun, left_on="geocode_from_filename", right_on="geocode", how="inner")
    dd["casos_notificados"] = cudf.to_numeric(dd["casos"], errors="coerce").fillna(0).astype("int32")
    dd["ano_epidemiologico"] = dd["year_partition"].astype("int32")
    dd = dd.rename(columns={"nome":"nome_municipio","umidmed":"umidade_media","tempmed":"temperatura_media"})
    total = int(dd.shape[0].compute())
    if total == 0:
        logger.error("❌ No rows after RAPIDS transform")
        return False
    os.makedirs(SILVER_PATH, exist_ok=True)
    dd = dd[[
        "geocode_from_filename","uf","nome_municipio","data_inicio_semana","semana_epidemiologica","casos_notificados","casos_est","casos_est_min","casos_est_max",
        "casconf","notif_accum_year","p_inc100k","nivel","receptivo","transmissao","pop","temperatura_media","umidade_media",
        "casprov","casprov_est","casprov_est_min","casprov_est_max","ano_epidemiologico"
    ]]
    dd = dd.rename(columns={"geocode_from_filename":"geocode"})
    dd.to_parquet(str(SILVER_PATH), compression="snappy", partition_on=["uf","ano_epidemiologico"])
    
    # Rename files to semantic convention
    rename_parquet_recursive(SILVER_PATH, "silver_dengue")
    
    logger.info("✅ RAPIDS Silver Layer Transformation Complete!")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["duckdb","rapids"], default="duckdb")
    parser.add_argument("--year-start", type=int, default=2020)
    parser.add_argument("--year-end", type=int, default=2025)
    args = parser.parse_args()
    if args.engine == "rapids":
        ok = run_rapids(args.year_start, args.year_end)
        if not ok:
            logger.info("ℹ️ Falling back to DuckDB")
            run_duckdb(args.year_start, args.year_end)
    else:
        run_duckdb(args.year_start, args.year_end)

if __name__ == "__main__":
    main()
