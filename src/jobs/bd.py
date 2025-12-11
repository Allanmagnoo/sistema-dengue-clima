"""
Carregamento de arquivos Parquet (Bronze/Silver) para PostgreSQL

Requisitos de configuração do PostgreSQL:
- Variáveis de ambiente (.env ou sistema):
  - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
- Banco acessível via rede e usuário com permissão de CREATE TABLE e INSERT

Dependências necessárias (pip):
- python-dotenv, SQLAlchemy, psycopg2-binary, pandas, pyarrow, tqdm

Instruções de execução:
- Execução completa incremental: `python src/jobs/bd.py --mode incremental --layers silver,bronze`
- Apenas descoberta (dry-run): `python src/jobs/bd.py --dry-run --layers silver`
- Base de dados alternativa: `python src/jobs/bd.py --base-path d:/_data-science/GitHub/sistema-dengue-clima/data`

Configurações específicas:
- Descobre automaticamente arquivos `.parquet` em `data/<layer>/<dataset>` e inclui partições do caminho
- Cria/atualiza tabelas por dataset: `silver_<dataset>` e `bronze_<dataset>`
- Log de ingestão incremental em tabela `ingest_log_parquet`
"""

import os
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
import argparse

import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
# from dotenv import load_dotenv  # Removed in favor of common loader
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import (
    Integer, BigInteger, Float, Numeric, Boolean, Date, DateTime, String, Text
)

# Import custom env loader
import sys
# Add src to path to allow imports if running as script
src_path = Path(__file__).resolve().parent.parent
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

from common.env_loader import load_secure_env

load_secure_env()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def arrow_to_sqlalchemy(arrow_type: pa.DataType):
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
        return Integer()
    if pa.types.is_int64(arrow_type):
        return BigInteger()
    if pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type):
        return Float()
    if pa.types.is_float64(arrow_type):
        return Float()
    if pa.types.is_boolean(arrow_type):
        return Boolean()
    if pa.types.is_timestamp(arrow_type) or pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        return DateTime()
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return Date()
    if pa.types.is_decimal(arrow_type):
        try:
            return Numeric(precision=arrow_type.precision or 38, scale=arrow_type.scale or 9)
        except Exception:
            return Numeric()
    return Text()


def normalize_column(name: str) -> str:
    name = name.strip().replace(" ", "_")
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    return name.lower()


def parse_file_info(p: Path):
    parts = [str(x) for x in p.parts]
    try:
        data_idx = parts.index("data")
    except ValueError:
        return None
    layer = parts[data_idx + 1] if len(parts) > data_idx + 1 else None
    
    # Identify dataset
    if len(parts) > data_idx + 2:
        dataset = parts[data_idx + 2]
        # Special handling for Gold panels or files in generic folders
        if layer == 'gold' and dataset in ['paineis', 'reports', 'dashboards']:
            dataset = p.stem
        # Special handling for files directly in layer root (e.g. data/bronze/file.csv)
        if dataset == p.name:
            dataset = p.stem
    else:
        dataset = p.stem

    table_name = f"{layer}_{dataset}" if layer else dataset
    partitions = {}
    for seg in parts[data_idx + 3:]:
        if "=" in seg:
            k, v = seg.split("=", 1)
            partitions[normalize_column(k)] = v
    return {
        "layer": layer,
        "dataset": dataset,
        "table": normalize_column(table_name),
        "partitions": partitions,
    }


class PostgresLoader:
    def __init__(self, base_path: Path, layers: list, dry_run: bool, batch_size: int, schema: str = "public"):
        self.base_path = base_path
        self.layers = set(layers)
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.schema = schema
        self.host = os.getenv("DB_HOST") or "localhost"
        if str(self.host).lower() == "none":
            self.host = "localhost"
            
        self.user = os.getenv("DB_USER") or "postgres"
        self.password = os.getenv("DB_PASSWORD") or "123456"
        self.dbname = os.getenv("DB_NAME") or "postgres"
        self.port = os.getenv("DB_PORT") or "5432"
        self.db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        self.engine = None
        self.verified_tables = set()

    def connect(self):
        if self.dry_run:
            logger.info("🔌 Dry-run ativo: conexão com PostgreSQL será adiada")
            return None
        self.engine = create_engine(self.db_url)
        return self.engine

    def ensure_ingest_log(self):
        if self.dry_run:
            return
        with self.engine.begin() as conn:
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS ingest_log_parquet (
                    id SERIAL PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_count BIGINT,
                    status TEXT NOT NULL,
                    error TEXT,
                    modified_time TIMESTAMP,
                    load_time TIMESTAMP NOT NULL
                )
                """
            ))

    def already_ingested(self, file_path: Path, modified_time: datetime) -> bool:
        if self.dry_run:
            return False
        with self.engine.begin() as conn:
            res = conn.execute(text(
                "SELECT 1 FROM ingest_log_parquet WHERE file_path = :p AND modified_time = :m AND status = 'ok' LIMIT 1"
            ), {"p": str(file_path), "m": modified_time}).fetchone()
        return res is not None

    def discover_files(self):
        root = self.base_path
        files = []
        extensions = ["*.parquet", "*.csv"]
        for ext in extensions:
            for p in root.rglob(ext):
                info = parse_file_info(p)
                if not info:
                    continue
                if info["layer"] and info["layer"] not in self.layers:
                    continue
                files.append((p, info))
        files.sort(key=lambda x: x[0])
        logger.info(f"🔎 Arquivos encontrados: {len(files)}")
        return files

    def get_dtype_mapping(self, schema: pa.Schema):
        mapping = {}
        for f in schema:
            mapping[normalize_column(f.name)] = arrow_to_sqlalchemy(f.type)
        return mapping

    def ensure_table(self, table_name: str, dtype_mapping: dict):
        if table_name in self.verified_tables:
            return
        if self.dry_run:
            logger.info(f"🧱 [dry-run] Verificação de tabela {table_name}")
            self.verified_tables.add(table_name)
            return
        insp = inspect(self.engine)
        exists = insp.has_table(table_name, schema=self.schema)
        if not exists:
            cols_sql = []
            for c, t in dtype_mapping.items():
                col_type = t.compile(dialect=self.engine.dialect)
                cols_sql.append(f'"{normalize_column(c)}" {col_type}')
            ddl = f'CREATE TABLE {self.schema}."{table_name}" (' + ", ".join(cols_sql) + ")"
            with self.engine.begin() as conn:
                conn.execute(text(ddl))
            logger.info(f"🧱 Tabela criada: {table_name}")
        else:
            cols = {c['name'] for c in insp.get_columns(table_name, schema=self.schema)}
            missing = [c for c in dtype_mapping.keys() if c not in cols]
            if missing:
                with self.engine.begin() as conn:
                    for c in missing:
                        t = dtype_mapping[c]
                        ddl = text(f"ALTER TABLE {self.schema}.\"{table_name}\" ADD COLUMN \"{c}\" {t.compile(dialect=self.engine.dialect)}")
                        conn.execute(ddl)
                logger.info(f"🧱 Colunas adicionadas em {table_name}: {missing}")
        self.verified_tables.add(table_name)

    def load_file(self, file_path: Path, info: dict):
        modified_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        table_name = info["table"]
        is_csv = file_path.suffix.lower() == ".csv"
        
        try:
            if is_csv:
                # Detect schema from first block
                reader = csv.open_csv(str(file_path))
                schema = reader.schema
                reader.close()
            else:
                pf = pq.ParquetFile(str(file_path))
                schema = pf.schema_arrow
        except Exception as e:
            logger.error(f"❌ Erro ao ler schema de {file_path}: {e}")
            return

        dtype_map = self.get_dtype_mapping(schema)
        partition_df = pd.DataFrame({k: [v] for k, v in info["partitions"].items()})
        self.ensure_table(table_name, {**dtype_map, **{normalize_column(k): Text() for k in info["partitions"].keys()}})

        if not self.dry_run and self.already_ingested(file_path, modified_time):
            logger.info(f"⏭️  Pulando (sem alterações): {file_path}")
            return

        total_rows = 0
        status = "ok"
        err_msg = None
        
        try:
            iterator = None
            if is_csv:
                iterator = csv.open_csv(str(file_path))
            else:
                pf = pq.ParquetFile(str(file_path))
                iterator = pf.iter_batches(batch_size=self.batch_size)

            for batch in iterator:
                df = batch.to_pandas(types_mapper=lambda t: pd.ArrowDtype(t))
                if not partition_df.empty:
                    for col in partition_df.columns:
                        df[normalize_column(col)] = partition_df[col].iloc[0]
                df.columns = [normalize_column(c) for c in df.columns]
                total_rows += len(df)
                if self.dry_run:
                    continue
                try:
                    raw_conn = self.engine.raw_connection()
                    try:
                        buf = io.StringIO()
                        df.to_csv(buf, index=False, header=False)
                        buf.seek(0)
                        cols = ",".join([f'"{c}"' for c in df.columns])
                        sql = f'COPY {self.schema}."{table_name}" ({cols}) FROM STDIN WITH (FORMAT csv, NULL \'\')'
                        cur = raw_conn.cursor()
                        cur.copy_expert(sql=sql, file=buf)
                        raw_conn.commit()
                    finally:
                        raw_conn.close()
                except Exception:
                    # Fallback for complex types or errors
                    df.to_sql(table_name, self.engine, schema=self.schema, if_exists="append", index=False)
            logger.info(f"📥 {file_path} → {table_name} (linhas: {total_rows})")
        except Exception as e:
            status = "error"
            err_msg = str(e)
            logger.error(f"❌ Falha ao carregar {file_path}: {e}")
        finally:
            if not self.dry_run:
                with self.engine.begin() as conn:
                    conn.execute(text(
                        """
                        INSERT INTO ingest_log_parquet(file_path, table_name, row_count, status, error, modified_time, load_time)
                        VALUES (:p, :t, :r, :s, :e, :m, :l)
                        """
                    ), {
                        "p": str(file_path),
                        "t": table_name,
                        "r": total_rows,
                        "s": status,
                        "e": err_msg,
                        "m": modified_time,
                        "l": datetime.now(timezone.utc),
                    })

    def run(self):
        self.connect()
        self.ensure_ingest_log()
        files = self.discover_files()
        for p, info in files:
            self.load_file(p, info)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-path", default=str(Path(__file__).resolve().parents[2] / "data"))
    parser.add_argument("--layers", default="gold,silver,bronze")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--batch-size", type=int, default=200000)
    args = parser.parse_args()

    base_path = Path(args.base_path)
    layers = [l.strip() for l in args.layers.split(",") if l.strip()]
    loader = PostgresLoader(base_path=base_path, layers=layers, dry_run=args.dry_run, batch_size=args.batch_size)
    logger.info(f"🚀 Iniciando carga Parquet → PostgreSQL | base={base_path} | layers={layers} | dry_run={args.dry_run}")
    loader.run()


if __name__ == "__main__":
    main()
