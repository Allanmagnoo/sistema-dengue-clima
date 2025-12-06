"""
Upload de dados para AWS S3 (Otimizado com Paralelismo)
Faz upload das camadas Bronze, Silver e Gold para o S3 de forma concorrente.
"""
import boto3
import logging
from pathlib import Path
from botocore.exceptions import ClientError
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys

# Tenta importar tqdm para barra de progresso
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIGURAÇÕES GLOBAIS ===
AWS_REGION = "us-east-1"
MAX_WORKERS = 50  # Número de threads simultâneas

def get_s3_client(bucket_name):
    """Cria cliente S3 e garante existencia do bucket"""
    try:
        session = boto3.Session()
        s3_client = session.client('s3', region_name=AWS_REGION)
        
        # Testa a conexão / existencia
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"✅ Conectado ao bucket S3: {bucket_name}")
            return s3_client
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"⚠️ Bucket '{bucket_name}' não encontrado. Criando...")
                if AWS_REGION == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                    )
                logger.info(f"✅ Bucket '{bucket_name}' criado com sucesso!")
                return s3_client
            elif error_code == '403':
                logger.error(f"❌ Acesso negado ao bucket '{bucket_name}'. Verifique suas permissões.")
                sys.exit(1)
            else:
                raise
    except Exception as e:
        logger.error(f"❌ Erro crítico ao conectar ao S3: {e}")
        sys.exit(1)

def upload_single_file(s3_client, local_path, bucket, s3_key, progress_callback=None):
    """Faz upload de um único arquivo"""
    try:
        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={'ServerSideEncryption': 'AES256'}
        )
        if progress_callback:
            progress_callback(local_path.stat().st_size)
        return True
    except Exception as e:
        logger.error(f"❌ Falha no upload de {local_path}: {e}")
        return False

def upload_directory_parallel(s3_client, local_path, bucket, s3_prefix, layer_name):
    """
    Faz upload recursivo de um diretório para o S3 usando Threads
    """
    local_path = Path(local_path)
    
    if not local_path.exists():
        logger.warning(f"⚠️ Caminho não encontrado: {local_path}")
        return 0
    
    logger.info(f"\n📦 Preparando {layer_name}: {local_path} -> s3://{bucket}/{s3_prefix}")
    
    # 1. Listar arquivos
    files_to_upload = []
    if local_path.is_file():
        files_to_upload.append((local_path, f"{s3_prefix}/{local_path.name}"))
    else:
        for f in local_path.rglob('*'):
            if f.is_file():
                relative_path = f.relative_to(local_path)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
                files_to_upload.append((f, s3_key))
    
    total_files = len(files_to_upload)
    if total_files == 0:
        logger.info("   Nenhum arquivo encontrado.")
        return 0
        
    logger.info(f"   🚀 Iniciando upload de {total_files} arquivos com {MAX_WORKERS} threads...")
    
    uploaded_count = 0
    uploaded_bytes = 0
    
    # Barra de progresso
    pbar = None
    if HAS_TQDM:
        pbar = tqdm(total=total_files, unit='file', desc=f"Upload {layer_name}")
    
    def update_progress(size):
        nonlocal uploaded_count, uploaded_bytes
        uploaded_count += 1
        uploaded_bytes += size
        if pbar:
            pbar.update(1)
            
    # Thread Pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for f_path, s3_key in files_to_upload:
            futures.append(
                executor.submit(upload_single_file, s3_client, f_path, bucket, s3_key, update_progress)
            )
        
        # Aguarda conclusão
        for future in as_completed(futures):
            pass  # Exceções já são tratadas no upload_single_file
            
    if pbar:
        pbar.close()
        
    logger.info(f"✅ {layer_name} concluído: {uploaded_count} arquivos ({uploaded_bytes / (1024**2):.2f} MB)")
    return uploaded_count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET_NAME", "sistema-dengue-clima"))
    args = parser.parse_args()
    
    bucket_name = args.bucket
    
    logger.info("=" * 80)
    logger.info("🚀 UPLOAD DE DADOS PARA AWS S3 (PARALELO)")
    logger.info(f"📍 Bucket Alvo: {bucket_name}")
    logger.info("=" * 80)
    
    # Paths Locais
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    data_dir = base_dir / "data"
    
    # Cliente S3
    s3_client = get_s3_client(bucket_name)
    
    # === PLANO DE UPLOAD ===
    # Ajustado para os caminhos reais corretos
    upload_plan = [
        # Bronze Layer
        (data_dir / "bronze" / "DTB_2024", "data/bronze/DTB_2024", "Bronze - DTB"),
        (data_dir / "bronze" / "infodengue", "data/bronze/infodengue", "Bronze - InfoDengue (CSV)"),
        (data_dir / "bronze" / "inmet", "data/bronze/inmet", "Bronze - INMET"),
        
        # Silver Layer
        (data_dir / "silver" / "silver_dengue", "data/silver/silver_dengue", "Silver - Dengue"),
        (data_dir / "silver" / "silver_inmet", "data/silver/silver_inmet", "Silver - INMET"),
        (data_dir / "silver" / "silver_mapping_estacao_geocode.parquet", "data/silver/silver_mapping_estacao_geocode.parquet", "Silver - Mapping"),
        
        # Gold Layer
        (data_dir / "gold" / "gold_dengue_clima", "data/gold/gold_dengue_clima", "Gold - Final"),
    ]
    
    total_uploaded = 0
    for local_path, s3_prefix, description in upload_plan:
        count = upload_directory_parallel(s3_client, local_path, bucket_name, s3_prefix, description)
        total_uploaded += count
        
    logger.info("\n" + "=" * 80)
    logger.info(f"🎉 TODO O PROCESSO CONCLUÍDO!")
    logger.info(f"📊 Total de arquivos processados: {total_uploaded}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
