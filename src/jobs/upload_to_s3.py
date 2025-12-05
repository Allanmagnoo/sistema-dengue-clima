"""
Upload de dados para AWS S3
Faz upload das camadas Bronze, Silver e Gold para o S3
"""
import boto3
import logging
from pathlib import Path
from botocore.exceptions import ClientError
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIGURAÇÕES ===
S3_BUCKET = "sistema-dengue-clima"  # ALTERE PARA O NOME DO SEU BUCKET
S3_PREFIX = "data"  # Prefixo no S3 (pode deixar 'data' ou alterar)
AWS_REGION = "us-east-1"  # ALTERE PARA SUA REGIÃO

def get_s3_client():
    """Cria cliente S3 usando credenciais do ambiente ou AWS CLI"""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        # Testa a conexão
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"✅ Conectado ao bucket S3: {S3_BUCKET}")
        return s3_client
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"❌ Bucket '{S3_BUCKET}' não encontrado. Criando...")
            s3_client = boto3.client('s3', region_name=AWS_REGION)
            try:
                if AWS_REGION == 'us-east-1':
                    s3_client.create_bucket(Bucket=S3_BUCKET)
                else:
                    s3_client.create_bucket(
                        Bucket=S3_BUCKET,
                        CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                    )
                logger.info(f"✅ Bucket '{S3_BUCKET}' criado com sucesso!")
                return s3_client
            except Exception as create_error:
                logger.error(f"❌ Erro ao criar bucket: {create_error}")
                raise
        else:
            logger.error(f"❌ Erro ao conectar ao S3: {e}")
            raise

def upload_directory(s3_client, local_path, s3_prefix, layer_name):
    """
    Faz upload recursivo de um diretório para o S3
    """
    local_path = Path(local_path)
    
    if not local_path.exists():
        logger.warning(f"⚠️ Caminho não encontrado: {local_path}")
        return 0
    
    logger.info(f"\n📤 Uploading {layer_name} de: {local_path}")
    logger.info(f"   Para: s3://{S3_BUCKET}/{s3_prefix}")
    
    uploaded_count = 0
    total_size = 0
    
    # Lista todos os arquivos recursivamente
    files_to_upload = []
    if local_path.is_file():
        files_to_upload = [local_path]
    else:
        files_to_upload = list(local_path.rglob('*'))
        files_to_upload = [f for f in files_to_upload if f.is_file()]
    
    total_files = len(files_to_upload)
    logger.info(f"   Total de arquivos: {total_files}")
    
    for i, file_path in enumerate(files_to_upload, 1):
        try:
            # Calcula o caminho relativo
            if local_path.is_file():
                relative_path = file_path.name
            else:
                relative_path = file_path.relative_to(local_path)
            
            # Monta a key no S3
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
            
            # Faz o upload
            file_size = file_path.stat().st_size
            s3_client.upload_file(
                str(file_path),
                S3_BUCKET,
                s3_key,
                ExtraArgs={'ServerSideEncryption': 'AES256'}  # Criptografia
            )
            
            uploaded_count += 1
            total_size += file_size
            
            # Log de progresso
            if uploaded_count % 100 == 0 or uploaded_count == total_files:
                logger.info(f"   Progresso: {uploaded_count}/{total_files} arquivos ({total_size / (1024**2):.2f} MB)")
        
        except Exception as e:
            logger.error(f"   ❌ Erro ao fazer upload de {file_path}: {e}")
    
    logger.info(f"✅ {layer_name}: {uploaded_count} arquivos enviados ({total_size / (1024**2):.2f} MB)")
    return uploaded_count

def main():
    logger.info("=" * 80)
    logger.info("🚀 UPLOAD DE DADOS PARA AWS S3")
    logger.info("=" * 80)
    
    # Paths
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    data_dir = base_dir / "data"
    
    # Verificar credenciais AWS
    logger.info("\n🔑 Verificando credenciais AWS...")
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            logger.error("❌ Credenciais AWS não encontradas!")
            logger.info("\nConfigure suas credenciais usando:")
            logger.info("  1. aws configure (AWS CLI)")
            logger.info("  2. Variáveis de ambiente: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            logger.info("  3. IAM Role (se estiver em EC2/Lambda)")
            return
        
        logger.info(f"✅ Credenciais encontradas para: {session.region_name or 'default region'}")
    except Exception as e:
        logger.error(f"❌ Erro ao verificar credenciais: {e}")
        return
    
    # Criar cliente S3
    try:
        s3_client = get_s3_client()
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao S3: {e}")
        return
    
    # === ESTRUTURA DE UPLOAD ===
    upload_plan = [
        # (local_path, s3_path, description)
        (data_dir / "bronze" / "DTB_2024", f"{S3_PREFIX}/bronze/DTB_2024", "Bronze - DTB 2024"),
        (data_dir / "bronze" / "infodengue", f"{S3_PREFIX}/bronze/infodengue", "Bronze - InfoDengue"),
        (data_dir / "bronze" / "inmet", f"{S3_PREFIX}/bronze/inmet", "Bronze - INMET"),
        (data_dir / "silver" / "infodengue", f"{S3_PREFIX}/silver/infodengue", "Silver - InfoDengue"),
        (data_dir / "silver" / "inmet", f"{S3_PREFIX}/silver/inmet", "Silver - INMET"),
        (data_dir / "silver" / "mapping_estacao_geocode.parquet", f"{S3_PREFIX}/silver/mapping_estacao_geocode.parquet", "Silver - Mapping"),
        (data_dir / "gold" / "dengue_clima_partitioned", f"{S3_PREFIX}/gold/dengue_clima_partitioned", "Gold - Dengue + Clima"),
    ]
    
    total_uploaded = 0
    
    for local_path, s3_path, description in upload_plan:
        count = upload_directory(s3_client, local_path, s3_path, description)
        total_uploaded += count
    
    logger.info("\n" + "=" * 80)
    logger.info(f"✅ UPLOAD CONCLUÍDO!")
    logger.info(f"   Total de arquivos enviados: {total_uploaded}")
    logger.info(f"   Bucket: s3://{S3_BUCKET}/{S3_PREFIX}")
    logger.info("=" * 80)
    
    # Listar estrutura no S3
    logger.info("\n📁 Estrutura criada no S3:")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        folders = set()
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX, Delimiter='/'):
            for prefix in page.get('CommonPrefixes', []):
                folder = prefix['Prefix']
                folders.add(folder)
        
        for folder in sorted(folders):
            logger.info(f"   📂 s3://{S3_BUCKET}/{folder}")
    except Exception as e:
        logger.error(f"❌ Erro ao listar estrutura: {e}")
    
    logger.info("\n💡 Próximos passos:")
    logger.info("   1. Configure Glue Crawler para catalogar os dados")
    logger.info("   2. Use Athena para consultar os dados diretamente do S3")
    logger.info("   3. Configure EMR/Databricks para processamento em larga escala")

if __name__ == "__main__":
    main()
