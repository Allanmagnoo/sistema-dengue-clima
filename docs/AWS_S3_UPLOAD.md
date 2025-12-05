# Upload de Dados para AWS S3

## Pré-requisitos

### 1. Instalar boto3
```bash
pip install boto3
```

### 2. Configurar Credenciais AWS

Você tem 3 opções:

#### Opção 1: AWS CLI (Recomendado)
```bash
aws configure
```
Será solicitado:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name (ex: us-east-1)
- Default output format (json)

#### Opção 2: Variáveis de Ambiente
```bash
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="sua_access_key"
$env:AWS_SECRET_ACCESS_KEY="sua_secret_key"
$env:AWS_DEFAULT_REGION="us-east-1"

# Linux/Mac
export AWS_ACCESS_KEY_ID="sua_access_key"
export AWS_SECRET_ACCESS_KEY="sua_secret_key"
export AWS_DEFAULT_REGION="us-east-1"
```

#### Opção 3: IAM Role (se estiver em EC2)
Se estiver rodando em uma instância EC2, configure uma IAM Role com permissões S3.

### 3. Configurar o Script

Edite o arquivo `upload_to_s3.py` e ajuste:

```python
S3_BUCKET = "sistema-dengue-clima"  # Nome do seu bucket (deve ser único globalmente)
S3_PREFIX = "data"                   # Prefixo/pasta no S3
AWS_REGION = "us-east-1"            # Sua região AWS
```

## Permissões IAM Necessárias

Sua conta AWS precisa das seguintes permissões:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::sistema-dengue-clima",
                "arn:aws:s3:::sistema-dengue-clima/*"
            ]
        }
    ]
}
```

## Executando o Upload

```bash
cd d:\_data-science\GitHub\sistema-dengue-clima\src\jobs
python upload_to_s3.py
```

## Estrutura que será criada no S3

```
s3://sistema-dengue-clima/data/
├── bronze/
│   ├── DTB_2024/                    # Tabela de municípios IBGE
│   ├── infodengue/                  # Dados brutos de dengue (CSV)
│   │   └── municipios/disease=dengue/year=YYYY/*.csv
│   └── inmet/                       # Dados brutos de clima (CSV)
│       └── YYYY/*.CSV
├── silver/
│   ├── infodengue/                  # Dados de dengue tratados (Parquet)
│   │   └── uf=XX/ano_epidemiologico=YYYY/*.parquet
│   ├── inmet/                       # Dados de clima tratados (Parquet)
│   │   └── uf=XX/ano=YYYY/*.parquet
│   └── mapping_estacao_geocode.parquet  # Mapeamento estação->município
└── gold/
    └── dengue_clima_partitioned/    # Dados integrados (Parquet)
        └── uf=XX/*.parquet
```

## Estimativa de Dados

- **Bronze**: ~2-3 GB (CSVs comprimidos)
- **Silver**: ~500 MB (Parquet otimizado)
- **Gold**: ~200 MB (Parquet particionado)
- **Total**: ~3-4 GB

## Tempo Estimado de Upload

- Conexão 10 Mbps: ~40-60 minutos
- Conexão 50 Mbps: ~8-12 minutos
- Conexão 100 Mbps: ~4-6 minutos

## Após o Upload - Próximos Passos

### 1. AWS Glue Crawler
Configure um Crawler para catalogar automaticamente os dados:
- Crie um Crawler apontando para `s3://sistema-dengue-clima/data/`
- Configure partitioning para as camadas Silver e Gold
- Rode o Crawler para popular o Data Catalog

### 2. Amazon Athena
Consulte os dados diretamente com SQL:
```sql
-- Exemplo de query
SELECT 
    uf,
    ano_epidemiologico,
    SUM(casos_notificados) as total_casos
FROM "default"."dengue_clima_partitioned"
WHERE ano_epidemiologico = 2024
GROUP BY uf, ano_epidemiologico
ORDER BY total_casos DESC;
```

### 3. AWS EMR ou Databricks
Para processamento em larga escala:
- Configure um cluster EMR com Spark
- Ou use Databricks conectado ao S3
- Processe os dados Silver/Gold para ML

### 4. Amazon QuickSight
Crie dashboards interativos:
- Conecte QuickSight ao Athena
- Crie visualizações da evolução temporal
- Compartilhe insights com stakeholders

## Custos Estimados (us-east-1)

**S3 Storage (Standard)**
- 4 GB x $0.023/GB = ~$0.09/mês

**S3 Requests**
- Upload inicial: ~$0.01
- Queries mensais (1000): ~$0.01/mês

**Athena**
- $5 por TB scaneado
- Queries típicas: $0.001 - $0.01 cada

**Total estimado**: < $1/mês para uso moderado

## Troubleshooting

### Erro: "NoCredentialsError"
```bash
# Verifique se as credenciais estão configuradas
aws sts get-caller-identity
```

### Erro: "AccessDenied"
- Verifique se sua conta tem permissões S3
- Confira se o bucket name está disponível (deve ser único globalmente)

### Erro: "Bucket already exists"
- Escolha outro nome de bucket (deve ser único globalmente)
- Ou apenas altere S3_PREFIX para usar um bucket existente

## Monitoramento do Upload

O script exibe progresso a cada 100 arquivos:
```
Progresso: 100/500 arquivos (45.23 MB)
Progresso: 200/500 arquivos (89.45 MB)
...
```

## Segurança

- ✅ Criptografia server-side (AES256) habilitada por padrão
- ✅ Use IAM Roles em vez de Access Keys quando possível
- ✅ Nunca commite credenciais no Git
- ✅ Configure bucket policies para acesso restrito
