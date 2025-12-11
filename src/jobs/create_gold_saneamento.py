import pandas as pd
import logging
import argparse
from pathlib import Path
import sys

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_gold_panel(data_path: Path):
    """
    Cria o painel Gold unificando Dengue (Silver), SNIS (Silver) e IBGE (Silver).
    """
    silver_path = data_path / "silver"
    gold_path = data_path / "gold" / "paineis"
    gold_path.mkdir(parents=True, exist_ok=True)

    # 1. Carregar Dados de Dengue
    dengue_path = silver_path / "silver_dengue"
    logger.info(f"Carregando dados de Dengue de: {dengue_path}")
    try:
        # Lê o dataset particionado
        df_dengue = pd.read_parquet(dengue_path)
        logger.info(f"Dados de Dengue carregados. Shape: {df_dengue.shape}")
    except Exception as e:
        logger.error(f"Erro ao carregar Dengue: {e}")
        return

    # Normalizar colunas de chave
    if 'geocode' in df_dengue.columns:
        df_dengue.rename(columns={'geocode': 'id_municipio'}, inplace=True)
    
    # Garantir tipos
    df_dengue['id_municipio'] = df_dengue['id_municipio'].astype(str).str.replace(r'\.0$', '', regex=True)
    # Criar coluna 'ano' se não existir (usando ano_epidemiologico)
    if 'ano' not in df_dengue.columns and 'ano_epidemiologico' in df_dengue.columns:
        df_dengue['ano'] = df_dengue['ano_epidemiologico'].astype(int)

    # Agregar Dengue por Município e Ano (somar casos)
    # O dataset original é semanal. Para o painel anual de saneamento, precisamos agregar.
    # Mas se o objetivo for manter granularidade semanal, precisamos repetir os dados anuais de saneamento.
    # A estratégia aqui será criar um painel ANUAL primeiro, pois o SNIS é anual.
    # Se o modelo precisar de dados semanais, o join deve ser feito explodindo o ano.
    # VAMOS MANTER A GRANULARIDADE ANUAL PARA ESTE PAINEL DE SANEAMENTO.
    
    logger.info("Agregando dados de Dengue por Município e Ano...")
    df_dengue_anual = df_dengue.groupby(['id_municipio', 'ano']).agg({
        'casos_notificados': 'sum',
        # 'casos_provaveis': 'sum', # Se existir
        # 'obitos': 'sum' # Se existir
    }).reset_index()
    df_dengue_anual.rename(columns={'casos_notificados': 'total_casos_dengue'}, inplace=True)
    
    # 2. Carregar Dados do SNIS
    snis_path = silver_path / "snis" / "snis_agregado_1995_2021.parquet"
    logger.info(f"Carregando dados do SNIS de: {snis_path}")
    if not snis_path.exists():
        logger.error(f"Arquivo SNIS não encontrado: {snis_path}")
        return
    
    df_snis = pd.read_parquet(snis_path)
    logger.info(f"Dados do SNIS carregados. Shape: {df_snis.shape}")
    
    # Normalizar chave
    df_snis['id_municipio'] = df_snis['id_municipio'].astype(str).str.replace(r'\.0$', '', regex=True)
    df_snis['ano'] = df_snis['ano'].astype(int)

    # 3. Carregar Dados do IBGE (Censo 2022 - Fixo)
    ibge_path = silver_path / "ibge" / "censo_agregado_2022.parquet"
    logger.info(f"Carregando dados do IBGE de: {ibge_path}")
    if not ibge_path.exists():
        logger.error(f"Arquivo IBGE não encontrado: {ibge_path}")
        # Continuar sem IBGE se falhar? Melhor falhar para garantir integridade se for crítico.
        # Mas vamos permitir continuar sem IBGE para não bloquear tudo se o arquivo não tiver sido gerado ainda.
        df_ibge = pd.DataFrame(columns=['id_municipio']) # Empty
    else:
        df_ibge = pd.read_parquet(ibge_path)
        logger.info(f"Dados do IBGE carregados. Shape: {df_ibge.shape}")
        # O IBGE não tem ano (é 2022 estático), então fazemos merge apenas pelo município
        # O arquivo original tem 'id_setor_censitario'? O script de transformação agregava?
        # Vamos checar se o script 'transform_silver_ibge.py' agrega por município.
        # O script lido anteriormente salvava 'censo_agregado_2022.parquet'.
        # Se ele mantém dados por setor censitário, precisamos agregar por município aqui.
        # Mas o nome sugere 'agregado'. Vamos assumir que já está ou verificar as colunas.
        if 'id_setor_censitario' in df_ibge.columns:
             # O código do setor censitário (15 dígitos) contem o município (primeiros 7 dígitos)
             df_ibge['id_municipio'] = df_ibge['id_setor_censitario'].astype(str).str.slice(0, 7)
             # Agregar
             cols_to_sum = ['populacao_total', 'domicilios_total', 'domicilios_particulares_ocupados']
             cols_to_mean = ['densidade_domiciliada', 'densidade_geral'] # Média ponderada seria melhor, mas média simples ok por enquanto
             
             agg_dict = {c: 'sum' for c in cols_to_sum if c in df_ibge.columns}
             agg_dict.update({c: 'mean' for c in cols_to_mean if c in df_ibge.columns})
             
             df_ibge = df_ibge.groupby('id_municipio').agg(agg_dict).reset_index()

    # 4. Join (Left Join no Dengue Anual)
    logger.info("Realizando Join dos dados...")
    
    # Merge Dengue + SNIS (por municipio e ano)
    df_gold = pd.merge(df_dengue_anual, df_snis, on=['id_municipio', 'ano'], how='left')
    
    # Merge + IBGE (por municipio)
    if not df_ibge.empty:
        df_gold = pd.merge(df_gold, df_ibge, on=['id_municipio'], how='left')
    
    # Preencher nulos do IBGE (que são constantes) ou SNIS?
    # Deixar nulos para tratamento no modelo.
    
    logger.info(f"Painel Gold criado. Shape final: {df_gold.shape}")
    
    # Salvar
    output_file = gold_path / "painel_municipal_dengue_saneamento.parquet"
    logger.info(f"Salvando em: {output_file}")
    df_gold.to_parquet(output_file, index=False)
    logger.info("Processo concluído.")

def main():
    parser = argparse.ArgumentParser(description="Cria painel Gold unificado")
    parser.add_argument("--data-dir", default=r"d:\_data-science\GitHub\sistema-dengue-clima\data", help="Diretório Data")
    args = parser.parse_args()
    
    data_path = Path(args.data_dir)
    create_gold_panel(data_path)

if __name__ == "__main__":
    main()
