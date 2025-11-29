#!/usr/bin/env python3
"""
Script de Transformação Bronze com Rapids (cuDF)
=================================================

Este script demonstra o processamento de múltiplos arquivos CSV
da camada Bronze usando aceleração GPU com Rapids/cuDF.

Funcionalidades:
- Leitura paralela de múltiplos arquivos CSV
- Transformações e limpeza de dados na GPU
- Agregações e análises estatísticas
- Benchmark de performance
- Geração de relatórios

Autor: Eco-Sentinel Team
Data: 2025-11-29
"""

import cudf
import os
import glob
import sys
import time
from pathlib import Path
from typing import List, Dict, Tuple
import warnings

warnings.filterwarnings('ignore')


class BronzeRapidsProcessor:
    """Processador de dados Bronze usando Rapids/cuDF"""
    
    def __init__(self, base_path: str = None):
        """
        Inicializa o processador
        
        Args:
            base_path: Caminho base para os dados Bronze
        """
        if base_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(script_dir))
            base_path = os.path.join(
                project_root, "data", "bronze", "infodengue", 
                "municipios", "disease=dengue", "year=2024"
            )
        
        self.base_path = base_path
        self.stats = {}
        
    def discover_files(self, limit: int = None) -> List[str]:
        """
        Descobre arquivos CSV disponíveis
        
        Args:
            limit: Limite de arquivos para processar (None = todos)
            
        Returns:
            Lista de caminhos dos arquivos
        """
        pattern = os.path.join(self.base_path, "*.csv")
        files = sorted(glob.glob(pattern))
        
        if limit:
            files = files[:limit]
            
        print(f"📁 Arquivos descobertos: {len(files)}")
        return files
    
    def read_single_file(self, filepath: str) -> cudf.DataFrame:
        """
        Lê um arquivo CSV individual
        
        Args:
            filepath: Caminho do arquivo
            
        Returns:
            DataFrame cuDF
        """
        try:
            df = cudf.read_csv(filepath)
            return df
        except Exception as e:
            print(f"⚠️  Erro ao ler {os.path.basename(filepath)}: {e}")
            return None
    
    def read_multiple_files(self, files: List[str], verbose: bool = True) -> cudf.DataFrame:
        """
        Lê múltiplos arquivos e concatena
        
        Args:
            files: Lista de caminhos de arquivos
            verbose: Mostrar progresso
            
        Returns:
            DataFrame cuDF concatenado
        """
        start_time = time.time()
        
        if verbose:
            print(f"\n⏳ Lendo {len(files)} arquivos...")
        
        dfs = []
        for i, filepath in enumerate(files, 1):
            df = self.read_single_file(filepath)
            if df is not None:
                # Adiciona coluna com o código do município
                municipio_code = os.path.basename(filepath).replace('.csv', '')
                df['municipio_code'] = municipio_code
                dfs.append(df)
                
            if verbose and i % 100 == 0:
                print(f"   Processados {i}/{len(files)} arquivos...")
        
        # Concatena todos os DataFrames
        combined_df = cudf.concat(dfs, ignore_index=True)
        
        elapsed_time = time.time() - start_time
        self.stats['read_time'] = elapsed_time
        self.stats['files_read'] = len(dfs)
        self.stats['total_rows'] = len(combined_df)
        
        if verbose:
            print(f"   ✓ Leitura concluída em {elapsed_time:.2f}s")
            print(f"   ✓ Total de registros: {len(combined_df):,}")
        
        return combined_df
    
    def clean_data(self, df: cudf.DataFrame, verbose: bool = True) -> cudf.DataFrame:
        """
        Limpa e prepara os dados
        
        Args:
            df: DataFrame cuDF
            verbose: Mostrar progresso
            
        Returns:
            DataFrame limpo
        """
        if verbose:
            print(f"\n🧹 Limpando dados...")
        
        start_time = time.time()
        initial_rows = len(df)
        
        # Remove linhas com valores nulos em colunas críticas
        critical_cols = ['data_iniSE', 'SE', 'Localidade_id']
        df = df.dropna(subset=critical_cols)
        
        # Converte tipos de dados
        if 'SE' in df.columns:
            df['SE'] = df['SE'].astype('int32')
        
        if 'casos' in df.columns:
            df['casos'] = df['casos'].fillna(0).astype('int32')
            
        if 'casos_est' in df.columns:
            df['casos_est'] = df['casos_est'].fillna(0).astype('float32')
        
        # Remove duplicatas
        df = df.drop_duplicates()
        
        elapsed_time = time.time() - start_time
        rows_removed = initial_rows - len(df)
        
        if verbose:
            print(f"   ✓ Limpeza concluída em {elapsed_time:.2f}s")
            print(f"   ✓ Linhas removidas: {rows_removed:,}")
            print(f"   ✓ Linhas restantes: {len(df):,}")
        
        return df
    
    def aggregate_data(self, df: cudf.DataFrame, verbose: bool = True) -> Dict:
        """
        Realiza agregações nos dados
        
        Args:
            df: DataFrame cuDF
            verbose: Mostrar progresso
            
        Returns:
            Dicionário com estatísticas agregadas
        """
        if verbose:
            print(f"\n📊 Agregando dados...")
        
        start_time = time.time()
        
        aggregations = {}
        
        # Estatísticas gerais
        if 'casos' in df.columns:
            aggregations['total_casos'] = int(df['casos'].sum())
            aggregations['media_casos'] = float(df['casos'].mean())
            aggregations['max_casos'] = int(df['casos'].max())
            aggregations['min_casos'] = int(df['casos'].min())
        
        # Casos por município
        if 'municipio_code' in df.columns and 'casos' in df.columns:
            casos_por_municipio = df.groupby('municipio_code')['casos'].sum().sort_values(ascending=False)
            # Converte apenas os top 10 para dicionário nativo do Python
            top_10 = casos_por_municipio.head(10)
            aggregations['top_10_municipios'] = {
                str(k): int(v) for k, v in zip(top_10.index.to_arrow().to_pylist(), top_10.to_arrow().to_pylist())
            }
        
        # Casos por semana epidemiológica
        if 'SE' in df.columns and 'casos' in df.columns:
            casos_por_semana = df.groupby('SE')['casos'].sum().sort_values(ascending=False)
            # Limita a semanas com mais casos
            top_semanas = casos_por_semana.head(20)
            aggregations['casos_por_semana'] = {
                int(k): int(v) for k, v in zip(top_semanas.index.to_arrow().to_pylist(), top_semanas.to_arrow().to_pylist())
            }
        
        # Estatísticas de nível de alerta
        if 'nivel' in df.columns:
            nivel_counts = df['nivel'].value_counts()
            aggregations['distribuicao_nivel'] = {
                int(k): int(v) for k, v in zip(nivel_counts.index.to_arrow().to_pylist(), nivel_counts.to_arrow().to_pylist())
            }
        
        elapsed_time = time.time() - start_time
        
        if verbose:
            print(f"   ✓ Agregação concluída em {elapsed_time:.2f}s")
        
        return aggregations
    
    def generate_report(self, aggregations: Dict, verbose: bool = True):
        """
        Gera relatório de análise
        
        Args:
            aggregations: Dicionário com agregações
            verbose: Mostrar relatório
        """
        if not verbose:
            return
        
        print("\n" + "=" * 70)
        print("📈 RELATÓRIO DE ANÁLISE - DADOS DE DENGUE 2024")
        print("=" * 70)
        
        # Estatísticas Gerais
        if 'total_casos' in aggregations:
            print(f"\n📊 Estatísticas Gerais:")
            print(f"   Total de casos: {aggregations['total_casos']:,}")
            print(f"   Média de casos: {aggregations['media_casos']:.2f}")
            print(f"   Máximo: {aggregations['max_casos']:,}")
            print(f"   Mínimo: {aggregations['min_casos']:,}")
        
        # Top 10 Municípios
        if 'top_10_municipios' in aggregations:
            print(f"\n🏆 Top 10 Municípios com Mais Casos:")
            for i, (municipio, casos) in enumerate(list(aggregations['top_10_municipios'].items())[:10], 1):
                print(f"   {i:2}. {municipio}: {casos:,} casos")
        
        # Distribuição por Nível
        if 'distribuicao_nivel' in aggregations:
            print(f"\n⚠️  Distribuição por Nível de Alerta:")
            for nivel, count in aggregations['distribuicao_nivel'].items():
                print(f"   Nível {nivel}: {count:,} registros")
        
        # Performance
        if self.stats:
            print(f"\n⚡ Performance:")
            print(f"   Arquivos processados: {self.stats.get('files_read', 0)}")
            print(f"   Tempo de leitura: {self.stats.get('read_time', 0):.2f}s")
            print(f"   Registros totais: {self.stats.get('total_rows', 0):,}")
            
            if self.stats.get('read_time', 0) > 0:
                throughput = self.stats.get('total_rows', 0) / self.stats.get('read_time', 0)
                print(f"   Throughput: {throughput:,.0f} registros/segundo")
        
        print("\n" + "=" * 70)


def main():
    """Função principal"""
    print("=" * 70)
    print("🚀 RAPIDS BRONZE PROCESSOR - Processamento GPU Acelerado")
    print("=" * 70)
    print(f"cuDF version: {cudf.__version__}")
    print(f"Python: {sys.version.split()[0]}")
    
    # Inicializa processador
    processor = BronzeRapidsProcessor()
    
    # Descobre arquivos (limite para teste inicial)
    files = processor.discover_files(limit=100)  # Processa 100 arquivos
    
    if not files:
        print("❌ Nenhum arquivo encontrado!")
        return
    
    # Processa dados
    try:
        # 1. Leitura
        df = processor.read_multiple_files(files, verbose=True)
        
        # 2. Limpeza
        df_clean = processor.clean_data(df, verbose=True)
        
        # 3. Agregação
        aggregations = processor.aggregate_data(df_clean, verbose=True)
        
        # 4. Relatório
        processor.generate_report(aggregations, verbose=True)
        
        print("\n✅ Processamento concluído com sucesso!")
        
    except Exception as e:
        print(f"\n❌ Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
