import duckdb

# Check data structure
con = duckdb.connect()
df = con.execute("SELECT * FROM read_parquet('data/gold/dengue_clima.parquet')").df()

print("Anos únicos:", sorted(df['semana_epidemiologica'].astype(str).str[:4].unique()))
print("Total registros:", len(df))
print("\nSemanas epidemiológicas (primeiras 10):")
print(sorted(df['semana_epidemiologica'].unique())[:10])
print("\nPrimeiras linhas:")
print(df[['semana_epidemiologica', 'casos_notificados', 'inmet_temp_media_lag1']].head())
print("\nColunas:")
print(df.columns.tolist())
print("\nEstatísticas dos lag features:")
for col in ['inmet_temp_media_lag1', 'inmet_precip_tot_lag1']:
    if col in df.columns:
        print(f"{col}: {df[col].count()} valores não-nulos")