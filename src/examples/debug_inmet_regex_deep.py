import duckdb
import glob
import os

BRONZE_PATH = r"D:\_data-science\GitHub\eco-sentinel\data\bronze\inmet"

con = duckdb.connect()

# Vamos testar se o problema é que o filename retornado no Windows inclui o caminho completo
# e a regex assume que começa com INMET...
# Na regex do script principal: regexp_extract(filename, 'INMET_[A-Z]{2}_([A-Z]{2})_', 1)
# Se o filename for D:\...\INMET_..., a regex funciona porque busca o padrão em qualquer lugar da string?
# Vamos verificar.

print("🔍 Testing regex on full paths...")

# Get a real full path example
files = glob.glob(os.path.join(BRONZE_PATH, "**/*.CSV"), recursive=True)
if not files:
    print("No files found to test.")
else:
    sample_file = files[0]
    print(f"Sample Path: {sample_file}")
    
    # DuckDB query to test regex on this string
    query = f"SELECT regexp_extract('{sample_file.replace(chr(92), '/')}', 'INMET_[A-Z]{{2}}_([A-Z]{{2}})_', 1)"
    result = con.execute(query).fetchone()[0]
    print(f"Extracted UF (Forward Slash): '{result}'")
    
    query_back = f"SELECT regexp_extract('{sample_file.replace(chr(92), chr(92)+chr(92))}', 'INMET_[A-Z]{{2}}_([A-Z]{{2}})_', 1)"
    result_back = con.execute(query_back).fetchone()[0]
    print(f"Extracted UF (Back Slash): '{result_back}'")

    # Agora vamos ver se existe algum arquivo onde o padrão não casa
    # Talvez algum arquivo tenha nome diferente, tipo "INMET_NE_BA_A401_SALVADOR..." vs "INMET_CO_DF_A001..."
    # O padrão espera: INMET_ + 2 letras (Regiao) + _ + 2 letras (UF) + _
    
    # Vamos buscar arquivos que NÃO casam com esse padrão
    con.execute(f"""
        CREATE TABLE debug_filenames AS 
        SELECT filename
        FROM read_csv('{BRONZE_PATH}/**/*.CSV', 
            header=False, 
            skip=9, 
            sep=';',
            filename=True,
            encoding='latin-1',
            all_varchar=True,
            auto_detect=False,
            null_padding=True,
            ignore_errors=True,
            columns={{'c0': 'VARCHAR'}}
        )
    """)
    
    print("\nScanning for files mismatching the regex 'INMET_[A-Z]{2}_([A-Z]{2})_' ...")
    mismatches = con.execute("""
        SELECT DISTINCT filename 
        FROM debug_filenames 
        WHERE regexp_extract(filename, 'INMET_[A-Z]{2}_([A-Z]{2})_', 1) = ''
    """).fetchall()
    
    if mismatches:
        print(f"❌ Found {len(mismatches)} mismatches. Examples:")
        for m in mismatches[:5]:
            print(m[0])
    else:
        print("✅ All filenames match the regex pattern.")
