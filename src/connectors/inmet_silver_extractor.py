import os
import glob
import pandas as pd


def _read_metadata(fp):
    meta = {"uf": None, "station_name": None, "station_code": None}
    with open(fp, "r", encoding="latin-1", errors="ignore") as f:
        lines = [next(f) for _ in range(8)]
    for line in lines:
        if line.startswith("UF:"):
            parts = line.strip().split(";")
            meta["uf"] = parts[1].strip() if len(parts) > 1 else None
        elif line.startswith("ESTACAO:") or line.startswith("ESTAÇÃO:"):
            parts = line.strip().split(";")
            meta["station_name"] = parts[1].strip() if len(parts) > 1 else None
        elif line.startswith("CODIGO") or line.startswith("CÓDIGO"):
            parts = line.strip().split(";")
            meta["station_code"] = parts[1].strip() if len(parts) > 1 else None
    return meta


def _to_float_series(s):
    return pd.to_numeric(s.str.replace(",", "."), errors="coerce")


def _read_data(fp):
    df = pd.read_csv(
        fp,
        sep=";",
        skiprows=8,
        encoding="latin-1",
        dtype=str,
        na_values=["", "NaN"],
    )
    df = df.dropna(how="all")
    cols = list(df.columns)
    data = df.iloc[:, 0]
    hora = df.iloc[:, 1]
    precip = df.iloc[:, 2]
    radiacao = df.iloc[:, 6] if len(cols) > 6 else None
    temperatura = df.iloc[:, 7] if len(cols) > 7 else None
    umidade = df.iloc[:, 15] if len(cols) > 15 else None
    out = pd.DataFrame()
    out["data"] = pd.to_datetime(data.str.replace("/", "-"), format="%Y-%m-%d", errors="coerce")
    out["hora"] = hora
    out["precipitacao_mm"] = _to_float_series(precip)
    out["radiacao_kj_m2"] = _to_float_series(radiacao) if radiacao is not None else pd.NA
    out["temperatura_c"] = _to_float_series(temperatura) if temperatura is not None else pd.NA
    out["umidade_rel_%"] = _to_float_series(umidade) if umidade is not None else pd.NA
    out["ano"] = out["data"].dt.year
    return out


def extract_inmet_silver(bronze_root="data/bronze/inmet", silver_root="data/silver"):
    files = glob.glob(os.path.join(bronze_root, "**", "*.CSV"), recursive=True)
    if not files:
        return "no_files"
    for fp in files:
        meta = _read_metadata(fp)
        df = _read_data(fp)
        df["uf"] = meta.get("uf")
        df["station_code"] = meta.get("station_code")
        df["station_name"] = meta.get("station_name")
        df = df.dropna(subset=["data"]).reset_index(drop=True)
        uf = df["uf"].iloc[0] if df["uf"].notna().any() else "NA"
        ano = int(df["ano"].dropna().iloc[0]) if df["ano"].notna().any() else 0
        part_dir = os.path.join(silver_root, "inmet", f"uf={uf}", f"ano={ano}")
        os.makedirs(part_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(fp))[0]
        out_fp = os.path.join(part_dir, f"{base}.parquet")
        try:
            df.to_parquet(out_fp, index=False)
        except Exception:
            df.to_csv(out_fp.replace(".parquet", ".csv"), index=False)
    return "ok"


if __name__ == "__main__":
    extract_inmet_silver()
