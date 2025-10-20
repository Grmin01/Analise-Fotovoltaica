# gera_csvs_sam_number0.py
# Gera SAM_ERA5_HIST_YYYY.csv com DateTime,WindSpeed prontos pro SAM
# - trata ZIP/GZIP
# - abre com netcdf4/h5netcdf/scipy
# - filtra only number==0 (membro principal)
# - preserva 'valid_time' e usa como eixo temporal se existir (senão usa 'time')
# - remove dims extras (expver/step/realization) e coords problemáticas
# - seleciona grid mais próximo (LAT/LON)
# - calcula WindSpeed
# - garante 1 linha/hora (8760/8784), sem duplicatas

from pathlib import Path
import re, zipfile, gzip, shutil, time
import numpy as np
import pandas as pd
import xarray as xr

# ---- CONFIG ----
LAT, LON = -21.75, -41.33
TMP_DIR = Path(r"C:\Users\alexs\scripts\tmp_nc")
OUT_DIR = Path(r"C:\Users\alexs\OneDrive\Área de Trabalho\clima_campos\dados")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ENGINES = ("netcdf4", "h5netcdf", "scipy")
OPEN_RETRIES = 120
SLEEP = 1.0

pat = re.compile(r"era5land_(\d{4})_(\d{2})_.*\.nc$", re.IGNORECASE)
files = sorted([p for p in TMP_DIR.glob("*.nc") if pat.search(p.name)])
if not files:
    print("Nenhum .nc 'era5land_YYYY_MM_*.nc' em", TMP_DIR); raise SystemExit
print(f"Arquivos encontrados: {len(files)}")

def sniff(p: Path) -> bytes:
    with open(p, "rb") as f:
        return f.read(8)

def ensure_nc(p: Path) -> Path:
    head = sniff(p)
    if head.startswith(b"PK"):  # ZIP
        with zipfile.ZipFile(p, "r") as z:
            mems = [m for m in z.namelist() if m.lower().endswith(".nc")]
            if not mems: raise RuntimeError(f"ZIP sem .nc: {p}")
            out = TMP_DIR / (p.stem + "_unz.nc")
            with z.open(mems[0], "r") as zin, open(out, "wb") as fout:
                shutil.copyfileobj(zin, fout)
            return out
    if head.startswith(b"\x1f\x8b"):  # GZIP
        out = TMP_DIR / (p.stem + "_unz.nc")
        with gzip.open(p, "rb") as gz, open(out, "wb") as fout:
            shutil.copyfileobj(gz, fout)
        return out
    return p

def open_any(nc: Path) -> xr.Dataset:
    last = None
    for _ in range(OPEN_RETRIES):
        for eng in ENGINES:
            try:
                with xr.open_dataset(nc, engine=eng) as ds:
                    return ds.load()
            except Exception as e:
                last = e
        time.sleep(SLEEP)
    raise RuntimeError(f"Falha ao abrir {nc}: {last}")

def normalize_dims(ds: xr.Dataset) -> xr.Dataset:
    # renomeia variáveis
    if "10m_u_component_of_wind" in ds:
        ds = ds.rename({"10m_u_component_of_wind": "u10"})
    if "10m_v_component_of_wind" in ds:
        ds = ds.rename({"10m_v_component_of_wind": "v10"})

    # filtra membro principal se houver 'number'
    if "number" in ds.dims:
        try:
            ds = ds.sel(number=0)
        except Exception:
            ds = ds.isel(number=0, drop=True)

    # colapsa dims extras (NÃO remover valid_time aqui!)
    for dim in ("expver", "step", "realization"):
        if dim in ds.dims:
            ds = ds.isel({dim: 0}, drop=True)

    # algumas fontes trazem 'forecast_reference_time'/'surface' etc.
    drop_coords = [c for c in ("forecast_reference_time", "surface") if c in ds.coords]
    if drop_coords:
        ds = ds.drop_vars(drop_coords)

    return ds

def select_point(ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    # ajusta 0..360 se necessário
    if "longitude" in ds.coords:
        lons = ds["longitude"].values
        if lons.min() >= 0 and lon < 0:
            lon = 360 + lon
    if {"latitude","longitude"}.issubset(ds.coords):
        ds = ds.sel(latitude=lat, longitude=lon, method="nearest")
    return ds

# agrupa por ano
by_year = {}
for p in files:
    m = pat.search(p.name)
    if not m:
        continue
    year = int(m.group(1))
    by_year.setdefault(year, []).append(p)

years = sorted(by_year.keys())
print("Anos detectados:", years)

def expected_hourly_index(year: int) -> pd.DatetimeIndex:
    start = pd.Timestamp(year=year, month=1, day=1, hour=0)
    end   = pd.Timestamp(year=year, month=12, day=31, hour=23)
    return pd.date_range(start, end, freq="h")

def extract_datetime_and_values(wspd: xr.DataArray, target_year: int) -> pd.DataFrame:
    """
    Constrói DataFrame DateTime/WindSpeed priorizando 'valid_time' como eixo temporal.
    Corrige o caso em que o índice é DatetimeIndex (sem .dt).
    """
    s = wspd.to_series()

    def get_dt_from_index(idx):
        # prioridade: valid_time > time
        names = list(idx.names) if isinstance(idx, pd.MultiIndex) else [idx.name]
        candidate = None
        if isinstance(idx, pd.MultiIndex):
            if "valid_time" in names:
                candidate = idx.get_level_values("valid_time")
            elif "time" in names:
                candidate = idx.get_level_values("time")
            else:
                for name in names:
                    vals = idx.get_level_values(name)
                    try:
                        parsed = pd.to_datetime(vals, errors="coerce")
                        if parsed.notna().any():
                            candidate = parsed
                            break
                    except Exception:
                        pass
        else:
            candidate = idx

        dt = pd.to_datetime(candidate, errors="coerce")
        # remove tz se houver
        if hasattr(dt, "tz_localize"):
            try:
                dt = dt.tz_localize(None)
            except Exception:
                pass
        # arredonda p/ hora
        dt = dt.floor("h")

        # filtro por ano alvo (Index.year vs Series.dt.year)
        years_arr = dt.year if isinstance(dt, pd.DatetimeIndex) else dt.dt.year
        mask_year = (years_arr == target_year)

        if isinstance(idx, pd.MultiIndex):
            vals = s.values[mask_year]
        else:
            vals = s.values[mask_year]

        return dt[mask_year], vals

    dt, vals = get_dt_from_index(s.index)
    df = pd.DataFrame({"DateTime": dt, "WindSpeed": vals})
    df = df.loc[df["DateTime"].notna()].copy()
    # dedup por hora (média)
    df = df.groupby("DateTime", as_index=False)["WindSpeed"].mean()
    return df

for year in years:
    out_csv = OUT_DIR / f"SAM_ERA5_HIST_{year}.csv"
    if out_csv.exists():
        print(f"→ {year}: já existe, pulando ({out_csv})")
        continue

    ds_list = []
    for p in sorted(by_year[year]):
        try:
            real_nc = ensure_nc(p)
            ds = open_any(real_nc)
            ds = normalize_dims(ds)
            ds = select_point(ds, LAT, LON)

            if not all(v in ds for v in ("u10","v10")):
                raise RuntimeError(f"u10/v10 ausentes em {real_nc}. Vars: {list(ds.data_vars)}")

            sub = ds[["u10","v10"]]
            # reduz qualquer dimensão além de 'time' e 'valid_time'
            extra_dims = [d for d in sub.dims if d not in ("time", "valid_time")]
            for d in extra_dims:
                sub = sub.isel({d: 0}, drop=True)

            ds_list.append(sub)
        except Exception as e:
            print(f"  ERRO {p.name} -> {e}")

    if not ds_list:
        print(f"✖ {year}: nenhum mês processado.")
        continue

    # concatena e ordena
    ds_all = xr.concat(ds_list, dim="time").sortby("time")

    # calcula WindSpeed
    wspd = np.sqrt(ds_all["u10"]**2 + ds_all["v10"]**2).rename("WindSpeed")

    # extrai DateTime/WindSpeed (valid_time > time)
    df = extract_datetime_and_values(wspd, target_year=year)

    # reindexa ao calendário horário do ano
    idx = expected_hourly_index(year)
    df = df.set_index("DateTime").reindex(idx)
    df.index.name = "DateTime"

    missing = int(df["WindSpeed"].isna().sum())
    total   = len(df)
    print(f"  {year}: linhas={total} | faltando={missing}")

    if missing > 0:
        # descomente se quiser interpolar em vez de falhar:
        # df["WindSpeed"] = df["WindSpeed"].interpolate(limit_direction="both")
        # missing_after = int(df["WindSpeed"].isna().sum())
        # print(f"  {year}: faltando após interpolação = {missing_after}")
        # if missing_after > 0:
        #     raise RuntimeError(f"{year}: ainda faltam {missing_after} horas após interpolar.")
        raise RuntimeError(f"{year}: faltam {missing} horas no índice horário esperado.")

    df.reset_index().to_csv(out_csv, index=False)
    print(f"✔ Salvo: {out_csv}")

print("\nConcluído.")
