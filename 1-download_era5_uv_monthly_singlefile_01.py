# download_era5_uv_monthly_singlefile.py
# ------------------------------------------------------------
# Baixa ERA5-Land (reanalysis-era5-land) por MÊS, com u10 + v10
# num ÚNICO arquivo .nc por mês (ex.: era5land_1997_01.nc).
# Evita arquivos duplicados, espera finalizar no disco e
# descompacta zip/gzip automaticamente.
#
# Reqs:
#   pip install --upgrade cdsapi xarray netCDF4 h5netcdf numpy pandas
# ------------------------------------------------------------

import os
import time
import uuid
import zipfile
import gzip
import shutil
from pathlib import Path
import cdsapi

# ====== CONFIG ======
LAT, LON = -21.75, -41.33
PAD = 0.25
AREA = [LAT + PAD, LON - PAD, LAT - PAD, LON + PAD]

YEARS  = list(range(2012, 2025))        # 1994–2024
MONTHS = [f"{m:02d}" for m in range(1, 13)]
DAYS   = [f"{d:02d}" for d in range(1, 32)]
HOURS  = [f"{h:02d}:00" for h in range(24)]

# pasta onde ficam os .nc (o seu gerador já lê daqui)
OUT_DIR = Path(r"C:\Users\alexs\scripts\tmp_nc")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# dataset e variáveis (as duas juntas na mesma requisição!)
DATASET   = "reanalysis-era5-land"
VARIABLES = ["10m_u_component_of_wind", "10m_v_component_of_wind"]

# Robustez
RETRIES_PER_MONTH = 3
POST_DOWNLOAD_DELAY_S = 5            # folga pós-download p/ soltar lock
WAIT_STABLE_TIMEOUT_S = 240
WAIT_STABLE_INTERVAL_S = 1.0


# ====== UTILS ======
def _wait_file_stable(path: Path, timeout=WAIT_STABLE_TIMEOUT_S, interval=WAIT_STABLE_INTERVAL_S) -> bool:
    """Espera o arquivo parar de crescer (estável) antes de abrir/mover."""
    t0 = time.time()
    last = -1
    while time.time() - t0 < timeout:
        if not path.exists():
            time.sleep(interval); continue
        try:
            sz = path.stat().st_size
        except Exception:
            time.sleep(interval); continue
        if sz > 0 and sz == last:
            return True
        last = sz
        time.sleep(interval)
    return False

def _sniff(path: Path) -> bytes:
    try:
        with open(path, "rb") as f:
            return f.read(8)
    except Exception:
        return b""

def _unpack_if_needed(path: Path) -> Path:
    """Se for ZIP/GZIP, descompacta para .nc e retorna o novo caminho; senão devolve o mesmo."""
    head = _sniff(path)
    if head.startswith(b"PK"):  # ZIP
        with zipfile.ZipFile(path, "r") as z:
            members = [m for m in z.namelist() if m.lower().endswith(".nc")]
            if not members:
                return path
            target = OUT_DIR / f"{path.stem}_unz.nc"
            with z.open(members[0], "r") as zin, open(target, "wb") as out:
                shutil.copyfileobj(zin, out)
            try: os.remove(path)
            except: pass
            return target
    if head.startswith(b"\x1f\x8b"):  # GZIP
        target = OUT_DIR / f"{path.stem}_unz.nc"
        with gzip.open(path, "rb") as gz, open(target, "wb") as out:
            shutil.copyfileobj(gz, out)
        try: os.remove(path)
        except: pass
        return target
    return path

def _cleanup_previous_same_month(year: int, month: str):
    """Apaga arquivos antigos do mesmo mês (para não ficar com 2-3 por mês)."""
    stem = f"era5land_{year}_{month}"
    for p in OUT_DIR.glob(f"{stem}*.nc"):
        try: os.remove(p)
        except: pass


# ====== DOWNLOAD ======
def retrieve_month(client: cdsapi.Client, year: int, month: str) -> Path:
    """
    Faz UMA requisição com as duas variáveis (u10 e v10) e salva como
    C:/Users/alexs/scripts/tmp_nc/era5land_YYYY_MM.nc
    """
    # garante que não há restos anteriores
    _cleanup_previous_same_month(year, month)

    tmp_name = OUT_DIR / f"era5land_{year}_{month}_{uuid.uuid4().hex}.nc"
    req = {
        "variable": VARIABLES,        # <- ambas variáveis juntas
        "year": str(year),
        "month": month,
        "day": DAYS,
        "time": HOURS,
        "area": AREA,
        "format": "netcdf",
    }
    client.retrieve(DATASET, req, str(tmp_name))

    # espera o arquivo estabilizar + folga para soltar lock
    _wait_file_stable(tmp_name)
    time.sleep(POST_DOWNLOAD_DELAY_S)

    # se veio zip/gzip, descompacta
    final_tmp = _unpack_if_needed(tmp_name)

    # renomeia para nome limpo (sem UUID)
    final_path = OUT_DIR / f"era5land_{year}_{month}.nc"
    # remove se já existir
    if final_path.exists():
        try: os.remove(final_path)
        except: pass
    os.replace(final_tmp, final_path)
    return final_path


def process_year(client: cdsapi.Client, year: int):
    print(f"\n↓ Baixando {year} (1 arquivo por mês, u+v juntos)...")
    for m in MONTHS:
        ok = False
        for attempt in range(1, RETRIES_PER_MONTH + 1):
            try:
                nc = retrieve_month(client, year, m)
                print(f"  {year}-{m}: OK -> {nc.name}")
                ok = True
                break
            except Exception as e:
                print(f"  {year}-{m}: tentativa {attempt}/{RETRIES_PER_MONTH} falhou -> {e}")
                time.sleep(5 * attempt)
        if not ok:
            print(f"✖ {year}-{m}: falhou após {RETRIES_PER_MONTH} tentativas.")


def main():
    # usa ~/.cdsapirc
    try:
        client = cdsapi.Client()
    except Exception as e:
        print("Erro ao criar cdsapi.Client():", e)
        print("Verifique seu ~/.cdsapirc (url e key).")
        return

    for y in YEARS:
        process_year(client, y)

    print("\nConcluído. Agora rode o gerador de CSVs (gera_csvs_sam_number0.py).")


if __name__ == "__main__":
    main()
