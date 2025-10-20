# pipeline_morph_validate_sam.py
# ============================================================
# Morph (ERA5 -> NEX-GDDP-CMIP6) → Validar CSV → Rodar SAM (PVWatts v8)
# Ajustes:
#  - Anos < 2015 vão para .../historical/ (não em ssp245/ssp585).
#  - (Opcional) sfcWind do ano-alvo (NEX) substitui a climatologia ao montar o delta mensal.
# Requer: pandas, numpy, xarray, PySAM, tqdm, matplotlib (opcional p/ gráficos)
# ============================================================
"""
pipeline_morph_validate_sam.py
===============================================================
Morph (ERA5 -> NEX-GDDP-CMIP6) → valida CSV → roda SAM (PVWatts v8)

RESUMO
- Usa um CSV horário de ERA5 (um único ano completo, 8760 linhas) como
  PERFIL "moldável" (template). Esse perfil recebe deltas/fatores MENSAIS
  derivados do NEX-GDDP-CMIP6 (rsds, tas, sfcWind, hurs) para gerar um
  novo CSV horário para cada ano-alvo (histórico ou futuro). Em seguida,
  valida o CSV e executa o SAM (PySAM/Pvwattsv8) para obter Annual MWh e CF.

PAPÉIS DAS FONTES (por que "ERA5" aparece 2 vezes?)
1) CSV_ERA5_BASE  (ex.: 2019)
   - Um ÚNICO ANO horário (8760) com colunas: DateTime, GHI, DNI, DHI,
     TempC, WindSpeed, RelHum.
   - É o PERFIL que será "deformado" mês a mês pelos deltas/fatores do NEX.
   - Reindexamos apenas o ano no DateTime para o ano-alvo.

2) ERA5_HOURLY_DIR (1994–2014)
   - VÁRIOS ANOS históricos em CSV para calcular a CLIMATOLOGIA MENSAL
     de vento (WindSpeed). Serve de baseline de vento.
   - Gera 12 valores (jan–dez) para formar k_wspd = fut_wspd / clim_wspd.
   - Se indisponível, cai no fallback NEX historical para 'sfcWind'.

3) NEX_DIR_ROOT (NEX-GDDP-CMIP6)
   - Fonte dos valores MENSAIS por ano-alvo e da climatologia histórica:
     * rsds  (radiação de onda curta)
     * tas   (temperatura do ar a 2m)
     * sfcWind (vento 10m)
     * hurs  (umidade relativa)
   - Para 1994–2014: monta climatologia (baseline).
   - Para 2015+ (e também 1994–2014 quando desejado): lê o ano-alvo mensal.

4) PLANILHA_ERA5 (fallback)
   - Plano B apenas para rsds/tas históricos caso o NEX historical esteja
     ausente. Extrai SSR (proxy de rsds) e SKT (proxy de temperatura)
     médios mensais (1994–2014).

MORPH – DELTAS/ FATORES MENSAIS (aplicados ao CSV_ERA5_BASE):
- k_rsds = fut_rsds / clim_rsds         → aplicado multiplicativamente em GHI
- d_tas  = fut_tas  - clim_tas          → aplicado aditivamente em TempC (°C)
- k_wspd = fut_wspd / clim_wspd         → aplicado multiplicativamente em WindSpeed
- d_hurs = fut_hurs - clim_hurs         → aplicado aditivamente em RelHum (%)
Obs.: DNI/DHI são recalculados mantendo a fração DNI/GHI do perfil.

OPÇÕES IMPORTANTES
- USE_YEARLY_WIND=True: usa vento do ANO-ALVO (NEX) nos fatores (recomendado).
  Se desligar, pode usar apenas climatologia (ajuste metodológico).
- IRRAD_SCALE: None = autodetecta escala de irradiância do CSV base (GHI/DNI/DHI)
  para evitar picos físicos irreais; ou fixe um fator manual.

JANELA E CENÁRIOS
- YEARS define a faixa de anos (ex.: 1994–2054).
- SCENARIOS define os cenários (ex.: ["ssp245","ssp585"]).
- Anos < 2015 são salvos em subpasta 'historical'; demais, em ssp correspondente.
- Evita duplicar histórico no 2º cenário.

SAÍDAS
- CSV horário morfado por ano/cenário:
  {OUT_SAM_CSV_DIR}/{historical|ssp245|ssp585}/SAM_{MODEL}_{subdir}_{YYYY}_morph.csv
- LOGs por execução de SAM em LOG_DIR (JSON ou mensagem de erro).
- CSV resumo com Annual MWh/CF, se PLOT_RESULTS=True também salva um PNG simples.

VALIDAÇÃO
- Confere existência das colunas esperadas e ausência de NaN após morph.
- Ajusta ano no DateTime e garante 8760 linhas (remove 29/02 se necessário).

PRÉ-REQUISITOS
- Python: pandas, numpy, xarray, PySAM, tqdm (opcional), matplotlib (opcional).
- Estrutura local dos dados NEX: .../MODEL/{historical|ssp245|ssp585}/
  com arquivos day_* por variável/ano no padrão do NEX-GDDP-CMIP6.
- CSV_ERA5_BASE com 8760 e colunas padrão do SAM.

FLUXO NO PROJETO
1) (Opcional/externo) Baixar/processar ERA5 e gerar CSV_ERA5_BASE e ERA5_HOURLY_DIR.
2) Este script: morph mensal (NEX vs climatologias) + valida + roda SAM.
3) Consolidação/análise posterior:
   - Use um consolidador de logs/CSVs para gerar resultado_sam_consolidado_morph.csv.
   - Rode analisar_sam_results.py para tendências (MK, Sen, Pettitt) e gráficos.

AVISOS
- Se faltar NEX historical para algum 'var', o script tenta fallback (planilha para rsds/tas
  e NEX/ERA5 para vento). Para 'hurs' ausente, usa delta 0%.
- Ajuste MODEL e caminhos (NEX_DIR_ROOT, CSV_ERA5_BASE, ERA5_HOURLY_DIR, PLANILHA_ERA5)
  de acordo com seu ambiente.
"""

import os, json, time, warnings, re
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

warnings.filterwarnings("ignore", category=FutureWarning)

try:
    from tqdm import tqdm
    HAS_TQDM = True
except Exception:
    HAS_TQDM = False

# ================ CONFIG (AJUSTE AQUI) =================
# Coordenadas “Campos dos Goytacazes (RJ)” aprox. (~-21.7, -41.3)
LAT, LON, ELEV, TZ = -21.7, -41.3, 20, -3

# Sistema (igual ao seu)
SYSTEM_CAP_KW = 1000.0
TILT_DEG      = 21.5
AZIMUTH_DEG   = 0.0
DC_AC_RATIO   = 1.2
LOSSES_PCT    = 14.0
INVERTER_EFF  = 96.0
MODULE_TYPE   = 0
ARRAY_TYPE    = 0
GCR           = 0.40


# Modelo e cenários
MODEL     = "ACCESS-CM2"
SCENARIOS = ["ssp245", "ssp585"]         # <=== AJUSTADO

# Janela de anos
YEARS = list(range(1994, 2055))          # <=== AJUSTADO (1994–2054)

# ERA5 base hora-a-hora (perfil “moldável”)
ERA5_BASE_YEAR = 2019
CSV_ERA5_BASE  = r"C:\dados\SAM_CSV_ERA5\SAM_ERA5_HIST_2019_clean_clean.csv"

# NEX-GDDP-CMIP6 (.nc por var/ano), com subpastas: historical, ssp245, ssp585
NEX_DIR_ROOT   = rf"C:\dados\nex_cmip6\{MODEL}"

# CSVs ERA5 horários (1994–2014) com DateTime, WindSpeed (climatologia de vento)
ERA5_HOURLY_DIR = r"C:\Users\alexs\OneDrive\Área de Trabalho\clima_campos\dados"

# Planilha com SSR/SKT históricos (fallback p/ rsds/tas)
PLANILHA_ERA5 = r"C:\dados\exportacao_era5_com_precip_e_radiacao.xlsx"

# Saídas
OUT_SAM_CSV_DIR = rf"C:\Users\alexs\OneDrive\Área de Trabalho\SAM_CSV_MORPH\{MODEL}"
OUT_RESULTS_CSV = rf"C:\Users\alexs\OneDrive\Área de Trabalho\resultado_sam_TEST_1994_2023_2047.csv"
LOG_DIR         = rf"C:\Users\alexs\OneDrive\Área de Trabalho\logs_sam_morph_TEST"
os.makedirs(LOG_DIR, exist_ok=True)

# Escala opcional de irradiância do CSV base (GHI/DNI/DHI)
# None = autodetecta (recomendado); ou fixe um número (ex.: 23.5)
IRRAD_SCALE = None

# Preferir vento do ano-alvo (NEX) p/ montar delta mensal?
USE_YEARLY_WIND = True

# Gráficos ao final
PLOT_RESULTS = True

VARS = ["rsds", "tas", "sfcWind", "hurs"]

# ===================== Utilidades ======================
def ensure_datetime(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=False)
    if getattr(s.dtype, "tz", None) is not None:
        try:
            s = s.dt.tz_convert(None)
        except Exception:
            s = s.dt.tz_localize(None)
    if s.isna().any():
        raise ValueError(f"Falha ao converter DateTime: {int(s.isna().sum())} linhas inválidas.")
    return s

def _guess_irr_scale(df: pd.DataFrame) -> float:
    ghi = pd.to_numeric(df.get("GHI", pd.Series(dtype=float)), errors="coerce")
    p95 = np.nanpercentile(ghi, 95) if len(ghi) else np.nan
    if not np.isfinite(p95) or p95 <= 0:
        return 1.0
    target = 900.0  # W/m² típico de pico
    scale = target / p95
    return float(np.clip(scale, 0.1, 100.0))

def read_era5_base(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "DateTime" not in df.columns:
        raise ValueError("CSV ERA5 base precisa ter coluna DateTime.")
    df["DateTime"] = ensure_datetime(df["DateTime"]).dt.floor("h")
    df = df.sort_values("DateTime")
    req = ["GHI","DNI","DHI","TempC","WindSpeed","RelHum"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"Falta coluna {c} no ERA5 base.")
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if df[req].isna().any().any():
        raise ValueError("NaN no ERA5 base.")

    # Escala de irradiância
    scale = _guess_irr_scale(df) if IRRAD_SCALE is None else float(IRRAD_SCALE)
    if abs(scale - 1.0) > 1e-6:
        for c in ["GHI","DNI","DHI"]:
            df[c] = df[c].astype(float) * scale

    # remove 29/02 se tiver
    if len(df) == 8784:
        df = df.loc[~((df["DateTime"].dt.month==2) & (df["DateTime"].dt.day==29))]
    if len(df) != 8760:
        raise ValueError(f"ERA5 base tem {len(df)} linhas (esperado 8760).")
    df["month"] = df["DateTime"].dt.month
    return df

def _find_first_existing(base: Path, patterns: list) -> Path | None:
    for pat in patterns:
        hits = list(base.rglob(pat))
        if hits:
            return hits[0]
    return None

def _load_nex_one_year_monthly(var: str, year: int, ssp: str) -> pd.Series:
    base = Path(NEX_DIR_ROOT) / ssp
    patterns = [
        f"{var}_day_{MODEL}_{ssp}_*_gn_{year}.nc",
        f"{var}_day_{MODEL}_{ssp}_*_gn_{year}_*.nc",
        f"{var}_day_{MODEL}_{ssp}_r1i1p1f1_gn_{year}.nc",
        f"{var}_day_{MODEL}_{ssp}_r1i1p1f1_gn_{year}_*.nc",
        f"{var}_day_{MODEL}_{ssp}_*_{year}.nc",
        f"{var}_day_{MODEL}_{ssp}_*_{year}_*.nc",
        f"{var}_day_{MODEL}_{ssp}_*_gn_{year}_v2.0.nc",
        f"{var}_day_{MODEL}_{ssp}_*_gn_{year}_v2.0_*.nc",
    ]
    p = _find_first_existing(base, patterns)
    if p is None:
        raise FileNotFoundError(f"NEX {ssp} {year} {var} não encontrado em {base}")

    ds = xr.open_dataset(p)
    vname = var if var in ds.data_vars else list(ds.data_vars)[0]
    da = ds[vname]

    # seleciona ponto mais próximo
    latn = next((n for n in ["lat","latitude"] if n in da.coords), None)
    lonn = next((n for n in ["lon","longitude"] if n in da.coords), None)
    lat_pt, lon_pt = LAT, LON
    if lonn:
        lons = da[lonn].values
        if np.nanmin(lons) >= 0 and lon_pt < 0:
            lon_pt = 360 + lon_pt
    if latn and lonn:
        try:
            da = da.sel({latn: lat_pt, lonn: lon_pt}, method="nearest")
        except Exception:
            pass
    # média espacial se ainda houver grade
    spdims = [d for d in da.dims if d.lower() in ("lat","latitude","lon","longitude","x","y")]
    if spdims:
        da = da.mean(dim=spdims, keep_attrs=True)

    m = da.groupby("time.month").mean("time").to_pandas()
    m.index = range(1, 13)
    if len(m) != 12:
        raise ValueError(f"{ssp} {year} {var} com {len(m)} meses (esperado 12).")
    return m.astype(float)

def _load_nex_hist_monthly(var: str, years=range(1994, 2015)) -> pd.Series:
    base = Path(NEX_DIR_ROOT) / "historical"
    das = []
    for y in years:
        p = _find_first_existing(base, [
            f"{var}_day_{MODEL}_historical_*_gn_{y}_*.nc",
            f"{var}_day_{MODEL}_historical_*_{y}_*.nc",
            f"{var}_day_{MODEL}_historical_r1i1p1f1_gn_{y}.nc",
            f"{var}_day_{MODEL}_historical_r1i1p1f1_gn_{y}_v2.0.nc",
            f"{var}_day_{MODEL}_historical_r1i1p1f1_gn_{y}.nc4",
        ])
        if p is None:
            continue
        ds = xr.open_dataset(p)
        vname = var if var in ds.data_vars else list(ds.data_vars)[0]
        da = ds[vname]
        latn = next((n for n in ["lat","latitude"] if n in da.coords), None)
        lonn = next((n for n in ["lon","longitude"] if n in da.coords), None)
        lat_pt, lon_pt = LAT, LON
        if lonn is not None:
            lons = da[lonn].values
            if np.nanmin(lons) >= 0 and lon_pt < 0:
                lon_pt = 360 + lon_pt
        if latn and lonn:
            try:
                da = da.sel({latn: lat_pt, lonn: lon_pt}, method="nearest")
            except Exception:
                pass
        spdims = [d for d in da.dims if d.lower() in ("lat","latitude","lon","longitude","x","y")]
        if spdims:
            da = da.mean(dim=spdims, keep_attrs=True)
        das.append(da)

    if not das:
        raise FileNotFoundError(f"NEX historical {var} não encontrado em {base} (1994–2014).")

    da = xr.concat(das, dim="time")
    m = da.groupby("time.month").mean("time").to_pandas()
    m.index = range(1, 13)
    if len(m) != 12:
        raise ValueError(f"Climatologia histórica {var} com {len(m)} meses (esperado 12).")
    return m.astype(float)

def _load_hurs_hist_monthly_from_dir(years=range(1994, 2015)) -> pd.Series:
    base = Path(NEX_DIR_ROOT) / "historical"
    das = []
    for y in years:
        p = _find_first_existing(base, [
            f"hurs_day_{MODEL}_historical_*_gn_{y}_*.nc",
            f"hurs_day_{MODEL}_historical_*_{y}_*.nc",
            f"hurs_day_{MODEL}_historical_r1i1p1f1_gn_{y}.nc",
            f"hurs_day_{MODEL}_historical_r1i1p1f1_gn_{y}_v2.0.nc",
            f"hurs_day_{MODEL}_historical_r1i1p1f1_gn_{y}.nc4",
        ])
        if p is None:
            continue
        ds = xr.open_dataset(p)
        vname = "hurs" if "hurs" in ds.data_vars else list(ds.data_vars)[0]
        da = ds[vname]
        latn = next((n for n in ["lat","latitude"] if n in da.coords), None)
        lonn = next((n for n in ["lon","longitude"] if n in da.coords), None)
        lat_pt, lon_pt = LAT, LON
        if lonn is not None:
            lons = da[lonn].values
            if np.nanmin(lons) >= 0 and lon_pt < 0:
                lon_pt = 360 + lon_pt
        if latn and lonn:
            try:
                da = da.sel({latn: lat_pt, lonn: lon_pt}, method="nearest")
            except Exception:
                pass
        spdims = [d for d in da.dims if d.lower() in ("lat","latitude","lon","longitude","x","y")]
        if spdims:
            da = da.mean(dim=spdims, keep_attrs=True)
        das.append(da)

    if not das:
        raise FileNotFoundError("hurs histórico 1994–2014 não encontrado em 'historical'.")

    da = xr.concat(das, dim="time")
    m = da.groupby("time.month").mean("time").to_pandas()
    m.index = range(1, 13)
    if len(m) != 12:
        raise ValueError(f"Climatologia histórica hurs com {len(m)} meses (esperado 12).")
    return m.astype(float)

def _load_rsds_tas_from_planilha(planilha_path: str):
    def norm(c):
        c = str(c)
        c = re.sub(r"\(.*?\)", "", c)
        c = c.replace("%", "")
        return c.strip().lower()

    xls = pd.ExcelFile(planilha_path)
    chosen = None
    for sh in xls.sheet_names:
        d = pd.read_excel(planilha_path, sheet_name=sh)
        d = d.rename(columns={c: norm(c) for c in d.columns})
        has_date = any(c in d.columns for c in ["data","date","datetime"])
        has_ssr  = any("ssr" in c for c in d.columns)
        has_skt  = any(c.startswith("skt"))
        if has_date and (has_ssr or has_skt):
            col_date = next(c for c in d.columns if c in ["data","date","datetime"])
            col_ssr  = next((c for c in d.columns if "ssr" in c), None)
            col_skt  = next((c for c in d.columns if c.startswith("skt")), None)
            chosen = (sh, d, col_date, col_ssr, col_skt)
            break
    if not chosen:
        raise FileNotFoundError("Planilha não possui colunas esperadas (Data/SSR/SKT).")

    sh, d, col_date, col_ssr, col_skt = chosen
    d[col_date] = pd.to_datetime(d[col_date], dayfirst=True, errors="coerce")
    d = d.dropna(subset=[col_date])
    d = d[(d[col_date].dt.year>=1994) & (d[col_date].dt.year<=2014)]

    def to_num(s):
        if s.dtype == "O":
            s = s.astype(str).str.replace(",", ".", regex=False).str.replace("\u00a0","",regex=False)
        return pd.to_numeric(s, errors="coerce")

    out = {}
    if col_ssr:
        _ = to_num(d[col_ssr])
        d["month"] = d[col_date].dt.month
        m = d.groupby("month")[col_ssr].mean()
        m.index = range(1,13)
        out["rsds"] = m.astype(float)

    if col_skt:
        _ = to_num(d[col_skt])
        d["month"] = d[col_date].dt.month
        m = d.groupby("month")[col_skt].mean()
        m.index = range(1,13)
        out["tas"] = m.astype(float)

    if "rsds" not in out and "tas" not in out:
        raise FileNotFoundError("Planilha não trouxe rsds/tas utilizáveis.")
    return out

def _load_wind_clim_from_era5_csvs(csv_dir: str, years=range(1994,2015)) -> pd.Series:
    arr = []
    for y in years:
        p = Path(csv_dir) / f"SAM_ERA5_HIST_{y}.csv"
        if not p.exists():
            continue
        df = pd.read_csv(p, parse_dates=["DateTime"])
        df = df.dropna(subset=["DateTime","WindSpeed"])
        df["month"] = df["DateTime"].dt.month
        arr.append(df[["month","WindSpeed"]])
    if not arr:
        raise FileNotFoundError(f"ERA5 CSVs de vento não encontrados em {csv_dir} para 1994–2014.")
    dfc = pd.concat(arr, axis=0)
    m = dfc.groupby("month")["WindSpeed"].mean()
    m.index = range(1,13)
    return m.astype(float)

def quick_check(years_sample: list[int]):
    print("\n[QuickCheck] Verificando arquivos NEX-GDDP, planilha ERA5 e CSVs horários ERA5...")
    print("  Raiz NEX_DIR_ROOT:", Path(NEX_DIR_ROOT))
    for ssp in SCENARIOS:
        for y in years_sample:
            for v in ["rsds","tas","sfcWind","hurs"]:
                ssp_name = ssp if y >= 2015 else "historical"
                base = Path(NEX_DIR_ROOT) / ssp_name
                try:
                    p = _find_first_existing(base, [
                        f"{v}_day_{MODEL}_{ssp_name}_*_gn_{y}.nc",
                        f"{v}_day_{MODEL}_{ssp_name}_*_gn_{y}_*.nc",
                        f"{v}_day_{MODEL}_{ssp_name}_r1i1p1f1_gn_{y}.nc",
                        f"{v}_day_{MODEL}_{ssp_name}_r1i1p1f1_gn_{y}_*.nc",
                        f"{v}_day_{MODEL}_{ssp_name}_*_{y}.nc",
                        f"{v}_day_{MODEL}_{ssp_name}_*_{y}_*.nc",
                        f"{v}_day_{MODEL}_{ssp_name}_*_gn_{y}_v2.0.nc",
                    ])
                    tag = f"{p}" if p else "NADA"
                    if y < 2015 and p:
                        tag += " (usa historical)"
                except Exception:
                    tag = "NADA"
                print(f"  {v} {ssp} {y} -> {tag}")
    try:
        years = sorted([int(Path(p).stem.split("_")[-1]) for p in Path(ERA5_HOURLY_DIR).glob("SAM_ERA5_HIST_*.csv")])
        if years:
            print(f"  ERA5_HOURLY_DIR: {ERA5_HOURLY_DIR} -> {len(years)} anos (ex.: {years[:5]}...)")
        else:
            print(f"  ERA5_HOURLY_DIR: {ERA5_HOURLY_DIR} -> NADA")
    except Exception:
        print(f"  ERA5_HOURLY_DIR: {ERA5_HOURLY_DIR} -> NADA")
    print("[QuickCheck] Concluído.\n")

def _as_monthly_series(x, name: str, allow_none=False, fill=None) -> pd.Series:
    if x is None:
        if allow_none:
            return pd.Series({m: float(fill) for m in range(1, 13)}, dtype=float)
        raise ValueError(f"Climatologia/futuro de '{name}' está ausente (None).")
    if isinstance(x, (int, float, np.floating)):
        return pd.Series({m: float(x) for m in range(1, 13)}, dtype=float)
    s = pd.Series(x)
    if len(s) != 12:
        raise ValueError(f"'{name}' deveria ter 12 valores mensais, mas veio com {len(s)}.")
    s.index = range(1, 13)
    return s.astype(float)

def load_hist_clims():
    clim = {}
    for v in ["rsds", "tas"]:
        try:
            s = _load_nex_hist_monthly(v)
            clim[v] = s
        except Exception:
            if Path(PLANILHA_ERA5).exists():
                print(f"[AVISO] Usando climatologia da planilha para '{v}' (1994–2014).")
                plan = _load_rsds_tas_from_planilha(PLANILHA_ERA5)
                if v in plan and plan[v] is not None and len(plan[v]) == 12:
                    clim[v] = plan[v].astype(float)
            if v not in clim:
                raise
    try:
        print("[AVISO] Usando climatologia ERA5 (seus CSVs) para 'sfcWind' (1994–2014).")
        clim["sfcWind"] = _load_wind_clim_from_era5_csvs(ERA5_HOURLY_DIR)
    except Exception:
        print("  [Fallback] Tentando NEX historical para 'sfcWind'.")
        clim["sfcWind"] = _load_nex_hist_monthly("sfcWind")
    try:
        clim["hurs"] = _load_hurs_hist_monthly_from_dir()
    except Exception:
        print("[AVISO] Climatologia histórica de 'hurs' indisponível. Delta 0% será usado.")
    return clim

def load_future_monthly(ssp: str, year: int) -> dict:
    ssp_name = ssp if year >= 2015 else "historical"
    fut = {}
    for v in VARS:
        try:
            fut[v] = _load_nex_one_year_monthly(v, year, ssp_name)
        except Exception:
            if v == "hurs":
                fut[v] = None
            else:
                raise
    return fut

def morph_one_year(ssp: str, year: int) -> str:
    base = read_era5_base(CSV_ERA5_BASE)
    clim = load_hist_clims()
    fut  = load_future_monthly(ssp, year)

    clim_rsds = _as_monthly_series(clim.get("rsds"), "clim.rsds")
    clim_tas  = _as_monthly_series(clim.get("tas"),  "clim.tas")
    clim_wspd = _as_monthly_series(clim.get("sfcWind"), "clim.sfcWind")

    fut_rsds  = _as_monthly_series(fut.get("rsds"), "fut.rsds")
    fut_tas   = _as_monthly_series(fut.get("tas"),  "fut.tas")

    # vento do ano-alvo, se disponível
    fut_wspd = _as_monthly_series(fut.get("sfcWind"), "fut.sfcWind")

    # hurs pode faltar → delta 0
    if fut.get("hurs") is None:
        fut_hurs  = _as_monthly_series(None, "fut.hurs", allow_none=True, fill=0.0)
        clim_hurs = _as_monthly_series(clim.get("hurs", fut_hurs*0.0), "clim.hurs", allow_none=True, fill=0.0)
    else:
        fut_hurs  = _as_monthly_series(fut.get("hurs"), "fut.hurs")
        clim_hurs = _as_monthly_series(clim.get("hurs"), "clim.hurs")

    # Deltas/fatores mensais
    k_rsds = (fut_rsds / clim_rsds).astype(float)      # multiplicativo
    d_tas  = (fut_tas  - clim_tas).astype(float)       # aditivo (°C)
    k_wspd = (fut_wspd / clim_wspd).astype(float)      # multiplicativo
    d_hurs = (fut_hurs - clim_hurs).astype(float)      # aditivo (%)

    k_rsds_map = k_rsds.to_dict()
    d_tas_map  = d_tas.to_dict()
    k_wspd_map = k_wspd.to_dict()
    d_hurs_map = d_hurs.to_dict()

    df = base.copy()
    # GHI/DNI/DHI
    df["GHI"] = (df["GHI"] * df["month"].map(k_rsds_map)).clip(lower=0)
    eps = 1e-6
    with np.errstate(divide='ignore', invalid='ignore'):
        frac_dni = (df["DNI"] / (df["GHI"] + eps)).clip(0, 1).fillna(0.0)
    frac_dhi = (1.0 - frac_dni).clip(0, 1)
    df["DNI"] = df["GHI"] * frac_dni
    df["DHI"] = df["GHI"] * frac_dhi

    # Temp/vento/umidade
    df["TempC"]     = df["TempC"] + df["month"].map(d_tas_map)
    df["WindSpeed"] = (df["WindSpeed"] * df["month"].map(k_wspd_map)).clip(lower=0)
    df["RelHum"]    = (df["RelHum"] + df["month"].map(d_hurs_map)).clip(0, 100)

    # Reindexa para o ano alvo
    dt0 = df["DateTime"].dt
    df["DateTime"] = pd.to_datetime({"year": year, "month": dt0.month, "day": dt0.day, "hour": dt0.hour})

    # Salvar: anos < 2015 em 'historical'; demais em ssp
    out_subdir = "historical" if year < 2015 else ssp
    outdir = Path(OUT_SAM_CSV_DIR) / out_subdir
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"SAM_{MODEL}_{out_subdir}_{year}_morph.csv"
    df[["DateTime","GHI","DNI","DHI","TempC","WindSpeed","RelHum"]].to_csv(out_path, index=False)
    return str(out_path)

def validate_sam_csv(path: str) -> None:
    df = pd.read_csv(path)
    if "DateTime" not in df.columns:
        raise ValueError("CSV sem DateTime.")
    df["DateTime"] = ensure_datetime(df["DateTime"])
    req = ["GHI","DNI","DHI","TempC","WindSpeed","RelHum"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"Falta {c}.")
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if df[req].isna().any().any():
        raise ValueError("NaN após morph.")
    if len(df) == 8784:
        df = df.loc[~((df["DateTime"].dt.month==2) & (df["DateTime"].dt.day==29))]
    if len(df) != 8760:
        raise ValueError(f"{os.path.basename(path)} tem {len(df)} linhas (esperado 8760).")

def df_to_solar_resource(df: pd.DataFrame) -> dict:
    dt = df["DateTime"]
    return {
        "lat": LAT, "lon": LON, "elev": ELEV, "tz": TZ,
        "year":   dt.dt.year.tolist(),
        "month":  dt.dt.month.tolist(),
        "day":    dt.dt.day.tolist(),
        "hour":   dt.dt.hour.tolist(),
        "minute": [0]*len(df),
        "gh":   df["GHI"].astype(float).tolist(),
        "dn":   df["DNI"].astype(float).tolist(),
        "df":   df["DHI"].astype(float).tolist(),
        "tdry": df["TempC"].astype(float).tolist(),
        "wspd": df["WindSpeed"].astype(float).tolist(),
        "rh":   df["RelHum"].astype(float).tolist(),
    }

def run_sam_from_csv(path: str) -> dict:
    import PySAM.Pvwattsv8 as Pvwattsv8
    df = pd.read_csv(path, parse_dates=["DateTime"])
    if len(df) == 8784:
        df = df.loc[~((df["DateTime"].dt.month==2) & (df["DateTime"].dt.day==29))]
    assert len(df) == 8760, "CSV não está com 8760 linhas."
    m = Pvwattsv8.new()
    m.SystemDesign.system_capacity = SYSTEM_CAP_KW
    m.SystemDesign.module_type = MODULE_TYPE
    m.SystemDesign.array_type  = ARRAY_TYPE
    m.SystemDesign.tilt        = TILT_DEG
    m.SystemDesign.azimuth     = AZIMUTH_DEG
    m.SystemDesign.gcr         = GCR
    m.SystemDesign.dc_ac_ratio = DC_AC_RATIO
    m.SystemDesign.inv_eff     = INVERTER_EFF
    m.SystemDesign.losses      = LOSSES_PCT
    m.SolarResource.solar_resource_data = df_to_solar_resource(df)
    t0 = time.time()
    m.execute()
    elapsed = time.time() - t0
    annual_kwh = float(m.Outputs.annual_energy)
    res = {
        "arquivo": os.path.basename(path),
        "annual_mwh": round(annual_kwh/1000.0, 3),
        "capacity_factor": float(m.Outputs.capacity_factor)/100.0,
        "ac_monthly_kwh": [float(x) for x in m.Outputs.ac_monthly],
        "tempo_s": round(elapsed,2)
    }
    return res

def main():
    sample_years = list(dict.fromkeys([min(YEARS), max(YEARS), 2023]))[:3]
    quick_check(sample_years)

    results = []

    scen_iter = tqdm(SCENARIOS, desc="Cenários", leave=True) if HAS_TQDM else SCENARIOS
    for ssp in scen_iter:
        years_to_run = YEARS
        year_iter = tqdm(years_to_run, desc=f"{MODEL} | {ssp}", leave=False) if HAS_TQDM else years_to_run

        for year in year_iter:
            # evita duplicar anos históricos no 2º cenário
            if year < 2015 and ssp != SCENARIOS[0]:
                continue
            try:
                out_csv = morph_one_year(ssp, year)
                print(f"Morph OK -> {out_csv}")
                validate_sam_csv(out_csv)
                print("Validação OK (8760, sem NaN)")
                try:
                    res = run_sam_from_csv(out_csv)
                    res.update({"modelo": MODEL, "ssp": ("historical" if year < 2015 else ssp),
                                "ano": year, "erro": None})
                    results.append(res)
                    os.makedirs(LOG_DIR, exist_ok=True)
                    with open(Path(LOG_DIR) / f"log_{res['ssp']}_{year}.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(res, indent=2, ensure_ascii=False))
                    print(f"SAM OK | annual_mwh={res['annual_mwh']} | CF={res['capacity_factor']:.3f} | t={res['tempo_s']}s")
                except Exception as e:
                    msg = f"SAM falhou: {e}"
                    results.append({"modelo": MODEL, "ssp": ("historical" if year < 2015 else ssp),
                                    "ano": year, "erro": msg})
                    os.makedirs(LOG_DIR, exist_ok=True)
                    with open(Path(LOG_DIR) / f"log_{('historical' if year < 2015 else ssp)}_{year}.txt",
                              "w", encoding="utf-8") as f:
                        f.write(f"ERRO: {msg}\n")
                    print("ERRO (SAM) ->", msg)
            except Exception as e:
                msg = str(e)
                results.append({"modelo": MODEL, "ssp": ("historical" if year < 2015 else ssp),
                                "ano": year, "erro": msg})
                os.makedirs(LOG_DIR, exist_ok=True)
                with open(Path(LOG_DIR) / f"log_{('historical' if year < 2015 else ssp)}_{year}.txt",
                          "w", encoding="utf-8") as f:
                    f.write(f"ERRO: {msg}\n")
                print("ERRO ->", msg)

    dfres = pd.DataFrame(results)
    dfres.to_csv(OUT_RESULTS_CSV, index=False, encoding="utf-8")
    print("\nResumo salvo em:", OUT_RESULTS_CSV)
    print("Logs em:", LOG_DIR)

    if PLOT_RESULTS and not dfres.empty:
        try:
            import matplotlib.pyplot as plt
            plt.figure()
            dfp = dfres.copy()
            dfp["label"] = dfp["ano"].astype(str) + " - " + dfp["ssp"].astype(str)
            dfp = dfp.sort_values(["ano","ssp"])
            plt.bar(dfp["label"], dfp["annual_mwh"])
            plt.xticks(rotation=45, ha="right")
            plt.title(f"Annual energy (MWh) - {MODEL}")
            plt.ylabel("MWh")
            plt.tight_layout()
            out_png = str(Path(OUT_RESULTS_CSV).with_suffix("")) + "_annual_mwh.png"
            plt.savefig(out_png, dpi=140)
            print("Gráfico salvo:", out_png)
        except Exception as e:
            print("[Aviso] Não foi possível gerar gráficos:", e)

if __name__ == "__main__":
    xr.set_options(keep_attrs=True)
    main()
