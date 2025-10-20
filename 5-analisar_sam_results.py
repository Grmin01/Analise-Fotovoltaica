# -*- coding: utf-8 -*-
"""
analisar_sam_results.py
Lê o CSV de resultados do pipeline (SAM) e produz análises e gráficos.

Saídas:
- ./analise_sam/summary_by_year.csv
- ./analise_sam/summary_decadal.csv
- ./analise_sam/trends.csv
- (se possível) PNGs em ./analise_sam/figs/
"""

import os
import math
import json
from pathlib import Path
import numpy as np
import pandas as pd

# ========================= CONFIG =========================
# Troque abaixo se necessário:
RESULTS_CSV = r"C:\Users\alexs\OneDrive\Área de Trabalho\resultado_sam_TEST_1994_2023_2047.csv"
OUT_DIR = Path("./analise_sam")
FIG_DIR = OUT_DIR / "figs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

# Se quiser limitar cenários/anos específicos, deixe como None para usar tudo detectado.
SCENARIOS_ORDER = ["historical", "ssp245", "ssp585"]  # ordem preferida nos gráficos
BASELINE_YEARS = list(range(1994, 2015))              # 1994–2014

# ========================= UTILS =========================
def _safe_pct(a, b):
    """(a-b)/b*100 com proteção para divisão por zero/nan."""
    if b is None or not np.isfinite(b) or b == 0:
        return np.nan
    return 100.0 * (a - b) / b

def _rolling(series, win=5):
    try:
        return series.rolling(win, min_periods=max(1, win//2)).mean()
    except Exception:
        return pd.Series([np.nan]*len(series), index=series.index)

def _linear_trend(years, values):
    """Retorna slope por década (%/década relativo ao valor médio) e R²."""
    s = pd.Series(values, index=years).dropna()
    if len(s) < 3:
        return np.nan, np.nan
    x = s.index.values.astype(float)
    y = s.values.astype(float)

    # Ajuste linear simples
    coef = np.polyfit(x, y, 1)  # y = a*x + b
    a, b = coef[0], coef[1]
    # R²
    y_hat = a*x + b
    ss_res = np.sum((y - y_hat)**2)
    ss_tot = np.sum((y - np.mean(y))**2)
    r2 = 1.0 - ss_res/ss_tot if ss_tot > 0 else np.nan

    # slope por década em % relativo ao valor médio
    mean_val = np.mean(y) if np.isfinite(np.mean(y)) and np.mean(y) != 0 else np.nan
    if not np.isfinite(mean_val) or mean_val == 0:
        return np.nan, r2
    slope_decadal_pct = (a * 10.0 / mean_val) * 100.0
    return slope_decadal_pct, r2

def _try_import_matplotlib():
    try:
        import matplotlib.pyplot as plt
        return plt
    except Exception as e:
        print("[Aviso] Matplotlib indisponível. Gráficos serão pulados.")
        print(" Motivo:", str(e))
        return None

# ========================= LOAD & PREP =========================
df = pd.read_csv(RESULTS_CSV)

# Normaliza colunas
# esperado: modelo, ssp, ano, annual_mwh, capacity_factor, erro
colmap = {c.lower(): c for c in df.columns}
def _pick(name, *alts):
    for key, orig in colmap.items():
        if key == name.lower() or key in [a.lower() for a in alts]:
            return orig
    raise KeyError(f"Coluna '{name}' não encontrada nas colunas: {list(df.columns)}")

col_model  = _pick("modelo")
col_ssp    = _pick("ssp", "cenario")
col_year   = _pick("ano", "year")
col_mwh    = _pick("annual_mwh", "energia_mwh", "annual_mwh")
col_cf     = _pick("capacity_factor", "cf", "capacity_factor")
col_error  = None
for cand in ["erro","error","msg_erro"]:
    if cand in colmap:
        col_error = colmap[cand]
        break

df = df.rename(columns={
    col_model: "modelo",
    col_ssp:   "ssp",
    col_year:  "ano",
    col_mwh:   "annual_mwh",
    col_cf:    "capacity_factor",
    **({col_error: "erro"} if col_error else {})
})

# Tira registros com erro
if "erro" in df.columns:
    nerr = df["erro"].notna().sum()
    if nerr > 0:
        print(f"[Info] Removendo {nerr} linhas com erro reportado.")
    df = df[df["erro"].isna()]

# Tipos
df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
df["annual_mwh"] = pd.to_numeric(df["annual_mwh"], errors="coerce")
df["capacity_factor"] = pd.to_numeric(df["capacity_factor"], errors="coerce")  # já vem em fração (0-1)
df["ssp"] = df["ssp"].astype(str)

# Ordena
if SCENARIOS_ORDER:
    df["ssp"] = pd.Categorical(df["ssp"], categories=SCENARIOS_ORDER, ordered=True)
df = df.sort_values(["ssp", "ano"]).reset_index(drop=True)

# ========================= BASELINE =========================
# baseline calculada por cenário separadamente para permitir "historical" vs futuros
# como baseline, usamos SEMPRE 1994–2014 (se existir no cenário/histórico)
base_ref = df[(df["ano"].isin(BASELINE_YEARS)) & (df["ssp"].astype(str).str.contains("historical"))]
if base_ref.empty:
    # fallback: baseline por ssp se usuário quiser, mas preferimos historical
    print("[Aviso] Baseline historical 1994–2014 não encontrada. Calculando baseline por cenário (1994–2014 no próprio ssp).")
    base_ref = df[df["ano"].isin(BASELINE_YEARS)]

baseline_stats = (
    base_ref
    .groupby("ssp")[["annual_mwh", "capacity_factor"]]
    .mean()
    .rename(columns={"annual_mwh": "baseline_mwh", "capacity_factor": "baseline_cf"})
    .reset_index()
)

# Juntamos baseline no df (por cenário)
df = df.merge(baseline_stats, on="ssp", how="left")

# Anomalias/deltas relativos à baseline do próprio grupo ssp (historical tem baseline dele)
df["delta_mwh_pct"] = df.apply(lambda r: _safe_pct(r["annual_mwh"], r["baseline_mwh"]), axis=1)
df["delta_cf_pct"]  = df.apply(lambda r: _safe_pct(r["capacity_factor"], r["baseline_cf"]), axis=1)

# Rolling 5 anos (por cenário)
df["rolling5_mwh"] = (
    df.sort_values(["ssp","ano"])
      .groupby("ssp")["annual_mwh"]
      .transform(lambda s: _rolling(s, win=5))
)

# ========================= COBERTURA =========================
coverage = (
    df.groupby("ssp")["ano"]
      .agg(["min", "max", "count"])
      .rename(columns={"min":"ano_min","max":"ano_max","count":"n_anos"})
      .reset_index()
)
coverage.to_csv(OUT_DIR / "coverage.csv", index=False, encoding="utf-8")

# ========================= TENDÊNCIAS =========================
trend_rows = []
for ssp, g in df.groupby("ssp"):
    years = g["ano"].astype(int).tolist()
    mwh = g["annual_mwh"].tolist()
    cf  = g["capacity_factor"].tolist()

    slope_mwh_pct_dec, r2_mwh = _linear_trend(years, mwh)
    slope_cf_pct_dec,  r2_cf  = _linear_trend(years, cf)

    trend_rows.append({
        "ssp": str(ssp),
        "slope_mwh_pct_per_decade": slope_mwh_pct_dec,
        "r2_mwh": r2_mwh,
        "slope_cf_pct_per_decade": slope_cf_pct_dec,
        "r2_cf": r2_cf,
        "anos": f"{min(years)}–{max(years)}",
        "n": len(years)
    })

df_trends = pd.DataFrame(trend_rows)
df_trends.to_csv(OUT_DIR / "trends.csv", index=False, encoding="utf-8")

# ========================= MÉDIAS DECADAIS =========================
def decade_of(y: int) -> str:
    d0 = int(y//10)*10
    return f"{d0}s"

df["decada"] = df["ano"].astype(int).apply(decade_of)
df_decadal = (
    df.groupby(["ssp","decada"]) [["annual_mwh","capacity_factor","delta_mwh_pct","delta_cf_pct"]]
      .mean()
      .reset_index()
)
df_decadal.to_csv(OUT_DIR / "summary_decadal.csv", index=False, encoding="utf-8")

# ========================= EXPORT PRINCIPAL =========================
df_out = df[[
    "modelo","ssp","ano","annual_mwh","capacity_factor",
    "baseline_mwh","baseline_cf","delta_mwh_pct","delta_cf_pct","rolling5_mwh"
]].copy()
df_out.to_csv(OUT_DIR / "summary_by_year.csv", index=False, encoding="utf-8")

print("\n[OK] Arquivos gerados em:", OUT_DIR.resolve())
print(" - summary_by_year.csv")
print(" - summary_decadal.csv")
print(" - trends.csv")
print(" - coverage.csv")

# ========================= GRÁFICOS (opcional) =========================
plt = _try_import_matplotlib()
if plt is not None:
    try:
        # 1) Série temporal: Annual MWh por cenário
        plt.figure(figsize=(12,5))
        for ssp, g in df.groupby("ssp"):
            plt.plot(g["ano"], g["annual_mwh"], marker="o", label=str(ssp), linewidth=1)
            # Rolling
            plt.plot(g["ano"], g["rolling5_mwh"], linestyle="--", label=f"{ssp} (média 5a.)", linewidth=1)
        plt.title("Energia anual (MWh)")
        plt.xlabel("Ano"); plt.ylabel("MWh")
        plt.grid(True, alpha=0.3)
        plt.legend(ncols=2, fontsize=8)
        plt.tight_layout()
        f1 = FIG_DIR / "serie_annual_mwh.png"
        plt.savefig(f1, dpi=140)
        plt.close()

        # 2) Série temporal: Delta vs baseline (%)
        plt.figure(figsize=(12,5))
        for ssp, g in df.groupby("ssp"):
            plt.plot(g["ano"], g["delta_mwh_pct"], marker="o", label=str(ssp), linewidth=1)
        plt.axhline(0, color="k", linewidth=0.8)
        plt.title("Anomalia de MWh vs baseline (1994–2014) do próprio grupo")
        plt.xlabel("Ano"); plt.ylabel("Δ MWh (%)")
        plt.grid(True, alpha=0.3); plt.legend()
        plt.tight_layout()
        f2 = FIG_DIR / "serie_delta_mwh_pct.png"
        plt.savefig(f2, dpi=140)
        plt.close()

        # 3) Boxplot por década e cenário (Δ MWh %)
        # Preparar pivot simples para boxplot por ssp nos anos de cada década
        for dec, gd in df.groupby("decada"):
            plt.figure(figsize=(8,5))
            data = [gd.loc[gd["ssp"]==ssp, "delta_mwh_pct"].dropna().values for ssp in SCENARIOS_ORDER if ssp in gd["ssp"].unique()]
            labels = [ssp for ssp in SCENARIOS_ORDER if ssp in gd["ssp"].unique()]
            if len(labels) >= 1 and any(len(x)>0 for x in data):
                plt.boxplot(data, labels=labels, notch=False)
                plt.title(f"Δ MWh (%) por cenário - {dec}")
                plt.ylabel("Δ MWh (%)")
                plt.grid(True, axis="y", alpha=0.3)
                plt.tight_layout()
                fbox = FIG_DIR / f"box_delta_mwh_{dec}.png"
                plt.savefig(fbox, dpi=140)
                plt.close()
            else:
                plt.close()

        # 4) Barras compare 2015 vs 2050 por cenário (se existirem)
        compare_years = [2015, 2050]
        dsel = df[df["ano"].isin(compare_years)].copy()
        if not dsel.empty:
            plt.figure(figsize=(9,5))
            # eixo X: cenário (agrupado), barras: anos
            # Construir tabela
            piv = dsel.pivot_table(index="ssp", columns="ano", values="annual_mwh", aggfunc="mean")
            piv = piv.loc[[s for s in SCENARIOS_ORDER if s in piv.index]]
            piv.plot(kind="bar", ax=plt.gca())
            plt.title("Comparação MWh (2015 vs 2050)")
            plt.ylabel("MWh"); plt.xlabel("Cenário")
            plt.grid(True, axis="y", alpha=0.3)
            plt.tight_layout()
            fbar = FIG_DIR / "barras_mwh_2015_2050.png"
            plt.savefig(fbar, dpi=140)
            plt.close()

        print("[OK] Gráficos salvos em:", FIG_DIR.resolve())
    except Exception as e:
        print("[Aviso] Não foi possível gerar gráficos:", e)
