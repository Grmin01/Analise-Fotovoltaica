import os, re, json
from pathlib import Path
import pandas as pd
import numpy as np

# ====== AJUSTE AQUI SE PRECISAR ======
MODEL = "ACCESS-CM2"

DIR_LOGS = r"C:\Users\alexs\OneDrive\Área de Trabalho\logs_sam_morph_TEST"
DIR_CSVS = r"C:\Users\alexs\OneDrive\Área de Trabalho\SAM_CSV_MORPH"
OUT_PATH = r"C:\Users\alexs\OneDrive\Área de Trabalho\resultado_sam_consolidado_morph.csv"
# =====================================

p_logs = Path(DIR_LOGS)
p_csvs = Path(DIR_CSVS) / MODEL  # estrutura: ...\SAM_CSV_MORPH\ACCESS-CM2\{historical|ssp245|ssp585}\SAM_ACCESS-CM2_..._{ano}_morph.csv

rows = []

def parse_log_file(fp: Path):
    """
    Tenta ler o log como JSON (como salvo pelo pipeline);
    se não for JSON, tenta capturar uma linha de erro ("ERRO: ...").
    Retorna dict com chaves usuais ou {'erro': '...'}.
    """
    txt = fp.read_text(encoding="utf-8", errors="ignore").strip()
    # 1) JSON direto
    try:
        data = json.loads(txt)
        return {"ok": True, "data": data, "msg": None}
    except Exception:
        pass
    # 2) Procura um JSON dentro do texto
    try:
        m = re.search(r"\{.*\}", txt, flags=re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            return {"ok": True, "data": data, "msg": None}
    except Exception:
        pass
    # 3) Procura ERRO:
    m = re.search(r"ERRO:\s*(.+)", txt)
    if m:
        return {"ok": False, "data": None, "msg": m.group(1).strip()}
    # 4) Sem formato conhecido
    return {"ok": False, "data": None, "msg": "Formato de log não reconhecido."}

# Indexa todos os CSVs morphados para lookup rápido
csv_map = {}  # (ssp, ano) -> path
for sub in ["historical", "ssp245", "ssp585"]:
    base = p_csvs / sub
    if not base.exists():
        continue
    for fp in base.glob("SAM_*_%s_*_morph.csv" % sub):
        # extrai ano do nome
        m = re.search(r"_(\d{4})_morph\.csv$", fp.name)
        if not m:
            continue
        ano = int(m.group(1))
        csv_map[(sub, ano)] = str(fp)

# Lê todos os logs
for fp in sorted(p_logs.glob("log_*.txt")):
    # extrai ssp e ano do nome do log
    m = re.match(r"log_(historical|ssp245|ssp585)_(\d{4})\.txt$", fp.name)
    if not m:
        continue
    ssp = m.group(1)
    ano = int(m.group(2))

    parsed = parse_log_file(fp)
    modelo = MODEL
    annual_mwh = np.nan
    capacity_factor = np.nan
    tempo_s = np.nan
    log_status = "OK" if parsed["ok"] else "ERRO"
    log_msg = parsed["msg"]

    if parsed["ok"] and parsed["data"]:
        d = parsed["data"]
        # Tenta obter do JSON salvo pelo pipeline
        modelo = d.get("modelo", MODEL)
        annual_mwh = d.get("annual_mwh", np.nan)
        capacity_factor = d.get("capacity_factor", np.nan)
        tempo_s = d.get("tempo_s", np.nan)
        # Em alguns casos o pipeline guarda 'erro': None
        if d.get("erro"):
            log_status = "ERRO"
            log_msg = d["erro"]

    # caminho CSV esperado pelo padrão de nomes
    caminho_csv = csv_map.get((ssp, ano))
    if caminho_csv is None:
        # tenta construir pelo padrão mesmo que não esteja no índice
        out_sub = ssp
        csv_guess = Path(DIR_CSVS) / MODEL / out_sub / f"SAM_{MODEL}_{out_sub}_{ano}_morph.csv"
        caminho_csv = str(csv_guess)
        if not csv_guess.exists():
            caminho_csv += " (NAO_ENCONTRADO)"

    rows.append({
        "modelo": modelo,
        "ssp": ssp,
        "ano": ano,
        "annual_mwh": annual_mwh,
        "capacity_factor": capacity_factor,
        "tempo_s": tempo_s,
        "caminho_csv": caminho_csv,
        "log_arquivo": str(fp),
        "log_status": log_status,
        "log_msg": log_msg,
    })

# (Opcional) acrescenta CSVs que não têm log (se houver)
# Isso garante que tudo que existe em SAM_CSV_MORPH apareça na planilha, mesmo sem log.
for (ssp, ano), path_csv in csv_map.items():
    key = (ssp, ano)
    if not any((r["ssp"], r["ano"]) == key for r in rows):
        rows.append({
            "modelo": MODEL,
            "ssp": ssp,
            "ano": ano,
            "annual_mwh": np.nan,
            "capacity_factor": np.nan,
            "tempo_s": np.nan,
            "caminho_csv": path_csv,
            "log_arquivo": "",
            "log_status": "SEM_LOG",
            "log_msg": "Sem log correspondente.",
        })

df = pd.DataFrame(rows)
df = df.sort_values(["ssp","ano"]).reset_index(drop=True)

# Ordena colunas
cols = ["modelo","ssp","ano","annual_mwh","capacity_factor","tempo_s",
        "caminho_csv","log_arquivo","log_status","log_msg"]
df = df[cols]

# Salva
Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

print(f"Consolidado salvo em:\n{OUT_PATH}")
print(f"Linhas: {len(df)} | OK: {(df['log_status']=='OK').sum()} | ERRO: {(df['log_status']=='ERRO').sum()} | SEM_LOG: {(df['log_status']=='SEM_LOG').sum()}")
