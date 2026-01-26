# Analise-Fotovoltaica (SAM + Morph + QA + Figuras)

Pipeline completo para avaliar impactos climáticos na **geração fotovoltaica** usando:

- **ERA5-Land/ERA5** (perfil horário base)
- **NEX-GDDP–CMIP6** (projeções: `historical`, **ssp245**, **ssp585**)
- **PVWatts v8 (PySAM)** para simular geração anual/mensal

Abrange **morph climático mensal**, geração de **CSVs 8760h**, execução do **PVWatts**, **consolidação**, **análises** e **relatórios** (Markdown/TXT), além de um script de **QA mínimo defensável**.

> **Alvo do estudo**: Campos dos Goytacazes (RJ, Brasil) — ~(-21.7, -41.3)  
> **Janela temporal típica**: 1994–2054 (histórico + cenários **ssp245/ssp585**)  
> **Modelo CMIP6 (exemplo atual)**: `ACCESS-CM2`

---

## 🧩 Principais componentes

1. **`pipeline_sam_unificado_v4_report.py`**  
   Pipeline “tudo em um”:
   - Pré-check (ERA5 base, amostra NEX, PySAM)
   - Climatologia histórica (1994–2014) com **cache**
   - Morph mensal (delta-change) do ano-base
   - Validação do CSV morfado (8760, NaN, estatísticas)
   - Execução do PVWatts (PySAM)
   - Consolidação de logs (`OK/ERRO/SEM_LOG`)
   - Análise (tendências, anomalias, tabelas e gráficos)
   - **Relatório final (Markdown)** no `OUT_ROOT`

2. **`qa_validacao_minima_morph.py`**  
   Validação mínima “defensável” para os CSVs morfados:
   - Sanidade interna (8760, NaN, passo 1h)
   - Checagem `GHI ≈ DNI + DHI` (MAE/MAPE)
   - Faixas físicas e flags (picos, RH, vento, etc.)
   - Sazonalidade mensal agregada + gráfico
   - Comparação externa opcional com **NASA POWER**

3. **`gerar_figuras_fv_completas.py`** *(ajuste o nome do arquivo se necessário)*  
   Gera **tabelas + figuras finais (dissertação)** a partir dos CSVs morfados e/ou logs:
   - Reconstrói série anual (MWh/CF) via logs ou rodando PySAM se faltar
   - Monta séries **compostas** (historical + futuro)
   - Baseline (preferência: `historical 1994–2014`)
   - Estatísticas descritivas (Tabela 16)
   - Tendências OLS, Pettitt (ponto de mudança)
   - Heatmaps mensais (anomalia vs baseline mensal)
   - Boxplots por década/cenário, dispersões e comparativos
   - Relatório `.txt` final

---

## 🧰 Dependências

### Recomendado
- Python **3.10+**
- `pandas`, `numpy`
- `matplotlib`
- `xarray`, `netCDF4`
- `tqdm` (opcional)
- `NREL-PySAM` (PVWatts v8)
- `requests` (somente para NASA POWER no QA)

Instalação sugerida:

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
# source .venv/bin/activate

pip install --upgrade pip
pip install pandas numpy matplotlib xarray netCDF4 tqdm NREL-PySAM requests
🗂️ Estrutura de pastas (sugerida)
Os scripts usam caminhos configuráveis no topo. A estrutura abaixo é a lógica recomendada.

Analise-Fotovoltaica/
├── pipeline_sam_unificado_v4_report.py
├── qa_validacao_minima_morph.py
├── gerar_figuras_fv_completas.py
├── dados/
│   ├── ERA5_BASE/
│   │   └── solar_resource_hourly_lat-21.700_lon-41.300.csv
│   └── NEX_CMIP6/
│       └── ACCESS-CM2/
│           ├── historical/
│           ├── ssp245/
│           └── ssp585/
└── resultados/
    └── SAM_MORPH/
        ├── SAM_CSV_MORPH/ACCESS-CM2/
        │   ├── historical/
        │   ├── ssp245/
        │   └── ssp585/
        ├── logs_sam_morph_ACCESS-CM2/
        ├── analise_sam_ACCESS-CM2/
        ├── clim_cache_ACCESS-CM2_lat-21.700_lon-41.300.json
        ├── resultado_sam_ACCESS-CM2_morph.csv
        ├── resultado_sam_consolidado_ACCESS-CM2_morph.csv
        └── relatorio_pipeline_ACCESS-CM2_YYYYMMDD_HHMMSS.md
⚙️ Configuração rápida (o que você precisa ajustar)
No topo do pipeline_sam_unificado_v4_report.py:

Coordenadas/local:

LAT, LON, ELEV, TZ

Modelo/cenários/anos:

MODEL, SCENARIOS_DEFAULT, YEARS_DEFAULT

Caminhos (principais):

CSV_ERA5_BASE (CSV horário base com DateTime, GHI, DNI, DHI, TempC, WindSpeed, RelHum)

NEX_DIR_ROOT (raiz do NEX para o modelo)

OUT_ROOT (onde tudo será gerado)

🔄 Fluxo do pipeline (visão)
ERA5 base (8760h) + NEX (rsds,tas,sfcWind,hurs)
              │
              ▼
 Climatologia histórica 1994–2014 (cache)
              │
              ▼
 Morph mensal (delta change) → CSV morfado por ano/cenário (8760h)
              │
              ▼
 PVWatts v8 (PySAM) → annual_mwh, capacity_factor, ac_monthly_kwh
              │
              ▼
 Logs → Consolidação → Análise (tabelas, tendências, gráficos)
              │
              ▼
 Relatório final (Markdown)
🚀 Como rodar
1) Rodar o pipeline (com relatório final)
Teste curto (recomendado):

python pipeline_sam_unificado_v4_report.py --mode test
Rodar completo:

python pipeline_sam_unificado_v4_report.py --mode full
Rodar apenas alguns anos/cenário:

python pipeline_sam_unificado_v4_report.py --mode full --years 2020:2025 --scenarios ssp245
Forçar reprocessamento (ignora logs OK):

python pipeline_sam_unificado_v4_report.py --mode full --force
Somente consolidar + analisar (não executa morph/SAM):

python pipeline_sam_unificado_v4_report.py --mode analyze
✅ Saída importante:

relatorio_pipeline_{MODEL}_YYYYMMDD_HHMMSS.md em OUT_ROOT

2) Rodar QA mínimo dos CSVs morfados
python qa_validacao_minima_morph.py ^
  --input-dir "C:\Users\alexs\clima_campos\resultados\SAM_MORPH\SAM_CSV_MORPH\ACCESS-CM2" ^
  --out-dir   "C:\Users\alexs\clima_campos\resultados\SAM_MORPH\qa_ACCESS-CM2" ^
  --lat -21.7 --lon -41.3
Sem NASA POWER:

python qa_validacao_minima_morph.py --input-dir "..." --out-dir "..." --external none
3) Gerar tabelas e figuras finais (dissertação)
python gerar_figuras_fv_completas.py
Ele lê:

CSVs morfados em MORPHED_CSV_ROOT

Logs em LOG_DIR
E, se faltar resultado, pode rodar PySAM diretamente nos CSVs (controlado por RUN_PYSAM_IF_MISSING).

📦 Saídas geradas (checklist)
Script 1 — Pipeline (v4)
SAM_CSV_MORPH/{MODEL}/{ssp}/SAM_{MODEL}_{ssp}_{ano}_morph.csv

logs_sam_morph_{MODEL}/log_{ssp}_{ano}.txt

resultado_sam_{MODEL}_morph.csv

resultado_sam_consolidado_{MODEL}_morph.csv

analise_sam_{MODEL}/coverage.csv

analise_sam_{MODEL}/trends.csv

analise_sam_{MODEL}/summary_by_year.csv

analise_sam_{MODEL}/summary_decadal.csv

analise_sam_{MODEL}/figs/*.png

relatorio_pipeline_{MODEL}_*.md

clim_cache_{MODEL}_*.json

Script 2 — QA
qa_interno_por_arquivo.csv

medias_mensais_por_arquivo.csv

medias_mensais_agregado.csv

fig_sazonalidade_mensal.png

relatorio_validacao_minima.md

comparacao_nasa_power.csv (opcional)

Script 3 — Figuras + Tabelas (dissertação)
Em OUT_DIR:

TABELAS_FV/resultado_sam_reconstruido_{MODEL}.csv

TABELAS_FV/mensal_ac_kwh_reconstruido_{MODEL}.csv (se houver)

TABELAS_FV/Tabela_16_estatisticas_descritivas.csv

TABELAS_FV/Relatorio_geral_FV.txt

GRAFICOS_FV/*.png (gráficos numerados no script)

🧾 Nota metodológica (para dissertação)
A série horária é gerada por técnica de delta change mensal aplicada a um ano-base horário (ERA5).
Assim, a forma intradiária do ano-base é preservada e os níveis mensais são ajustados conforme o modelo climático (NEX-CMIP6).
Por isso, inferências de tendência são mais robustas em médias mensais/anuais do que em extremos horários.

🧯 Problemas comuns
ERA5 base não tem 8760 linhas
O script remove 29/02 se vier 8784. Se ainda não for 8760, há buracos/duplicações no DateTime.

NaN no ERA5 base / NaN após morph
Verifique colunas e conversão numérica. O pipeline falha cedo para não “contaminar” resultados.

NEX não encontrado (FileNotFoundError)
Confirme NEX_DIR_ROOT, MODEL e a presença de historical/ssp245/ssp585 com arquivos por ano.

PySAM não importa
Instale NREL-PySAM no mesmo .venv. Em Windows, cuidado com versão do Python e wheels disponíveis.

SEM_LOG no consolidado
Existe CSV morfado mas não existe log correspondente (por execução interrompida, etc.).
Rode --mode analyze para consolidar e checar, ou rode --force para gerar logs novamente.
