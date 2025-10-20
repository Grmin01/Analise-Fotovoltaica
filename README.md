# Analise-Fotovoltaica

Pipeline completo para avaliar impactos climáticos na **geração fotovoltaica** usando **ERA5-Land** (reanálise), **NEX-GDDP–CMIP6** (projeções), e **PVWatts v8 (PySAM)**.  
Abrange _download_ de dados, pré-processamento, “morph” climático mensal, simulação horária no PVWatts e análises (tendência, anomalia, médias decadais).

> **Alvo do estudo**: Campos dos Goytacazes (RJ, Brasil), ~(-21.7, -41.3)  
> **Janela temporal**: 1994–2054 (histórico e cenários **ssp245/ssp585**)

---

## ⚙️ Principais componentes

1. **`download_era5_uv_monthly_singlefile.py`**  
   Baixa ERA5-Land (u10, v10) **um arquivo `.nc` por mês**, garantindo robustez (retries, _file lock_, descompactação automática) e limpeza de duplicação.

2. **`gera_csvs_sam_number0.py`**  
   Converte os `.nc` baixados em **CSVs horários** para o SAM/PySAM (colunas `DateTime, WindSpeed`) usando o **membro principal** (number==0) e index horário completo (8760/8784).

3. **`pipeline_morph_validate_sam.py`**  
   Implementa o **morph mensal** (NEX vs. climatologias históricas) sobre um **perfil ERA5 base** (8760 linhas) para gerar **CSV horário por ano/cenário** com `GHI, DNI, DHI, TempC, WindSpeed, RelHum` e roda o **PVWatts v8 (PySAM)**.

4. **`consolida_sam_morph.py`**  
   Consolida **logs** e **CSVs** produzidos no morph/PVWatts numa única planilha (`resultado_sam_consolidado_morph.csv`).

5. **`analisar_sam_results.py`**  
   Produz **sumários**, **tendências lineares**, **anomalias vs. baseline (1994–2014)**, **médias decadais** e **gráficos** em `./analise_sam/`.

---

## 🧰 Dependências

- Python 3.10+
- `pandas`, `numpy`, `xarray`, `netCDF4`, `h5netcdf`, `scipy`
- `matplotlib` (para gráficos)
- `PySAM` (PVWatts v8)
- `cdsapi` (ERA5-Land, via CDS)
- `tqdm` (opcional, barra de progresso)

Instalação sugerida:

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install --upgrade pip
pip install pandas numpy xarray netCDF4 h5netcdf scipy matplotlib cdsapi PySAM tqdm
```

**Autenticação ERA5 (CDS):** criar `~/.cdsapirc` com:
```
url: https://cds.climate.copernicus.eu/api
key: <UID>:<API_KEY>
```
> Obtenha UID/API no site do Copernicus CDS (perfil do usuário).

---

## 🗂️ Estrutura de pastas (sugerida)

```
Analise-Fotovoltaica/
├── download_era5_uv_monthly_singlefile.py
├── gera_csvs_sam_number0.py
├── pipeline_morph_validate_sam.py
├── consolida_sam_morph.py
├── analisar_sam_results.py
├── data/                      # (opcional) .nc originais e derivados
├── outputs/                   # saídas consolidadas
└── analise_sam/               # tabelas e figuras geradas pelo analisador
```

> Os scripts têm **caminhos configuráveis** no topo (variáveis `OUT_DIR`, `NEX_DIR_ROOT`, etc.). Ajuste para seu ambiente local.

---

## 🔄 Fluxo do pipeline

```text
ERA5 (u10/v10)  ──►  .nc mensais  ──►  CSVs horários (WindSpeed)
                                 \           │
                                  \          ▼
                                   \   ERA5 base (perfil 8760h)  +  NEX-GDDP (rsds,tas,sfcWind,hurs)
                                    \                 │
                                     \                ▼
                                      └────► Morph mensal por ano/cenário ──► CSVs SAM por ano
                                                              │
                                                              ▼
                                                      PVWatts v8 (PySAM)
                                                              │
                                                              ▼
                                        Logs + resultados anuais (MWh, CF) por ano/cenário
                                                              │
                                                              ▼
                                  Consolidação  ──►  Análises (anomalia, tendência, décadas, gráficos)
```

---

## 🚀 Passo a passo

### 1) Baixar ERA5-Land (u10/v10) — `download_era5_uv_monthly_singlefile.py`

**O que faz**
- Define **lat/lon** do alvo e cria um **bounding box** pequeno (`PAD = 0.25`).
- Solicita ao **CDS** os dados `10m_u_component_of_wind` e `10m_v_component_of_wind`
  para **todas as horas** do mês (formato **NetCDF**).
- Garante robustez: _retry_ por mês, _wait for stable file_, remoção de duplicatas,
  e **descompactação automática** de ZIP/GZIP.
- Saída: `era5land_YYYY_MM.nc` em `OUT_DIR`.

**Funções-chave**
- `_wait_file_stable(path)`: espera o arquivo estabilizar no disco (tamanho fixo).
- `_unpack_if_needed(path)`: detecta e **descompacta** `zip`/`gz` para `.nc` limpo.
- `_cleanup_previous_same_month(year, month)`: evita múltiplos arquivos do mesmo mês.
- `retrieve_month(client, year, month)`: baixa **ambas variáveis juntas** e grava um `.nc` por mês.
- `process_year(client, year)`: itera 12 meses com **retries**.
- `main()`: instancia o `cdsapi.Client` e processa todos anos definidos.

**Como rodar**

```bash
python download_era5_uv_monthly_singlefile.py
# Ajuste YEARS, OUT_DIR e a área se necessário.
```

---

### 2) Gerar CSVs horários para SAM — `gera_csvs_sam_number0.py`

**O que faz**
- Varre `TMP_DIR` (ou `OUT_DIR` do passo 1) por arquivos `era5land_YYYY_MM*.nc`.
- Abre com `xarray` (tentando `netcdf4` → `h5netcdf` → `scipy`).
- Seleciona **membro 0** (`number==0`), renomeia `10m_u_component_of_wind`→`u10` e `v10`.
- Seleciona **ponto mais próximo** de `LAT/LON`.
- Calcula `WindSpeed = sqrt(u10^2 + v10^2)`.
- Constrói índice horário **completo** de cada ano (8760/8784), detecta faltas e aborta se houver lacunas.
- Saídas: `SAM_ERA5_HIST_YYYY.csv` com `DateTime,WindSpeed`.

**Funções-chave**
- `open_any(nc)`: abre `.nc` com **retries** alternando _engines_.
- `normalize_dims(ds)`: padroniza nomes/dimensões e **remove** coords problemáticas.
- `select_point(ds, lat, lon)`: busca o **grid mais próximo** (ajusta 0..360 se preciso).
- `extract_datetime_and_values(wspd, target_year)`: **prioriza** `valid_time` (se existir) e garante **1 linha/hora**.
- `expected_hourly_index(year)`: cria índice horário completo do ano.

**Como rodar**

```bash
python gera_csvs_sam_number0.py
# Ajuste TMP_DIR (onde os .nc foram baixados) e OUT_DIR (onde salvar CSVs).
```

---

### 3) Morph + validação + PVWatts — `pipeline_morph_validate_sam.py`

**O que faz**
- Usa um **CSV ERA5 base** (um **único ano** completo, 8760h) com `GHI,DNI,DHI,TempC,WindSpeed,RelHum` como **perfil horário**.
- Calcula **deltas/fatores mensais** usando **NEX-GDDP–CMIP6** e **climatologias históricas** (1994–2014).
- Aplica morph **mês a mês** no perfil base para gerar **CSVs por ano e cenário**:
  - `k_rsds = fut_rsds / clim_rsds` → escala **GHI**; **DNI/DHI** preservam a fração DNI/GHI original.
  - `d_tas  = fut_tas  - clim_tas`  → desloca **TempC** (°C).
  - `k_wspd = fut_wspd / clim_wspd` → escala **WindSpeed** (multiplicativo).
  - `d_hurs = fut_hurs - clim_hurs`  → desloca **RelHum** (%) — _fallback_ 0% se ausente.
- **Reindexa o ano** no `DateTime`, **remove 29/02** se existir e **valida 8760 linhas**.
- Roda o **PVWatts v8** (PySAM) com parâmetros do sistema (capacidade, tilt, azimute, perdas, GCR, etc.).
- Salva **CSVs por ano** e **logs** (JSON com `annual_mwh`, `capacity_factor`, etc.).

**Funções-chave (destaques)**
- `read_era5_base(path)`: carrega o perfil base e **autoescala irradiância** (pico ~900 W/m²) se necessário.
- `_load_nex_one_year_monthly(var, year, ssp)`: extrai **médias mensais** do NEX para o **ano-alvo** e **ponto**.
- `_load_nex_hist_monthly(var)`: monta **climatologia mensal** 1994–2014 (NEX historical).
- `_load_wind_clim_from_era5_csvs(csv_dir)`: climatologia de **vento** a partir dos seus **CSVs ERA5**.
- `morph_one_year(ssp, year)`: aplica os deltas/fatores mensais ao perfil, reindexa o ano e grava CSV final.
- `validate_sam_csv(path)`: checa campos, NaN e 8760 linhas.
- `run_sam_from_csv(path)`: monta `SolarResource` e executa **PVWatts v8**, retornando `annual_mwh`, `capacity_factor` e `ac_monthly_kwh`.

**Configuração relevante (no topo do script)**
- **Local/coords**: `LAT, LON, ELEV, TZ`
- **Sistema FV**: `SYSTEM_CAP_KW, TILT_DEG, AZIMUTH_DEG, DC_AC_RATIO, LOSSES_PCT, INVERTER_EFF, MODULE_TYPE, ARRAY_TYPE, GCR`
- **Cenários/anos**: `MODEL, SCENARIOS, YEARS, ERA5_BASE_YEAR`
- **Caminhos**: `CSV_ERA5_BASE, NEX_DIR_ROOT, ERA5_HOURLY_DIR, PLANILHA_ERA5, OUT_SAM_CSV_DIR, LOG_DIR`
- **Opções**: `IRRAD_SCALE=None` (auto), `USE_YEARLY_WIND=True` (recomendado), `PLOT_RESULTS=True`

**Como rodar**

```bash
python pipeline_morph_validate_sam.py
# Gera CSVs por ano/cenário em OUT_SAM_CSV_DIR e logs/resultado CSV agregado (OUT_RESULTS_CSV).
```

---

### 4) Consolidação — `consolida_sam_morph.py`

**O que faz**
- Percorre **logs** gerados no passo 3 e casa com os **CSVs morfados**.
- Monta um **CSV consolidado** com: `modelo, ssp, ano, annual_mwh, capacity_factor, tempo_s, caminho_csv, log_status, log_msg`.

**Funções-chave**
- `parse_log_file(fp)`: tenta ler o **JSON** dos logs; se não for JSON, captura **"ERRO: ..."** no texto.

**Como rodar**

```bash
python consolida_sam_morph.py
# Gera resultado_sam_consolidado_morph.csv
```

---

### 5) Análises e gráficos — `analisar_sam_results.py`

**O que faz**
- Lê o CSV de resultados (do passo 3 ou do consolidado) e produz em `./analise_sam/`:
  - `summary_by_year.csv`: série anual com baseline e deltas.
  - `summary_decadal.csv`: médias por década/cenário.
  - `trends.csv`: **inclinação linear por década** (%/década) e **R²** para MWh e CF.
  - `coverage.csv`: cobertura (anos min/max, contagem).
  - PNGs: séries, anomalias vs baseline, boxplots decadais e barras de comparação (ex.: 2015 vs 2050).

**Funções-chave**
- `_safe_pct(a, b)`: delta percentual seguro.
- `_linear_trend(years, values)`: inclinação linear (%/década) relativa ao valor médio + R².
- `_rolling(series, win=5)`: média móvel (suave) para facilita visualizações.
- `_try_import_matplotlib()`: lida com ausência do Matplotlib sem quebrar.

**Como rodar**

```bash
python analisar_sam_results.py
# Ajuste RESULTS_CSV se necessário; figuras e CSVs saem em ./analise_sam/
```

---

## 📑 Entradas & Saídas (resumo)

**Entradas**  
- ERA5-Land: u10/v10 horários por mês (`.nc`).  
- ERA5 CSVs horários (1994–2014) para **climatologia de vento**.  
- NEX-GDDP–CMIP6: `rsds, tas, sfcWind, hurs` (arquivos diários por **ano** e **cenário**).  
- Perfil base ERA5 (8760h) com `GHI, DNI, DHI, TempC, WindSpeed, RelHum`.

**Saídas principais**  
- `SAM_ERA5_HIST_YYYY.csv` (vento horário).  
- `SAM_<MODEL>_{historical|ssp245|ssp585}_YYYY_morph.csv` (recurso solar-horário morfado).  
- `resultado_sam_TEST_*.csv` (agregado com MWh/CF por ano/cenário).  
- `resultado_sam_consolidado_morph.csv` (consolidação final).  
- `analise_sam/*` (sumários e gráficos).

---

## 🧪 Validações embutidas

- **Arquivo estável** pós-download, descompressão transparente de `zip/gz`.  
- **8760 horas** por CSV (remove **29/02** quando necessário).  
- Checagem de **NaN** e de **índice horário contínuo** (gera erro se faltar hora).  
- **Autoescala** de irradiância do perfil base (pico ~900 W/m²) para evitar valores irreais.  
- **Seleção de ponto mais próximo** (lon 0..360 compatível).

---

## 🧯 Troubleshooting rápido

- **Faltam horas no CSV ERA5**: verifique `.nc` de meses específicos e se `valid_time` está presente; o script aborta quando há lacunas.
- **PySAM não encontrado**: confirme instalação (`pip install PySAM`) e versão do Python compatível.
- **Arquivos NEX não localizados**: ajuste `NEX_DIR_ROOT` e padrão de nomes; o script tenta múltiplos padrões.
- **HURS ausente**: o morph usa **delta 0%** como _fallback_.  
- **Resultados “estranhos” de irradiância**: defina `IRRAD_SCALE` manualmente (ex.: `IRRAD_SCALE=23.5`) ou revise o CSV base.

---

## 🔐 Reprodutibilidade

- Caminhos e versões **fixados** no início de cada script.
- Names de saída **determinísticos** por ano/cenário.
- Logs em texto/JSON por execução do PVWatts (tempo de execução, energia anual, CF).

---

## 📜 Licença

Sugerido: **MIT License** (adicione `LICENSE` conforme sua preferência institucional).

---

## ✨ Agradecimentos

- Copernicus Climate Data Store (ERA5-Land)  
- NEX-GDDP–CMIP6 (NASA/LLNL)  
- NREL SAM / PySAM (PVWatts v8)

