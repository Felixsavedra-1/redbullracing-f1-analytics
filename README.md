# Red Bull F1 Analytics

ETL pipeline for Formula 1 performance analysis — Red Bull, AlphaTauri, and RB, 2020–2025. Extracts from the Ergast API into a star-schema SQLite database, with 14 parameterized analytical queries, statistical models, and a Power BI dashboard.

---

## Stack

| Layer | Technology |
|---|---|
| Extraction | Python `requests` — resumable, adaptive backoff |
| Transformation | `pandas` — ref resolution, schema validation |
| Storage | SQLite (default) or MySQL |
| Analysis | SQL, `scipy` statistical models, Jupyter, Power BI |
| Source | [api.jolpi.ca/ergast/f1](https://api.jolpi.ca/ergast/f1) |

---

## Quickstart

```bash
pip install -r requirements.txt
cp scripts/config.example.py scripts/config.py  # optional — defaults to SQLite
python scripts/run_pipeline.py
```

Runs extract → transform → load, then prints a driver summary:

```
──────────────────────────────────────────────────────────────────────
  Red Bull Drivers  2020–2025
──────────────────────────────────────────────────────────────────────
  Driver              Team              Period    Races    Pts  Wins  Pods   Avg  DNFs
  ──────────────────  ────────────────  ──────  ───────  ────  ────  ────  ────  ────
  Max Verstappen      Red Bull          2020–25     131  3017    63    84   1.9     5
  Sergio Pérez        Red Bull          2021–24      84  1341     8    35   5.1     9
  Pierre Gasly        AlphaTauri · RB   2020–24      87   387     1     5   9.2    12
  Yuki Tsunoda        AlphaTauri · RB   2021–25      99   211     0     0  11.4    14
  ...
──────────────────────────────────────────────────────────────────────
```

---

## Pipeline flags

```bash
python scripts/run_pipeline.py --fast                        # demo mode: 2021–2025, reduced retries
python scripts/run_pipeline.py --start-year 2022 --end-year 2024
python scripts/run_pipeline.py --skip-extract                # skip API calls, use cached data
python scripts/run_pipeline.py --skip-pit-stops              # faster runs without stop data
python scripts/run_pipeline.py --incremental                 # upsert instead of full refresh
python scripts/run_pipeline.py --base-delay 2.0 --max-retries 8
```

---

## Analysis

Five statistical models on the loaded database. Prints summary tables; `--export` saves 300 DPI PNGs to `data/exports/charts/`.

```bash
python scripts/run_analysis.py
python scripts/run_analysis.py --export
```

| Model | Method |
|---|---|
| Championship trajectory | Cumulative points per driver per round |
| Teammate head-to-head | Mean position delta, 95% t-interval, p-value |
| Grid → finish regression | OLS with R², slope, and significance |
| Pit stop efficiency | Z-score vs season field distribution |
| DNF rate | Poisson MLE with exact 95% confidence intervals |

---

## Queries

14 named SQL queries, all parameterized by constructor. Run after the pipeline completes.

```bash
python scripts/run_queries.py --list                    # list all query names
python scripts/run_queries.py --query driver_summary    # print to stdout
python scripts/run_queries.py --query all --export      # write all to data/exports/
```

Representative queries: `driver_summary`, `championship_progression`, `pit_stop_efficiency`, `qualifying_vs_race_performance`, `reliability_analysis`, `failure_modes`. See `--list` for the full set.

---

## Data model

Star schema in `f1_analytics.db`.

**Dimensions:** `circuits` `seasons` `constructors` `drivers`  
**Facts:** `races` `results` `qualifying` `pit_stops` `constructor_standings` `driver_standings`

Schema DDL: `database/schema/create_tables_sqlite.sql`  
Schema contracts (DataFrame validation): `scripts/schema_contracts.py`

---

## MySQL

SQLite requires no configuration. To use MySQL:

```bash
cp scripts/config.example.py scripts/config.py
# edit DB_CONFIG in config.py
mysql -u root -p < database/schema/create_tables.sql
```

---

## Tests

```bash
python -m unittest discover -s tests
```

Post-load quality gates (15+ checks) run automatically at the end of each pipeline run. Pass `--skip-quality` to bypass.

---

## Structure

```
├── data/
│   ├── raw/               # extracted CSVs
│   ├── processed/         # transformed CSVs
│   └── cache/             # extraction resume state
├── database/
│   ├── queries/           # analytical_queries.yaml + .sql
│   └── schema/            # DDL for SQLite and MySQL
├── notebooks/
├── powerbi/
├── scripts/
│   ├── run_pipeline.py    # main entry point
│   ├── extract_data.py
│   ├── transform_data.py
│   ├── load_data.py
│   ├── run_queries.py
│   ├── analytics.py
│   └── schema_contracts.py
└── tests/
```

---

## Troubleshooting

**Rate limiting** — Raise `--base-delay` (default 1.5 s) or narrow the year range with `--start-year`/`--end-year`. Extraction is resumable; interrupted runs continue from where they left off.

**Incomplete season** — In-progress rounds that haven't been published by the API are skipped automatically.

**MySQL errors** — Verify credentials in `scripts/config.py` and confirm the server is reachable (`mysql.server status`).
