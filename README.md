# Red Bull F1 Analytics

ETL pipeline for Formula 1 performance analysis — Red Bull, AlphaTauri, and RB, 2020–2025. Extracts from the Ergast API into a star-schema SQLite database with 14 parameterized analytical queries and a Power BI dashboard.

---

## Stack

| Layer | Technology |
|---|---|
| Extraction | Python `requests` — resumable, adaptive rate-limit backoff |
| Transformation | `pandas` — ref-map resolution, schema validation |
| Storage | SQLite (default) or MySQL |
| Analysis | SQL queries, Jupyter notebooks, Power BI |
| Source | [api.jolpi.ca/ergast/f1](https://api.jolpi.ca/ergast/f1) |

---

## Quickstart

```bash
pip install -r requirements.txt
python scripts/run_pipeline.py
```

Extract → transform → load, then prints a driver summary on completion:

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
python scripts/run_pipeline.py --skip-extract          # skip API extraction
python scripts/run_pipeline.py --skip-pit-stops        # skip pit stop data (faster)
python scripts/run_pipeline.py --start-year 2022 --end-year 2024
python scripts/run_pipeline.py --incremental           # upsert instead of full refresh
python scripts/run_pipeline.py --base-delay 2.0 --max-retries 8
python scripts/run_pipeline.py --fast                  # demo mode: 2021–2025, reduced retries
```

---

## Analysis

Statistical models run after the pipeline completes. Prints summary tables; `--export` saves 300 DPI PNG charts to `data/exports/charts/`.

```bash
python scripts/run_analysis.py
python scripts/run_analysis.py --export
```

| Analysis | Method |
|---|---|
| Championship progression | Cumulative points per round, per season |
| Teammate head-to-head | Mean position delta with 95% t-interval and p-value |
| Grid → finish regression | OLS with R², slope, and significance test |
| Pit stop efficiency | Z-score vs season field distribution |
| DNF rate model | Poisson MLE with exact 95% confidence intervals |

---

## Queries

Run after the pipeline completes:

```bash
python scripts/run_queries.py --list
python scripts/run_queries.py --query driver_summary
python scripts/run_queries.py --query all --export
```

| Query | Description |
|---|---|
| `driver_summary` | Career stats across the Red Bull family |
| `team_performance_overview` | Points, wins, podiums by season |
| `driver_performance_comparison` | Head-to-head stats by season |
| `qualifying_vs_race_performance` | Grid-to-finish position delta |
| `circuit_performance_analysis` | Best and worst circuits (min 3 starts) |
| `pit_stop_efficiency` | Avg stop time and std dev by driver/season |
| `fastest_pit_stops` | Fastest individual stops on record |
| `championship_progression` | Cumulative points race by race |
| `driver_championship_battle` | Points gap between teammates by round |
| `fastest_laps_analysis` | Fastest lap count and share by driver |
| `reliability_analysis` | DNF rate by driver and season |
| `failure_modes` | Breakdown of retirement causes |
| `race_start_analysis` | Positions gained/lost on lap 1 |
| `key_performance_indicators` | Composite KPI summary per driver |

---

## Data model

Star schema in `f1_analytics.db`.

**Dimensions:** `circuits`, `seasons`, `constructors`, `drivers`  
**Facts:** `races`, `results`, `qualifying`, `pit_stops`, `constructor_standings`, `driver_standings`

Schema DDL: `database/schema/create_tables_sqlite.sql`  
Schema contracts: `scripts/schema_contracts.py`

---

## Configuration

SQLite requires no configuration. For MySQL:

```bash
cp scripts/config.example.py scripts/config.py
mysql -u root -p < database/schema/create_tables_mysql.sql
```

---

## Notebooks

```bash
pip install -r requirements-notebooks.txt
jupyter notebook  # open notebooks/F1_Analysis.ipynb
```

---

## Tests

```bash
python -m unittest discover -s tests
```

---

## Structure

```
├── data/
│   ├── raw/               # extracted CSVs
│   ├── processed/         # transformed CSVs
│   └── cache/             # extraction resume state
├── database/
│   ├── queries/           # .sql and .yaml analytical queries
│   └── schema/            # DDL for SQLite and MySQL
├── notebooks/
├── powerbi/
├── scripts/
│   ├── constants.py
│   ├── extract_data.py
│   ├── transform_data.py
│   ├── load_data.py
│   ├── run_pipeline.py
│   ├── run_queries.py
│   └── schema_contracts.py
└── tests/
```

---

## Troubleshooting

**Rate limiting** — Raise `--base-delay` (default 1.5s) or narrow the year range. Extraction is resumable; interrupted runs pick up where they left off.

**Incomplete season** — In-progress rounds are skipped automatically.

**MySQL errors** — Verify credentials in `scripts/config.py` and confirm the server is running (`mysql.server status`).
