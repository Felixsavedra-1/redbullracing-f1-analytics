# Red Bull F1 Analytics

ETL pipeline and analytical database for Oracle Red Bull Racing performance data, 2020–2025. Extracts from the Ergast API, transforms into a star-schema SQLite database, and exposes nine SQL queries plus a Power BI dashboard.

---

## Stack

| Layer | Technology |
|---|---|
| Extraction | Python `requests`, resumable with rate-limit backoff |
| Transformation | `pandas` — ref-map resolution, schema validation |
| Storage | SQLite (default) or MySQL |
| Analysis | SQL queries, Jupyter notebooks, Power BI |
| API source | [api.jolpi.ca/ergast/f1](https://api.jolpi.ca/ergast/f1) |

---

## Quickstart

```bash
pip install -r requirements.txt
python scripts/run_pipeline.py
```

The pipeline runs in three stages — extract, transform, load — and prints a driver comparison table on completion:

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

Covers both Oracle Red Bull Racing and the junior team (AlphaTauri / RB).

---

## Pipeline flags

```bash
# Skip extraction if raw data is already downloaded
python scripts/run_pipeline.py --skip-extract

# Skip pit stop extraction (faster, pit stop data is large)
python scripts/run_pipeline.py --skip-pit-stops

# Custom year range
python scripts/run_pipeline.py --start-year 2022 --end-year 2024

# Incremental load (upsert instead of full refresh)
python scripts/run_pipeline.py --incremental

# Adjust API rate limiting
python scripts/run_pipeline.py --base-delay 2.0 --max-retries 8
```

---

## Queries

Nine analytical queries are available. Run any of them after the pipeline completes:

```bash
python scripts/run_queries.py --list
python scripts/run_queries.py --query driver_summary
python scripts/run_queries.py --query team_performance_overview
python scripts/run_queries.py --query all
python scripts/run_queries.py --query driver_performance_comparison --export
```

| Query | Description |
|---|---|
| `driver_summary` | Career stats per driver across the Red Bull family |
| `team_performance_overview` | Points, wins, podiums by season |
| `driver_performance_comparison` | Head-to-head by season |
| `qualifying_vs_race_performance` | Positions gained from grid to finish |
| `circuit_performance_analysis` | Best and worst circuits (min 3 appearances) |
| `pit_stop_efficiency` | Avg stop time and std dev by driver/season |
| `championship_progression` | Points accumulation race by race |
| `fastest_laps_analysis` | Fastest lap count and percentage by driver |
| `reliability_analysis` | DNF rate and failure modes by season |

---

## Data model

Star schema in `f1_analytics.db`.

**Dimensions:** `circuits`, `seasons`, `constructors`, `drivers`  
**Facts:** `races`, `results`, `qualifying`, `pit_stops`, `constructor_standings`, `driver_standings`

Schema DDL: `database/schema/create_tables_sqlite.sql`  
Schema contracts: `scripts/schema_contracts.py`

---

## Configuration

SQLite requires no configuration. For MySQL, copy and edit the config template:

```bash
cp scripts/config.example.py scripts/config.py
```

Then create the schema and set credentials in `config.py`:

```bash
mysql -u root -p < database/schema/create_tables_mysql.sql
```

---

## Notebooks

```bash
pip install -r requirements-notebooks.txt
jupyter notebook
```

Open `notebooks/F1_Analysis.ipynb`.

---

## Tests

```bash
python -m unittest discover -s tests
```

---

## Project structure

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

**API rate limiting** — Increase `--base-delay` (default 1.5s) or reduce the year range. Extraction is fully resumable; interrupted runs continue from where they left off.

**Incomplete data** — If a season is still in progress, the current-year rounds that haven't been raced yet will be skipped automatically.

**MySQL connection errors** — Verify credentials in `scripts/config.py` and that the server is running (`mysql.server status`).
