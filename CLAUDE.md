# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Red Bull F1 Analytics is a Python ETL pipeline that extracts F1 data from the Ergast-compatible API (`api.jolpi.ca/ergast/f1`), transforms it, loads it into SQLite (or MySQL), and exposes it for analysis via Jupyter notebooks, SQL queries, and a Power BI dashboard.

## Common Commands

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-notebooks.txt  # for notebook/visualization work

# Run the full pipeline
python scripts/run_pipeline.py

# Run all tests
python -m unittest discover -s tests

# Run a single test file
python -m unittest tests.test_smoke
python -m unittest tests.test_quality_checks
```

## Pipeline Architecture

The ETL pipeline flows through these stages:

1. **Extract** (`scripts/extract_data.py`) — fetches from Ergast API into `data/raw/*.csv` with resume state in `data/cache/*.json`
2. **Transform** (`scripts/transform_data.py`) — `F1DataTransformer` cleans raw CSVs into `data/processed/*_clean.csv`
3. **Load** (`scripts/load_data.py`) — `F1DataLoader` validates against schema contracts and inserts into the database
4. **Query/Analyze** (`scripts/run_queries.py`, `notebooks/`, Power BI) — consumes the database

## Configuration

`scripts/config.py` is gitignored and must be created before running the pipeline:

```bash
cp scripts/config.example.py scripts/config.py
```

Key settings in `config.py`:
- `DB_CONFIG` — SQLite (default, uses `f1_analytics.db`) or MySQL
- `EXTRACTION_CONFIG` — year range (default 2015–2025) and API rate limiting
- `DATA_PATHS` — raw/processed data directories

If `config.py` is missing, scripts fall back to SQLite defaults automatically.

## Data Model

Star schema with two dimension groups:

**Dimensions**: `circuits`, `seasons`, `constructors`, `drivers`  
**Facts**: `races`, `results`, `qualifying`, `pit_stops`, `constructor_standings`, `driver_standings`

Key FKs: `results` links to `races`, `drivers`, and `constructors` via `_id` surrogate keys. `qualifying` and `pit_stops` join on `race_id` + `driver_id`.

Schema DDL lives in `database/schema/` (SQLite and MySQL variants). Schema contracts for DataFrame validation are in `scripts/schema_contracts.py` — add new tables there when extending the data model.

## Constants

All shared magic values live in `scripts/constants.py`:

| Constant | Value | Meaning |
|---|---|---|
| `RED_BULL_CONSTRUCTOR_ID` | `9` | Oracle Red Bull Racing ID in the Ergast API |
| `DNF_POSITION_ORDER` | `999` | Sentinel for non-finishers in `position_order` |
| `DEFAULT_START_YEAR` | `2015` | Project data scope start |
| `DEFAULT_END_YEAR` | `2025` | Project data scope end |

Never hardcode `9` for the constructor or `999` for DNF position — import from `constants`.

## SQL Compatibility

The project targets **SQLite** as the default database. All SQL in `database/queries/` and inline queries must be SQLite-compatible:

- String concat: `a || ' ' || b` (not `CONCAT`)
- Integer cast: `CAST(x AS INTEGER)` (not `CAST AS SIGNED`)
- Standard deviation: `SQRT(AVG(x*x) - AVG(x)*AVG(x))` (not `STDDEV`)

## Key Design Decisions

- **`F1DataLoader` `mode`**: `"full_refresh"` (default) drops and recreates the SQLite DB; `"incremental"` upserts. Pass `strict_schema=False` to warn instead of raise on schema violations.
- **`ref`-based joining during transform**: raw CSVs use `*_ref` string keys (e.g., `driver_ref`, `constructor_ref`); the transform step resolves these to integer surrogate `*_id` keys before loading.
- **Resume state**: extraction writes progress to `data/cache/*.json` so interrupted runs can resume without re-fetching all data.
- **Logging**: all scripts use `scripts/logging_utils.py`'s `setup_logging()` — do not use `print()` for operational output in pipeline scripts.

## Power BI

Dashboard connects directly to `f1_analytics.db`. Run `python scripts/run_pipeline.py` to populate the database before opening Power BI. See `powerbi/README.md` for dashboard page descriptions.
