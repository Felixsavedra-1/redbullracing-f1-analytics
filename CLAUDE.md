# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Red Bull F1 Analytics is a production-style Python ETL pipeline for Formula 1 performance analysis, scoped to the Red Bull family of constructors (Red Bull Racing, AlphaTauri, and RB) across the 2020–2025 seasons. It extracts from the Ergast-compatible API (`api.jolpi.ca/ergast/f1`) with resumable progress tracking, transforms and validates data against explicit schema contracts, loads into SQLite (or MySQL), and exposes 14 parameterized analytical queries covering everything from pit stop efficiency to championship progression. Post-load quality gates (15+ checks) are wired into CI to catch data integrity issues before they reach analysis.

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
python -m unittest tests.test_smoke        # end-to-end pipeline smoke test
python -m unittest tests.test_quality_checks  # quality check integration tests
python -m unittest tests.test_etl_unit     # unit tests: missing files, unmapped refs, schema violations, DNF sentinel
```

## Pipeline CLI Flags

`run_pipeline.py` accepts the following flags:

| Flag | Default | Description |
|---|---|---|
| `--start-year N` | `2020` | First season to extract |
| `--end-year N` | `2025` | Last season to extract |
| `--skip-extract` | off | Skip API extraction step |
| `--skip-transform` | off | Skip transformation step |
| `--skip-load` | off | Skip database load step |
| `--skip-pit-stops` | off | Skip pit stop extraction |
| `--skip-quality` | off | Skip post-load quality checks |
| `--incremental` | off | Upsert instead of full refresh |
| `--no-strict-schema` | off | Warn (not raise) on schema violations |
| `--fast` | off | Demo mode: 2021–2025, reduced retries/backoff, skips pit stops |
| `--base-delay N` | `1.5` | Seconds between API requests |
| `--max-retries N` | `6` | Max retries on API errors |
| `--max-base-delay N` | `8.0` | Upper bound for adaptive backoff delay |

After a successful load, the pipeline prints a Red Bull driver summary table to stdout (all drivers across Red Bull, AlphaTauri, and RB, ranked by points).

## Pipeline Architecture

The ETL pipeline flows through these stages:

1. **Extract** (`scripts/extract_data.py`) — fetches from Ergast API into `data/raw/*.csv` with resume state in `data/cache/*.json`
2. **Transform** (`scripts/transform_data.py`) — `F1DataTransformer` cleans raw CSVs into `data/processed/*_clean.csv`
3. **Load** (`scripts/load_data.py`) — `F1DataLoader` validates against schema contracts and inserts into the database
4. **Quality Check** (`scripts/data_quality.py`) — `run_quality_checks()` runs 15+ checks after load (non-empty tables, year bounds, PK uniqueness, FK integrity, non-negative values). Returns a failures list; in CI (`GITHUB_ACTIONS=true`) any failure raises `RuntimeError`.
5. **Query/Analyze** (`scripts/run_queries.py`, `notebooks/`, Power BI) — consumes the database via `database/queries/analytical_queries.yaml` or the raw `.sql` file

## Data Quality

`scripts/data_quality.py` runs automatically after each load (unless `--skip-quality` is passed). Checks fall into four categories:

- **Non-empty**: `results`, `drivers`, `races` tables must have rows
- **Year bounds**: no races outside the requested year range; all expected years present
- **Uniqueness**: `drivers`, `constructors`, `circuits`, `races` have no duplicate primary keys
- **FK integrity**: `results`, `qualifying`, and `pit_stops` have no orphaned foreign keys; no races missing results or qualifying data (excluding rounds explicitly skipped during extraction)
- **Non-negative values**: `points`, `laps`, `grid`, `position_order` in `results`

Returns a `List[Dict[str, str]]` of failures. In local runs, failures log as warnings. In CI, any failure raises `RuntimeError` to fail the pipeline.

## Query Registry

`database/queries/analytical_queries.yaml` defines 14 named SQLite queries, all parameterized by `:cid` (constructor ID). Run via `scripts/run_queries.py`:

```bash
python scripts/run_queries.py --list                   # list all query names
python scripts/run_queries.py --query driver_summary   # print to stdout
python scripts/run_queries.py --query all --export     # write all to data/exports/
```

Available queries: `driver_summary`, `team_performance_overview`, `driver_performance_comparison`, `qualifying_vs_race_performance`, `circuit_performance_analysis`, `pit_stop_efficiency`, `fastest_pit_stops`, `championship_progression`, `driver_championship_battle`, `fastest_laps_analysis`, `reliability_analysis`, `failure_modes`, `race_start_analysis`, `key_performance_indicators`.

`database/queries/analytical_queries.sql` remains for direct SQL client use.

## Configuration

`scripts/config.py` is gitignored and must be created before running the pipeline:

```bash
cp scripts/config.example.py scripts/config.py
```

Key settings in `config.py`:
- `DB_CONFIG` — SQLite (default, uses `f1_analytics.db`) or MySQL
- `EXTRACTION_CONFIG` — year range (default 2020–2025) and API rate limiting
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
| `DEFAULT_START_YEAR` | `2020` | Project data scope start |
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
- **Logging**: all scripts use `scripts/logging_utils.py`'s `setup_logging()` — do not use `print()` for operational output in pipeline scripts. `format_table(headers, rows, right_cols)` renders fixed-width text tables with optional right-aligned columns; used by `run_pipeline.py` for the post-pipeline driver summary.

## Power BI

Dashboard connects directly to `f1_analytics.db`. Run `python scripts/run_pipeline.py` to populate the database before opening Power BI. See `powerbi/README.md` for dashboard page descriptions.
