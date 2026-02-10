# Architecture Overview

This document summarizes the pipeline and data model at a glance.

## Pipeline Flow

```text
Ergast-compatible API
        |
        v
scripts/extract_data.py
        |
        v
data/raw/*.csv
        |
        v
data/cache/*.json (resume state)
        |
        v
scripts/transform_data.py
        |
        v
data/processed/*_clean.csv
        |
        v
scripts/load_data.py
        |
        v
SQLite (f1_analytics.db) or MySQL
        |
        v
scripts/run_queries.py / notebooks / Power BI
```

## Data Model (Core Tables)

```text
dimensions
  circuits
  seasons
  constructors
  drivers

facts
  races
  results
  qualifying
  pit_stops
  constructor_standings
  driver_standings
```

Key relationships:
- `races.circuit_id -> circuits.circuit_id`
- `results.race_id -> races.race_id`
- `results.driver_id -> drivers.driver_id`
- `results.constructor_id -> constructors.constructor_id`
- `qualifying` and `pit_stops` share `race_id` and `driver_id` with `results`
