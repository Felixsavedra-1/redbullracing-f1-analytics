# F1 Red Bull Racing Analytics

Analytics pipeline for Oracle Red Bull Racing F1 performance (2015–2025) using SQL, Python, and Power BI.

## Technologies

- Python
- SQL (SQLite/MySQL)
- pandas, requests, sqlalchemy
- matplotlib, seaborn
- Jupyter Notebook
- Ergast F1 API (via Jolpi proxy)

## Highlights

- Automated data extraction with resumable, rate‑limit aware ingestion
- Normalized analytical database with schema contracts and data‑quality checks
- Reproducible SQL KPI queries and longitudinal performance analysis
- Notebook‑based exploratory analysis (2022–2025 emphasized)
- Power BI dashboard workflow

## Quickstart

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Optional notebook dependencies:
```bash
pip install -r requirements-notebooks.txt
```

3. Run the pipeline (2015–2025 default):
```bash
python scripts/run_pipeline.py
```

4. Fast demo run (skips pit stops):
```bash
python scripts/run_pipeline.py --fast
```

### Database

SQLite is default and creates `f1_analytics.db` in the repo root.

For MySQL:
1. Create the schema:
```bash
mysql -u root -p < database/schema/create_tables.sql
```
2. Configure credentials in `scripts/config.py`.

## Queries

List queries:
```bash
python scripts/run_queries.py --list
```

Run a query:
```bash
python scripts/run_queries.py --query kpi_summary
```

Export results:
```bash
python scripts/run_queries.py --query kpi_summary --export
```

## Notebook

Run Jupyter:
```bash
jupyter notebook
```

Open `notebooks/F1_Analysis.ipynb` and run all cells.

## Dashboard

Power BI documentation: `powerbi/README.md`.

## Project Structure

```
f1-redbull-analytics/
├── data/
├── database/
├── docs/
├── notebooks/
├── scripts/
├── requirements.txt
└── README.md
```

## Troubleshooting

Database connection issues:
- Check MySQL is running: `mysql.server status`
- Verify credentials in `scripts/config.py`

API rate limiting:
- Increase `--base-delay` or reduce the year range
- Qualifying and pit stop extraction are resumable

Data availability:
- Project scope is 2015–2025 and is clamped
- Pit stop data is limited before 2012

## Data Sources

Ergast F1 API via Jolpi proxy:
```
https://api.jolpi.ca/ergast/f1
```

## License

Educational and personal use. Please respect API rate limits and terms of service.
