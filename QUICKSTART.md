# Quick Start Guide

This guide summarises the minimal steps required to reproduce the analysis.

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Run the Pipeline
SQLite is used by default; no separate database service needs to be configured.

```bash
python scripts/run_pipeline.py
```

This will:
- Download F1 data from the Ergast API
- Clean and transform the data
- Load the processed data into your database

## Step 3: Run Your First Query

```bash
# See what queries are available
python scripts/run_queries.py --list

# Run a query
python scripts/run_queries.py --query kpi_summary

# Export results
python scripts/run_queries.py --query kpi_summary --export
```

## Summary

You now have a complete F1 Red Bull analytics database running locally.

### Next Steps
- Explore queries in `database/queries/analytical_queries.sql`
- Connect Power BI to MySQL for visualizations
- Run custom queries: `mysql -u root -p F1_RedBull_Analytics`

### Need Help?
- Consult the full [README.md](README.md) for detailed documentation.
- For connection issues, confirm that your database service is running and `config.py` contains valid credentials.

