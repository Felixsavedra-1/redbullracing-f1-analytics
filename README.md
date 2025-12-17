# F1 Red Bull Racing Analytics

Data analytics project tracking Oracle Red Bull Racing F1 performance using SQL, Python, and Power BI.

## Technologies

- **Python** - Data processing and automation
- **SQL, SQLite/MySQL** - Database and analytics
- **pandas, requests, sqlalchemy** - Data manipulation and API access
- **matplotlib, seaborn** - Data visualization
- **Jupyter Notebooks** - Interactive analysis
- **Ergast F1 API** - Free F1 data source

## Features

- Automated data extraction from the Ergast F1 API
- Normalized analytical database with more than ten tables
- SQL queries for a broad set of key performance indicators
- Longitudinal performance tracking and descriptive statistics
- End-to-end, scriptable data pipeline
- Jupyter-based exploratory analysis (with 2022–2024 highlighted)
- Power BI dashboard for interactive visualization

## KPIs Tracked

- Win rate and podium percentage
- Driver comparisons
- Circuit performance
- Pit stop efficiency
- Championship progression
- Fastest laps analysis
- Reliability metrics

---

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Database Setup
The project uses **SQLite** by default for portability; no additional database service is required.

If you prefer **MySQL**:
1. Install MySQL
2. Create database: `mysql -u root -p < database/schema/create_tables.sql`
3. Edit `scripts/config.py` to switch to MySQL configuration.

### 3. Run the Pipeline

**Option A: Run the complete pipeline (recommended)**
```bash
python scripts/run_pipeline.py
```

This will:
1. Extract data from the Ergast F1 API (2005–2024 by default)
2. Transform and clean the data
3. Load data into the configured database

**Option B: Run steps individually**
```bash
# Step 1: Extract data
python scripts/extract_data.py --start-year 2005 --end-year 2024

# Step 2: Transform data
python scripts/transform_data.py

# Step 3: Load into database
python scripts/load_data.py
```

**Custom year range:**
```bash
python scripts/run_pipeline.py --start-year 2010 --end-year 2023
```

**Skip steps (if you've already run parts):**
```bash
# Skip extraction (already have raw data)
python scripts/run_pipeline.py --skip-extract

# Skip transformation (already have processed data)
python scripts/run_pipeline.py --skip-extract --skip-transform
```

---

## Running Queries

### List Available Queries
```bash
python scripts/run_queries.py --list
```

### Run a Specific Query
```bash
# Display results in terminal
python scripts/run_queries.py --query kpi_summary

# Export results to CSV
python scripts/run_queries.py --query kpi_summary --export

# Run all queries
python scripts/run_queries.py --query all --export
```

### Run SQL Queries Directly

You can also run queries from `database/queries/analytical_queries.sql` directly in MySQL:
```bash
mysql -u root -p F1_RedBull_Analytics < database/queries/analytical_queries.sql
```

---

## Jupyter Notebook Analysis

The project includes a comprehensive Jupyter notebook for interactive analysis and visualization.

### Running the Notebook

1. **Start Jupyter Notebook:**
   ```bash
   jupyter notebook
   ```

2. **Open the analysis notebook:**
   - Navigate to `notebooks/F1_Analysis.ipynb`
   - The notebook includes:
     - Team performance overview and KPIs
     - Season-by-season analysis with 2022–2024 highlighted
     - Driver comparison and statistics
     - A focused analysis of recent performance (2022–2024)
     - Visualizations styled with Red Bull team colours

3. **Requirements:**
   - Run the data pipeline first to populate the database.
   - All dependencies are listed in `requirements.txt`.

---

## Project Structure

```
f1-redbull-analytics/
├── data/
│   ├── raw/              # Raw data from API
│   └── processed/        # Cleaned/transformed data
├── database/
│   ├── schema/
│   │   └── create_tables.sql    # Database schema
│   └── queries/
│       └── analytical_queries.sql   # Analysis queries
├── notebooks/
│   └── F1_Analysis.ipynb  # Jupyter notebook for analysis
├── scripts/
│   ├── extract_data.py      # Extract from API
│   ├── transform_data.py    # Clean and transform
│   ├── load_data.py         # Load into MySQL
│   ├── run_queries.py       # Execute queries
│   ├── run_pipeline.py      # Main workflow script
│   ├── config.example.py    # Config template
│   └── config.py            # Your config (create this)
├── requirements.txt
└── README.md
```

```

---

## Dashboard

The project includes a Power BI dashboard for visual analytics.

### Features
- **Executive Summary**: High-level team KPIs.
- **Driver Analysis**: Head-to-head performance metrics.
- **Technical Metrics**: Pit stop efficiency and reliability stats.

For more details, see the [Power BI Documentation](powerbi/README.md).

## Available Queries

### Key Performance Indicators
- **KPI Summary** - Overall team statistics (wins, podiums, win rate)
- **Season Summary** - Performance by year

### Performance Analysis
- **Team Performance Overview** - Points, wins, podiums by season
- **Driver Performance Comparison** - Head-to-head driver stats
- **Circuit Performance** - Best and worst tracks for Red Bull
- **Pit Stop Analysis** - Efficiency and fastest stops
- **Fastest Laps Analysis** - Fastest lap statistics
- **Reliability Analysis** - DNF rates and common issues
- **Race Start Analysis** - Grid position vs finish position

### Championship Tracking
- **Championship Progression** - Constructor and driver standings over time

---

## Troubleshooting

### Database connection issues
- Verify MySQL is running (if used): `mysql.server status`
- Check credentials in `scripts/config.py`
- Ensure the database exists: `mysql -u root -p -e "SHOW DATABASES;"`

### API rate limiting
- The extraction script includes basic rate limiting (0.5s delay between requests).
- For large ranges, you may prefer to run extraction in smaller year windows.

### Data availability
- Some data (for example, pit stops) is only available from 2012 onward.
- Driver numbers may not be available for all historical entries.

---

## Data Sources

This project uses the [Ergast F1 API](http://ergast.com/mrd/), a free and open-source Formula 1 API that provides historical and current F1 data.

---

## Next Steps

1. **Scheduled Updates** - Set up cron jobs or scheduled tasks to update data regularly
2. **Custom Analysis** - Add your own queries to `database/queries/analytical_queries.sql`
4. **Dashboards** - Create visualizations using the query results

---

## License

This project is for educational and personal use. Please respect the Ergast API rate limits and terms of service.
