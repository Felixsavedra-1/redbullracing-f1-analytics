# Copy this file to config.py. Do not commit config.py to version control.

# --- Team ---
# constructor_id: Ergast constructor ID for the primary team (used in parameterized queries)
# family_refs:    All constructor_ref values in the team family (used in family-wide queries)
# name:           Display name shown in terminal output
TEAM_CONFIG = {
    "constructor_id": 9,
    "family_refs": ["red_bull", "alphatauri", "rb"],
    "name": "Red Bull",
    "colors": {
        "primary": "#C9A96E",   # primary chart color
        "accent":  "#8B5E3C",   # secondary / highlight color
        "neutral": "#D4C5A9",   # axis labels, annotations
    },
}

# --- Database ---
# Option 1: SQLite (default, no setup required)
DB_CONFIG = {
    "type": "sqlite",
    "filename": "f1_analytics.db",
}

# Option 2: MySQL
# DB_CONFIG = {
#     "type": "mysql",
#     "host": "localhost",
#     "port": 3306,
#     "user": "root",
#     "password": "your_password_here",
#     "database": "f1_analytics",
# }

# --- Data paths ---
DATA_PATHS = {
    "raw_data": "data/raw/",
    "processed_data": "data/processed/",
}

# --- Extraction ---
EXTRACTION_CONFIG = {
    "start_year": 2020,
    "end_year": 2025,
    "base_delay": 1.5,
    "max_retries": 6,
    "max_base_delay": 8.0,
}
