"""
Database configuration template.

Copy this file to config.py and provide database credentials.
Do not commit config.py to version control.
"""

# Database configuration. Uncomment the section you want to use.

# Option 1: SQLite (recommended for local testing and portability).
DB_CONFIG = {
    'type': 'sqlite',
    'filename': 'f1_analytics.db'
}

# Option 2: MySQL.
# DB_CONFIG = {
#     'type': 'mysql',
#     'host': 'localhost',
#     'port': 3306,
#     'user': 'root',
#     'password': 'your_password_here',
#     'database': 'F1_RedBull_Analytics'
# }

# API configuration (reserved for future use).
API_CONFIG = {
    'base_url': 'http://ergast.com/api/f1',
    'rate_limit_delay': 0.5  # seconds between requests
}

# Data paths.
DATA_PATHS = {
    'raw_data': 'data/raw/',
    'processed_data': 'data/processed/'
}

# Extraction settings.
EXTRACTION_CONFIG = {
    'start_year': 2005,
    'end_year': 2024
}

