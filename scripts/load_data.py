import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

try:
    from config import DB_CONFIG, DATA_PATHS
except ImportError:
    print("config.py not found; using default database settings.")
    print("Copy scripts/config.example.py to scripts/config.py and configure your database.")
    DB_CONFIG = {
        'type': 'sqlite',
        'filename': 'f1_analytics.db'
    }
    DATA_PATHS = {
        'processed_data': 'data/processed/'
    }

class F1DataLoader:
    """Load transformed F1 data into a relational database."""
    
    def __init__(self, config=None, processed_data_path=None):
        self.config = config or DB_CONFIG
        self.processed_path = processed_data_path or DATA_PATHS.get('processed_data', 'data/processed/')
        self.engine = None
        self._connect()
    
    def _connect(self):
        """Create a database engine and validate the connection."""
        try:
            if self.config.get('type') == 'sqlite':
                db_file = self.config.get('filename', 'f1_analytics.db')
                if not os.path.isabs(db_file) and '/' in db_file:
                    os.makedirs(os.path.dirname(db_file), exist_ok=True)
                    
                connection_string = f"sqlite:///{db_file}"
                print(f"Connecting to SQLite database at {db_file}.")
            else:
                connection_string = (
                    f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
                    f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
                )
                print(f"Connecting to MySQL at {self.config['host']}.")

            self.engine = create_engine(connection_string)
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print("Database connection established.")
            
        except Exception as e:
            print(f"Error connecting to database: {e}")
            print("Check your database configuration in scripts/config.py.")
            raise
    
    def _load_table(self, df, table_name):
        """Load a DataFrame into a database table."""
        if df.empty:
            print(f"Skipping {table_name}: no rows to load.")
            return

        try:
            if self.config.get('type') == 'sqlite':
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            else:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM {table_name}"))
                    conn.commit()
                    df.to_sql(table_name, self.engine, if_exists='append', index=False)
                    conn.commit()
            print(f"Loaded {len(df)} rows into {table_name}.")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")
            raise

    def load_circuits(self):
        """Load circuits data."""
        print("Loading circuits...")
        df = pd.read_csv(f'{self.processed_path}circuits_clean.csv')
        self._load_table(df, 'circuits')
    
    def load_seasons(self):
        """Load seasons data."""
        print("Loading seasons...")
        df = pd.read_csv(f'{self.processed_path}../raw/seasons.csv')
        self._load_table(df, 'seasons')
    
    def load_constructors(self):
        """Load constructors data."""
        print("Loading constructors...")
        df = pd.read_csv(f'{self.processed_path}../raw/constructors.csv')
        self._load_table(df, 'constructors')
    
    def load_drivers(self):
        """Load drivers data."""
        print("Loading drivers...")
        df = pd.read_csv(f'{self.processed_path}drivers_clean.csv')
        
        if 'dob' in df.columns:
            df['dob'] = pd.to_datetime(df['dob'], errors='coerce').dt.date
        
        self._load_table(df, 'drivers')
    
    def load_races(self):
        """Load races data."""
        print("Loading races...")
        df = pd.read_csv(f'{self.processed_path}races_clean.csv')
        
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'], errors='coerce').dt.date
        if 'race_time' in df.columns:
            df['race_time'] = df['race_time'].fillna('00:00:00')
        
        self._load_table(df, 'races')
    
    def load_results(self):
        """Load results data."""
        print("Loading results...")
        df = pd.read_csv(f'{self.processed_path}results_clean.csv')
        
        required_cols = [
            'race_id', 'driver_id', 'constructor_id', 'number', 'grid', 
            'position', 'position_text', 'position_order', 'points', 'laps',
            'time_result', 'milliseconds', 'fastest_lap', 'fastest_lap_rank',
            'fastest_lap_time', 'fastest_lap_speed', 'status_id'
        ]
        
        df = df[[col for col in required_cols if col in df.columns]]
        
        self._load_table(df, 'results')
    
    def load_qualifying(self):
        """Load qualifying data."""
        print("Loading qualifying...")
        df = pd.read_csv(f'{self.processed_path}qualifying_clean.csv')
        
        required_cols = ['race_id', 'driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3']
        df = df[[col for col in required_cols if col in df.columns]]
        
        self._load_table(df, 'qualifying')
    
    def load_pit_stops(self):
        """Load pit stops data."""
        print("Loading pit stops...")
        df = pd.read_csv(f'{self.processed_path}pit_stops_clean.csv')
        
        if 'time_of_day' in df.columns:
            df['time_of_day'] = df['time_of_day'].fillna('00:00:00')
        
        required_cols = ['race_id', 'driver_id', 'stop', 'lap', 'time_of_day', 'duration', 'milliseconds']
        df = df[[col for col in required_cols if col in df.columns]]
        
        self._load_table(df, 'pit_stops')
    
    def load_standings(self):
        """Load constructor and driver standings."""
        print("Loading standings...")
        
        df_const = pd.read_csv(f'{self.processed_path}constructor_standings_clean.csv')
        required_cols = ['race_id', 'constructor_id', 'points', 'position', 'position_text', 'wins']
        df_const = df_const[[col for col in required_cols if col in df_const.columns]]
        self._load_table(df_const, 'constructor_standings')
        
        df_driver = pd.read_csv(f'{self.processed_path}driver_standings_clean.csv')
        required_cols = ['race_id', 'driver_id', 'points', 'position', 'position_text', 'wins']
        df_driver = df_driver[[col for col in required_cols if col in df_driver.columns]]
        self._load_table(df_driver, 'driver_standings')
    
    def load_all(self):
        """Load all transformed data into the configured database."""
        print("=" * 60)
        print("Starting data loading into database.")
        print("=" * 60)
        
        try:
            self.load_seasons()
            self.load_circuits()
            self.load_constructors()
            self.load_drivers()
            self.load_races()
            self.load_results()
            self.load_qualifying()
            self.load_pit_stops()
            self.load_standings()
            
            print("=" * 60)
            print("All data loaded successfully into database.")
            print("=" * 60)
            
        except Exception as e:
            print(f"Error during loading: {e}")
            import traceback
            traceback.print_exc()
            raise

def main():
    loader = F1DataLoader()
    loader.load_all()

if __name__ == "__main__":
    main()

