import pandas as pd
from sqlalchemy import create_engine, text
import argparse
import os

try:
    from config import DB_CONFIG, DATA_PATHS
except ImportError:
    print("config.py not found; using default database settings.")
    DB_CONFIG = {
        'type': 'sqlite',
        'filename': 'f1_analytics.db'
    }
    DATA_PATHS = {
        'processed_data': 'data/processed/'
    }

def create_db_connection(config=None):
    """Create a SQLAlchemy engine for the configured database."""
    config = config or DB_CONFIG
    
    if config.get('type') == 'sqlite':
        db_file = config.get('filename', 'f1_analytics.db')
        connection_string = f"sqlite:///{db_file}"
    else:
        connection_string = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
    
    return create_engine(connection_string)

def execute_query(engine, query_name, query_text):
    """Execute a SQL query and return the result as a DataFrame."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query_text), conn)
        return df
    except Exception as e:
        print(f"Error executing {query_name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def export_results(df, filename, output_path=None):
    """Export query results to a CSV file."""
    output_path = output_path or DATA_PATHS.get('processed_data', 'data/processed/')
    os.makedirs(output_path, exist_ok=True)
    filepath = os.path.join(output_path, filename)
    df.to_csv(filepath, index=False)
    print(f"Exported results to {filepath}.")

def load_queries_from_file(query_file='database/queries/analytical_queries.sql'):
    """Load named SELECT queries from a SQL file."""
    queries = {}
    
    if not os.path.exists(query_file):
        print(f"Query file {query_file} not found.")
        return queries
    
    with open(query_file, 'r') as f:
        content = f.read()
        
        # Parse queries (simple approach - splits by comment headers)
        sections = content.split('-- ============================================================')
        
        for section in sections:
            if not section.strip():
                continue
            
            lines = section.strip().split('\n')
            query_name = None
            query_lines = []
            
            for line in lines:
                line = line.strip()
                if line.startswith('--') and len(line) > 5:
                    if 'QUERY' in line.upper() or 'ANALYSIS' in line.upper():
                        continue
                    query_name = line.replace('--', '').strip().lower().replace(' ', '_')
                elif line and not line.startswith('--'):
                    query_lines.append(line)
            
                if query_lines and query_name:
                    query_text = '\n'.join(query_lines)
                    if query_text.strip().upper().startswith('SELECT'):
                        queries[query_name] = query_text
    
    return queries

def main():
    parser = argparse.ArgumentParser(description='Run F1 analytical queries')
    parser.add_argument('--query', type=str, help='Query name to execute (or "all" for all queries)')
    parser.add_argument('--export', action='store_true', help='Export results to CSV')
    parser.add_argument('--list', action='store_true', help='List available queries')
    parser.add_argument('--file', type=str, help='Load query from SQL file', 
                       default='database/queries/analytical_queries.sql')
    
    args = parser.parse_args()
    
    engine = create_db_connection()
    
    # Define built-in queries
    queries = {
        'kpi_summary': """
            SELECT 
                'Total Races' AS metric,
                COUNT(*) AS value
            FROM results 
            WHERE constructor_id = 9
            UNION ALL
            SELECT 'Total Wins', COUNT(*) FROM results WHERE constructor_id = 9 AND position = 1
            UNION ALL
            SELECT 'Total Podiums', COUNT(*) FROM results WHERE constructor_id = 9 AND position <= 3
            UNION ALL
            SELECT 'Win Rate %', ROUND(COUNT(CASE WHEN position = 1 THEN 1 END) * 100.0 / COUNT(*), 2)
            FROM results WHERE constructor_id = 9;
        """,
        'season_summary': """
            SELECT 
                r.year AS season,
                SUM(res.points) AS total_points,
                COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
                COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
                ROUND(AVG(res.position_order), 2) AS avg_finish
            FROM results res
            JOIN races r ON res.race_id = r.race_id
            WHERE res.constructor_id = 9
            GROUP BY r.year
            ORDER BY r.year DESC;
        """
    }
    
    # Try to load additional queries from file
    file_queries = load_queries_from_file(args.file)
    queries.update(file_queries)
    
    if args.list:
        print("Available queries:")
        for query_name in sorted(queries.keys()):
            print(f"  - {query_name}")
        return
    
    if args.query:
        if args.query == 'all':
            for query_name, query_text in queries.items():
                print(f"\n{'='*60}")
                print(f"Executing {query_name}...")
                print('='*60)
                df = execute_query(engine, query_name, query_text)
                
                if df is not None and not df.empty:
                    print(df.to_string())
                    
                    if args.export:
                        export_results(df, f"{query_name}_results.csv")
        elif args.query in queries:
            print(f"Executing {args.query}...")
            df = execute_query(engine, args.query, queries[args.query])
            
            if df is not None and not df.empty:
                print(df.to_string())
                
                if args.export:
                    export_results(df, f"{args.query}_results.csv")
            else:
                print("No results returned or query failed")
        else:
            print(f"Query '{args.query}' not found.")
            print("\nAvailable queries:")
            for query_name in sorted(queries.keys()):
                print(f"  - {query_name}")
    else:
        print("Please specify a query to execute with --query")
        print("Use --list to see available queries")
        print("Use --query all to execute all queries")

if __name__ == "__main__":
    main()
