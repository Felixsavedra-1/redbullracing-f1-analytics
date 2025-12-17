import requests
import pandas as pd
import os
import time
from typing import List, Dict, Optional

class F1DataExtractor:
    """Extract F1 data from Ergast API"""
    
    BASE_URL = "https://api.jolpi.ca/ergast/f1"
    
    def __init__(self, output_path='data/raw/'):
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)
    
    def _make_request(self, endpoint: str, limit: int = 1000, offset: int = 0) -> Optional[Dict]:
        """Issue a single API request with basic rate limiting."""
        url = f"{self.BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(0.5)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {endpoint}: {e}")
            return None
    
    def _extract_table(self, json_data: Dict, table_name: str) -> List[Dict]:
        """Extract the main table payload from an Ergast response."""
        if not json_data or 'MRData' not in json_data:
            return []
        
        mr_data = json_data['MRData']
        table_data = mr_data.get(table_name)
        if not table_data:
            for key in mr_data.keys():
                if key.endswith('Table'):
                    table_data = mr_data[key]
                    break
        if not table_data:
            return []
        
        for key, value in table_data.items():
            if isinstance(value, list):
                return value
        
        if 'StandingsLists' in table_data:
            return table_data['StandingsLists']
        
        return []
    
    def extract_circuits(self):
        """Extract circuits data."""
        print("Extracting circuits...")
        all_circuits = []
        offset = 0
        limit = 100
        
        while True:
            data = self._make_request("circuits", limit=limit, offset=offset)
            if not data:
                break
            
            circuits = self._extract_table(data, "CircuitsTable")
            if not circuits:
                break
            
            for circuit in circuits:
                circuit_info = circuit
                location = circuit_info.get('Location', {})
                all_circuits.append({
                    'circuit_ref': circuit_info.get('circuitId', ''),
                    'circuit_name': circuit_info.get('circuitName', ''),
                    'location': location.get('locality', ''),
                    'country': location.get('country', ''),
                    'lat': float(location.get('lat', 0)) if location.get('lat') else None,
                    'lng': float(location.get('long', 0)) if location.get('long') else None,
                    'altitude': None,
                    'url': circuit_info.get('url', '')
                })
            
            if len(circuits) < limit:
                break
            offset += limit
        
        df = pd.DataFrame(all_circuits)
        df.to_csv(f'{self.output_path}circuits.csv', index=False)
        print(f"Extracted {len(df)} circuits.")
        return df
    
    def extract_seasons(self):
        """Extract seasons data."""
        print("Extracting seasons...")
        data = self._make_request("seasons", limit=100)
        if not data:
            return None
        
        seasons = self._extract_table(data, "SeasonTable")
        all_seasons = []
        
        for season in seasons:
            all_seasons.append({
                'year': int(season.get('season', 0)),
                'url': season.get('url', '')
            })
        
        df = pd.DataFrame(all_seasons)
        df.to_csv(f'{self.output_path}seasons.csv', index=False)
        print(f"Extracted {len(df)} seasons.")
        return df
    
    def extract_constructors(self):
        """Extract constructors data."""
        print("Extracting constructors...")
        all_constructors = []
        offset = 0
        limit = 100
        
        while True:
            data = self._make_request("constructors", limit=limit, offset=offset)
            if not data:
                break
            
            constructors = self._extract_table(data, "ConstructorTable")
            if not constructors:
                break
            
            for constructor in constructors:
                const_info = constructor
                all_constructors.append({
                    'constructor_ref': const_info.get('constructorId', ''),
                    'constructor_name': const_info.get('name', ''),
                    'nationality': const_info.get('nationality', ''),
                    'url': const_info.get('url', '')
                })
            
            if len(constructors) < limit:
                break
            offset += limit
        
        df = pd.DataFrame(all_constructors)
        df.insert(0, 'constructor_id', range(1, len(df) + 1))
        df.to_csv(f'{self.output_path}constructors.csv', index=False)
        print(f"Extracted {len(df)} constructors.")
        return df
    
    def extract_drivers(self):
        """Extract drivers data."""
        print("Extracting drivers...")
        all_drivers = []
        offset = 0
        limit = 100
        
        while True:
            data = self._make_request("drivers", limit=limit, offset=offset)
            if not data:
                break
            
            drivers = self._extract_table(data, "DriverTable")
            if not drivers:
                break
            
            for driver in drivers:
                driver_info = driver
                dob = driver_info.get('dateOfBirth', '')
                all_drivers.append({
                    'driver_ref': driver_info.get('driverId', ''),
                    'driver_number': None,
                    'code': driver_info.get('code', ''),
                    'forename': driver_info.get('givenName', ''),
                    'surname': driver_info.get('familyName', ''),
                    'dob': dob if dob else None,
                    'nationality': driver_info.get('nationality', ''),
                    'url': driver_info.get('url', '')
                })
            
            if len(drivers) < limit:
                break
            offset += limit
        
        df = pd.DataFrame(all_drivers)
        df.insert(0, 'driver_id', range(1, len(df) + 1))
        df.to_csv(f'{self.output_path}drivers.csv', index=False)
        print(f"Extracted {len(df)} drivers.")
        return df
    
    def extract_races(self, start_year: int = 2005, end_year: int = 2024):
        """Extract race metadata for a range of years."""
        print(f"Extracting races ({start_year}-{end_year})...")
        all_races = []
        
        for year in range(start_year, end_year + 1):
            offset = 0
            limit = 100
            
            while True:
                data = self._make_request(f"{year}/races", limit=limit, offset=offset)
                if not data:
                    break
                
                races = self._extract_table(data, "RaceTable")
                if not races:
                    break
                
                for race in races:
                    race_info = race
                    circuit = race_info.get('Circuit', {})
                    first_race = race_info.get('FirstPractice', {})
                    
                    race_date = race_info.get('date', '')
                    race_time = race_info.get('time', '00:00:00Z').replace('Z', '')
                    
                    all_races.append({
                        'year': year,
                        'round': int(race_info.get('round', 0)),
                        'race_id': int(f"{year}{int(race_info.get('round', 0)):02d}"),
                        'circuit_ref': circuit.get('circuitId', ''),
                        'race_name': race_info.get('raceName', ''),
                        'race_date': race_date,
                        'race_time': race_time,
                        'url': race_info.get('url', '')
                    })
                
                if len(races) < limit:
                    break
                offset += limit
        
        df = pd.DataFrame(all_races)
        df.to_csv(f'{self.output_path}races.csv', index=False)
        print(f"Extracted {len(df)} races.")
        return df
    
    def extract_results(self, start_year: int = 2005, end_year: int = 2024):
        """Extract race results."""
        print(f"Extracting results ({start_year}-{end_year})...")
        all_results = []
        
        for year in range(start_year, end_year + 1):
            offset = 0
            limit = 100
            
            while True:
                data = self._make_request(f"{year}/results", limit=limit, offset=offset)
                if not data:
                    break
                
                races = self._extract_table(data, "RaceTable")
                if not races:
                    break
                
                for race in races:
                    race_info = race
                    round_num = int(race_info.get('round', 0))
                    race_id = int(f"{year}{round_num:02d}")
                    
                    results = race_info.get('Results', [])
                    if not isinstance(results, list):
                        results = [results]
                    
                    for result in results:
                        driver = result.get('Driver', {})
                        constructor = result.get('Constructor', {})
                        fastest_lap = result.get('FastestLap', {})
                        
                        position = result.get('position', '')
                        position_text = result.get('positionText', '')
                        
                        all_results.append({
                            'race_id': race_id,
                            'driver_ref': driver.get('driverId', ''),
                            'constructor_ref': constructor.get('constructorId', ''),
                            'number': int(result.get('number', 0)) if result.get('number') else None,
                            'grid': int(result.get('grid', 0)) if result.get('grid') else None,
                            'position': int(position) if position.isdigit() else None,
                            'position_text': position_text,
                            'position_order': int(result.get('position', 999)) if position.isdigit() else 999,
                            'points': float(result.get('points', 0)),
                            'laps': int(result.get('laps', 0)) if result.get('laps') else None,
                            'time_result': result.get('Time', {}).get('time', '') if result.get('Time') else None,
                            'milliseconds': int(result.get('Time', {}).get('millis', 0)) if result.get('Time') and result.get('Time').get('millis') else None,
                            'fastest_lap': int(fastest_lap.get('lap', 0)) if fastest_lap.get('lap') else None,
                            'fastest_lap_rank': int(fastest_lap.get('rank', 0)) if fastest_lap.get('rank') else None,
                            'fastest_lap_time': fastest_lap.get('Time', {}).get('time', '') if fastest_lap.get('Time') else None,
                            'fastest_lap_speed': fastest_lap.get('AverageSpeed', {}).get('speed', '') if fastest_lap.get('AverageSpeed') else None,
                            'status': result.get('status', 'Finished')
                        })
                
                if len(races) < limit:
                    break
                offset += limit
        
        df = pd.DataFrame(all_results)
        df.to_csv(f'{self.output_path}results.csv', index=False)
        print(f"Extracted {len(df)} results.")
        return df
    
    def extract_qualifying(self, start_year: int = 2005, end_year: int = 2024):
        """Extract qualifying results."""
        print(f"Extracting qualifying ({start_year}-{end_year})...")
        all_qualifying = []
        
        for year in range(start_year, end_year + 1):
            offset = 0
            limit = 100
            
            while True:
                data = self._make_request(f"{year}/qualifying", limit=limit, offset=offset)
                if not data:
                    break
                
                races = self._extract_table(data, "RaceTable")
                if not races:
                    break
                
                for race in races:
                    race_info = race.get('QualifyingResults', [])
                    if not isinstance(race_info, list):
                        race_info = [race_info]
                    
                    parent_race = race
                    round_num = int(parent_race.get('round', 0)) if parent_race else 0
                    race_id = int(f"{year}{round_num:02d}") if parent_race else 0
                    
                    for qualifying in race_info:
                        driver = qualifying.get('Driver', {})
                        constructor = qualifying.get('Constructor', {})
                        
                        all_qualifying.append({
                            'race_id': race_id,
                            'driver_ref': driver.get('driverId', ''),
                            'constructor_ref': constructor.get('constructorId', ''),
                            'number': int(qualifying.get('number', 0)) if qualifying.get('number') else None,
                            'position': int(qualifying.get('position', 0)) if qualifying.get('position') else None,
                            'q1': qualifying.get('Q1', ''),
                            'q2': qualifying.get('Q2', ''),
                            'q3': qualifying.get('Q3', '')
                        })
                
                if len(races) < limit:
                    break
                offset += limit
        
        df = pd.DataFrame(all_qualifying)
        df.to_csv(f'{self.output_path}qualifying.csv', index=False)
        print(f"Extracted {len(df)} qualifying results.")
        return df
    
    def extract_pit_stops(self, start_year: int = 2012, end_year: int = 2024):
        """Extract pit stop data (available from 2012 onward)."""
        print(f"Extracting pit stops ({start_year}-{end_year})...")
        all_pit_stops = []
        
        for year in range(start_year, end_year + 1):
            offset = 0
            limit = 100
            
            while True:
                data = self._make_request(f"{year}/pitstops", limit=limit, offset=offset)
                if not data:
                    break
                
                races = self._extract_table(data, "RaceTable")
                if not races:
                    break
                
                for race in races:
                    race_info = race
                    round_num = int(race_info.get('round', 0))
                    race_id = int(f"{year}{round_num:02d}")
                    
                    pit_stops = race_info.get('PitStops', [])
                    if not isinstance(pit_stops, list):
                        pit_stops = [pit_stops]
                    
                    for pit_stop in pit_stops:
                        driver = pit_stop.get('Driver', {})
                        time_of_day = pit_stop.get('time', '')
                        duration = pit_stop.get('duration', '')
                        
                        all_pit_stops.append({
                            'race_id': race_id,
                            'driver_ref': driver.get('driverId', ''),
                            'stop': int(pit_stop.get('stop', 0)),
                            'lap': int(pit_stop.get('lap', 0)),
                            'time_of_day': time_of_day,
                            'duration': duration,
                            'milliseconds': int(float(duration) * 1000) if duration else None
                        })
                
                if len(races) < limit:
                    break
                offset += limit
        
        df = pd.DataFrame(all_pit_stops)
        df.to_csv(f'{self.output_path}pit_stops.csv', index=False)
        print(f"Extracted {len(df)} pit stops.")
        return df
    
    def extract_standings(self, start_year: int = 2005, end_year: int = 2024):
        """Extract constructor and driver standings."""
        print(f"Extracting standings ({start_year}-{end_year})...")
        all_constructor_standings = []
        all_driver_standings = []
        
        for year in range(start_year, end_year + 1):
            for round_num in range(1, 25):
                data = self._make_request(f"{year}/{round_num}/constructorStandings")
                if not data:
                    continue
                
                standings = self._extract_table(data, "StandingsTable")
                if not standings:
                    continue
                
                for standing_list in standings:
                    standings_list = standing_list.get('StandingsLists', [])
                    if not isinstance(standings_list, list):
                        standings_list = [standings_list]
                    
                    for standing in standings_list:
                        round_num_actual = int(standing.get('round', round_num))
                        race_id = int(f"{year}{round_num_actual:02d}")
                        
                        constructor_standings = standing.get('ConstructorStandings', [])
                        if not isinstance(constructor_standings, list):
                            constructor_standings = [constructor_standings]
                        
                        for cs in constructor_standings:
                            constructor = cs.get('Constructor', {})
                            all_constructor_standings.append({
                                'race_id': race_id,
                                'constructor_ref': constructor.get('constructorId', ''),
                                'points': float(cs.get('points', 0)),
                                'position': int(cs.get('position', 0)),
                                'position_text': cs.get('positionText', ''),
                                'wins': int(cs.get('wins', 0))
                            })
            
            for round_num in range(1, 25):
                data = self._make_request(f"{year}/{round_num}/driverStandings")
                if not data:
                    continue
                
                standings = self._extract_table(data, "StandingsTable")
                if not standings:
                    continue
                
                for standing_list in standings:
                    standings_list = standing_list.get('StandingsLists', [])
                    if not isinstance(standings_list, list):
                        standings_list = [standings_list]
                    
                    for standing in standings_list:
                        round_num_actual = int(standing.get('round', round_num))
                        race_id = int(f"{year}{round_num_actual:02d}")
                        
                        driver_standings = standing.get('DriverStandings', [])
                        if not isinstance(driver_standings, list):
                            driver_standings = [driver_standings]
                        
                        for ds in driver_standings:
                            driver = ds.get('Driver', {})
                            all_driver_standings.append({
                                'race_id': race_id,
                                'driver_ref': driver.get('driverId', ''),
                                'points': float(ds.get('points', 0)),
                                'position': int(ds.get('position', 0)),
                                'position_text': ds.get('positionText', ''),
                                'wins': int(ds.get('wins', 0))
                            })
        
        df_const = pd.DataFrame(all_constructor_standings)
        df_driver = pd.DataFrame(all_driver_standings)
        
        df_const.to_csv(f'{self.output_path}constructor_standings.csv', index=False)
        df_driver.to_csv(f'{self.output_path}driver_standings.csv', index=False)
        
        print(f"Extracted {len(df_const)} constructor standings.")
        print(f"Extracted {len(df_driver)} driver standings.")
        return df_const, df_driver
    
    def extract_all(self, start_year: int = 2005, end_year: int = 2024):
        """Run the full extraction pipeline."""
        print("=" * 60)
        print("Starting F1 data extraction from the Ergast API")
        print("=" * 60)
        
        try:
            self.extract_circuits()
            self.extract_seasons()
            self.extract_constructors()
            self.extract_drivers()
            self.extract_races(start_year, end_year)
            self.extract_results(start_year, end_year)
            self.extract_qualifying(start_year, end_year)
            self.extract_pit_stops(max(2012, start_year), end_year)
            self.extract_standings(start_year, end_year)
            
            print("=" * 60)
            print("All data extraction steps completed successfully.")
            print(f"Raw data saved to: {self.output_path}")
            print("=" * 60)
            
        except Exception as e:
            print(f"Error during extraction: {e}")
            import traceback
            traceback.print_exc()
            raise

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract F1 data from Ergast API')
    parser.add_argument('--start-year', type=int, default=2005, help='Start year (default: 2005)')
    parser.add_argument('--end-year', type=int, default=2024, help='End year (default: 2024)')
    parser.add_argument('--output', type=str, default='data/raw/', help='Output directory (default: data/raw/)')
    
    args = parser.parse_args()
    
    extractor = F1DataExtractor(output_path=args.output)
    extractor.extract_all(start_year=args.start_year, end_year=args.end_year)

if __name__ == "__main__":
    main()
