import json
import os
import random
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from logging_utils import setup_logging
from constants import DEFAULT_START_YEAR, DEFAULT_END_YEAR

class F1DataExtractor:
    """Extract F1 data from Ergast-compatible API with rate-limit handling."""

    BASE_URL = "https://api.jolpi.ca/ergast/f1"

    def __init__(
        self,
        output_path: str = "data/raw/",
        base_delay: float = 1.5,
        max_retries: int = 6,
        max_backoff: float = 20.0,
        max_base_delay: float = 8.0,
        timeout: int = 30,
    ):
        self.output_path = output_path
        base_dir = os.path.dirname(os.path.normpath(output_path)) or "."
        self.cache_path = os.path.join(base_dir, "cache")
        self.base_delay = base_delay
        self.max_retries = max_retries
        self.max_backoff = max_backoff
        self.max_base_delay = max_base_delay
        self.timeout = timeout
        self.session = requests.Session()
        self._last_request_ts = 0.0
        self._consecutive_rate_limits = 0
        self.logger = setup_logging()
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(self.cache_path, exist_ok=True)

    def _backoff(self, attempt: int, retry_after: Optional[str]) -> None:
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = self.base_delay
        else:
            delay = min(self.base_delay * (2 ** attempt), self.max_backoff)
        if retry_after:
            self.base_delay = min(max(self.base_delay, delay), self.max_base_delay)
        else:
            self.base_delay = min(self.base_delay * 1.25, self.max_base_delay)
        jitter = random.uniform(0, min(0.25, self.base_delay))
        wait_for = delay + jitter
        self.logger.info("Backoff %.1fs (base_delay=%.2fs)", wait_for, self.base_delay)
        time.sleep(wait_for)
    
    def _rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.base_delay:
            time.sleep(self.base_delay - elapsed)
        self._last_request_ts = time.time()

    def _get_total(self, json_data: Optional[Dict]) -> int:
        if not json_data:
            return 0
        try:
            total = json_data.get("MRData", {}).get("total")
            return int(total) if total is not None else 0
        except (TypeError, ValueError):
            return 0

    def _parse_duration_ms(self, duration: str) -> Optional[int]:
        if not duration:
            return None
        value = duration.strip()
        try:
            return int(float(value) * 1000)
        except ValueError:
            pass

        try:
            parts = value.split(":")
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = float(parts[1])
                return int((minutes * 60 + seconds) * 1000)
        except ValueError:
            return None
        return None

    def _output_file_empty(self, filename: str) -> bool:
        path = os.path.join(self.output_path, filename)
        return not os.path.exists(path) or os.path.getsize(path) < 10

    def _output_has_rows(self, filename: str) -> bool:
        path = os.path.join(self.output_path, filename)
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r") as handle:
                handle.readline()
                return bool(handle.readline())
        except Exception:
            return False

    def _count_rows(self, filename: str) -> int:
        path = os.path.join(self.output_path, filename)
        if not os.path.exists(path):
            return 0
        try:
            with open(path, "r") as handle:
                count = -1
                for count, _ in enumerate(handle):
                    pass
                return max(count, 0)
        except Exception:
            return 0

    def _write_csv_atomic(self, df: pd.DataFrame, filename: str) -> None:
        path = os.path.join(self.output_path, filename)
        tmp_path = f"{path}.tmp"
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, path)
        if os.path.getsize(path) < 10:
            self.logger.warning("%s was written but appears empty.", filename)

    def _make_request(self, endpoint: str, limit: int = 1000, offset: int = 0) -> Optional[Dict]:
        """Issue a single API request with retry/backoff and rate-limit handling."""
        url = f"{self.BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"

        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    self.logger.info("Rate limited on %s; retrying...", endpoint)
                    self._consecutive_rate_limits += 1
                    if self._consecutive_rate_limits >= 3:
                        self.base_delay = min(self.base_delay * 1.5, self.max_base_delay)
                    self._backoff(attempt, retry_after)
                    continue
                if response.status_code in {400, 404}:
                    self.logger.warning("Invalid request for %s: %s. Skipping.", endpoint, response.status_code)
                    return None
                response.raise_for_status()
                self._consecutive_rate_limits = 0
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning("Error fetching %s: %s", endpoint, e)
                self._backoff(attempt, None)

        self.logger.error("Failed to fetch %s after %s retries.", endpoint, self.max_retries)
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
        self.logger.info("Extracting circuits...")
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
        self.logger.info("Extracted %s circuits.", len(df))
        return df
    
    def extract_seasons(self):
        """Extract seasons data."""
        self.logger.info("Extracting seasons...")
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
        self.logger.info("Extracted %s seasons.", len(df))
        return df
    
    def extract_constructors(self):
        """Extract constructors data."""
        self.logger.info("Extracting constructors...")
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
        self.logger.info("Extracted %s constructors.", len(df))
        return df
    
    def extract_drivers(self):
        """Extract drivers data."""
        self.logger.info("Extracting drivers...")
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
        self.logger.info("Extracted %s drivers.", len(df))
        return df
    
    def extract_races(self, start_year: int = DEFAULT_START_YEAR, end_year: int = DEFAULT_END_YEAR):
        """Extract race metadata for a range of years."""
        self.logger.info("Extracting races (%s-%s)...", start_year, end_year)
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
        self._write_csv_atomic(df, "races.csv")
        self.logger.info("Extracted %s races.", len(df))
        return df
    
    def extract_results(self, start_year: int = DEFAULT_START_YEAR, end_year: int = DEFAULT_END_YEAR):
        """Extract race results."""
        self.logger.info("Extracting results (%s-%s)...", start_year, end_year)
        all_results = []
        rounds_by_year = self._get_rounds_by_year(start_year, end_year)
        progress = self._load_progress("results_progress.json", start_year, end_year)
        if self._output_file_empty("results.csv") or not self._output_has_rows("results.csv"):
            self.logger.warning("results.csv is empty or missing; rebuilding results extraction state.")
            progress = {"years": {}, "skipped": {}}
        else:
            races_count = sum(len(rounds_by_year.get(y) or []) for y in range(start_year, end_year + 1))
            row_count = self._count_rows("results.csv")
            if races_count and row_count < races_count * 10:
                self.logger.warning(
                    "results.csv looks incomplete (%s rows for %s races). Rebuilding.",
                    row_count,
                    races_count,
                )
                progress = {"years": {}, "skipped": {}}

        for year in range(start_year, end_year + 1):
            rounds = rounds_by_year.get(year) or list(range(1, 25))
            progress_years = progress.get("years", {})
            progress_skipped = progress.get("skipped", {})
            done_rounds = set(progress_years.get(str(year), [])) | set(progress_skipped.get(str(year), []))
            total_rounds = len(rounds)
            for round_num in rounds:
                if round_num in done_rounds:
                    continue
                self.logger.info("Results %s R%s/%s", year, round_num, total_rounds)
                data = self._make_request(f"{year}/{round_num}/results", limit=1000, offset=0)
                if not data:
                    progress_skipped.setdefault(str(year), [])
                    progress_skipped[str(year)] = sorted(set(progress_skipped[str(year)] + [round_num]))
                    self._save_progress("results_progress.json", progress, start_year, end_year)
                    continue

                races = self._extract_table(data, "RaceTable")
                if not races:
                    progress_skipped.setdefault(str(year), [])
                    progress_skipped[str(year)] = sorted(set(progress_skipped[str(year)] + [round_num]))
                    self._save_progress("results_progress.json", progress, start_year, end_year)
                    continue

                for race in races:
                    race_info = race
                    round_num_actual = int(race_info.get("round", round_num))
                    race_id = int(f"{year}{round_num_actual:02d}")

                    results = race_info.get("Results", [])
                    if not isinstance(results, list):
                        results = [results]

                    for result in results:
                        driver = result.get("Driver", {})
                        constructor = result.get("Constructor", {})
                        fastest_lap = result.get("FastestLap", {})

                        position = result.get("position", "")
                        position_text = result.get("positionText", "")

                        all_results.append({
                            "race_id": race_id,
                            "driver_ref": driver.get("driverId", ""),
                            "constructor_ref": constructor.get("constructorId", ""),
                            "number": int(result.get("number", 0)) if result.get("number") else None,
                            "grid": int(result.get("grid", 0)) if result.get("grid") else None,
                            "position": int(position) if position.isdigit() else None,
                            "position_text": position_text,
                            "position_order": int(result.get("position", 999)) if position.isdigit() else 999,
                            "points": float(result.get("points", 0)),
                            "laps": int(result.get("laps", 0)) if result.get("laps") else None,
                            "time_result": result.get("Time", {}).get("time", "") if result.get("Time") else None,
                            "milliseconds": int(result.get("Time", {}).get("millis", 0)) if result.get("Time") and result.get("Time").get("millis") else None,
                            "fastest_lap": int(fastest_lap.get("lap", 0)) if fastest_lap.get("lap") else None,
                            "fastest_lap_rank": int(fastest_lap.get("rank", 0)) if fastest_lap.get("rank") else None,
                            "fastest_lap_time": fastest_lap.get("Time", {}).get("time", "") if fastest_lap.get("Time") else None,
                            "fastest_lap_speed": fastest_lap.get("AverageSpeed", {}).get("speed", "") if fastest_lap.get("AverageSpeed") else None,
                            "status": result.get("status", "Finished"),
                        })

                progress_years.setdefault(str(year), [])
                progress_years[str(year)] = sorted(set(progress_years[str(year)] + [round_num]))
                self._save_progress("results_progress.json", progress, start_year, end_year)
        
        df = pd.DataFrame(all_results)
        self._write_csv_atomic(df, "results.csv")
        self.logger.info("Extracted %s results.", len(df))
        return df
    
    def extract_qualifying(self, start_year: int = DEFAULT_START_YEAR, end_year: int = DEFAULT_END_YEAR):
        """Extract qualifying results."""
        self.logger.info("Extracting qualifying (%s-%s)...", start_year, end_year)
        all_qualifying = []

        rounds_by_year = self._get_rounds_by_year(start_year, end_year)
        races_count = sum(len(rounds_by_year.get(y) or []) for y in range(start_year, end_year + 1))

        for year in range(start_year, end_year + 1):
            total_rounds = len(rounds_by_year.get(year) or [])
            self.logger.info("Qualifying %s (%s rounds)", year, total_rounds or "unknown")
            progress = self._load_progress("qualifying_progress.json", start_year, end_year)
            if self._output_file_empty("qualifying.csv") or not self._output_has_rows("qualifying.csv"):
                self.logger.warning("qualifying.csv is empty or missing; rebuilding qualifying extraction state.")
                progress = {"years": {}, "skipped": {}}
            elif races_count:
                row_count = self._count_rows("qualifying.csv")
                if row_count < races_count * 10:
                    self.logger.warning(
                        "qualifying.csv looks incomplete (%s rows for %s races). Rebuilding.",
                        row_count,
                        races_count,
                    )
                    progress = {"years": {}, "skipped": {}}

            rounds = rounds_by_year.get(year) or list(range(1, 25))
            progress_years = progress.get("years", {})
            progress_skipped = progress.get("skipped", {})
            done_rounds = set(progress_years.get(str(year), [])) | set(progress_skipped.get(str(year), []))
            for round_num in rounds:
                if round_num in done_rounds:
                    continue
                self.logger.info("Qualifying %s R%s/%s", year, round_num, total_rounds)
                data = self._make_request(f"{year}/{round_num}/qualifying", limit=1000, offset=0)
                if not data:
                    progress_skipped.setdefault(str(year), [])
                    progress_skipped[str(year)] = sorted(set(progress_skipped[str(year)] + [round_num]))
                    self._save_progress("qualifying_progress.json", progress, start_year, end_year)
                    continue

                races = self._extract_table(data, "RaceTable")
                if not races:
                    progress_skipped.setdefault(str(year), [])
                    progress_skipped[str(year)] = sorted(set(progress_skipped[str(year)] + [round_num]))
                    self._save_progress("qualifying_progress.json", progress, start_year, end_year)
                    continue

                for race in races:
                    round_num_actual = int(race.get("round", round_num))
                    race_id = int(f"{year}{round_num_actual:02d}")

                    qualifying_results = race.get("QualifyingResults", [])
                    if not isinstance(qualifying_results, list):
                        qualifying_results = [qualifying_results]

                    for qualifying in qualifying_results:
                        driver = qualifying.get("Driver", {})
                        constructor = qualifying.get("Constructor", {})

                        all_qualifying.append(
                            {
                                "race_id": race_id,
                                "driver_ref": driver.get("driverId", ""),
                                "constructor_ref": constructor.get("constructorId", ""),
                                "number": int(qualifying.get("number", 0)) if qualifying.get("number") else None,
                                "position": int(qualifying.get("position", 0)) if qualifying.get("position") else None,
                                "q1": qualifying.get("Q1", ""),
                                "q2": qualifying.get("Q2", ""),
                                "q3": qualifying.get("Q3", ""),
                            }
                        )

                progress_years.setdefault(str(year), [])
                progress_years[str(year)] = sorted(set(progress_years[str(year)] + [round_num]))
                self._save_progress("qualifying_progress.json", progress, start_year, end_year)

        qualifying_columns = [
            "race_id",
            "driver_ref",
            "constructor_ref",
            "number",
            "position",
            "q1",
            "q2",
            "q3",
        ]
        df = pd.DataFrame(all_qualifying, columns=qualifying_columns)
        self._write_csv_atomic(df, "qualifying.csv")
        self.logger.info("Extracted %s qualifying results.", len(df))
        return df
    
    def _normalize_progress(
        self, data: Dict, start_year: int, end_year: int
    ) -> Dict[str, Dict[str, List[int]]]:
        if isinstance(data, dict) and isinstance(data.get("years"), dict):
            data_years = data.get("years", {})
        elif isinstance(data, dict):
            data_years = data
        else:
            data_years = {}

        data_skipped = {}
        if isinstance(data, dict) and isinstance(data.get("skipped"), dict):
            data_skipped = data.get("skipped", {})

        normalized_years: Dict[str, List[int]] = {}
        normalized_skipped: Dict[str, List[int]] = {}
        for year in range(start_year, end_year + 1):
            year_key = str(year)
            rounds = data_years.get(year_key, [])
            cleaned: List[int] = []
            if isinstance(rounds, list):
                for value in rounds:
                    if isinstance(value, int):
                        cleaned.append(value)
                    elif isinstance(value, str) and value.isdigit():
                        cleaned.append(int(value))
            normalized_years[year_key] = sorted(set(cleaned))

            skipped_rounds = data_skipped.get(year_key, [])
            cleaned_skipped: List[int] = []
            if isinstance(skipped_rounds, list):
                for value in skipped_rounds:
                    if isinstance(value, int):
                        cleaned_skipped.append(value)
                    elif isinstance(value, str) and value.isdigit():
                        cleaned_skipped.append(int(value))
            normalized_skipped[year_key] = sorted(set(cleaned_skipped))

        return {"years": normalized_years, "skipped": normalized_skipped}

    def _progress_path(self, filename: str) -> str:
        return os.path.join(self.cache_path, filename)

    def _legacy_progress_path(self, filename: str) -> str:
        return os.path.join(self.output_path, filename)

    def _load_progress(self, filename: str, start_year: int, end_year: int) -> Dict[str, Dict[str, List[int]]]:
        path = self._progress_path(filename)
        legacy_path = self._legacy_progress_path(filename)
        try_paths = [path, legacy_path]
        for candidate in try_paths:
            if not os.path.exists(candidate):
                continue
            try:
                with open(candidate, "r") as handle:
                    data = json.load(handle)
                normalized = self._normalize_progress(data, start_year, end_year)
                if candidate == legacy_path and not os.path.exists(path):
                    self._save_progress(filename, normalized, start_year, end_year)
                return normalized
            except Exception:
                return {"years": {}, "skipped": {}}
        return {"years": {}, "skipped": {}}

    def _save_progress(
        self,
        filename: str,
        data: Dict[str, Dict[str, List[int]]],
        start_year: int,
        end_year: int,
    ) -> None:
        path = self._progress_path(filename)
        normalized = self._normalize_progress(data, start_year, end_year)
        payload = {
            "version": 1,
            "updated_at": datetime.now().strftime("%Y-%m-%d"),
            "years": normalized.get("years", {}),
            "skipped": normalized.get("skipped", {}),
        }
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)

    def extract_pit_stops(self, start_year: int = DEFAULT_START_YEAR, end_year: int = DEFAULT_END_YEAR):
        """Extract pit stop data (available from 2012 onward)."""
        self.logger.info("Extracting pit stops (%s-%s)...", start_year, end_year)
        all_pit_stops = []

        rounds_by_year = self._get_rounds_by_year(start_year, end_year)
        progress = self._load_progress("pit_stops_progress.json", start_year, end_year)
        if self._output_file_empty("pit_stops.csv") or not self._output_has_rows("pit_stops.csv"):
            self.logger.warning("pit_stops.csv is empty or missing; rebuilding pit stop extraction state.")
            progress = {"years": {}, "skipped": {}}

        for year in range(start_year, end_year + 1):
            rounds = rounds_by_year.get(year) or list(range(1, 25))
            progress_years = progress.get("years", {})
            progress_skipped = progress.get("skipped", {})
            done_rounds = set(progress_years.get(str(year), [])) | set(progress_skipped.get(str(year), []))
            total_rounds = len(rounds)
            for round_num in rounds:
                if round_num in done_rounds:
                    continue
                self.logger.info("Pit stops %s R%s/%s", year, round_num, total_rounds)
                offset = 0
                limit = 1000
                saw_data = False
                while True:
                    data = self._make_request(f"{year}/{round_num}/pitstops", limit=limit, offset=offset)
                    if not data:
                        break

                    races = self._extract_table(data, "RaceTable")
                    if not races:
                        break

                    saw_data = True
                    for race in races:
                        race_info = race
                        round_num_actual = int(race_info.get("round", round_num))
                        race_id = int(f"{year}{round_num_actual:02d}")

                        pit_stops = race_info.get("PitStops", [])
                        if not isinstance(pit_stops, list):
                            pit_stops = [pit_stops]

                        for pit_stop in pit_stops:
                            driver = pit_stop.get("Driver", {})
                            time_of_day = pit_stop.get("time", "")
                            duration = pit_stop.get("duration", "")

                            all_pit_stops.append(
                                {
                                    "race_id": race_id,
                                    "driver_ref": driver.get("driverId", ""),
                                    "stop": int(pit_stop.get("stop", 0)),
                                    "lap": int(pit_stop.get("lap", 0)),
                                    "time_of_day": time_of_day,
                                    "duration": duration,
                                    "milliseconds": self._parse_duration_ms(duration),
                                }
                            )

                    total = self._get_total(data)
                    offset += limit
                    if total == 0 or offset >= total:
                        break

                if saw_data:
                    progress_years.setdefault(str(year), [])
                    progress_years[str(year)] = sorted(set(progress_years[str(year)] + [round_num]))
                else:
                    progress_skipped.setdefault(str(year), [])
                    progress_skipped[str(year)] = sorted(set(progress_skipped[str(year)] + [round_num]))
                self._save_progress("pit_stops_progress.json", progress, start_year, end_year)
        
        pit_stop_columns = [
            "race_id",
            "driver_ref",
            "stop",
            "lap",
            "time_of_day",
            "duration",
            "milliseconds",
        ]
        df = pd.DataFrame(all_pit_stops, columns=pit_stop_columns)
        self._write_csv_atomic(df, "pit_stops.csv")
        self.logger.info("Extracted %s pit stops.", len(df))
        return df
    
    def _get_rounds_by_year(self, start_year: int, end_year: int) -> Dict[int, List[int]]:
        races_path = os.path.join(self.output_path, "races.csv")
        if not os.path.exists(races_path):
            return {}
        try:
            races_df = pd.read_csv(races_path)
            rounds_by_year: Dict[int, List[int]] = {}
            for year in range(start_year, end_year + 1):
                year_rounds = races_df[races_df["year"] == year]["round"].dropna().astype(int).tolist()
                rounds_by_year[year] = sorted(set(year_rounds))
            return rounds_by_year
        except Exception:
            return {}

    def extract_standings(self, start_year: int = DEFAULT_START_YEAR, end_year: int = DEFAULT_END_YEAR):
        """Extract constructor and driver standings."""
        self.logger.info("Extracting standings (%s-%s)...", start_year, end_year)
        all_constructor_standings = []
        all_driver_standings = []

        for year in range(start_year, end_year + 1):
            limit = 1000
            offset = 0
            while True:
                data = self._make_request(f"{year}/constructorStandings", limit=limit, offset=offset)
                if not data:
                    break

                standings_lists = self._extract_table(data, "StandingsTable")
                if not standings_lists:
                    break

                for standings_list in standings_lists:
                    round_num_actual = int(standings_list.get("round", 0) or 0)
                    if round_num_actual == 0:
                        continue
                    race_id = int(f"{year}{round_num_actual:02d}")

                    constructor_standings = standings_list.get("ConstructorStandings", [])
                    if not isinstance(constructor_standings, list):
                        constructor_standings = [constructor_standings]

                    for cs in constructor_standings:
                        constructor = cs.get("Constructor", {})
                        all_constructor_standings.append({
                            "race_id": race_id,
                            "constructor_ref": constructor.get("constructorId", ""),
                            "points": float(cs.get("points", 0)),
                            "position": int(cs.get("position", 0)),
                            "position_text": cs.get("positionText", ""),
                            "wins": int(cs.get("wins", 0)),
                        })

                if len(standings_lists) < limit:
                    break
                offset += limit

            offset = 0
            while True:
                data = self._make_request(f"{year}/driverStandings", limit=limit, offset=offset)
                if not data:
                    break

                standings_lists = self._extract_table(data, "StandingsTable")
                if not standings_lists:
                    break

                for standings_list in standings_lists:
                    round_num_actual = int(standings_list.get("round", 0) or 0)
                    if round_num_actual == 0:
                        continue
                    race_id = int(f"{year}{round_num_actual:02d}")

                    driver_standings = standings_list.get("DriverStandings", [])
                    if not isinstance(driver_standings, list):
                        driver_standings = [driver_standings]

                    for ds in driver_standings:
                        driver = ds.get("Driver", {})
                        all_driver_standings.append({
                            "race_id": race_id,
                            "driver_ref": driver.get("driverId", ""),
                            "points": float(ds.get("points", 0)),
                            "position": int(ds.get("position", 0)),
                            "position_text": ds.get("positionText", ""),
                            "wins": int(ds.get("wins", 0)),
                        })

                if len(standings_lists) < limit:
                    break
                offset += limit
        
        df_const = pd.DataFrame(all_constructor_standings)
        df_driver = pd.DataFrame(all_driver_standings)
        
        self._write_csv_atomic(df_const, "constructor_standings.csv")
        self._write_csv_atomic(df_driver, "driver_standings.csv")
        
        self.logger.info("Extracted %s constructor standings.", len(df_const))
        self.logger.info("Extracted %s driver standings.", len(df_driver))
        return df_const, df_driver
    
    def extract_all(
        self,
        start_year: int = DEFAULT_START_YEAR,
        end_year: int = DEFAULT_END_YEAR,
        skip_pit_stops: bool = False,
    ):
        """Run the full extraction pipeline."""
        start_year = max(DEFAULT_START_YEAR, start_year)
        end_year = min(DEFAULT_END_YEAR, end_year)
        if start_year > end_year:
            raise ValueError(f"Invalid year range after clamping to {DEFAULT_START_YEAR}-{DEFAULT_END_YEAR}.")
        self.logger.info("Starting F1 data extraction from the Ergast API")
        
        try:
            self.extract_circuits()
            self.extract_seasons()
            self.extract_constructors()
            self.extract_drivers()
            self.extract_races(start_year, end_year)
            self.extract_results(start_year, end_year)
            self.extract_qualifying(start_year, end_year)
            if skip_pit_stops:
                self.logger.info("Skipping pit stop extraction (--skip-pit-stops flag).")
            else:
                self.extract_pit_stops(start_year, end_year)
            self.extract_standings(start_year, end_year)
            
            self.logger.info("All data extraction steps completed successfully.")
            self.logger.info("Raw data saved to: %s", self.output_path)
            
        except Exception as e:
            self.logger.error("Error during extraction: %s", e)
            import traceback
            traceback.print_exc()
            raise

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract F1 data from Ergast API')
    parser.add_argument(
        '--start-year',
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"Start year (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=DEFAULT_END_YEAR,
        help=f"End year (default: {DEFAULT_END_YEAR})",
    )
    parser.add_argument('--output', type=str, default='data/raw/', help='Output directory (default: data/raw/)')
    parser.add_argument('--base-delay', type=float, default=0.75, help='Delay between API requests in seconds')
    parser.add_argument('--max-retries', type=int, default=6, help='Max retries on API errors or rate limits')
    
    args = parser.parse_args()
    
    extractor = F1DataExtractor(
        output_path=args.output,
        base_delay=args.base_delay,
        max_retries=args.max_retries,
    )
    extractor.extract_all(start_year=args.start_year, end_year=args.end_year)

if __name__ == "__main__":
    main()
