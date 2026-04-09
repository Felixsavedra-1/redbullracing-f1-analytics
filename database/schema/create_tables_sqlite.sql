-- SQLite schema for Oracle Red Bull Racing F1 Performance Database

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS circuits (
    circuit_id INTEGER PRIMARY KEY,
    circuit_ref TEXT UNIQUE,
    circuit_name TEXT,
    location TEXT,
    country TEXT,
    lat REAL,
    lng REAL,
    altitude INTEGER,
    url TEXT
);

CREATE TABLE IF NOT EXISTS seasons (
    year INTEGER PRIMARY KEY,
    url TEXT
);

CREATE TABLE IF NOT EXISTS constructors (
    constructor_id INTEGER PRIMARY KEY,
    constructor_ref TEXT UNIQUE,
    constructor_name TEXT,
    nationality TEXT,
    url TEXT
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY,
    driver_ref TEXT UNIQUE,
    driver_number INTEGER,
    code TEXT,
    forename TEXT,
    surname TEXT,
    dob TEXT,
    nationality TEXT,
    url TEXT
);

CREATE TABLE IF NOT EXISTS races (
    race_id INTEGER PRIMARY KEY,
    year INTEGER,
    round INTEGER,
    circuit_id INTEGER,
    race_name TEXT,
    race_date TEXT,
    race_time TEXT,
    url TEXT,
    FOREIGN KEY (year) REFERENCES seasons(year),
    FOREIGN KEY (circuit_id) REFERENCES circuits(circuit_id)
);

CREATE TABLE IF NOT EXISTS qualifying (
    qualify_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER,
    driver_id INTEGER,
    constructor_id INTEGER,
    number INTEGER,
    position INTEGER,
    q1 TEXT,
    q2 TEXT,
    q3 TEXT,
    UNIQUE (race_id, driver_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
    FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

CREATE TABLE IF NOT EXISTS results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER,
    driver_id INTEGER,
    constructor_id INTEGER,
    number INTEGER,
    grid INTEGER,
    position INTEGER,
    position_text TEXT,
    position_order INTEGER,
    points REAL,
    laps INTEGER,
    time_result TEXT,
    milliseconds INTEGER,
    fastest_lap INTEGER,
    fastest_lap_rank INTEGER,
    fastest_lap_time TEXT,
    fastest_lap_speed TEXT,
    status_id INTEGER,
    status TEXT,
    UNIQUE (race_id, driver_id, constructor_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
    FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

CREATE TABLE IF NOT EXISTS pit_stops (
    pit_stop_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER,
    driver_id INTEGER,
    stop INTEGER,
    lap INTEGER,
    time_of_day TEXT,
    duration TEXT,
    milliseconds INTEGER,
    UNIQUE (race_id, driver_id, stop),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE TABLE IF NOT EXISTS constructor_standings (
    standing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER,
    constructor_id INTEGER,
    points REAL,
    position INTEGER,
    position_text TEXT,
    wins INTEGER,
    UNIQUE (race_id, constructor_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

CREATE TABLE IF NOT EXISTS driver_standings (
    standing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER,
    driver_id INTEGER,
    points REAL,
    position INTEGER,
    position_text TEXT,
    wins INTEGER,
    UNIQUE (race_id, driver_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE TABLE IF NOT EXISTS status (
    status_id INTEGER PRIMARY KEY,
    status TEXT
);

CREATE INDEX IF NOT EXISTS idx_races_year ON races(year);
CREATE INDEX IF NOT EXISTS idx_races_circuit ON races(circuit_id);
CREATE INDEX IF NOT EXISTS idx_results_race ON results(race_id);
CREATE INDEX IF NOT EXISTS idx_results_driver ON results(driver_id);
CREATE INDEX IF NOT EXISTS idx_results_constructor ON results(constructor_id);
CREATE INDEX IF NOT EXISTS idx_qualifying_race ON qualifying(race_id);
CREATE INDEX IF NOT EXISTS idx_pit_stops_race ON pit_stops(race_id);
CREATE INDEX IF NOT EXISTS idx_constructor_standings_race ON constructor_standings(race_id);
CREATE INDEX IF NOT EXISTS idx_driver_standings_race ON driver_standings(race_id);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT,
    ended_at TEXT,
    status TEXT,
    source_url TEXT,
    mode TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_run_tables (
    run_id TEXT,
    table_name TEXT,
    rows_loaded INTEGER,
    PRIMARY KEY (run_id, table_name)
);
