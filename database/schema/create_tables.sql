-- Oracle Red Bull Racing F1 Performance Database
-- Database Schema Setup

-- Create database (if needed)
CREATE DATABASE IF NOT EXISTS F1_RedBull_Analytics;
USE F1_RedBull_Analytics;

-- 1. CIRCUITS TABLE
CREATE TABLE circuits (
    circuit_id INT PRIMARY KEY,
    circuit_ref VARCHAR(50) UNIQUE,
    circuit_name VARCHAR(100),
    location VARCHAR(100),
    country VARCHAR(100),
    lat DECIMAL(10, 6),
    lng DECIMAL(10, 6),
    altitude INT,
    url VARCHAR(255)
);

-- 2. SEASONS TABLE
CREATE TABLE seasons (
    year INT PRIMARY KEY,
    url VARCHAR(255)
);

-- 3. CONSTRUCTORS TABLE
CREATE TABLE constructors (
    constructor_id INT PRIMARY KEY,
    constructor_ref VARCHAR(50) UNIQUE,
    constructor_name VARCHAR(100),
    nationality VARCHAR(50),
    url VARCHAR(255)
);

-- 4. DRIVERS TABLE
CREATE TABLE drivers (
    driver_id INT PRIMARY KEY,
    driver_ref VARCHAR(50) UNIQUE,
    driver_number INT,
    code VARCHAR(3),
    forename VARCHAR(50),
    surname VARCHAR(50),
    dob DATE,
    nationality VARCHAR(50),
    url VARCHAR(255)
);

-- 5. RACES TABLE
CREATE TABLE races (
    race_id INT PRIMARY KEY,
    year INT,
    round INT,
    circuit_id INT,
    race_name VARCHAR(100),
    race_date DATE,
    race_time TIME,
    url VARCHAR(255),
    FOREIGN KEY (year) REFERENCES seasons(year),
    FOREIGN KEY (circuit_id) REFERENCES circuits(circuit_id)
);

-- 6. QUALIFYING RESULTS TABLE
CREATE TABLE qualifying (
    qualify_id INT PRIMARY KEY AUTO_INCREMENT,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    constructor_id INT NOT NULL,
    number INT,
    position INT,
    q1 VARCHAR(20),
    q2 VARCHAR(20),
    q3 VARCHAR(20),
    UNIQUE KEY uq_qualifying_race_driver (race_id, driver_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
    FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

-- 7. RACE RESULTS TABLE
CREATE TABLE results (
    result_id INT PRIMARY KEY AUTO_INCREMENT,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    constructor_id INT NOT NULL,
    number INT,
    grid INT,
    position INT,
    position_text VARCHAR(10),
    position_order INT CHECK (position_order >= 0),
    points DECIMAL(5, 2) CHECK (points >= 0),
    laps INT CHECK (laps >= 0),
    time_result VARCHAR(50),
    milliseconds BIGINT,
    fastest_lap INT,
    fastest_lap_rank INT,
    fastest_lap_time VARCHAR(20),
    fastest_lap_speed VARCHAR(20),
    status VARCHAR(50),
    UNIQUE KEY uq_results_race_driver_constructor (race_id, driver_id, constructor_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
    FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

-- 8. PIT STOPS TABLE
CREATE TABLE pit_stops (
    pit_stop_id INT PRIMARY KEY AUTO_INCREMENT,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    stop INT,
    lap INT,
    time_of_day TIME,
    duration VARCHAR(20),
    milliseconds INT,
    UNIQUE KEY uq_pit_stops_race_driver_stop (race_id, driver_id, stop),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

-- 9. CONSTRUCTOR STANDINGS TABLE
CREATE TABLE constructor_standings (
    standing_id INT PRIMARY KEY AUTO_INCREMENT,
    race_id INT NOT NULL,
    constructor_id INT NOT NULL,
    points DECIMAL(6, 2),
    position INT,
    position_text VARCHAR(10),
    wins INT,
    UNIQUE KEY uq_constructor_standings_race_constructor (race_id, constructor_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

-- 10. DRIVER STANDINGS TABLE
CREATE TABLE driver_standings (
    standing_id INT PRIMARY KEY AUTO_INCREMENT,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    points DECIMAL(6, 2),
    position INT,
    position_text VARCHAR(10),
    wins INT,
    UNIQUE KEY uq_driver_standings_race_driver (race_id, driver_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

-- 11. LAPS TABLE (FastF1 lap-by-lap telemetry: sector times, tyre compound, stint data)
CREATE TABLE laps (
    race_id          INT NOT NULL,
    driver_id        INT NOT NULL,
    lap_number       INT NOT NULL,
    lap_time_s       DECIMAL(10, 6),
    sector1_s        DECIMAL(10, 6),
    sector2_s        DECIMAL(10, 6),
    sector3_s        DECIMAL(10, 6),
    compound         VARCHAR(20),
    tyre_life        INT,
    stint            INT,
    is_personal_best TINYINT DEFAULT 0,
    pit_in           TINYINT DEFAULT 0,
    pit_out          TINYINT DEFAULT 0,
    track_status     VARCHAR(10),
    PRIMARY KEY (race_id, driver_id, lap_number),
    FOREIGN KEY (race_id)   REFERENCES races(race_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

-- Pipeline metadata
CREATE TABLE pipeline_runs (
    run_id VARCHAR(36) PRIMARY KEY,
    started_at DATETIME,
    ended_at DATETIME,
    status VARCHAR(20),
    source_url VARCHAR(255),
    mode VARCHAR(20)
);

CREATE TABLE pipeline_run_tables (
    run_id VARCHAR(36),
    table_name VARCHAR(50),
    rows_loaded INT,
    PRIMARY KEY (run_id, table_name)
);

-- Create indexes for better query performance
CREATE INDEX idx_races_year ON races(year);
CREATE INDEX idx_races_circuit ON races(circuit_id);
CREATE INDEX idx_results_race ON results(race_id);
CREATE INDEX idx_results_driver ON results(driver_id);
CREATE INDEX idx_results_constructor ON results(constructor_id);
CREATE INDEX idx_qualifying_race ON qualifying(race_id);
CREATE INDEX idx_qualifying_driver ON qualifying(driver_id);
CREATE INDEX idx_pit_stops_race ON pit_stops(race_id);
CREATE INDEX idx_constructor_standings_race ON constructor_standings(race_id);
CREATE INDEX idx_driver_standings_race ON driver_standings(race_id);
CREATE INDEX idx_laps_race     ON laps(race_id);
CREATE INDEX idx_laps_driver   ON laps(driver_id);
CREATE INDEX idx_laps_compound ON laps(compound);

-- Insert Red Bull Racing constructor data
INSERT INTO constructors (constructor_id, constructor_ref, constructor_name, nationality, url)
VALUES (9, 'red_bull', 'Red Bull', 'Austrian', 'http://en.wikipedia.org/wiki/Red_Bull_Racing')
ON DUPLICATE KEY UPDATE constructor_name = constructor_name;

