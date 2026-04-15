-- Oracle Red Bull Racing F1 Performance Analysis Queries
-- All queries accept :cid as a bind parameter for the constructor ID.
-- position_order = 999 is the sentinel for non-finishers (DNF/DSQ).

-- ============================================================
-- 0. DRIVER SUMMARY (Red Bull family: main team + junior team)
-- ============================================================

-- Career stats per driver across Red Bull Racing and AlphaTauri/RB (2020-2025)
SELECT
    d.forename || ' ' || d.surname AS driver,
    GROUP_CONCAT(DISTINCT con.constructor_name) AS team,
    COUNT(DISTINCT r.year) AS seasons,
    COUNT(*) AS races,
    SUM(res.points) AS points,
    COUNT(CASE WHEN res.position = 1  THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    ROUND(AVG(CASE WHEN res.position_order < 999 THEN res.position_order END), 1) AS avg_finish,
    COUNT(CASE WHEN res.position_order = 999 THEN 1 END) AS dnfs,
    MIN(r.year) AS from_yr,
    MAX(r.year) AS to_yr
FROM results res
JOIN races        r   ON res.race_id        = r.race_id
JOIN drivers      d   ON res.driver_id      = d.driver_id
JOIN constructors con ON res.constructor_id = con.constructor_id
WHERE con.constructor_ref IN ('red_bull', 'alphatauri', 'rb')
GROUP BY d.driver_id, d.forename, d.surname
ORDER BY points DESC;

-- ============================================================
-- 1. TEAM PERFORMANCE OVERVIEW
-- ============================================================

SELECT
    r.year AS season,
    SUM(res.points) AS total_points,
    COUNT(CASE WHEN res.position = 1  THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    COUNT(*) AS total_races,
    ROUND(AVG(CASE WHEN res.position_order < 999 THEN res.position_order END), 2) AS avg_finish_position,
    ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS win_percentage
FROM results res
JOIN races r ON res.race_id = r.race_id
WHERE res.constructor_id = :cid
GROUP BY r.year
ORDER BY r.year DESC;

-- ============================================================
-- 2. DRIVER PERFORMANCE COMPARISON
-- ============================================================

-- Head-to-head by season
SELECT
    r.year AS season,
    d.forename || ' ' || d.surname AS driver_name,
    COUNT(*) AS races,
    SUM(res.points) AS total_points,
    COUNT(CASE WHEN res.position = 1   THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3  THEN 1 END) AS podiums,
    COUNT(CASE WHEN res.position <= 10 THEN 1 END) AS points_finishes,
    ROUND(AVG(CASE WHEN res.position_order < 999 THEN res.position_order END), 2) AS avg_finish,
    COUNT(CASE WHEN res.position_order = 999 THEN 1 END) AS dnfs
FROM results res
JOIN races   r ON res.race_id  = r.race_id
JOIN drivers d ON res.driver_id = d.driver_id
WHERE res.constructor_id = :cid
GROUP BY r.year, d.driver_id, d.forename, d.surname
ORDER BY r.year DESC, total_points DESC;

-- Qualifying vs race performance
SELECT
    d.forename || ' ' || d.surname AS driver_name,
    r.year,
    COUNT(*) AS races,
    ROUND(AVG(q.position), 2) AS avg_qual_position,
    ROUND(AVG(CASE WHEN res.position_order < 999 THEN res.position_order END), 2) AS avg_race_position,
    ROUND(AVG(CASE WHEN res.position_order < 999
                   THEN CAST(res.grid AS INTEGER) - CAST(res.position_order AS INTEGER) END), 2) AS avg_positions_gained,
    COUNT(CASE WHEN res.position_order < res.grid AND res.position_order < 999 THEN 1 END) AS races_gained_positions
FROM results res
JOIN races      r ON res.race_id  = r.race_id
JOIN drivers    d ON res.driver_id = d.driver_id
LEFT JOIN qualifying q ON res.race_id = q.race_id AND res.driver_id = q.driver_id
WHERE res.constructor_id = :cid
GROUP BY d.driver_id, d.forename, d.surname, r.year
ORDER BY r.year DESC, avg_race_position;

-- ============================================================
-- 3. CIRCUIT PERFORMANCE ANALYSIS
-- ============================================================

-- Min 3 appearances
SELECT
    c.circuit_name,
    c.country,
    COUNT(*) AS races_held,
    COUNT(CASE WHEN res.position = 1  THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    ROUND(AVG(CASE WHEN res.position_order < 999 THEN res.position_order END), 2) AS avg_finish,
    SUM(res.points) AS total_points,
    ROUND(AVG(res.points), 2) AS avg_points_per_race,
    ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS win_rate
FROM results res
JOIN races    r ON res.race_id  = r.race_id
JOIN circuits c ON r.circuit_id = c.circuit_id
WHERE res.constructor_id = :cid
GROUP BY c.circuit_id, c.circuit_name, c.country
HAVING COUNT(*) >= 3
ORDER BY avg_points_per_race DESC
LIMIT 15;

-- ============================================================
-- 4. PIT STOP ANALYSIS
-- ============================================================

-- std_dev_ms: sqrt(E[x²] - E[x]²) — SQLite-compatible stddev
SELECT
    r.year AS season,
    d.forename || ' ' || d.surname AS driver_name,
    COUNT(*) AS total_stops,
    MIN(ps.milliseconds) AS fastest_stop_ms,
    ROUND(AVG(ps.milliseconds), 0) AS avg_stop_ms,
    MAX(ps.milliseconds) AS slowest_stop_ms,
    ROUND(SQRT(AVG(ps.milliseconds * ps.milliseconds) - AVG(ps.milliseconds) * AVG(ps.milliseconds)), 0) AS std_dev_ms
FROM pit_stops ps
JOIN races   r   ON ps.race_id   = r.race_id
JOIN drivers d   ON ps.driver_id = d.driver_id
JOIN results res ON ps.race_id   = res.race_id AND ps.driver_id = res.driver_id
WHERE res.constructor_id = :cid
GROUP BY r.year, d.driver_id, d.forename, d.surname
ORDER BY r.year DESC, avg_stop_ms;

-- Top 10 fastest stops
SELECT
    r.year,
    r.race_name,
    d.forename || ' ' || d.surname AS driver_name,
    ps.stop AS stop_number,
    ps.lap,
    ps.duration,
    ps.milliseconds
FROM pit_stops ps
JOIN races   r   ON ps.race_id   = r.race_id
JOIN drivers d   ON ps.driver_id = d.driver_id
JOIN results res ON ps.race_id   = res.race_id AND ps.driver_id = res.driver_id
WHERE res.constructor_id = :cid
ORDER BY ps.milliseconds
LIMIT 10;

-- ============================================================
-- 5. CHAMPIONSHIP PROGRESSION
-- ============================================================

SELECT
    cs.race_id,
    r.year,
    r.round,
    r.race_name,
    cs.position AS championship_position,
    cs.points AS points_accumulated,
    cs.wins AS wins_accumulated,
    LAG(cs.points) OVER (PARTITION BY r.year ORDER BY r.round) AS prev_points,
    cs.points - LAG(cs.points) OVER (PARTITION BY r.year ORDER BY r.round) AS points_gained
FROM constructor_standings cs
JOIN races r ON cs.race_id = r.race_id
WHERE cs.constructor_id = :cid
ORDER BY r.year DESC, r.round;

SELECT
    ds.race_id,
    r.year,
    r.round,
    r.race_name,
    d.forename || ' ' || d.surname AS driver_name,
    ds.position AS championship_position,
    ds.points AS points_accumulated,
    ds.wins AS wins_accumulated
FROM driver_standings ds
JOIN races   r   ON ds.race_id   = r.race_id
JOIN drivers d   ON ds.driver_id = d.driver_id
JOIN results res ON ds.race_id   = res.race_id AND ds.driver_id = res.driver_id
WHERE res.constructor_id = :cid
ORDER BY r.year DESC, r.round, ds.position;

-- ============================================================
-- 6. FASTEST LAPS ANALYSIS
-- ============================================================

SELECT
    r.year AS season,
    d.forename || ' ' || d.surname AS driver_name,
    COUNT(CASE WHEN res.fastest_lap_rank = 1 THEN 1 END) AS fastest_laps,
    COUNT(*) AS total_races,
    ROUND(COUNT(CASE WHEN res.fastest_lap_rank = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS fastest_lap_percentage
FROM results res
JOIN races   r ON res.race_id  = r.race_id
JOIN drivers d ON res.driver_id = d.driver_id
WHERE res.constructor_id = :cid
GROUP BY r.year, d.driver_id, d.forename, d.surname
HAVING COUNT(CASE WHEN res.fastest_lap_rank = 1 THEN 1 END) > 0
ORDER BY r.year DESC, fastest_laps DESC;

-- ============================================================
-- 7. RELIABILITY ANALYSIS
-- ============================================================

SELECT
    r.year AS season,
    COUNT(*) AS total_races,
    COUNT(CASE WHEN res.position_order = 999 THEN 1 END) AS dnfs,
    ROUND(COUNT(CASE WHEN res.position_order = 999 THEN 1 END) * 100.0 / COUNT(*), 2) AS dnf_percentage,
    COUNT(CASE WHEN res.position_order < 999 THEN 1 END) AS finishes,
    ROUND(COUNT(CASE WHEN res.position_order < 999 THEN 1 END) * 100.0 / COUNT(*), 2) AS finish_rate
FROM results res
JOIN races r ON res.race_id = r.race_id
WHERE res.constructor_id = :cid
GROUP BY r.year
ORDER BY r.year DESC;

-- Most common outcomes / failure modes
SELECT
    res.status,
    COUNT(*) AS occurrences,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM results WHERE constructor_id = :cid), 2) AS percentage
FROM results res
WHERE res.constructor_id = :cid
GROUP BY res.status
ORDER BY occurrences DESC
LIMIT 10;

-- ============================================================
-- 8. RACE START ANALYSIS
-- ============================================================

SELECT
    res.grid AS starting_position,
    COUNT(*) AS races_started,
    ROUND(AVG(CASE WHEN res.position_order < 999 THEN res.position_order END), 2) AS avg_finish_position,
    COUNT(CASE WHEN res.position = 1  THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    ROUND(AVG(CASE WHEN res.position_order < 999
                   THEN CAST(res.grid AS INTEGER) - CAST(res.position_order AS INTEGER) END), 2) AS avg_positions_gained
FROM results res
WHERE res.constructor_id = :cid
  AND res.grid > 0
  AND res.position IS NOT NULL
GROUP BY res.grid
ORDER BY res.grid;

-- ============================================================
-- 9. KEY PERFORMANCE INDICATORS
-- ============================================================

SELECT 'Total Races'             AS metric, COUNT(*) AS value                                                    FROM results WHERE constructor_id = :cid
UNION ALL
SELECT 'Total Wins',             COUNT(*)                                                                         FROM results WHERE constructor_id = :cid AND position = 1
UNION ALL
SELECT 'Total Podiums',          COUNT(*)                                                                         FROM results WHERE constructor_id = :cid AND position <= 3
UNION ALL
SELECT 'Total Points',           SUM(points)                                                                      FROM results WHERE constructor_id = :cid
UNION ALL
SELECT 'Avg Finish Position',    ROUND(AVG(CASE WHEN position_order < 999 THEN position_order END), 2)           FROM results WHERE constructor_id = :cid
UNION ALL
SELECT 'Win Rate %',             ROUND(COUNT(CASE WHEN position = 1 THEN 1 END) * 100.0 / COUNT(*), 2)          FROM results WHERE constructor_id = :cid
UNION ALL
SELECT 'DNF Rate %',             ROUND(COUNT(CASE WHEN position_order = 999 THEN 1 END) * 100.0 / COUNT(*), 2)  FROM results WHERE constructor_id = :cid;
