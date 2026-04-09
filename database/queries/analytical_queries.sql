-- Oracle Red Bull Racing F1 Performance Analysis Queries
-- These queries demonstrate data exploration and KPI calculations

-- ============================================================
-- 1. TEAM PERFORMANCE OVERVIEW
-- ============================================================

-- Total points and wins by season
SELECT 
    r.year AS season,
    SUM(res.points) AS total_points,
    COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    COUNT(*) AS total_races,
    ROUND(AVG(res.position_order), 2) AS avg_finish_position,
    ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS win_percentage
FROM results res
JOIN races r ON res.race_id = r.race_id
WHERE res.constructor_id = 9
GROUP BY r.year
ORDER BY r.year DESC;

-- ============================================================
-- 2. DRIVER PERFORMANCE COMPARISON
-- ============================================================

-- Head-to-head driver comparison by season
SELECT 
    r.year AS season,
    CONCAT(d.forename, ' ', d.surname) AS driver_name,
    COUNT(*) AS races,
    SUM(res.points) AS total_points,
    COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    COUNT(CASE WHEN res.position <= 10 THEN 1 END) AS points_finishes,
    ROUND(AVG(res.position_order), 2) AS avg_finish,
    COUNT(CASE WHEN res.position_order > 20 THEN 1 END) AS dnfs
FROM results res
JOIN races r ON res.race_id = r.race_id
JOIN drivers d ON res.driver_id = d.driver_id
WHERE res.constructor_id = 9
GROUP BY r.year, d.driver_id, d.forename, d.surname
ORDER BY r.year DESC, total_points DESC;

-- Qualifying vs Race Performance
SELECT 
    CONCAT(d.forename, ' ', d.surname) AS driver_name,
    r.year,
    COUNT(*) AS races,
    ROUND(AVG(q.position), 2) AS avg_qual_position,
    ROUND(AVG(res.position_order), 2) AS avg_race_position,
    ROUND(AVG(CAST(res.grid AS SIGNED) - CAST(res.position_order AS SIGNED)), 2) AS avg_positions_gained,
    COUNT(CASE WHEN res.position_order < res.grid THEN 1 END) AS races_gained_positions
FROM results res
JOIN races r ON res.race_id = r.race_id
JOIN drivers d ON res.driver_id = d.driver_id
LEFT JOIN qualifying q ON res.race_id = q.race_id AND res.driver_id = q.driver_id
WHERE res.constructor_id = 9
GROUP BY d.driver_id, d.forename, d.surname, r.year
ORDER BY r.year DESC, avg_race_position;

-- ============================================================
-- 3. CIRCUIT PERFORMANCE ANALYSIS
-- ============================================================

-- Best and worst circuits for Red Bull
SELECT 
    c.circuit_name,
    c.country,
    COUNT(*) AS races_held,
    COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    ROUND(AVG(res.position_order), 2) AS avg_finish,
    SUM(res.points) AS total_points,
    ROUND(AVG(res.points), 2) AS avg_points_per_race,
    ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS win_rate
FROM results res
JOIN races r ON res.race_id = r.race_id
JOIN circuits c ON r.circuit_id = c.circuit_id
WHERE res.constructor_id = 9
GROUP BY c.circuit_id, c.circuit_name, c.country
HAVING COUNT(*) >= 3
ORDER BY avg_points_per_race DESC
LIMIT 15;

-- ============================================================
-- 4. PIT STOP ANALYSIS
-- ============================================================

-- Pit stop efficiency by driver and season
SELECT 
    r.year AS season,
    CONCAT(d.forename, ' ', d.surname) AS driver_name,
    COUNT(*) AS total_stops,
    MIN(ps.milliseconds) AS fastest_stop_ms,
    ROUND(AVG(ps.milliseconds), 0) AS avg_stop_ms,
    MAX(ps.milliseconds) AS slowest_stop_ms,
    ROUND(STDDEV(ps.milliseconds), 0) AS std_dev_ms
FROM pit_stops ps
JOIN races r ON ps.race_id = r.race_id
JOIN drivers d ON ps.driver_id = d.driver_id
JOIN results res ON ps.race_id = res.race_id AND ps.driver_id = res.driver_id
WHERE res.constructor_id = 9
GROUP BY r.year, d.driver_id, d.forename, d.surname
ORDER BY r.year DESC, avg_stop_ms;

-- Top 10 fastest pit stops
SELECT 
    r.year,
    r.race_name,
    CONCAT(d.forename, ' ', d.surname) AS driver_name,
    ps.stop AS stop_number,
    ps.lap,
    ps.duration,
    ps.milliseconds
FROM pit_stops ps
JOIN races r ON ps.race_id = r.race_id
JOIN drivers d ON ps.driver_id = d.driver_id
JOIN results res ON ps.race_id = res.race_id AND ps.driver_id = res.driver_id
WHERE res.constructor_id = 9
ORDER BY ps.milliseconds
LIMIT 10;

-- ============================================================
-- 5. CHAMPIONSHIP PROGRESSION
-- ============================================================

-- Constructor championship position throughout each season
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
WHERE cs.constructor_id = 9
ORDER BY r.year DESC, r.round;

-- Driver championship battle
SELECT 
    ds.race_id,
    r.year,
    r.round,
    r.race_name,
    CONCAT(d.forename, ' ', d.surname) AS driver_name,
    ds.position AS championship_position,
    ds.points AS points_accumulated,
    ds.wins AS wins_accumulated
FROM driver_standings ds
JOIN races r ON ds.race_id = r.race_id
JOIN drivers d ON ds.driver_id = d.driver_id
JOIN results res ON ds.race_id = res.race_id AND ds.driver_id = res.driver_id
WHERE res.constructor_id = 9
ORDER BY r.year DESC, r.round, ds.position;

-- ============================================================
-- 6. FASTEST LAPS ANALYSIS
-- ============================================================

-- Fastest laps by driver and season
SELECT 
    r.year AS season,
    CONCAT(d.forename, ' ', d.surname) AS driver_name,
    COUNT(CASE WHEN res.fastest_lap_rank = 1 THEN 1 END) AS fastest_laps,
    COUNT(*) AS total_races,
    ROUND(COUNT(CASE WHEN res.fastest_lap_rank = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS fastest_lap_percentage
FROM results res
JOIN races r ON res.race_id = r.race_id
JOIN drivers d ON res.driver_id = d.driver_id
WHERE res.constructor_id = 9
GROUP BY r.year, d.driver_id, d.forename, d.surname
HAVING COUNT(CASE WHEN res.fastest_lap_rank = 1 THEN 1 END) > 0
ORDER BY r.year DESC, fastest_laps DESC;

-- ============================================================
-- 7. RELIABILITY ANALYSIS
-- ============================================================

-- DNF analysis by season
SELECT 
    r.year AS season,
    COUNT(*) AS total_races,
    COUNT(CASE WHEN res.position IS NULL THEN 1 END) AS dnfs,
    ROUND(COUNT(CASE WHEN res.position IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS dnf_percentage,
    COUNT(CASE WHEN res.position IS NOT NULL THEN 1 END) AS finishes,
    ROUND(COUNT(CASE WHEN res.position IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS finish_rate
FROM results res
JOIN races r ON res.race_id = r.race_id
WHERE res.constructor_id = 9
GROUP BY r.year
ORDER BY r.year DESC;

-- Most common issues
SELECT 
    res.status,
    COUNT(*) AS occurrences,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM results WHERE constructor_id = 9), 2) AS percentage
FROM results res
WHERE res.constructor_id = 9
GROUP BY res.status
ORDER BY occurrences DESC
LIMIT 10;

-- ============================================================
-- 8. RACE START ANALYSIS
-- ============================================================

-- Grid position vs finish position analysis
SELECT 
    res.grid AS starting_position,
    COUNT(*) AS races_started,
    ROUND(AVG(res.position_order), 2) AS avg_finish_position,
    COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
    COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS podiums,
    ROUND(AVG(CAST(res.grid AS SIGNED) - CAST(res.position_order AS SIGNED)), 2) AS avg_positions_gained
FROM results res
WHERE res.constructor_id = 9 
    AND res.grid > 0
    AND res.position IS NOT NULL
GROUP BY res.grid
ORDER BY res.grid;

-- ============================================================
-- 9. KEY PERFORMANCE INDICATORS
-- ============================================================

-- Overall KPIs for dashboard
SELECT 
    'Total Races' AS metric,
    COUNT(*) AS value
FROM results 
WHERE constructor_id = 9

UNION ALL

SELECT 
    'Total Wins',
    COUNT(*)
FROM results 
WHERE constructor_id = 9 AND position = 1

UNION ALL

SELECT 
    'Total Podiums',
    COUNT(*)
FROM results 
WHERE constructor_id = 9 AND position <= 3

UNION ALL

SELECT 
    'Total Points',
    SUM(points)
FROM results 
WHERE constructor_id = 9

UNION ALL

SELECT 
    'Average Finish Position',
    ROUND(AVG(position_order), 2)
FROM results 
WHERE constructor_id = 9 AND position IS NOT NULL

UNION ALL

SELECT 
    'Win Rate %',
    ROUND(COUNT(CASE WHEN position = 1 THEN 1 END) * 100.0 / COUNT(*), 2)
FROM results 
WHERE constructor_id = 9

UNION ALL

SELECT 
    'DNF Rate %',
    ROUND(COUNT(CASE WHEN position IS NULL THEN 1 END) * 100.0 / COUNT(*), 2)
FROM results 
WHERE constructor_id = 9;