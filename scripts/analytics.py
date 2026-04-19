from __future__ import annotations

import os
import re
import sys

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from constants import CONSTRUCTOR_ID, TEAM_REFS

_REF_RE = re.compile(r'^[a-z0-9_]+$')


def _refs_sql(refs: list[str]) -> str:
    for r in refs:
        if not _REF_RE.match(r):
            raise ValueError(f"Invalid team ref (must be lowercase alphanumeric/underscore): {r!r}")
    return ", ".join(f"'{r}'" for r in refs)


def teammate_delta(
    engine,
    constructor_id: int = CONSTRUCTOR_ID,
    min_shared_races: int = 5,
) -> pd.DataFrame:
    """
    Position delta for each driver vs their teammate in shared finished races.
    Returns: driver_a | driver_b | mean_delta | ci_lower | ci_upper | n | p_value
    Negative mean_delta = driver_a finishes ahead of driver_b on average.
    DNFs excluded from both sides to isolate pace, not reliability.
    """
    sql = """
    SELECT
        da.forename || ' ' || da.surname AS driver_a,
        db.forename || ' ' || db.surname AS driver_b,
        CAST(ra.position_order AS INTEGER) - CAST(rb.position_order AS INTEGER) AS delta
    FROM results ra
    JOIN results rb
        ON  ra.race_id        = rb.race_id
        AND ra.constructor_id = rb.constructor_id
        AND ra.driver_id      < rb.driver_id
    JOIN drivers da ON ra.driver_id = da.driver_id
    JOIN drivers db ON rb.driver_id = db.driver_id
    WHERE ra.constructor_id = :cid
      AND ra.position_order < 999
      AND rb.position_order < 999
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"cid": constructor_id})

    rows = []
    for (a, b), g in df.groupby(["driver_a", "driver_b"]):
        d = g["delta"].dropna().values.astype(float)
        n = len(d)
        if n < min_shared_races:
            continue
        mean = d.mean()
        _, p = stats.ttest_1samp(d, 0)
        ci = stats.t.interval(0.95, n - 1, loc=mean, scale=stats.sem(d))
        rows.append(dict(
            driver_a=a, driver_b=b,
            mean_delta=round(mean, 3),
            ci_lower=round(ci[0], 3),
            ci_upper=round(ci[1], 3),
            n=n, p_value=round(p, 6),
        ))

    return pd.DataFrame(rows).sort_values("mean_delta").reset_index(drop=True)


def qualifying_race_ols(
    engine,
    constructor_id: int = CONSTRUCTOR_ID,
) -> tuple[dict, pd.DataFrame]:
    """
    OLS regression of grid position on race finish position.
    Returns (stats_dict, scatter_df). DNFs excluded.
    stats_dict: slope | intercept | r2 | p_value | n
    """
    sql = """
    SELECT
        da.forename || ' ' || da.surname AS driver,
        CAST(r.grid          AS INTEGER) AS grid,
        CAST(r.position_order AS INTEGER) AS finish
    FROM results r
    JOIN drivers da ON r.driver_id = da.driver_id
    WHERE r.constructor_id = :cid
      AND r.grid           > 0
      AND r.position_order < 999
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"cid": constructor_id})

    slope, intercept, r, p, _ = stats.linregress(df["grid"], df["finish"])
    return dict(slope=slope, intercept=intercept, r2=r ** 2, p_value=p, n=len(df)), df


def pit_stop_efficiency(
    engine,
    team_refs: list[str] = TEAM_REFS,
    min_stops: int = 5,
) -> pd.DataFrame:
    """
    Z-score each stop against the season field distribution, return per-driver aggregates.
    Returns: driver | mean_z | std_z | n_stops  (sorted ascending by mean_z).
    Stops outside [15 s, 60 s] dropped — safety-car pits and data errors skew the baseline.
    """
    refs = _refs_sql(team_refs)  # from config, not user input
    sql = f"""
    WITH season_stats AS (
        SELECT
            ra.year,
            AVG(p.milliseconds) AS mu,
            SQRT(AVG(p.milliseconds * p.milliseconds)
                 - AVG(p.milliseconds) * AVG(p.milliseconds)) AS sigma
        FROM pit_stops p
        JOIN races ra ON p.race_id = ra.race_id
        WHERE p.milliseconds BETWEEN 15000 AND 60000
        GROUP BY ra.year
    )
    SELECT
        da.forename || ' ' || da.surname AS driver,
        (p.milliseconds - ss.mu) / NULLIF(ss.sigma, 0) AS z
    FROM pit_stops p
    JOIN races ra         ON p.race_id          = ra.race_id
    JOIN season_stats ss  ON ss.year             = ra.year
    JOIN results res      ON res.race_id         = p.race_id
                         AND res.driver_id       = p.driver_id
    JOIN constructors c   ON res.constructor_id  = c.constructor_id
    JOIN drivers da       ON p.driver_id         = da.driver_id
    WHERE c.constructor_ref IN ({refs})
      AND p.milliseconds BETWEEN 15000 AND 60000
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn).dropna(subset=["z"])

    agg = (
        df.groupby("driver")["z"]
        .agg(mean_z="mean", std_z="std", n_stops="count")
        .reset_index()
    )
    return agg[agg["n_stops"] >= min_stops].sort_values("mean_z").reset_index(drop=True)


def championship_trajectory(engine, team_refs: list[str] = TEAM_REFS) -> pd.DataFrame:
    """
    Cumulative championship points per driver per round, all seasons.
    Returns: year | round | driver | points | position
    """
    refs = _refs_sql(team_refs)
    sql = f"""
    SELECT
        ra.year, ra.round,
        da.forename || ' ' || da.surname AS driver,
        ds.points, ds.position
    FROM driver_standings ds
    JOIN races ra       ON ds.race_id          = ra.race_id
    JOIN drivers da     ON ds.driver_id        = da.driver_id
    JOIN results res    ON res.race_id         = ra.race_id
                       AND res.driver_id       = ds.driver_id
    JOIN constructors c ON res.constructor_id  = c.constructor_id
    WHERE c.constructor_ref IN ({refs})
    ORDER BY da.driver_id, ra.year, ra.round
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def dnf_rate_model(
    engine,
    team_refs: list[str] = TEAM_REFS,
    min_races: int = 10,
) -> pd.DataFrame:
    """
    Poisson MLE for DNF rate per driver with exact 95% confidence intervals.
    Returns: driver | races | dnfs | rate | ci_lower | ci_upper  (sorted desc by rate).
    CI uses chi-squared exact method: lower = χ²(0.025, 2k)/(2n), upper = χ²(0.975, 2k+2)/(2n).
    """
    refs = _refs_sql(team_refs)
    sql = f"""
    SELECT
        da.forename || ' ' || da.surname AS driver,
        COUNT(*) AS races,
        SUM(CASE WHEN r.position_order = 999 THEN 1 ELSE 0 END) AS dnfs
    FROM results r
    JOIN drivers da     ON r.driver_id        = da.driver_id
    JOIN constructors c ON r.constructor_id   = c.constructor_id
    WHERE c.constructor_ref IN ({refs})
    GROUP BY r.driver_id, da.forename, da.surname
    HAVING races >= {min_races}
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)

    df["rate"] = df["dnfs"] / df["races"]
    df["ci_lower"] = df.apply(
        lambda r: stats.chi2.ppf(0.025, 2 * r["dnfs"]) / (2 * r["races"])
        if r["dnfs"] > 0 else 0.0,
        axis=1,
    )
    df["ci_upper"] = df.apply(
        lambda r: stats.chi2.ppf(0.975, 2 * (r["dnfs"] + 1)) / (2 * r["races"]),
        axis=1,
    )
    return df.sort_values("rate", ascending=False).reset_index(drop=True)


def tyre_degradation(
    engine,
    team_refs: list[str] = TEAM_REFS,
    min_laps: int = 5,
) -> pd.DataFrame:
    """
    OLS degradation rate (seconds lost per additional lap on tyre) per driver per compound.
    Green-flag laps only (track_status='1'), tyre_life > 1.
    Returns: driver | compound | deg_rate_s | r2 | n
    Positive deg_rate_s = lap time grows with tyre age (expected).
    """
    refs = _refs_sql(team_refs)
    sql = f"""
    SELECT COALESCE(d.forename,'') || ' ' || COALESCE(d.surname,'') AS driver,
           l.compound, l.tyre_life, l.lap_time_s
    FROM laps l
    JOIN results      res ON l.race_id         = res.race_id
                         AND l.driver_id       = res.driver_id
    JOIN constructors c   ON res.constructor_id = c.constructor_id
    JOIN drivers      d   ON l.driver_id        = d.driver_id
    WHERE c.constructor_ref IN ({refs})
      AND l.compound     IN ('SOFT','MEDIUM','HARD')
      AND l.lap_time_s   IS NOT NULL
      AND l.tyre_life    > 1
      AND l.track_status = '1'
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    if df.empty:
        return pd.DataFrame(columns=["driver", "compound", "deg_rate_s", "r2", "n"])

    rows = []
    for (driver, compound), g in df.groupby(["driver", "compound"]):
        g = g.dropna(subset=["lap_time_s", "tyre_life"])
        if len(g) < min_laps:
            continue
        slope, _, r_val, _, _ = stats.linregress(g["tyre_life"], g["lap_time_s"])
        rows.append(dict(
            driver=driver, compound=compound,
            deg_rate_s=round(slope, 4), r2=round(r_val ** 2, 3), n=len(g),
        ))

    return pd.DataFrame(rows).sort_values(["compound", "deg_rate_s"]).reset_index(drop=True)


def sector_deltas(
    engine,
    constructor_id: int = CONSTRUCTOR_ID,
    min_laps: int = 10,
) -> pd.DataFrame:
    """
    Mean sector times per driver on green-flag laps.
    Returns: driver | s1_mean | s2_mean | s3_mean | n
    Sorted by combined sector time (fastest first).
    """
    sql = """
    SELECT COALESCE(d.forename,'') || ' ' || COALESCE(d.surname,'') AS driver,
           AVG(l.sector1_s) AS s1_mean,
           AVG(l.sector2_s) AS s2_mean,
           AVG(l.sector3_s) AS s3_mean,
           COUNT(*)          AS n
    FROM laps l
    JOIN results res ON l.race_id  = res.race_id AND l.driver_id = res.driver_id
    JOIN drivers d   ON l.driver_id = d.driver_id
    WHERE res.constructor_id = :cid
      AND l.track_status  = '1'
      AND l.lap_time_s   IS NOT NULL
      AND l.sector1_s    IS NOT NULL
      AND l.sector2_s    IS NOT NULL
      AND l.sector3_s    IS NOT NULL
    GROUP BY l.driver_id, d.forename, d.surname
    HAVING COUNT(*) >= :min_laps
    ORDER BY s1_mean + s2_mean + s3_mean
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"cid": constructor_id, "min_laps": min_laps})
