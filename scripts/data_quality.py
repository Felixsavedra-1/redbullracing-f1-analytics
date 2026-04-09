from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple
import os
import sys

from sqlalchemy import text

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from constants import DEFAULT_START_YEAR, DEFAULT_END_YEAR


def _as_round_set(skipped: Optional[Dict[str, List[int]]]) -> Set[Tuple[int, int]]:
    if not skipped:
        return set()
    rounds = set()
    for year, values in skipped.items():
        try:
            year_int = int(year)
        except (TypeError, ValueError):
            continue
        for round_num in values:
            try:
                rounds.add((year_int, int(round_num)))
            except (TypeError, ValueError):
                continue
    return rounds


def run_quality_checks(
    engine,
    start_year: int = DEFAULT_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
    skipped_rounds: Optional[Dict[str, Dict[str, List[int]]]] = None,
) -> List[Dict[str, str]]:
    """Run minimal quality checks and return failures."""
    checks = []

    def add_check(name: str, query: str, expected_zero: bool = True) -> None:
        checks.append({"name": name, "query": query, "expected_zero": expected_zero})

    add_check(
        "results_non_empty",
        "SELECT COUNT(*) AS value FROM results",
        expected_zero=False,
    )
    add_check(
        "drivers_non_empty",
        "SELECT COUNT(*) AS value FROM drivers",
        expected_zero=False,
    )
    add_check(
        "races_non_empty",
        "SELECT COUNT(*) AS value FROM races",
        expected_zero=False,
    )
    add_check(
        "races_outside_year_range",
        "SELECT COUNT(*) AS value FROM races WHERE year < :start_year OR year > :end_year",
    )

    add_check(
        "drivers_unique",
        "SELECT COUNT(*) - COUNT(DISTINCT driver_id) AS value FROM drivers",
    )
    add_check(
        "constructors_unique",
        "SELECT COUNT(*) - COUNT(DISTINCT constructor_id) AS value FROM constructors",
    )
    add_check(
        "circuits_unique",
        "SELECT COUNT(*) - COUNT(DISTINCT circuit_id) AS value FROM circuits",
    )
    add_check(
        "races_unique",
        "SELECT COUNT(*) - COUNT(DISTINCT race_id) AS value FROM races",
    )

    add_check(
        "results_race_fk",
        """
        SELECT COUNT(*) AS value
        FROM results r
        LEFT JOIN races ra ON r.race_id = ra.race_id
        WHERE ra.race_id IS NULL
        """,
    )
    add_check(
        "results_driver_fk",
        """
        SELECT COUNT(*) AS value
        FROM results r
        LEFT JOIN drivers d ON r.driver_id = d.driver_id
        WHERE d.driver_id IS NULL
        """,
    )
    add_check(
        "results_constructor_fk",
        """
        SELECT COUNT(*) AS value
        FROM results r
        LEFT JOIN constructors c ON r.constructor_id = c.constructor_id
        WHERE c.constructor_id IS NULL
        """,
    )
    add_check(
        "qualifying_race_fk",
        """
        SELECT COUNT(*) AS value
        FROM qualifying q
        LEFT JOIN races ra ON q.race_id = ra.race_id
        WHERE ra.race_id IS NULL
        """,
    )
    add_check(
        "pit_stops_race_fk",
        """
        SELECT COUNT(*) AS value
        FROM pit_stops p
        LEFT JOIN races ra ON p.race_id = ra.race_id
        WHERE ra.race_id IS NULL
        """,
    )

    add_check(
        "results_points_non_negative",
        "SELECT COUNT(*) AS value FROM results WHERE points < 0",
    )
    add_check(
        "results_laps_non_negative",
        "SELECT COUNT(*) AS value FROM results WHERE laps < 0",
    )
    add_check(
        "results_grid_non_negative",
        "SELECT COUNT(*) AS value FROM results WHERE grid < 0",
    )
    add_check(
        "results_position_order_non_negative",
        "SELECT COUNT(*) AS value FROM results WHERE position_order < 0",
    )

    failures: List[Dict[str, str]] = []
    with engine.connect() as conn:
        for check in checks:
            result = conn.execute(
                text(check["query"]),
                {"start_year": start_year, "end_year": end_year},
            ).fetchone()
            value = result[0] if result else 0
            if check["expected_zero"]:
                if value != 0:
                    failures.append({
                        "check": check["name"],
                        "value": str(value),
                        "expected": "0",
                    })
            else:
                if value == 0:
                    failures.append({
                        "check": check["name"],
                        "value": str(value),
                        "expected": "> 0",
                    })

        try:
            year_rows = conn.execute(
                text(
                    "SELECT DISTINCT year FROM races WHERE year BETWEEN :start_year AND :end_year"
                ),
                {"start_year": start_year, "end_year": end_year},
            ).fetchall()
            present_years = {row[0] for row in year_rows}
            expected_years = set(range(start_year, end_year + 1))
            missing_years = sorted(expected_years - present_years)
            if missing_years:
                failures.append(
                    {
                        "check": "missing_race_years",
                        "value": ", ".join(str(y) for y in missing_years),
                        "expected": f"All years {start_year}-{end_year}",
                    }
                )
        except Exception as exc:
            failures.append(
                {
                    "check": "missing_race_years",
                    "value": f"error: {exc}",
                    "expected": "query_success",
                }
            )

        try:
            missing_results = conn.execute(
                text(
                    """
                    SELECT ra.year, ra.round
                    FROM races ra
                    LEFT JOIN results r ON r.race_id = ra.race_id
                    WHERE r.race_id IS NULL AND ra.year BETWEEN :start_year AND :end_year
                    """
                ),
                {"start_year": start_year, "end_year": end_year},
            ).fetchall()
            skipped_results = _as_round_set((skipped_rounds or {}).get("results"))
            missing_results_filtered = [
                (row[0], row[1]) for row in missing_results if (row[0], row[1]) not in skipped_results
            ]
            value = len(missing_results_filtered)
            if value != 0:
                failures.append(
                    {
                        "check": "races_missing_results",
                        "value": str(value),
                        "expected": "0",
                    }
                )
        except Exception as exc:
            failures.append(
                {
                    "check": "races_missing_results",
                    "value": f"error: {exc}",
                    "expected": "query_success",
                }
            )

        try:
            missing_qualifying = conn.execute(
                text(
                    """
                    SELECT ra.year, ra.round
                    FROM races ra
                    LEFT JOIN qualifying q ON q.race_id = ra.race_id
                    WHERE q.race_id IS NULL AND ra.year BETWEEN :start_year AND :end_year
                    """
                ),
                {"start_year": start_year, "end_year": end_year},
            ).fetchall()
            skipped_qualifying = _as_round_set((skipped_rounds or {}).get("qualifying"))
            missing_qualifying_filtered = [
                (row[0], row[1]) for row in missing_qualifying if (row[0], row[1]) not in skipped_qualifying
            ]
            value = len(missing_qualifying_filtered)
            if value != 0:
                failures.append(
                    {
                        "check": "races_missing_qualifying",
                        "value": str(value),
                        "expected": "0",
                    }
                )
        except Exception as exc:
            failures.append(
                {
                    "check": "races_missing_qualifying",
                    "value": f"error: {exc}",
                    "expected": "query_success",
                }
            )

    return failures
