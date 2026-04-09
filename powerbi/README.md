# F1 Red Bull Analytics - Power BI Dashboard

This directory contains documentation and assets for the Power BI dashboard used to visualize the project's data.

## Dashboard Overview

The dashboard provides a comprehensive view of Oracle Red Bull Racing's performance, leveraging the data extracted and processed by the Python pipeline.

### Key Metrics
- **Win Rate**: Percentage of races won by the team.
- **Podium Percentage**: Frequency of top-3 finishes.
- **Total Points**: Cumulative constructor and driver points.
- **Average Pit Stop Time**: Efficiency of the pit crew.

### Pages

1.  **Executive Summary**
    *   High-level KPIs (Wins, Podiums, Points).
    *   Season-over-season comparison.
    *   Championship standing progression.

2.  **Driver Performance**
    *   Head-to-head comparison (e.g., Verstappen vs. Perez).
    *   Qualifying vs. Race result correlation.
    *   Points contribution per driver.

3.  **Technical Analysis**
    *   Pit stop time distribution.
    *   Fastest lap analysis by circuit.
    *   Reliability metrics (DNF reasons).

## Data Model

The dashboard connects to the project's SQL database (SQLite or MySQL) and uses a Star Schema:

- **Fact Tables**: `race_results`, `pit_stops`, `lap_times`
- **Dimension Tables**: `drivers`, `constructors`, `circuits`, `races`, `status`

## Setup

To view or edit the dashboard:

1.  Ensure the database is populated by running `python scripts/run_pipeline.py`.
2.  Open Power BI Desktop.
3.  Connect to the SQLite database `f1_analytics.db` in the repo root (or your MySQL instance).
4.  Refresh the data to load the latest race results.
