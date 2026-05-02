from __future__ import annotations

import logging
import os
import sys

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from sqlalchemy.engine import Engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from analytics import _ref_params, championship_trajectory

logger = logging.getLogger("f1_analytics")

_BG   = "#000000"
_FONT = "Courier New, monospace"
_GRID    = "#1e1e1e"
_TICK    = "#888888"
_RED     = "#E8002D"

_DRIVER_COLORS = [
    "#3399FF", "#FF6B00", "#00DDFF", "#FFD700",
    "#CC44FF", "#00FF88", "#FF44AA", "#FFFFFF",
]


# --------------------------------------------------------------------------- #
#  HTML template                                                                #
# --------------------------------------------------------------------------- #

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PLACEHOLDER_TITLE</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#000;color:#fff;font-family:'Courier New',monospace}
header{padding:40px 32px 32px;border-bottom:3px solid #E8002D}
h1{font-size:1.5rem;font-weight:700;text-transform:uppercase;letter-spacing:.2em}
.sub{color:#666;font-size:.7rem;letter-spacing:.15em;margin-top:8px;text-transform:uppercase}
.charts{padding:32px;display:grid;grid-template-columns:1fr;gap:32px}
.chart-row{display:grid;grid-template-columns:1fr 1fr;gap:32px}
.chart-section{border-top:1px solid #E8002D;padding-top:16px}
@media(max-width:860px){.chart-row{grid-template-columns:1fr}}
</style>
</head>
<body>
<header>
  <h1>PLACEHOLDER_HEADING</h1>
  <p class="sub">PLACEHOLDER_SUBTITLE</p>
</header>
<div class="charts">
  <div class="chart-section">PLACEHOLDER_C1</div>
  <div class="chart-row">
    <div class="chart-section">PLACEHOLDER_C2</div>
    <div class="chart-section">PLACEHOLDER_C3</div>
  </div>
</div>
</body>
</html>
"""

# --------------------------------------------------------------------------- #
#  2D chart helpers                                                             #
# --------------------------------------------------------------------------- #

def _axis_2d(title: str = "") -> dict:
    return dict(
        title=dict(text=title, font=dict(color="#888888", family=_FONT, size=10)),
        gridcolor=_GRID,
        zerolinecolor=_GRID,
        tickfont=dict(color=_TICK, family=_FONT, size=10),
        linecolor=_RED,
        linewidth=2,
        showline=True,
        showgrid=True,
        mirror=False,
    )


def _layout_2d(title: str, xaxis_title: str = "", yaxis_title: str = "",
               height: int = 420, hovermode: str = "x unified") -> go.Layout:
    return go.Layout(
        title=dict(
            text=title,
            font=dict(color="#ffffff", family=_FONT, size=14),
            x=0,
            xanchor="left",
            pad=dict(t=4, l=0),
        ),
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        xaxis=_axis_2d(xaxis_title),
        yaxis=_axis_2d(yaxis_title),
        font=dict(family=_FONT, color="#ffffff"),
        margin=dict(l=52, r=16, t=56, b=48),
        height=height,
        showlegend=True,
        legend=dict(
            font=dict(family=_FONT, color="#888888", size=10),
            bgcolor="rgba(0,0,0,0)",
            bordercolor=_RED,
            borderwidth=1,
        ),
        hovermode=hovermode,
        hoverlabel=dict(
            bgcolor="#000000",
            bordercolor=_RED,
            font=dict(family=_FONT, size=11, color="#ffffff"),
            namelength=-1,
        ),
    )


# --------------------------------------------------------------------------- #
#  SQL helpers                                                                  #
# --------------------------------------------------------------------------- #

def _grid_finish_df(engine: Engine, team_refs: list[str]) -> pd.DataFrame:
    placeholders, params = _ref_params(team_refs)
    sql = (
        "SELECT COALESCE(da.forename,'') || ' ' || COALESCE(da.surname,'') AS driver,"
        " ra.year, CAST(r.grid AS INTEGER) AS grid,"
        " CAST(r.position_order AS INTEGER) AS finish"
        " FROM results r"
        " JOIN constructors c ON r.constructor_id = c.constructor_id"
        " JOIN drivers da     ON r.driver_id       = da.driver_id"
        " JOIN races ra       ON r.race_id          = ra.race_id"
        f" WHERE c.constructor_ref IN ({placeholders})"
        "   AND r.grid > 0 AND r.position_order < 999"
    )
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# --------------------------------------------------------------------------- #
#  2D chart builders                                                            #
# --------------------------------------------------------------------------- #

def chart_championship_2d(traj_df: pd.DataFrame) -> go.Figure:
    """Championship points per round — latest season, one line per driver."""
    if traj_df.empty:
        return go.Figure(layout=_layout_2d(
            "CHAMPIONSHIP TRAJECTORY", xaxis_title="ROUND", yaxis_title="POINTS", height=440,
        ))

    latest = int(traj_df["year"].max())
    df = traj_df[traj_df["year"] == latest].sort_values("round")
    layout = _layout_2d(
        f"CHAMPIONSHIP TRAJECTORY · {latest}",
        xaxis_title="ROUND",
        yaxis_title="POINTS",
        height=440,
    )
    fig = go.Figure(layout=layout)

    for i, (driver, g) in enumerate(df.groupby("driver")):
        color = _DRIVER_COLORS[i % len(_DRIVER_COLORS)]
        fig.add_trace(go.Scatter(
            x=g["round"], y=g["points"],
            mode="lines+markers",
            name=driver.split()[-1],
            line=dict(color=color, width=2.5),
            marker=dict(size=5, color=color, line=dict(color="#000000", width=1)),
            hovertemplate="<b>%{fullData.name}</b>  %{y} pts<extra></extra>",
        ))
    return fig


def chart_race_positions_2d(traj_df: pd.DataFrame) -> go.Figure:
    """Race finish positions per round — latest season, one trace per driver.
    Y axis is inverted so P1 sits at the top."""
    if traj_df.empty:
        layout = _layout_2d("RACE POSITIONS", xaxis_title="ROUND", yaxis_title="FINISH", height=400)
        layout.yaxis.update(autorange="reversed", dtick=5)
        return go.Figure(layout=layout)

    latest = int(traj_df["year"].max())
    df = traj_df[traj_df["year"] == latest].sort_values("round")
    layout = _layout_2d(
        f"RACE POSITIONS · {latest}",
        xaxis_title="ROUND",
        yaxis_title="FINISH",
        height=400,
    )
    layout.yaxis.update(autorange="reversed", dtick=5)
    fig = go.Figure(layout=layout)

    for i, (driver, g) in enumerate(df.groupby("driver")):
        color = _DRIVER_COLORS[i % len(_DRIVER_COLORS)]
        fig.add_trace(go.Scatter(
            x=g["round"], y=g["position"],
            mode="lines+markers",
            name=driver.split()[-1],
            line=dict(color=color, width=1.8),
            marker=dict(size=6, color=color, line=dict(color="#000000", width=1)),
            hovertemplate="<b>%{fullData.name}</b>  P%{y}<extra></extra>",
        ))
    return fig


def chart_grid_finish_2d(df: pd.DataFrame) -> go.Figure:
    """Grid position vs finish position scatter — all seasons combined.
    Points below the diagonal gained positions; above lost them."""
    layout = _layout_2d(
        "GRID vs FINISH · ALL SEASONS",
        xaxis_title="GRID",
        yaxis_title="FINISH",
        height=400,
        hovermode="closest",
    )
    fig = go.Figure(layout=layout)
    if df.empty:
        return fig

    # Diagonal reference line (grid = finish, no change)
    mx = max(df["grid"].max(), df["finish"].max()) + 1
    fig.add_trace(go.Scatter(
        x=[1, mx], y=[1, mx],
        mode="lines",
        name="no change",
        line=dict(color=_RED, width=1, dash="dot"),
        showlegend=False,
        hoverinfo="skip",
    ))

    for i, (driver, g) in enumerate(df.groupby("driver")):
        color = _DRIVER_COLORS[i % len(_DRIVER_COLORS)]
        delta = g["grid"] - g["finish"]   # positive = gained positions
        fig.add_trace(go.Scatter(
            x=g["grid"], y=g["finish"],
            mode="markers",
            name=driver.split()[-1],
            customdata=list(zip(g["year"], delta)),
            marker=dict(
                size=6, color=color, opacity=0.80,
                line=dict(color="#000000", width=0.5),
            ),
            hovertemplate=(
                "<b>%{fullData.name}</b>  %{customdata[0]}<br>"
                "Grid P%{x} → Finish P%{y}<br>"
                "%{customdata[1]:+d} positions"
                "<extra></extra>"
            ),
        ))
    return fig


# --------------------------------------------------------------------------- #
#  Dashboard generator                                                          #
# --------------------------------------------------------------------------- #

def generate_dashboard(
    engine: Engine,
    team_refs: list[str],
    team_name: str,
    output_path: str,
) -> None:
    traj = championship_trajectory(engine, team_refs)
    if traj.empty:
        logger.warning("championship_trajectory returned no data — dashboard charts will be blank")

    gf = _grid_finish_df(engine, team_refs)

    fig1 = chart_championship_2d(traj)
    fig2 = chart_race_positions_2d(traj)
    fig3 = chart_grid_finish_2d(gf)

    _cfg = {"displayModeBar": "hover", "scrollZoom": True}
    div1 = fig1.to_html(full_html=False, include_plotlyjs="cdn",  config=_cfg)
    div2 = fig2.to_html(full_html=False, include_plotlyjs=False, config=_cfg)
    div3 = fig3.to_html(full_html=False, include_plotlyjs=False, config=_cfg)

    years = sorted(traj["year"].unique()) if not traj.empty else []
    year_range = f"{years[0]}–{years[-1]}" if years else ""
    subtitle = f"PERFORMANCE DASHBOARD \xb7 {year_range}" if year_range else "PERFORMANCE DASHBOARD"

    html = (
        _HTML_TEMPLATE
        .replace("PLACEHOLDER_TITLE",    f"{team_name} — F1 ANALYTICS")
        .replace("PLACEHOLDER_HEADING",  f"{team_name} — F1 ANALYTICS")
        .replace("PLACEHOLDER_SUBTITLE", subtitle)
        .replace("PLACEHOLDER_C1",       div1)
        .replace("PLACEHOLDER_C2",       div2)
        .replace("PLACEHOLDER_C3",       div3)
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(html)
