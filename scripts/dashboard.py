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

_BG      = "#0d0d0d"
_PLOT_BG = "#111111"
_FONT    = "Courier New, monospace"
_GRID    = "#252525"
_TICK    = "#aaaaaa"

_DRIVER_COLORS = [
    "#E8002D", "#3399FF", "#FF6B00", "#00DDFF",
    "#FFD700", "#CC44FF", "#00FF88", "#FF44AA",
]

# One car per Red Bull family team — body/accent drives the Three.js material colors
_TEAM_CARS = [
    {"name": "ORACLE RED BULL RACING", "body": "#E8002D", "accent": "#1E3A8A"},
    {"name": "SCUDERIA ALPHATAURI",    "body": "#E0E0E8", "accent": "#E8002D"},
    {"name": "VISA CASH APP RB",       "body": "#E0E0E8", "accent": "#0044CC"},
]

# --------------------------------------------------------------------------- #
#  Three.js car builder (embedded JS)                                          #
# --------------------------------------------------------------------------- #

_CAR_JS = """
function _h(hex) { return parseInt(hex.replace('#',''), 16); }

function buildF1Car(bodyHex, accentHex) {
  var B = _h(bodyHex), A = _h(accentHex), D = 0x111111, C = 0x222222;
  var g = new THREE.Group();

  function mat(c, s) {
    return new THREE.MeshPhongMaterial({color: c, shininess: s || 90});
  }
  function box(w, h, d, x, y, z, c, s) {
    var m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), mat(c, s));
    m.position.set(x, y, z);
    g.add(m);
  }

  // Monocoque
  box(2.05, 0.27, 0.66,  0.00, 0.22,  0.00, B);
  // Engine cover
  box(0.70, 0.40, 0.58,  0.54, 0.37,  0.00, B);
  // Nose box
  box(0.88, 0.16, 0.40, -1.30, 0.20,  0.00, B);
  // Cockpit (accent colour)
  box(0.40, 0.25, 0.50,  0.02, 0.39,  0.00, A, 130);
  // Underfloor
  box(2.05, 0.04, 0.86,  0.00, 0.07,  0.00, D);
  // Sidepods
  box(0.88, 0.21, 0.25,  0.08, 0.17,  0.48, B);
  box(0.88, 0.21, 0.25,  0.08, 0.17, -0.48, B);

  // Nose cone tip
  var nc = new THREE.Mesh(new THREE.ConeGeometry(0.08, 0.55, 8), mat(B));
  nc.rotation.z = -Math.PI / 2;
  nc.position.set(-1.97, 0.18, 0);
  g.add(nc);

  // Front wing
  box(0.15, 0.03, 1.65, -1.70, 0.06,  0.00, A, 130);
  box(0.22, 0.13, 0.04, -1.65, 0.08,  0.82, C);
  box(0.22, 0.13, 0.04, -1.65, 0.08, -0.82, C);

  // Rear wing
  box(0.15, 0.03, 1.10,  1.04, 0.77,  0.00, A, 130);
  box(0.15, 0.03, 1.10,  1.04, 0.69,  0.00, B);
  box(0.09, 0.50, 0.04,  1.04, 0.51,  0.55, C);
  box(0.09, 0.50, 0.04,  1.04, 0.51, -0.55, C);

  // Wheels
  var wGeo = new THREE.CylinderGeometry(0.23, 0.23, 0.27, 20);
  var rGeo = new THREE.CylinderGeometry(0.13, 0.13, 0.28,  6);
  [[-0.94, 0, 0.60], [-0.94, 0, -0.60],
   [ 0.82, 0, 0.60], [ 0.82, 0, -0.60]].forEach(function(p) {
    var w = new THREE.Mesh(wGeo, mat(0x111111, 30));
    w.rotation.x = Math.PI / 2;
    w.position.set(p[0], p[1], p[2]);
    g.add(w);
    var r = new THREE.Mesh(rGeo, mat(A, 160));
    r.rotation.x = Math.PI / 2;
    r.position.set(p[0], p[1], p[2] > 0 ? p[2] + 0.01 : p[2] - 0.01);
    g.add(r);
  });

  return g;
}

function initCar(canvasId, bodyHex, accentHex) {
  var canvas = document.getElementById(canvasId);
  if (!canvas || typeof THREE === 'undefined') return;
  var W = canvas.parentElement.offsetWidth || 340;
  var H = 160;
  canvas.width  = W;
  canvas.height = H;
  var renderer = new THREE.WebGLRenderer({canvas: canvas, antialias: true, alpha: true});
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
  renderer.setSize(W, H);
  var scene  = new THREE.Scene();
  var camera = new THREE.PerspectiveCamera(36, W / H, 0.1, 50);
  camera.position.set(0, 1.5, 6.0);
  camera.lookAt(0, 0.25, 0);
  scene.add(new THREE.AmbientLight(0x404060, 2.5));
  var key = new THREE.DirectionalLight(0xffffff, 5.0);
  key.position.set(4, 6, 4);
  scene.add(key);
  var fill = new THREE.DirectionalLight(0x6688cc, 1.8);
  fill.position.set(-3, 2, -2);
  scene.add(fill);
  var car = buildF1Car(bodyHex, accentHex);
  scene.add(car);
  var t = 0;
  (function loop() {
    requestAnimationFrame(loop);
    t += 0.012;
    car.rotation.y = Math.sin(t * 0.55) * 0.35;
    car.position.y = Math.sin(t * 0.9) * 0.07;
    renderer.render(scene, camera);
  })();
}
"""

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
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d0d0d;color:#fff;font-family:'Courier New',monospace}
header{padding:32px 32px 12px;border-bottom:1px solid #1c1c1c;margin-bottom:28px}
h1{letter-spacing:.25em;font-size:1.05rem;text-transform:uppercase}
.sub{color:#777;letter-spacing:.12em;font-size:.68rem;margin-top:6px}
.cars{display:grid;grid-template-columns:1fr 1fr 1fr;gap:0;padding:0 24px 8px}
.car-col{display:flex;flex-direction:column;align-items:center;padding:0 8px}
canvas.car{display:block;width:100%;height:160px}
.car-label{font-size:.58rem;letter-spacing:.18em;color:#555;text-transform:uppercase;
  padding:8px 0 20px;text-align:center}
.charts{display:grid;grid-template-columns:1fr;gap:20px;padding:0 24px 40px}
.chart-row{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:860px){.chart-row,.cars{grid-template-columns:1fr}}
</style>
</head>
<body>
<header>
  <h1>PLACEHOLDER_HEADING</h1>
  <p class="sub">PLACEHOLDER_SUBTITLE</p>
</header>
<div class="cars">
  <div class="car-col">
    <canvas id="car1" class="car"></canvas>
    <div class="car-label">ORACLE RED BULL RACING</div>
  </div>
  <div class="car-col">
    <canvas id="car2" class="car"></canvas>
    <div class="car-label">SCUDERIA ALPHATAURI</div>
  </div>
  <div class="car-col">
    <canvas id="car3" class="car"></canvas>
    <div class="car-label">VISA CASH APP RB</div>
  </div>
</div>
<div class="charts">
  <div id="c1">PLACEHOLDER_C1</div>
  <div class="chart-row">
    <div id="c2">PLACEHOLDER_C2</div>
    <div id="c3">PLACEHOLDER_C3</div>
  </div>
</div>
<script>
PLACEHOLDER_CAR_JS
window.addEventListener('load',function(){
  initCar('car1','PLACEHOLDER_CAR1_BODY','PLACEHOLDER_CAR1_ACCENT');
  initCar('car2','PLACEHOLDER_CAR2_BODY','PLACEHOLDER_CAR2_ACCENT');
  initCar('car3','PLACEHOLDER_CAR3_BODY','PLACEHOLDER_CAR3_ACCENT');
});
</script>
</body>
</html>
"""

# --------------------------------------------------------------------------- #
#  2D chart helpers                                                             #
# --------------------------------------------------------------------------- #

def _axis_2d(title: str = "") -> dict:
    return dict(
        title=dict(text=title, font=dict(color="#cccccc", family=_FONT, size=10)),
        gridcolor=_GRID,
        zerolinecolor="#333333",
        tickfont=dict(color=_TICK, family=_FONT, size=10),
        linecolor="#2a2a2a",
        showgrid=True,
    )


def _layout_2d(title: str, xaxis_title: str = "", yaxis_title: str = "",
               height: int = 420, hovermode: str = "x unified") -> go.Layout:
    return go.Layout(
        title=dict(
            text=title,
            font=dict(color="#ffffff", family=_FONT, size=13),
            x=0.5,
            xanchor="center",
            pad=dict(t=6),
        ),
        paper_bgcolor=_BG,
        plot_bgcolor=_PLOT_BG,
        xaxis=_axis_2d(xaxis_title),
        yaxis=_axis_2d(yaxis_title),
        font=dict(family=_FONT, color="#ffffff"),
        margin=dict(l=52, r=16, t=56, b=48),
        height=height,
        showlegend=True,
        legend=dict(
            font=dict(family=_FONT, color="#bbbbbb", size=10),
            bgcolor="rgba(13,13,13,0.85)",
            bordercolor="#2a2a2a",
            borderwidth=1,
        ),
        hovermode=hovermode,
        hoverlabel=dict(
            bgcolor="#1a1a1a",
            bordercolor="#3a3a3a",
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
            marker=dict(size=5, color=color, line=dict(color="#0d0d0d", width=1)),
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
            marker=dict(size=6, color=color, line=dict(color="#0d0d0d", width=1)),
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
        line=dict(color="#444444", width=1, dash="dot"),
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
                line=dict(color="#0d0d0d", width=0.8),
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

    c = _TEAM_CARS
    html = (
        _HTML_TEMPLATE
        .replace("PLACEHOLDER_TITLE",       f"{team_name} — F1 ANALYTICS")
        .replace("PLACEHOLDER_HEADING",     f"{team_name} — F1 ANALYTICS")
        .replace("PLACEHOLDER_SUBTITLE",    subtitle)
        .replace("PLACEHOLDER_C1",          div1)
        .replace("PLACEHOLDER_C2",          div2)
        .replace("PLACEHOLDER_C3",          div3)
        .replace("PLACEHOLDER_CAR_JS",      _CAR_JS)
        .replace("PLACEHOLDER_CAR1_BODY",   c[0]["body"])
        .replace("PLACEHOLDER_CAR1_ACCENT", c[0]["accent"])
        .replace("PLACEHOLDER_CAR2_BODY",   c[1]["body"])
        .replace("PLACEHOLDER_CAR2_ACCENT", c[1]["accent"])
        .replace("PLACEHOLDER_CAR3_BODY",   c[2]["body"])
        .replace("PLACEHOLDER_CAR3_ACCENT", c[2]["accent"])
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(html)
