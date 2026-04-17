DEFAULT_START_YEAR = 2020
DEFAULT_END_YEAR = 2025
DNF_POSITION_ORDER = 999  # sentinel for non-finishers in position_order

# Team config — overridden by config.py when present
try:
    from config import TEAM_CONFIG  # type: ignore[import]
    CONSTRUCTOR_ID: int = TEAM_CONFIG["constructor_id"]
    TEAM_REFS: list[str] = TEAM_CONFIG["family_refs"]
    TEAM_NAME: str = TEAM_CONFIG["name"]
    TEAM_COLORS: dict = TEAM_CONFIG.get("colors", {})
except (ImportError, KeyError):
    CONSTRUCTOR_ID = 9
    TEAM_REFS = ["red_bull", "alphatauri", "rb"]
    TEAM_NAME = "Red Bull"
    TEAM_COLORS = {}

TEAM_COLORS.setdefault("primary", "#C9A96E")
TEAM_COLORS.setdefault("accent",  "#8B5E3C")
TEAM_COLORS.setdefault("neutral", "#D4C5A9")

# Ergast-specific constant kept for backward compatibility
RED_BULL_CONSTRUCTOR_ID = 9
