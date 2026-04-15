import logging
import sys
from typing import List, Set


class _CleanFormatter(logging.Formatter):
    _PREFIXES = {
        logging.WARNING:  "warn   ",
        logging.ERROR:    "error  ",
        logging.CRITICAL: "fatal  ",
    }

    def format(self, record: logging.LogRecord) -> str:
        prefix = self._PREFIXES.get(record.levelno, "")
        ts = self.formatTime(record, "%H:%M:%S")
        msg = record.getMessage()
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return f"{ts}  {prefix}{msg}"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("f1_analytics")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_CleanFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger


def format_table(headers: List[str], rows: list, right_cols: Set[int] = None) -> str:
    """Return a fixed-width text table. right_cols is a set of column indices to right-align."""
    right_cols = right_cols or set()
    all_rows = [headers] + [[str(v) for v in row] for row in rows]
    widths = [max(len(r[i]) for r in all_rows) for i in range(len(headers))]

    def fmt(row: list) -> str:
        cells = [
            v.rjust(w) if i in right_cols else v.ljust(w)
            for i, (v, w) in enumerate(zip(row, widths))
        ]
        return "  " + "  ".join(cells).rstrip()

    rule = "  " + "  ".join("─" * w for w in widths)
    return "\n".join([fmt(headers), rule] + [fmt(r) for r in all_rows[1:]])
