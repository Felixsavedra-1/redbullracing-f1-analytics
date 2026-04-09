import logging
import sys


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure basic structured logging and return a module logger."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("f1_analytics")
