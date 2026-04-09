import argparse
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from logging_utils import setup_logging
from constants import DEFAULT_START_YEAR, DEFAULT_END_YEAR
import json
from extract_data import F1DataExtractor
from transform_data import F1DataTransformer
from load_data import F1DataLoader
from data_quality import run_quality_checks


def _normalize_year_range(start_year: int, end_year: int) -> tuple[int, int, bool]:
    clamped = False
    if start_year < DEFAULT_START_YEAR:
        start_year = DEFAULT_START_YEAR
        clamped = True
    if end_year > DEFAULT_END_YEAR:
        end_year = DEFAULT_END_YEAR
        clamped = True
    return start_year, end_year, clamped


def run_full_pipeline(
    start_year: int = DEFAULT_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
    skip_extract: bool = False,
    skip_transform: bool = False,
    skip_load: bool = False,
    skip_pit_stops: bool = False,
    skip_quality: bool = False,
    mode: str = "full_refresh",
    strict_schema: bool = True,
    base_delay: float = 1.5,
    max_retries: int = 6,
    max_base_delay: float = 8.0,
) -> None:
    """Run extraction, transformation, and loading for the requested year range."""

    logger = setup_logging()

    start_year, end_year, clamped = _normalize_year_range(start_year, end_year)
    if start_year > end_year:
        raise ValueError(
            f"Invalid year range after clamping to {DEFAULT_START_YEAR}-{DEFAULT_END_YEAR}."
        )

    logger.info("F1 Red Bull Analytics - Complete Pipeline")
    if clamped:
        logger.warning(
            "Year range clamped to %s-%s for this project scope.",
            DEFAULT_START_YEAR,
            DEFAULT_END_YEAR,
        )

    if not skip_extract:
        logger.info("[1/3] EXTRACTING DATA FROM API")
        extractor = F1DataExtractor(
            output_path="data/raw/",
            base_delay=base_delay,
            max_retries=max_retries,
            max_base_delay=max_base_delay,
        )
        extractor.extract_all(
            start_year=start_year,
            end_year=end_year,
            skip_pit_stops=skip_pit_stops,
        )
    else:
        logger.info("[1/3] SKIPPING EXTRACTION (--skip-extract flag)")

    if not skip_transform:
        logger.info("[2/3] TRANSFORMING DATA")
        transformer = F1DataTransformer(
            raw_data_path="data/raw/",
            processed_data_path="data/processed/",
        )
        transformer.transform_all()
    else:
        logger.info("[2/3] SKIPPING TRANSFORMATION (--skip-transform flag)")

    if not skip_load:
        logger.info("[3/3] LOADING DATA INTO DATABASE")
        loader = F1DataLoader(
            mode=mode,
            strict_schema=strict_schema,
            source_url=F1DataExtractor.BASE_URL,
        )
        loader.load_all()

        if not skip_quality:
            def load_skipped(name: str) -> dict:
                cache_path = os.path.join("data", "cache", name)
                legacy_path = os.path.join("data", "raw", name)
                for path in (cache_path, legacy_path):
                    if not os.path.exists(path):
                        continue
                    try:
                        with open(path, "r") as handle:
                            data = json.load(handle)
                        return data.get("skipped", {}) if isinstance(data, dict) else {}
                    except Exception:
                        return {}
                return {}

            skipped_rounds = {
                "results": load_skipped("results_progress.json"),
                "qualifying": load_skipped("qualifying_progress.json"),
            }

            failures = run_quality_checks(
                loader.engine,
                start_year=start_year,
                end_year=end_year,
                skipped_rounds=skipped_rounds,
            )
            if failures:
                fail_on_quality = os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"
                if fail_on_quality:
                    logger.error("Data quality checks failed: %s", failures)
                    raise RuntimeError("Data quality checks failed")
                logger.warning("Data quality checks had warnings: %s", failures)
            else:
                logger.info("Data quality checks passed")
    else:
        logger.info("[3/3] SKIPPING DATABASE LOAD (--skip-load flag)")

    logger.info("Pipeline completed successfully.")
    logger.info("Next steps:")
    logger.info("  - Run queries: python scripts/run_queries.py --list")
    logger.info("  - Export results: python scripts/run_queries.py --query kpi_summary --export")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the complete F1 Red Bull Analytics pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python scripts/run_pipeline.py

  # Run only extraction
  python scripts/run_pipeline.py --skip-transform --skip-load

  # Run only transformation and loading (skip extraction)
  python scripts/run_pipeline.py --skip-extract

  # Custom year range
  python scripts/run_pipeline.py --start-year 2010 --end-year 2023

  # Incremental load
  python scripts/run_pipeline.py --incremental

  # Fast demo run
  python scripts/run_pipeline.py --fast
        """,
    )

    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"Start year for data extraction (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=DEFAULT_END_YEAR,
        help=f"End year for data extraction (default: {DEFAULT_END_YEAR})",
    )
    parser.add_argument("--skip-extract", action="store_true", help="Skip data extraction step")
    parser.add_argument("--skip-transform", action="store_true", help="Skip data transformation step")
    parser.add_argument("--skip-load", action="store_true", help="Skip database loading step")
    parser.add_argument("--skip-pit-stops", action="store_true", help="Skip pit stop extraction")
    parser.add_argument("--skip-quality", action="store_true", help="Skip data quality checks")
    parser.add_argument("--incremental", action="store_true", help="Use incremental load instead of full refresh")
    parser.add_argument("--no-strict-schema", action="store_true", help="Do not fail on schema contract warnings")
    parser.add_argument("--base-delay", type=float, default=1.5, help="Delay between API requests in seconds")
    parser.add_argument("--max-retries", type=int, default=6, help="Max retries on API errors or rate limits")
    parser.add_argument("--max-base-delay", type=float, default=8.0, help="Upper bound for adaptive delay")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Run a faster demo extraction (2021–2025, reduced retries/backoff)",
    )

    args = parser.parse_args()

    mode = "incremental" if args.incremental else "full_refresh"
    strict_schema = not args.no_strict_schema

    if args.fast:
        args.start_year = max(args.start_year, 2021)
        args.base_delay = min(args.base_delay, 0.3)
        args.max_retries = min(args.max_retries, 3)
        args.max_base_delay = min(args.max_base_delay, 2.0)
        args.skip_pit_stops = True

    try:
        run_full_pipeline(
            start_year=args.start_year,
            end_year=args.end_year,
            skip_extract=args.skip_extract,
            skip_transform=args.skip_transform,
            skip_load=args.skip_load,
            skip_pit_stops=args.skip_pit_stops,
            skip_quality=args.skip_quality,
            mode=mode,
            strict_schema=strict_schema,
            base_delay=args.base_delay,
            max_retries=args.max_retries,
            max_base_delay=args.max_base_delay,
        )
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        print(f"\n\nPipeline failed: {exc}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
