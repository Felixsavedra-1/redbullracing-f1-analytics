import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract_data import F1DataExtractor
from transform_data import F1DataTransformer
from load_data import F1DataLoader

def run_full_pipeline(start_year: int = 2005,
                      end_year: int = 2024,
                      skip_extract: bool = False,
                      skip_transform: bool = False,
                      skip_load: bool = False) -> None:
    """Run extraction, transformation, and loading for the requested year range."""
    
    print("=" * 60)
    print("F1 Red Bull Analytics - Complete Pipeline")
    print("=" * 60)
    
    if not skip_extract:
        print("\n[1/3] EXTRACTING DATA FROM API")
        print("-" * 60)
        extractor = F1DataExtractor(output_path='data/raw/')
        extractor.extract_all(start_year=start_year, end_year=end_year)
    else:
        print("\n[1/3] SKIPPING EXTRACTION (--skip-extract flag)")
    
    if not skip_transform:
        print("\n[2/3] TRANSFORMING DATA")
        print("-" * 60)
        transformer = F1DataTransformer(
            raw_data_path='data/raw/',
            processed_data_path='data/processed/'
        )
        transformer.transform_all()
    else:
        print("\n[2/3] SKIPPING TRANSFORMATION (--skip-transform flag)")
    
    if not skip_load:
        print("\n[3/3] LOADING DATA INTO DATABASE")
        print("-" * 60)
        loader = F1DataLoader()
        loader.load_all()
    else:
        print("\n[3/3] SKIPPING DATABASE LOAD (--skip-load flag)")
    
    print("\n" + "=" * 60)
    print("Pipeline completed successfully.")
    print("=" * 60)
    print("\nNext steps:")
    print("  - Run queries: python scripts/run_queries.py --list")
    print("  - Export results: python scripts/run_queries.py --query kpi_summary --export")

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Run the complete F1 Red Bull Analytics pipeline',
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
        """
    )
    
    parser.add_argument('--start-year', type=int, default=2005,
                       help='Start year for data extraction (default: 2005)')
    parser.add_argument('--end-year', type=int, default=2024,
                       help='End year for data extraction (default: 2024)')
    parser.add_argument('--skip-extract', action='store_true',
                       help='Skip data extraction step')
    parser.add_argument('--skip-transform', action='store_true',
                       help='Skip data transformation step')
    parser.add_argument('--skip-load', action='store_true',
                       help='Skip database loading step')
    
    args = parser.parse_args()
    
    try:
        run_full_pipeline(
            start_year=args.start_year,
            end_year=args.end_year,
            skip_extract=args.skip_extract,
            skip_transform=args.skip_transform,
            skip_load=args.skip_load
        )
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nPipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

