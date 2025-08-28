import sys
import logging
from pathlib import Path

# === Path setup ===
# Vai até ...\ifood-case\src
project_root = Path(__file__).resolve().parents[5]
src_path = project_root
sys.path.insert(0, str(src_path))

# === Imports from the project ===
from utils.playwright_utils import fetch_taxi_trip_links
from utils.io import load_yaml_config

# === Logging configuration ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def main():
    """Run the NYC TLC trip record scraper with YAML configuration."""
    # Load configuration
    config_path = src_path / "config" / "source_to_raw.yaml"
    config = load_yaml_config(config_path)
    scraper_config = config.get("scraper", {}).get("nyc_tlc", {}).get("trip_records", {})

    # Check for required parameter
    if not scraper_config.get("years"):
        logger.warning("No years specified in configuration.")
        sys.exit(0)

    logger.info(f"Starting NYC TLC trip record scraper with config: {scraper_config}")

    try:
        results = list(fetch_taxi_trip_links(**scraper_config))

        if results:
            logger.info(f"Successfully processed {len(results)} files:")
            for result in results:
                logger.info(
                    f"Processed: {result['year']} — {result['title']} — {result['filename']} — {result['url']}"
                )
        else:
            logger.warning("No files were processed.")

    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
