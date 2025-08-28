import logging
import time
import os
import json
import re
from typing import Optional, Generator
from datetime import datetime
from playwright.sync_api import sync_playwright
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from .io import save_if_changed, generate_content_hash

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

def download_file(page, url: str, title: str, year: int, month: str, download_dir: str, extraction_timestamp: str, retries: int = 3) -> Optional[dict]:
    """
    Download a single file and save its metadata.

    Args:
        page: Playwright page object.
        url: URL of the file to download.
        title: Title of the trip record.
        year: Year of the trip record.
        month: Month of the trip record (e.g., 'January').
        download_dir: Base directory to save the parquet file and metadata (e.g., 'datalake/raw/external/web_scraping/nyc_tlc/trip_records').
        extraction_timestamp: Timestamp for the dt=extraction folder (e.g., '2025-08-28T17-22-00').
        retries: Number of retry attempts.

    Returns:
        dict: Metadata of the downloaded file or None if failed.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Processing {year} — {title} — {url} (attempt {attempt})")
            link = page.locator(f"a[href='{url}']").first
            link.scroll_into_view_if_needed()

            with page.expect_download() as dl_info:
                link.click()
            download = dl_info.value
            filename = download.suggested_filename

            # Save parquet file temporarily
            temp_dir = os.path.join(download_dir, "temp")
            os.makedirs(temp_dir, exist_ok=True)
            temp_parquet_path = os.path.join(temp_dir, filename)
            download.save_as(temp_parquet_path)
            logger.info(f"Temporary parquet saved: {temp_parquet_path}")

            # Initialize Spark session to read parquet and generate hash
            spark = SparkSession.builder.appName("TaxiDataHash").getOrCreate()
            df = spark.read.parquet(temp_parquet_path)
            unique_id = generate_content_hash(df)
            spark.stop()

            # Generate a unique identifier for the file
            identifier = filename.replace(".parquet", "")

            # Create metadata
            metadata = {
                "year": year,
                "title": title,
                "url": url,
                "filename": filename,
                "file_path": os.path.join(download_dir, f"dt=extraction{extraction_timestamp}", str(year), month, filename),
                "unique_id": unique_id
            }

            # Save parquet and metadata in the data lake
            save_if_changed(
                data=metadata,
                identifier=identifier,
                download_dir=download_dir,
                extraction_timestamp=extraction_timestamp,
                year=str(year),
                month=month,
                file_prefix="taxi_metadata",
                file_suffix="meta",
                parquet_file_path=temp_parquet_path
            )

            return metadata

        except Exception as e:
            logger.warning(f"Error processing {title} for {year}, attempt {attempt}: {e}")
            if attempt == retries:
                logger.error(f"Failed after {retries} attempts for {title}")
            else:
                logger.info("Retrying...")
            continue
    return None

def fetch_taxi_trip_links(
    years: list[int],
    months: Optional[list[str]] = None,
    base_url: str = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
    headless: bool = True,
    timeout: int = 30000,
    limit: Optional[int] = None,
    retries: int = 3,
    download_dir: str = "datalake/raw/external/web_scraping/nyc_tlc/trip_records"
) -> Generator[dict, None, None]:
    """
    Extract TLC datasets (Yellow, Green, FHV, HVFHV) for selected months/years and save them locally.

    Args:
        years (list[int]): List of years to scrape (e.g., [2023]).
        months (Optional[list[str]]): Months to filter (e.g., ["January", "February"]). If None, include all.
        base_url (str): TLC trip records page.
        headless (bool): Whether to run browser in headless mode.
        timeout (int): Page timeout in ms.
        limit (Optional[int]): Maximum number of downloads per year.
        retries (int): Max retries in case of failure.
        download_dir (str): Base directory to save downloaded parquet files and metadata.

    Yields:
        dict: Metadata containing year, title, url, filename, and file_path.
    """
    # Map English month names to numerical format for filtering
    month_map = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    # Reverse map for converting numerical months to English names
    reverse_month_map = {v: k for k, v in month_map.items()}
    months = [m.capitalize() for m in months] if months else None

    start_time = time.time()
    # Create a single timestamp for the entire pipeline run
    extraction_timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # Ensure download directory exists
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            logger.info(f"Navigating to {base_url}")
            page.goto(base_url, timeout=timeout)
            page.wait_for_load_state("load")

            for year in years:
                logger.info(f"Expanding section for year {year}")
                try:
                    # Expand the accordion for the given year
                    year_locator = page.locator(f"text={year}").first
                    year_locator.scroll_into_view_if_needed()
                    year_locator.click()
                    page.wait_for_load_state("load")

                    # Find all Trip Record links for this year
                    year_container = page.locator(f"//div[contains(., '{year}')]/following-sibling::div[1]")
                    links = year_container.locator("a:has-text('Trip Records')")

                    # Collect valid links for specified months
                    valid_links = []
                    for i in range(links.count()):
                        link = links.nth(i)
                        title = link.inner_text().strip()
                        url = link.get_attribute("href")

                        if not url:
                            continue

                        # Month filter: check if URL matches selected year-month
                        if months:
                            month_matched = False
                            month_num = None
                            month_name = None
                            for m in months:
                                month_num = month_map.get(m)
                                if month_num and f"{year}-{month_num}" in url:
                                    month_matched = True
                                    month_name = m
                                    break
                            if not month_matched:
                                continue
                        else:
                            # Extract month from URL if no months specified
                            match = re.search(r"(\d{4})-(\d{2})", url)
                            month_num = match.group(2) if match else "unknown"
                            month_name = reverse_month_map.get(month_num, "unknown")

                        # Check for valid trip record types
                        valid_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]
                        if not any(t in url for t in valid_types):
                            continue

                        valid_links.append((title, url, month_name))

                    # Log found files
                    if valid_links:
                        logger.info(f"Found {len(valid_links)} trip record files for {year} {', '.join(months or ['all months'])}:")
                        for title, url, _ in valid_links:
                            logger.info(f"- {title} ({url})")
                    else:
                        logger.info(f"No trip record files found for {year} {', '.join(months or ['all months'])}")

                    # Process downloads sequentially
                    collected = 0
                    for title, url, month_name in valid_links:
                        if limit is not None and collected >= limit:
                            break
                        result = download_file(page, url, title, year, month_name, download_dir, extraction_timestamp, retries)
                        if result:
                            yield result
                            collected += 1

                except Exception as e:
                    logger.warning(f"Failed to process year {year}: {e}")
                    continue

        finally:
            browser.close()
            elapsed = time.time() - start_time
            logger.info(f"Extraction finished in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    results = list(fetch_taxi_trip_links(
        years=[2023],
        months=["January"],
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        headless=False,
        download_dir="datalake/raw/external/web_scraping/nyc_tlc/trip_records"
    ))

    for r in results:
        logger.info(f"Processed: {r['year']} — {r['title']} — {r['filename']} — {r['url']}")