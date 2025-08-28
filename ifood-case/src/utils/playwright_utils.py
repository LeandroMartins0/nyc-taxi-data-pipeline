import logging
import time
import os
import hashlib
import json
from typing import Optional, Generator
from playwright.sync_api import sync_playwright
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from .io import save_if_changed, save_if_historical

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

def generate_content_hash(df: DataFrame, sample_limit: int = 1000) -> str:
    """
    Gera um hash MD5 consistente de um DataFrame Spark.
    Para evitar coletar datasets enormes, limita a um número de linhas (default=1000).
    """
    try:
        cols = sorted(df.columns)
        # coleta linhas limitadas e ordenadas
        rows = (
            df.select(*cols)
              .orderBy(*cols)
              .limit(sample_limit)
              .toPandas()
              .to_dict(orient="records")
        )
        content_str = json.dumps(rows, ensure_ascii=False, sort_keys=True)
        return hashlib.md5(content_str.encode("utf-8")).hexdigest()
    except Exception as e:
        logging.error(f"Erro ao gerar hash do conteúdo Spark: {e}")
        return ""

def download_file(page, url: str, title: str, year: int, download_dir: str, latest_dir: str, historical_dir: str, retries: int = 3) -> Optional[dict]:
    """
    Download a single file and save its metadata.

    Args:
        page: Playwright page object.
        url: URL of the file to download.
        title: Title of the trip record.
        year: Year of the trip record.
        download_dir: Directory to save the parquet file.
        latest_dir: Directory to save latest metadata.
        historical_dir: Directory to save historical metadata.
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
            file_path = os.path.join(download_dir, filename)

            # Save the downloaded file
            download.save_as(file_path)
            logger.info(f"Download saved: {file_path}")

            # Initialize Spark session to read parquet and generate hash
            spark = SparkSession.builder.appName("TaxiDataHash").getOrCreate()
            df = spark.read.parquet(file_path)
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
                "file_path": file_path,
                "unique_id": unique_id
            }

            # Save metadata using IO functions
            save_if_changed(
                data=metadata,
                identifier=identifier,
                latest_dir=latest_dir,
                historical_dir=historical_dir,
                file_prefix="taxi_metadata",
                file_suffix="meta"
            )
            save_if_historical(
                data=metadata,
                identifier=identifier,
                latest_dir=latest_dir,
                historical_dir=historical_dir,
                file_prefix="taxi_metadata",
                file_suffix="meta"
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
    download_dir: str = "./downloads/taxi_data",
    latest_dir: str = "./downloads/latest",
    historical_dir: str = "./downloads/historical"
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
        download_dir (str): Directory to save downloaded parquet files.
        latest_dir (str): Directory to save latest metadata.
        historical_dir (str): Directory to save historical metadata.

    Yields:
        dict: Metadata containing year, title, url, filename, and file_path.
    """
    # Map English month names to numerical format
    month_map = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    months = [m.capitalize() for m in months] if months else None

    start_time = time.time()

    # Ensure download directories exist
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(latest_dir, exist_ok=True)
    os.makedirs(historical_dir, exist_ok=True)

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
                            for m in months:
                                month_num = month_map.get(m)
                                if month_num and f"{year}-{month_num}" in url:
                                    month_matched = True
                                    break
                            if not month_matched:
                                continue

                        # Check for valid trip record types
                        valid_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]
                        if not any(t in url for t in valid_types):
                            continue

                        valid_links.append((title, url))

                    # Log found files
                    if valid_links:
                        logger.info(f"Found {len(valid_links)} trip record files for {year} {', '.join(months or ['all months'])}:")
                        for title, url in valid_links:
                            logger.info(f"- {title} ({url})")
                    else:
                        logger.info(f"No trip record files found for {year} {', '.join(months or ['all months'])}")

                    # Process downloads sequentially
                    collected = 0
                    for title, url in valid_links:
                        if limit is not None and collected >= limit:
                            break
                        result = download_file(page, url, title, year, download_dir, latest_dir, historical_dir, retries)
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
        months=["January", "February"],
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        headless=False,
        download_dir="./downloads/taxi_data",
        latest_dir="./downloads/latest",
        historical_dir="./downloads/historical"
    ))

    for r in results:
        logger.info(f"Processed: {r['year']} — {r['title']} — {r['filename']} — {r['url']}")