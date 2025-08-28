import logging
import time
from typing import Optional, Generator
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

def fetch_taxi_trip_links(
    years: list[int],
    months: Optional[list[str]] = None,
    base_url: str = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
    headless: bool = True,
    timeout: int = 30000,
    limit: Optional[int] = None,
    retries: int = 3,
) -> Generator[dict, None, None]:
    """
    Extract TLC datasets (Yellow, Green, FHV, HVFHV) for selected months/years.

    Args:
        years (list[int]): List of years to scrape (e.g., [2023]).
        months (Optional[list[str]]): Months to filter (e.g., ["January", "February"]). If None, include all.
        base_url (str): TLC trip records page.
        headless (bool): Whether to run browser in headless mode.
        timeout (int): Page timeout in ms.
        limit (Optional[int]): Maximum number of downloads per year.
        retries (int): Max retries in case of failure.

    Yields:
        dict: Metadata containing year, title, url, and downloaded filename.
    """
    # Map English month names to numerical format
    month_map = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    months = [m.capitalize() for m in months] if months else None

    start_time = time.time()

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

                    total = links.count()
                    logger.info(f"{total} trip record links found for {year}")

                    collected = 0
                    for i in range(total):
                        if limit is not None and collected >= limit:
                            break

                        for attempt in range(1, retries + 1):
                            try:
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

                                logger.info(f"[CLICK] {year} — {title} — {url} (attempt {attempt})")
                                link.scroll_into_view_if_needed()

                                with page.expect_download() as dl_info:
                                    link.click()
                                download = dl_info.value
                                logger.info(f"⬇️ Download started: {download.suggested_filename}")

                                yield {
                                    "year": year,
                                    "title": title,
                                    "url": url,
                                    "filename": download.suggested_filename,
                                }
                                collected += 1
                                break  # success, no more retries needed

                            except Exception as e:
                                logger.warning(f"Error on link {i + 1}/{total} for {year}, attempt {attempt}: {e}")
                                if attempt == retries:
                                    logger.error(f"Failed after {retries} attempts for link {i + 1}/{total}")
                                else:
                                    logger.info("Retrying...")
                                continue

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
        months=["January", "February"],  # Only process January and February
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        headless=False
    ))

    logger.info(f"{len(results)} datasets extracted successfully.")
    for r in results:
        logger.info(f"{r['year']} — {r['title']} — {r['filename']} — {r['url']}")