import time
import logging
from pathlib import Path
from typing import Optional, Generator

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

def fetch_taxi_trip_links(
    years: list[int],
    base_url: str = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
    headless: bool = True,
    timeout: int = 20000,
    limit: Optional[int] = None,
) -> Generator[dict, None, None]:
    """
    Extrai os links de download dos datasets de taxi/FHV/HVFHV do site da NYC TLC.

    Args:
        years (list[int]): Lista de anos para extrair (ex: [2025, 2024]).
        base_url (str): Página base da NYC TLC com os datasets.
        headless (bool): Executar navegador em modo headless.
        timeout (int): Timeout em ms.
        limit (Optional[int]): Máx de arquivos por ano/mês a extrair.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto(base_url, timeout=timeout)
            page.wait_for_load_state("load")
            time.sleep(0.5)

            for year in years:
                logger.info(f"Processando ano {year}")
                try:
                    # Expande o ano
                    year_locator = page.locator(f"text={year}").first
                    year_locator.scroll_into_view_if_needed()
                    year_locator.click()
                    time.sleep(0.5)

                    # Coleta meses
                    months = page.locator(f"//div[contains(., '{year}')]/following-sibling::div//a[contains(text(),'Taxi Trip Records')]")
                    total = months.count()
                    logger.info(f"{total} datasets encontrados para {year}")

                    for i in range(total if limit is None else min(limit, total)):
                        try:
                            link = months.nth(i)
                            title = link.inner_text().strip()
                            url = link.get_attribute("href")

                            logger.info(f"{year} — {title} — {url}")

                            yield {
                                "year": year,
                                "title": title,
                                "url": url,
                            }

                        except Exception as e:
                            logger.warning(f"Erro ao processar item {i + 1}/{total} para {year}: {e}")
                            continue

                except Exception as e:
                    logger.warning(f"Falha ao processar ano {year}: {e}")
                    continue

        finally:
            browser.close()
            logger.info("Extração finalizada.")


if __name__ == "__main__":
    results = list(fetch_taxi_trip_links(
        years=[2025],
        base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        headless=False,
        limit=5  # 
    ))

    logger.info(f"{len(results)} datasets extraídos com sucesso.")
    for r in results:
        logger.info(f"{r['year']} — {r['title']} — {r['url']}")
