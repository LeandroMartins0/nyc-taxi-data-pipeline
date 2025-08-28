import os
import requests
import datetime
import logging
from pathlib import Path

from utils.playwright_utils import fetch_taxi_trip_links
from utils.io import save_if_changed

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def download_file(url: str, output_dir: Path) -> Path:
    """Baixa um arquivo e salva no diretório especificado."""
    filename = url.split("/")[-1]
    filepath = output_dir / filename

    logger.info(f"⬇️ Baixando {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"✅ Salvo em {filepath}")
    return filepath


if __name__ == "__main__":
    # Define estrutura de pastas do datalake local
    today_str = datetime.date.today().strftime("%Y%m%d")
    base_dir = Path(f"datalake/raw/trip_record/dt=extraction_{today_str}")
    arquivos_dir = base_dir / "arquivos"
    latest_dir = base_dir / "latest"
    historical_dir = base_dir / "historical"

    arquivos_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)
    historical_dir.mkdir(parents=True, exist_ok=True)

    # Escolha dos anos/meses
    anos = [2023]
    meses = ["January", "February", "March", "April", "May"]

    # Coleta dos links
    results = list(fetch_taxi_trip_links(
        years=anos,
        headless=True
    ))

    logger.info(f"Total de {len(results)} links coletados.")

    # Filtra pelos meses desejados
    filtered = [r for r in results if any(m in r["title"] for m in meses)]
    logger.info(f"{len(filtered)} links após aplicar filtro de meses {meses}")

    # Faz download e salva metadados
    for r in filtered:
        try:
            file_path = download_file(r["url"], arquivos_dir)

            # Cria metadados básicos
            metadata = {
                "unique_id": r["url"],  # pode depois trocar pelo hash do parquet
                "year": r["year"],
                "title": r["title"],
                "url": r["url"],
                "local_path": str(file_path),
                "dt_extraction": today_str,
            }

            save_if_changed(
                data=metadata,
                identifier=f"taxi_{r['year']}",
                latest_dir=str(latest_dir),
                historical_dir=str(historical_dir),
                file_prefix="trip_record"
            )

        except Exception as e:
            logger.error(f"Erro no arquivo {r['url']}: {e}")
