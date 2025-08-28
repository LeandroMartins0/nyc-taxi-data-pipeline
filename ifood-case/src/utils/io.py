import os
import json
import hashlib
import logging
import yaml
import re
import time
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def load_yaml_config(config_path: str) -> dict:
    """Carrega configurações de um arquivo YAML."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


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


def is_same_content(filepath: str, new_hash: str) -> bool:
    """Verifica se o hash salvo em arquivo é igual ao novo hash."""
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            old = json.load(f)
            return old.get("unique_id") == new_hash
    except Exception as e:
        logging.warning(f"Falha ao comparar hash em {filepath}: {e}")
        return False


def save_if_changed(
    data: dict,
    identifier: str,
    latest_dir: str,
    historical_dir: str,
    file_prefix: str = "content",
    file_suffix: str = None,
    subfolder_override: str = None
) -> None:
    """
    Salva dados localmente apenas se houve mudança (baseado em hash).
    Estrutura:
      - latest/<subfolder>/<filename>
      - historical/<subfolder>/<filename>
    """
    # cria nome do arquivo
    if file_suffix:
        filename = f"{file_prefix}_{identifier}_{file_suffix}.json"
    else:
        filename = f"{file_prefix}_{identifier}.json"

    if subfolder_override:
        subfolder = subfolder_override
    else:
        match = re.match(r"([a-zA-Z0-9_]+)_(\d+)", identifier)
        item_id = match.group(2) if match else identifier
        subfolder = f"content_{item_id}"

    latest_path = os.path.join(latest_dir, subfolder, filename)
    historical_path = os.path.join(historical_dir, subfolder, filename)

    logging.info("[DEBUG] latest_path = %s", os.path.abspath(latest_path))
    logging.info("[DEBUG] historical_path = %s", os.path.abspath(historical_path))

    os.makedirs(os.path.dirname(latest_path), exist_ok=True)
    os.makedirs(os.path.dirname(historical_path), exist_ok=True)

    changed = not is_same_content(latest_path, data.get("unique_id", ""))
    logging.info("changed=%s | filename=%s", changed, filename)

    if changed:
        try:
            with open(latest_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            with open(historical_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logging.info("Salvo em: %s e %s", latest_path, historical_path)
        except Exception as e:
            logging.error("Falha ao salvar arquivo localmente: %s", e)
    else:
        logging.info("Nenhuma alteração detectada. Ignorando: %s", filename)


def save_if_historical(
    data: dict,
    identifier: str,
    latest_dir: str,
    historical_dir: str,
    file_prefix: str = "content",
    file_suffix: str = None,
    subfolder_override: str = None
) -> None:
    """
    Salva sempre (mesmo se não houve alteração).
    Estrutura:
      - latest/<subfolder>/<filename>
      - historical/<subfolder>/<filename>
    """
    if file_suffix:
        filename = f"{file_prefix}_{identifier}_{file_suffix}.json"
    else:
        filename = f"{file_prefix}_{identifier}.json"

    if subfolder_override:
        subfolder = subfolder_override
    else:
        match = re.match(r"([a-zA-Z0-9_]+)_(\d+)", identifier)
        item_id = match.group(2) if match else identifier
        subfolder = f"content_{item_id}"

    latest_path = os.path.join(latest_dir, subfolder, filename)
    historical_path = os.path.join(historical_dir, subfolder, filename)

    logging.info("[DEBUG] latest_path = %s", os.path.abspath(latest_path))
    logging.info("[DEBUG] historical_path = %s", os.path.abspath(historical_path))

    os.makedirs(os.path.dirname(latest_path), exist_ok=True)
    os.makedirs(os.path.dirname(historical_path), exist_ok=True)

    try:
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        with open(historical_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info("[HISTORICAL] Salvo em: %s e %s", latest_path, historical_path)
    except Exception as e:
        logging.error("Falha ao salvar arquivo localmente (historical): %s", e)
