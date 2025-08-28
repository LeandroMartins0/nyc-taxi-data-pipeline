import os
import json
import hashlib
import logging
import yaml
import re
import shutil
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import *

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
    Converte todas as colunas para string, lidando com tipos complexos.
    """
    try:
        # Log schema for debugging
        logger.info(f"DataFrame schema: {df.schema}")
        cols = sorted(df.columns)
        # Handle complex types by converting to string
        select_expr = []
        for col in cols:
            col_type = df.schema[col].dataType
            if isinstance(col_type, (TimestampType, ArrayType, StructType, MapType)):
                # Convert complex types to string
                select_expr.append(df[col].cast("string").alias(col))
            elif col_type in (BinaryType(), DateType(), DecimalType()):
                # Convert other potentially problematic types to string
                select_expr.append(df[col].cast("string").alias(col))
            else:
                select_expr.append(df[col])
        df = df.select(select_expr)
        # Coletar linhas limitadas e ordenadas
        rows = (
            df.select(*cols)
              .orderBy(*cols)
              .limit(sample_limit)
              .toPandas()
              .to_dict(orient="records")
        )
        # Handle non-serializable objects in JSON
        content_str = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.md5(content_str.encode("utf-8")).hexdigest()
    except Exception as e:
        logging.error(f"Erro ao gerar hash do conteúdo Spark: {e}")
        # Log a sample of the data for debugging
        try:
            sample_data = df.limit(5).toPandas().to_dict(orient="records")
            logging.error(f"Sample data (first 5 rows): {json.dumps(sample_data, ensure_ascii=False, default=str)}")
        except Exception as sample_e:
            logging.error(f"Erro ao coletar amostra de dados: {sample_e}")
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
    download_dir: str,
    extraction_timestamp: str,
    year: str,
    month: str,
    file_prefix: str = "content",
    file_suffix: str = None,
    parquet_file_path: str = None
) -> None:
    """
    Salva metadados e arquivo parquet no data lake.
    Estrutura:
      - download_dir/dt=extraction{YYYY-MM-DDTHH-MM-SS}/{year}/{month}/{filename} (parquet, sempre salvo)
      - download_dir/dt=extraction{YYYY-MM-DDTHH-MM-SS}/{year}/{month}/metadata/{filename} (metadata, sempre salvo)
      - download_dir/latest/{year}/{month}/{filename} (parquet, se mudou)
      - download_dir/latest/{year}/{month}/metadata/{filename} (metadata, se mudou)
    """
    # Cria nome do arquivo de metadados
    if file_suffix:
        filename = f"{file_prefix}_{identifier}_{file_suffix}.json"
    else:
        filename = f"{file_prefix}_{identifier}.json"

    # Caminhos no data lake
    extraction_metadata_path = os.path.join(download_dir, f"dt=extraction{extraction_timestamp}", year, month, "metadata", filename)
    latest_metadata_path = os.path.join(download_dir, "latest", year, month, "metadata", filename)
    extraction_parquet_path = os.path.join(download_dir, f"dt=extraction{extraction_timestamp}", year, month, data.get("filename"))
    latest_parquet_path = os.path.join(download_dir, "latest", year, month, data.get("filename"))

    os.makedirs(os.path.dirname(extraction_metadata_path), exist_ok=True)
    os.makedirs(os.path.dirname(latest_metadata_path), exist_ok=True)
    os.makedirs(os.path.dirname(extraction_parquet_path), exist_ok=True)
    os.makedirs(os.path.dirname(latest_parquet_path), exist_ok=True)

    logging.info("[DEBUG] extraction_metadata_path = %s", os.path.abspath(extraction_metadata_path))
    logging.info("[DEBUG] latest_metadata_path = %s", os.path.abspath(latest_metadata_path))
    if parquet_file_path:
        logging.info("[DEBUG] extraction_parquet_path = %s", os.path.abspath(extraction_parquet_path))
        logging.info("[DEBUG] latest_parquet_path = %s", os.path.abspath(latest_parquet_path))

    changed = not is_same_content(latest_metadata_path, data.get("unique_id", ""))
    logging.info("changed=%s | filename=%s", changed, filename)

    try:
        # Sempre salvar metadata no dt=extraction
        with open(extraction_metadata_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info("Salvo em: %s", extraction_metadata_path)

        # Sempre salvar parquet no dt=extraction
        if parquet_file_path and os.path.exists(parquet_file_path):
            shutil.copy2(parquet_file_path, extraction_parquet_path)
            logging.info("Parquet salvo em: %s", extraction_parquet_path)

        # Salvar no latest apenas se houve mudança
        if changed:
            with open(latest_metadata_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logging.info("Salvo em: %s", latest_metadata_path)
            if parquet_file_path and os.path.exists(parquet_file_path):
                shutil.copy2(parquet_file_path, latest_parquet_path)
                logging.info("Parquet salvo em: %s", latest_parquet_path)
        else:
            logging.info("Nenhuma alteração detectada. Ignorando: %s e %s", latest_metadata_path, latest_parquet_path)

        # Limpar arquivo temporário
        if parquet_file_path and os.path.exists(parquet_file_path):
            os.remove(parquet_file_path)
            logging.info(f"Arquivo temporário removido: {parquet_file_path}")

    except Exception as e:
        logging.error("Falha ao salvar arquivos: %s", e)