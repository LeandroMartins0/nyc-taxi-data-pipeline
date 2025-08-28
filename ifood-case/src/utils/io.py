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
    """Load configurations from a YAML file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def generate_content_hash(df: DataFrame, sample_limit: int = 1000) -> str:
    """
    Generate a consistent MD5 hash of a Spark DataFrame.
    To avoid collecting huge datasets, it is limited to a number of rows (default=1000).
    Converts all columns to string, handling complex types.
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
        # Collect limited and ordered rows
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
        logging.error(f"Error generating Spark content hash: {e}")
        # Log a sample of the data for debugging
        try:
            sample_data = df.limit(5).toPandas().to_dict(orient="records")
            logging.error(f"Sample data (first 5 rows): {json.dumps(sample_data, ensure_ascii=False, default=str)}")
        except Exception as sample_e:
            logging.error(f"Error collecting data sample: {sample_e}")
        return ""

def is_same_content(filepath: str, new_hash: str) -> bool:
    """Check if the hash saved in file is equal to the new hash."""
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            old = json.load(f)
            return old.get("unique_id") == new_hash
    except Exception as e:
        logging.warning(f"Failed to compare hash in {filepath}: {e}")
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
    Save metadata and parquet file in the data lake.
    Structure:
      - download_dir/dt=extraction{YYYY-MM-DDTHH-MM-SS}/{year}/{month}/{filename} (parquet, always saved)
      - download_dir/dt=extraction{YYYY-MM-DDTHH-MM-SS}/{year}/{month}/metadata/{filename} (metadata, always saved)
      - download_dir/latest/{year}/{month}/{filename} (parquet, if changed)
      - download_dir/latest/{year}/{month}/metadata/{filename} (metadata, if changed)
    """
    # Create metadata filename
    if file_suffix:
        filename = f"{file_prefix}_{identifier}_{file_suffix}.json"
    else:
        filename = f"{file_prefix}_{identifier}.json"

    # Paths in the data lake
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
        # Always save metadata in dt=extraction
        with open(extraction_metadata_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info("Saved at: %s", extraction_metadata_path)

        # Always save parquet in dt=extraction
        if parquet_file_path and os.path.exists(parquet_file_path):
            shutil.copy2(parquet_file_path, extraction_parquet_path)
            logging.info("Parquet saved at: %s", extraction_parquet_path)

        # Save in latest only if there was a change
        if changed:
            with open(latest_metadata_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logging.info("Saved at: %s", latest_metadata_path)
            if parquet_file_path and os.path.exists(parquet_file_path):
                shutil.copy2(parquet_file_path, latest_parquet_path)
                logging.info("Parquet saved at: %s", latest_parquet_path)
        else:
            logging.info("No changes detected. Ignoring: %s and %s", latest_metadata_path, latest_parquet_path)

        # Clean temporary file
        if parquet_file_path and os.path.exists(parquet_file_path):
            os.remove(parquet_file_path)
            logging.info(f"Temporary file removed: {parquet_file_path}")

    except Exception as e:
        logging.error("Failed to save files: %s", e)
