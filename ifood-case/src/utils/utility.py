# core/src/source_to_raw/utils/utility.py

import yaml
import json
from pathlib import Path
from typing import Any, Dict, List


def load_yaml_config(path: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file.

    Args:
        path (str): Path to the YAML file.

    Returns:
        dict: Parsed YAML as a dictionary.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_to_json(data: Dict[str, Any], path: str) -> None:
    """
    Save a dictionary to a JSON file.

    Args:
        data (dict): Data to save.
        path (str): Destination file path.
    """
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(path_obj, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def ensure_dir_exists(path: str) -> None:
    """
    Ensure that the parent directory of a path exists.

    Args:
        path (str): File or directory path.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
