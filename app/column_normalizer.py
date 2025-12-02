import json
import os
import pandas as pd
from typing import Dict

MAPPING_FILE = os.path.join(os.path.dirname(__file__), "column_mapping.json")

with open(MAPPING_FILE, "r", encoding="utf-8") as f:
    COLUMN_MAP = json.load(f)


def normalize_dataframe(df: pd.DataFrame, vendor: str = "default") -> pd.DataFrame:
    """
    Normalizes column names based on vendor-specific mappings.

    - global default mappings
    - vendor-specific overrides ("amazon", "darceree", "vendor_xyz")
    """
    mapping: Dict[str, list] = COLUMN_MAP.get(vendor.lower(), {})
    default_mapping: Dict[str, list] = COLUMN_MAP["default"]

    # merge vendor-specific overrides on top of defaults
    merged_mapping = {**default_mapping, **mapping}

    new_columns: Dict[str, str] = {}
    existing_cols = df.columns

    for norm_key, possible_names in merged_mapping.items():
        for col in existing_cols:
            cleaned = col.replace(" ", "").lower()
            if cleaned in [p.lower() for p in possible_names]:
                new_columns[col] = norm_key
                break

    return df.rename(columns=new_columns)
