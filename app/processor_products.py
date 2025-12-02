import io
from typing import Dict, Any

import pandas as pd

from app.column_normalizer import normalize_dataframe
from app.shopify_product_service import (
    build_product_payload,
    create_product,
    find_product_by_sku,
    update_existing_product,
)


def process_product_file_bytes(
    vendor: str, file_name: str, file_bytes: bytes
) -> Dict[str, Any]:
    """
    Reads CSV/XLS/XLSX → normalize → detect SKU duplicates → create/update in Shopify.
    """
    buffer = io.BytesIO(file_bytes)

    # -------------------------
    # Load DataFrame
    # -------------------------
    if file_name.lower().endswith(".csv"):
        df = pd.read_csv(buffer)
    else:
        df = pd.read_excel(buffer)

    # -------------------------
    # Normalize vendor-specific naming
    # -------------------------
    df = normalize_dataframe(df)

    if df.empty:
        return {"vendor": vendor, "file": file_name, "created": 0, "updated": 0, "skipped": 0}

    created = 0
    updated = 0
    skipped = 0

    # -------------------------
    # Row-by-row processing
    # -------------------------
    for _, row in df.iterrows():
        payload = build_product_payload(row)

        product = payload["product"]
        variant = product["variants"][0]

        title = product.get("title", "")
        sku = variant.get("sku", "")

        if not title.strip() or not sku.strip():
            skipped += 1
            continue

        # ----------------------------------
        # SKU lookup (Duplicate detection)
        # ----------------------------------
        existing = find_product_by_sku(sku)

        if existing:
            # FULL UPDATE
            update_existing_product(existing["id"], payload)
            updated += 1
        else:
            # CREATE NEW
            create_product(payload)
            created += 1

    # -------------------------
    # Response summary
    # -------------------------
    return {
        "vendor": vendor,
        "file": file_name,
        "created": created,
        "updated": updated,
        "skipped": skipped,
    }
