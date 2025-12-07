import io
import os
import logging
from typing import Dict, Any, List

import pandas as pd

from app.column_normalizer import normalize_dataframe
from app.shopify_product_service import (
    build_product_payload,
    create_product,
    find_product_by_sku,
    update_existing_product,
    add_images_to_product,
)
from app.smart_scraper.scraper_service import enrich_row_with_scraped_data

logger = logging.getLogger("drive_shopify_sync")


def process_product_file_bytes(
    vendor: str, file_name: str, file_bytes: bytes
) -> Dict[str, Any]:
    """
    Reads CSV/XLS/XLSX → normalize → scrape missing fields
    → detect SKU duplicates → create/update in Shopify
    → save enriched spreadsheet.
    """
    logger.info(f"Processing file '{file_name}' for vendor '{vendor}'")

    buffer = io.BytesIO(file_bytes)

    # -------------------------
    # Load DataFrame
    # -------------------------
    if file_name.lower().endswith(".csv"):
        df = pd.read_csv(buffer)
    else:
        df = pd.read_excel(buffer)

    logger.info(f"Loaded DataFrame with {len(df)} rows and {len(df.columns)} columns")

    # -------------------------
    # Normalize vendor-specific naming
    # -------------------------
    df = normalize_dataframe(df, vendor=vendor)
    logger.info("Normalized column names using mapping")

    if df.empty:
        logger.warning("DataFrame is empty after normalization")
        return {
            "vendor": vendor,
            "file": file_name,
            "created": 0,
            "updated": 0,
            "skipped": 0,
        }

    created = 0
    updated = 0
    skipped = 0

    enriched_rows: List[Dict[str, Any]] = []

    # -------------------------
    # Row-by-row processing
    # -------------------------
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        sku_raw = row_dict.get("SKU", "")
        logger.info(f"[Row {idx}] Start: SKU={sku_raw}")

        # 1️⃣ Enrich via scraper
        row_dict = enrich_row_with_scraped_data(row_dict)

        # 2️⃣ Build Shopify payload
        payload = build_product_payload(row_dict)
        product = payload["product"]
        variant = product["variants"][0]

        title = product.get("title", "")
        sku = variant.get("sku", "")

        logger.info(f"[Row {idx}] After enrichment: title='{title}', sku='{sku}'")

        if not title.strip() or not sku.strip():
            logger.warning(f"[Row {idx}] Missing title or SKU → skipped")
            skipped += 1
            enriched_rows.append(row_dict)
            continue

        # 3️⃣ Check SKU on Shopify
        existing = find_product_by_sku(sku)

        image_urls = row_dict.get("IMAGES")
        if isinstance(image_urls, str):
            image_urls = [u.strip() for u in image_urls.split(",") if u.strip()]
        elif not image_urls:
            image_urls = []

        if existing:
            # FULL UPDATE
            updated_product = update_existing_product(existing["id"], payload)
            updated += 1

            # Attach images to first variant if we have any
            if image_urls and updated_product["product"]["variants"]:
                main_variant_id = updated_product["product"]["variants"][0]["id"]
                add_images_to_product(updated_product["product"]["id"], image_urls, main_variant_id)

        else:
            # CREATE NEW
            created_product = create_product(payload)
            created += 1

            # For created product, Shopify will already have images if we passed them.
            # If you want to be explicit / additive, you could also call add_images_to_product here.
            # For now we trust the create payload's "images" field.

        enriched_rows.append(row_dict)

    # -------------------------
    # Save enriched file locally for dashboard
    # -------------------------
    output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)

    enriched_df = pd.DataFrame(enriched_rows)
    safe_file_name = file_name.replace("/", "_").replace("\\", "_")
    output_path = os.path.join(output_dir, f"{vendor}_{safe_file_name}_processed.xlsx")

    enriched_df.to_excel(output_path, index=False)
    logger.info(f"Saved enriched file to: {output_path}")

    # -------------------------
    # Response summary
    # -------------------------
    return {
        "vendor": vendor,
        "file": file_name,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "output_file": output_path,
    }
