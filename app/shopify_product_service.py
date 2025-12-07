import os
from typing import Dict, Any, Optional, List

import logging
import requests
from dotenv import load_dotenv

logger = logging.getLogger("drive_shopify_sync")

# ---------------------------------------------------------
# Load environment variables (.env)
# ---------------------------------------------------------
load_dotenv()

SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")

if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
    raise RuntimeError("SHOPIFY_STORE and SHOPIFY_TOKEN must be set in .env")

BASE_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2025-04"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if text.lower() == "nan":
        return default
    return text


def _parse_image_urls(row: Dict[str, Any]) -> List[str]:
    """
    Expecting scraped images in row["IMAGES"] as comma-separated URLs.
    """
    raw = _safe_str(row.get("IMAGES"))
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


# =========================================================
#  SKU LOOKUP
# =========================================================
def find_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    """
    Search Shopify for a product variant with a given SKU.
    Returns:
      - product JSON if found
      - None if not found
    """
    logger.info(f"Looking up product by SKU '{sku}'")

    url = f"{BASE_URL}/products.json?limit=250&fields=id,title,variants,body_html,vendor,product_type,status"

    resp = requests.get(
        url,
        headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN},
        timeout=30,
    )
    resp.raise_for_status()

    data = resp.json().get("products", [])

    for product in data:
        for variant in product.get("variants", []):
            if variant.get("sku", "").strip().lower() == sku.lower():
                logger.info(f"Found existing product id={product['id']} for SKU={sku}")
                return product

    logger.info(f"No existing product found for SKU={sku}")
    return None


# =========================================================
#  FULL CREATE PAYLOAD
# =========================================================
def build_product_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    title = _safe_str(row.get("TITLE"))
    description = _safe_str(row.get("DESCRIPTION_HTML"))
    vendor = _safe_str(row.get("VENDOR"))
    product_type = _safe_str(row.get("PRODUCT_TYPE"))
    status = _safe_str(row.get("STATUS"))

    sku = _safe_str(row.get("SKU"))
    price = _safe_str(row.get("PRICE"))
    quantity = _safe_str(row.get("QUANTITY"))

    image_urls = _parse_image_urls(row)

    payload: Dict[str, Any] = {
        "product": {
            "title": title,
            "body_html": description,
            "vendor": vendor,
            "product_type": product_type,
            "status": status or "draft",
            "variants": [
                {
                    "sku": sku,
                    "price": price,
                    "inventory_management": "shopify",
                    "inventory_policy": "deny",
                    "fulfillment_service": "manual",
                    "requires_shipping": True,
                    "inventory_quantity": int(quantity) if quantity else 0,
                }
            ],
        }
    }

    if image_urls:
        # let Shopify fetch & store them
        payload["product"]["images"] = [{"src": url} for url in image_urls]

    return payload


# =========================================================
#  CREATE PRODUCT
# =========================================================
def create_product(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}/products.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }

    logger.info(f"Creating new product: {payload['product'].get('title')}")
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    logger.info(f"Created product id={data['product']['id']}")
    return data


# =========================================================
#  FULL UPDATE PRODUCT (TITLE, DESCRIPTION, PRICE, QTY…)
# =========================================================
def update_existing_product(product_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    FULL update of product fields and its variant.
    """
    url = f"{BASE_URL}/products/{product_id}.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }

    payload["product"]["id"] = product_id

    logger.info(f"Updating existing product id={product_id}")
    resp = requests.put(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    logger.info(f"Updated product id={product_id}")
    return data


# =========================================================
#  ADD IMAGES TO EXISTING PRODUCT (OPTIONAL EXTRA STEP)
# =========================================================
def add_images_to_product(
    product_id: int,
    image_urls: List[str],
    variant_id: Optional[int] = None,
) -> None:
    """
    Upload/Add images to an existing product.
    If variant_id is provided, associate image to that variant.
    """
    if not image_urls:
        return

    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }

    for url in image_urls:
        body: Dict[str, Any] = {"image": {"src": url}}
        if variant_id:
            body["image"]["variant_ids"] = [variant_id]

        logger.info(
            f"Adding image to product id={product_id}, "
            f"variant_id={variant_id}, src={url}"
        )
        resp = requests.post(
            f"{BASE_URL}/products/{product_id}/images.json",
            json=body,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
