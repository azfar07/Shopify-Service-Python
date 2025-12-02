import os
from typing import Dict, Any, Optional, List

import requests
from dotenv import load_dotenv

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
    url = f"{BASE_URL}/products.json?limit=250&fields=id,title,variants,body_html,vendor,product_type,status"

    resp = requests.get(
        url,
        headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN},
        timeout=30
    )
    resp.raise_for_status()

    data = resp.json().get("products", [])

    for product in data:
        for variant in product.get("variants", []):
            if variant.get("sku", "").strip().lower() == sku.lower():
                return product

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

    return {
        "product": {
            "title": title,
            "body_html": description,
            "vendor": vendor,
            "product_type": product_type,
            "status": status,
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


# =========================================================
#  CREATE PRODUCT
# =========================================================
def create_product(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}/products.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


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

    # Shopify expects:
    # { "product": { "id": 123, ...updated fields... } }
    payload["product"]["id"] = product_id

    resp = requests.put(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()
