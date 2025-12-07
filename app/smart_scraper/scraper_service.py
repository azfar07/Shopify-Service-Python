from .search_engine import find_product_url
from .extractors import scrape_product

def enrich_row_with_scraped_data(row: dict) -> dict:
    name = row.get("TITLE", "")
    sku = row.get("SKU", "")
    website = row.get("WEBSITE") or row.get("URL") or row.get("BASE_URL")

    if not website:
        return row  # nothing to search

    # Find product URL
    product_url = find_product_url(website, name, sku)
    row["SCRAPED_PRODUCT_URL"] = product_url or ""

    if not product_url:
        return row

    scraped = scrape_product(product_url)

    if not row.get("DESCRIPTION_HTML"):
        row["DESCRIPTION_HTML"] = scraped.get("description")

    if not row.get("TITLE"):
        row["TITLE"] = scraped.get("title")

    if not row.get("PRICE"):
        row["PRICE"] = scraped.get("price")

    row["IMAGES"] = ", ".join(scraped.get("images", []))
    row["VARIANTS"] = ", ".join(scraped.get("variants", []))

    return row
