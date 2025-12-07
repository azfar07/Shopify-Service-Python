from bs4 import BeautifulSoup
import requests
from .settings import get_headers, get_random_proxy

def fetch(url):
    try:
        return requests.get(url, headers=get_headers(), proxies=get_random_proxy(), timeout=10)
    except:
        return None

def scrape_product(url):
    res = fetch(url)
    if not res:
        return {}

    soup = BeautifulSoup(res.text, "lxml")

    # Title
    title = soup.select_one("h1, .product_title")
    title = title.get_text(strip=True) if title else ""

    # Price
    price = soup.select_one(".price, .woocommerce-Price-amount, .product-price")
    price = price.get_text(strip=True) if price else ""

    # Description
    desc = soup.select_one("#description, .woocommerce-Tabs-panel--description, .product-single__description")
    desc = str(desc) if desc else ""

    # Images
    imgs = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src")
        if src and src.startswith("http"):
            imgs.append(src)

    imgs = list(set(imgs))

    # Variants
    variants = []
    for opt in soup.select("select option"):
        text = opt.get_text(strip=True)
        if text and text.lower() != "choose an option":
            variants.append(text)

    return {
        "title": title,
        "price": price,
        "description": desc,
        "images": imgs,
        "variants": variants,
    }
