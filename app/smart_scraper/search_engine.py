import requests
from bs4 import BeautifulSoup
import urllib.parse
from .settings import get_headers, get_random_proxy

def detect_platform(html):
    h = html.lower()
    if "woocommerce" in h or "wp-content" in h:
        return "wordpress"
    if "shopify" in h:
        return "shopify"
    return "custom"

def search_wordpress(base, query):
    q = urllib.parse.quote(query)
    return f"{base}/?s={q}&post_type=product"

def search_shopify(base, query):
    q = urllib.parse.quote(query)
    return f"{base}/search?q={q}"

def search_custom(base, query):
    q = urllib.parse.quote(query)
    return f"{base}/search?q={q}"

def google_search(query):
    q = urllib.parse.quote(query)
    return f"https://www.google.com/search?q={q}"

def get_search_page(url):
    try:
        return requests.get(url, headers=get_headers(), proxies=get_random_proxy(), timeout=10)
    except:
        return None

def find_product_url(base_url, name, sku):
    search_terms = [sku, name]

    home = get_search_page(base_url)
    if not home:
        return None

    platform = detect_platform(home.text)

    for term in search_terms:
        if not term:
            continue

        if platform == "wordpress":
            search_url = search_wordpress(base_url, term)
        elif platform == "shopify":
            search_url = search_shopify(base_url, term)
        else:
            search_url = search_custom(base_url, term)

        res = get_search_page(search_url)
        if not res:
            continue

        soup = BeautifulSoup(res.text, "lxml")

        link = soup.select_one("a[href*='/products/']")
        if link:
            return urllib.parse.urljoin(base_url, link["href"])

        link = soup.select_one("a.woocommerce-loop-product__link")
        if link:
            return urllib.parse.urljoin(base_url, link["href"])

        link = soup.select_one("a[href*='product']")
        if link:
            return urllib.parse.urljoin(base_url, link["href"])

    # Google fallback
    g_url = google_search(f"{name} site:{base_url}")
    gres = get_search_page(g_url)
    if gres:
        gsoup = BeautifulSoup(gres.text, "lxml")
        link = gsoup.select_one("a[href*='/product']")
        if link:
            href = link["href"].replace("/url?q=", "").split("&")[0]
            return href

    return None
