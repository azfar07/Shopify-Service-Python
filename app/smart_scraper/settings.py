import random
from fake_useragent import UserAgent

def get_headers():
    ua = UserAgent()
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9"
    }

# Optional proxy pool
PROXIES = [
    # "http://user:pass@proxy1:port",
    # "http://user:pass@proxy2:port",
]

def get_random_proxy():
    return {"http": random.choice(PROXIES)} if PROXIES else None
